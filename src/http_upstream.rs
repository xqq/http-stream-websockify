use std::str::FromStr;
use std::sync::Arc;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::body::Incoming;
use hyper::header::HeaderValue;
use hyper_tls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub struct BasicAuthInfo {
    pub username: String,
    pub password: String,
}

impl BasicAuthInfo {
    fn to_header_value(&self) -> String {
        let credentials = format!("{}:{}", self.username, self.password);
        format!("Basic {}", BASE64_STANDARD.encode(credentials))
    }
}

#[derive(Clone, Debug, PartialEq)]
enum ExitReason {
    EndOfStream,
    Error,
}

pub struct HttpUpstream {
    url: hyper::Uri,
    basic_auth: Option<BasicAuthInfo>,

    cancel_token: CancellationToken,
    output_sender: Arc<tokio::sync::mpsc::Sender<Bytes>>,

    exit_notifier: Arc<Notify>,
}

impl HttpUpstream {
    pub fn new(
        url: &str,
        basic_auth: Option<BasicAuthInfo>,
        output_sender: tokio::sync::mpsc::Sender<Bytes>,
    ) -> HttpUpstream {
        HttpUpstream {
            url: hyper::Uri::from_str(url).unwrap(),
            basic_auth,
            cancel_token: CancellationToken::new(),
            output_sender: Arc::new(output_sender),
            exit_notifier: Arc::new(Notify::new()),
        }
    }

    pub async fn start_polling(&mut self) -> Result<()> {
        let authority = self.url.authority().unwrap().clone();

        let mut req = hyper::Request::builder()
            .method(hyper::Method::GET)
            .uri(self.url.clone())
            .header(hyper::header::HOST, authority.as_str())
            .body(Empty::<Bytes>::new())?;

        let req_clone = req.clone();

        // Append Authorization header for basic auth
        if let Some(basic_auth) = &self.basic_auth {
            req.headers_mut().insert(hyper::header::AUTHORIZATION, HeaderValue::from_str(&basic_auth.to_header_value())?);
        }

        let https = HttpsConnector::new();
        let client = Client::builder(TokioExecutor::new())
            .build::<_, Empty::<Bytes>>(https);

        tracing::info!("Opening {}", self.url.to_string());

        let mut res = client.request(req).await?;

        tracing::info!("Response: {}", res.status());
        tracing::trace!("Headers: {:#?}", res.headers());

        let mut redirect_counter = 0;

        // Follow redirection
        while status_is_redirect(res.status()) && redirect_counter < 5 {
            if let Some(location) = res.headers().get(hyper::header::LOCATION) {
                let location = location.to_str().unwrap();
                tracing::info!("{}, redirecting to {}", res.status(), location);
                res = follow_redirect(location, req_clone.clone()).await?;
                redirect_counter += 1;
            } else {
                let description = "Missing location header for redirection";
                return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, description)));
            }
        }

        if res.status().as_u16() < 200 || res.status().as_u16() > 299 {
            let description = format!("Invalid HTTP status: {}", res.status());
            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, description)));
        }

        let mut body = res.into_body();

        let cloned_cancel_token = self.cancel_token.clone();
        let output_sender = self.output_sender.clone();
        let exit_notifier = self.exit_notifier.clone();

        tokio::spawn(async move {
            tracing::info!("Stream polling started");

            tokio::select! {
                _ = cloned_cancel_token.cancelled() => {
                    // Cancelled by external signal
                    tracing::info!("Cancelled by external signal");
                    // Send empty Bytes to notify EOF
                    let empty = Bytes::new();
                    let _ = output_sender.send(empty).await;
                },
                exit_reason = async {
                    // Return ExitReason::EndOfStream by default
                    let mut exit_reason = ExitReason::EndOfStream;

                    while let Some(frame) = body.frame().await {
                        match frame {
                            Ok(frame) => {
                                if frame.is_data() {
                                    let chunk = frame.into_data().unwrap();
                                    // tracing::trace!("chunk size: {}", chunk.len());
                                    let _ = output_sender.send(chunk).await;
                                }
                            },
                            Err(e) => {
                                tracing::info!("Stream meet Error: {:#?}", e);
                                exit_reason = ExitReason::Error;
                                break;
                            }
                        }
                    }
                    // When body.frame() return None, stream meet EOF
                    exit_reason
                } => {
                    if exit_reason == ExitReason::EndOfStream {
                        tracing::info!("Stream has closed with EOF normally");
                    }
                    // Send empty Bytes to notify EOF
                    let empty = Bytes::new();
                    let _ = output_sender.send(empty).await;
                }
            };

            exit_notifier.notify_waiters();
            tracing::info!("Stream polling exited");
        });

        Ok(())
    }

    pub fn stop_polling(&self) {
        self.cancel_token.cancel();
    }

    pub async fn join(&self) {
        self.exit_notifier.notified().await;
    }

}

fn status_is_redirect(status: hyper::StatusCode) -> bool {
    status.as_u16() >= 300 && status.as_u16() <= 399
}

async fn follow_redirect(location: &str, req: hyper::Request<Empty<Bytes>>) -> Result<hyper::Response<Incoming>> {
    let redirect_uri = hyper::Uri::from_str(location)?;
    let authority = redirect_uri.authority().unwrap().clone();

    let mut req = req;
    *req.uri_mut() = redirect_uri;

    // Fill in new Host header
    let headers = req.headers_mut();
    if headers.contains_key(hyper::header::HOST) {
        headers.remove(hyper::header::HOST);
        headers.insert(hyper::header::HOST, HeaderValue::from_str(authority.as_str())?);
    }

    let https = HttpsConnector::new();
    let client = Client::builder(TokioExecutor::new())
        .build::<_, Empty::<Bytes>>(https);

    tracing::info!("Opening {}", location);

    let res = client.request(req).await?;

    tracing::info!("Response: {}", res.status());
    tracing::trace!("Headers: {:#?}", res.headers());

    Ok(res)
}
