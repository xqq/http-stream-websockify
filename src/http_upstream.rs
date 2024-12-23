use std::str::FromStr;
use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::header::HeaderValue;
use hyper_tls::HttpsConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use tokio_util::sync::CancellationToken;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub struct BasicAuthInfo {
    pub username: String,
    pub password: String,
}

impl BasicAuthInfo {
    pub fn to_header_value(&self) -> String {
        let credentials = format!("{}:{}", self.username, self.password);
        format!("Basic {}", BASE64_STANDARD.encode(credentials))
    }
}

pub struct HttpUpstream {
    url: hyper::Uri,
    basic_auth: Option<BasicAuthInfo>,
    cancel_token: CancellationToken,
}

impl HttpUpstream {
    pub fn new(url: &str, basic_auth: Option<BasicAuthInfo>) -> HttpUpstream {
        HttpUpstream {
            url: hyper::Uri::from_str(url).unwrap(),
            basic_auth,
            cancel_token: CancellationToken::new(),
        }
    }

    pub async fn start_polling(&mut self) -> Result<()> {
        let authority = self.url.authority().unwrap().clone();

        let mut req = hyper::Request::builder()
            .method(hyper::Method::GET)
            .uri(self.url.clone())
            .header(hyper::header::HOST, authority.as_str())
            .body(Empty::<Bytes>::new())?;

        // Append Authorization header for basic auth
        if let Some(basic_auth) = &self.basic_auth {
            req.headers_mut().insert(hyper::header::AUTHORIZATION, HeaderValue::from_str(&basic_auth.to_header_value())?);
        }

        let https = HttpsConnector::new();
        let client = Client::builder(TokioExecutor::new())
            .build::<_, Empty::<Bytes>>(https);

        let res = client.request(req).await?;

        println!("Response: {}", res.status());
        println!("Headers: {:#?}", res.headers());

        let mut body = res.into_body();
        let cloned_cancel_token = self.cancel_token.clone();

        tokio::spawn(async move {
            while let Some(frame) = tokio::select! {
                frame = body.frame() => frame,  // Wait for a new frame (chunk)
                _ = cloned_cancel_token.cancelled() => None,  // Cancelled on signal
            } {
                match frame {
                    Ok(frame) => {
                        if let Some(chunk) = frame.data_ref() {
                            println!("chunk size: {}", chunk.len());
                        }
                    }
                    Err(e) => {
                        // EOF
                        println!("Stream meet Eror EOF: {:#?}", e);
                        return;
                    }
                }
            }
            if cloned_cancel_token.is_cancelled() {
                // Cancelled by external signal
                println!("Cancelled by external signal");
            }
        });

        Ok(())
    }

    pub fn stop_polling(&mut self) {
        self.cancel_token.cancel()
    }

}

