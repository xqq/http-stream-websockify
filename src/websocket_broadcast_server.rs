use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use async_tungstenite::tungstenite::handshake::derive_accept_key;
use async_tungstenite::tungstenite::Message;
use async_tungstenite::tokio::TokioAdapter;
use async_tungstenite::tungstenite::protocol::Role;
use async_tungstenite::WebSocketStream;
use bytes::Bytes;
use futures::{future, pin_mut, TryStreamExt};
use futures::stream::StreamExt;
use http_body_util::Full;
use hyper::{Method, Request, Response, StatusCode, Version};
use hyper::body::Incoming;
use hyper::header::{HeaderValue, ACCESS_CONTROL_ALLOW_ORIGIN, CONNECTION, CONTENT_TYPE, SEC_WEBSOCKET_ACCEPT, SEC_WEBSOCKET_KEY, SEC_WEBSOCKET_VERSION, UPGRADE};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::upgrade::Upgraded;
use hyper_util::rt::TokioIo;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Notify;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;

type Tx = UnboundedSender<Message>;
type PeerMap = Arc<Mutex<HashMap<SocketAddr, Tx>>>;
type Body = Full<Bytes>;

pub struct WebSocketBroadcastServer {
    listen_addr_port: SocketAddr,
    context: WebSocketServerContext,
}

#[derive(Clone)]
struct WebSocketServerContext {
    peer_map: PeerMap,
    mount_path: String,
    input_receiver: Arc<tokio::sync::Mutex<tokio::sync::mpsc::Receiver<Bytes>>>,
    cancel_token: CancellationToken,
    listener_exit_notifier: Arc<Notify>,
    broadcaster_exit_notifier: Arc<Notify>,
}

impl WebSocketBroadcastServer {

    pub fn new(listen_addr_port: SocketAddr,
               mount_path: String,
               input_receiver: tokio::sync::mpsc::Receiver<Bytes>) -> WebSocketBroadcastServer {
        let context = WebSocketServerContext {
            peer_map: PeerMap::new(Mutex::new(HashMap::new())),
            mount_path,
            input_receiver: Arc::new(tokio::sync::Mutex::new(input_receiver)),
            cancel_token: CancellationToken::new(),
            listener_exit_notifier: Arc::new(Notify::new()),
            broadcaster_exit_notifier: Arc::new(Notify::new()),
        };

        WebSocketBroadcastServer {
            listen_addr_port,
            context,
        }
    }

    pub async fn start(&mut self) -> Result<(), std::io::Error> {
        self.start_listener().await?;
        self.start_broadcaster().await?;
        Ok(())
    }

    async fn start_listener(&mut self) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(self.listen_addr_port).await?;

        let context = self.context.clone();

        tokio::spawn(async move {
            tracing::info!("Listener startup");

            loop {
                let option: Option<Result<(TcpStream, SocketAddr), std::io::Error>> = tokio::select! {
                    // Handle incoming connect request
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((socket, addr)) => Some(Ok((socket, addr))),
                            Err(e) => Some(Err(e))
                        }
                    },
                    // Handle CancellationToken
                    _ = context.cancel_token.cancelled() => None,
                };
                if option.is_none() {
                    // Cancelled by CancellationToken, exit loop
                    tracing::info!("Listener exit by cancellation");
                    break;
                }
                let (stream, addr) = match option.unwrap() {
                    Ok((stream, addr)) => (stream, addr),
                    Err(e) => {
                        tracing::error!("failed to accept connection: {:?}", e);
                        continue;
                    }
                };

                let context = context.clone();

                tokio::spawn(async move {
                    let cancel_token = context.cancel_token.clone();

                    let io = TokioIo::new(stream);
                    let service = service_fn(move |req: Request<Incoming>|
                        Self::handle_request(context.clone(), req, addr)
                    );
                    let connection = http1::Builder::new()
                        .serve_connection(io, service)
                        .with_upgrades();

                    tokio::select! {
                        // Do connection.await
                        result = connection => {
                            if let Err(err) = result {
                                tracing::error!("Failed to serve {} : {:?}", addr, err);
                            }
                        }
                        // Handle CancellationToken
                        _ = cancel_token.cancelled() => {
                            tracing::info!("Connection exited by cancellation");
                        },
                    }
                });
            }

            context.listener_exit_notifier.notify_waiters();
            tracing::info!("Listener exited");
        });

        Ok(())
    }

    async fn start_broadcaster(&mut self) -> Result<(), std::io::Error> {
        let context = self.context.clone();

        tokio::spawn(async move {
            tracing::info!("Broadcaster startup");

            tokio::select! {
                _ = async {
                    let mut input_receiver = context.input_receiver.lock().await;

                    loop {
                        match input_receiver.recv().await {
                            Some(chunk) => {
                                if !chunk.is_empty() {
                                    Self::broadcast_message(context.peer_map.clone(), Message::Binary(chunk.to_vec()));
                                } else {
                                    // Received empty Bytes, means end of stream
                                    tracing::info!("Closing all connections due to channel end of stream");
                                    Self::broadcast_message(context.peer_map.clone(), Message::Close(None));
                                    break;
                                }
                            },
                            None => {
                                tracing::info!("Closing all connections due to channel closed");
                                Self::broadcast_message(context.peer_map.clone(), Message::Close(None));
                                break;
                            }
                        }
                    }
                } => {},
                _ = context.cancel_token.cancelled() => {
                    tracing::info!("Broadcaster exit by cancellation");
                    Self::broadcast_message(context.peer_map.clone(), Message::Close(None));
                },
            }

            context.broadcaster_exit_notifier.notify_waiters();
            tracing::info!("Broadcaster exited");
        });

        Ok(())
    }

    fn broadcast_message(peer_map: PeerMap, message: Message) {
        let peers = peer_map.lock().unwrap();
        if peers.is_empty() {
            return;
        }

        let broadcast_recipients = peers
            .iter()
            .map(|(_addr, tx)| tx);

        for tx in broadcast_recipients {
            let _ = tx.send(message.clone());
        }
    }

    pub fn stop(&self) {
        self.context.cancel_token.cancel();
    }

    pub async fn join(&self) {
        tokio::join!(
            self.context.listener_exit_notifier.notified(),
            self.context.broadcaster_exit_notifier.notified()
        );
    }

    async fn handle_request(
        context: WebSocketServerContext,
        mut req: Request<Incoming>,
        addr: SocketAddr,
    ) -> Result<Response<Body>, Infallible> {
        tracing::trace!("Received request, potentially websocket handshake");
        tracing::trace!("Request uri: {}", req.uri().to_string());
        tracing::trace!("Request headers: {:#?}", req.headers());

        let headers = req.headers();

        let upgrade = HeaderValue::from_static("Upgrade");
        let websocket = HeaderValue::from_static("websocket");
        let sec_key = headers.get(SEC_WEBSOCKET_KEY);
        let derived = sec_key.map(|key| derive_accept_key(key.as_bytes()));

        if req.method() != Method::GET || req.version() < Version::HTTP_11 || headers.is_empty() {
            return Ok(make_400_bad_request("Invalid http method/version or no request headers".to_string()));
        }

        // Request headers checking

        // Request headers should contain "Connection: Upgrade"
        if !headers.get(CONNECTION)
            .and_then(|h| h.to_str().ok())
            .map(|h| {
                h.split([' ', ','])
                    .any(|p| p.eq_ignore_ascii_case(upgrade.to_str().unwrap()))
            })
            .unwrap_or(false) {
            return Ok(make_400_bad_request("Invalid connection header".to_string()));
        }

        // Request headers should contain "Upgrade: websocket"
        if !headers.get(UPGRADE)
            .and_then(|h| h.to_str().ok())
            .map(|h| h.eq_ignore_ascii_case(websocket.to_str().unwrap()))
            .unwrap_or(false) {
            return Ok(make_400_bad_request("Invalid upgrade header".to_string()));
        }

        // "sec-websocket-key" header must be existed
        if sec_key.is_none() {
            return Ok(make_400_bad_request("Missing sec-websocket-key header".to_string()));
        }

        // "sec-websocket-version" should be 13
        if !headers.get(SEC_WEBSOCKET_VERSION)
            .map(|h| h == "13")
            .unwrap_or(false) {
            return Ok(make_400_bad_request("Invalid sec-websocket-version header".to_string()));
        }

        if req.uri().path() != context.mount_path {
            return Ok(make_404_not_found(format!("{} not found", req.uri().path())));
        }

        let http_version = req.version();

        tokio::task::spawn(async move {
            match hyper::upgrade::on(&mut req).await {
                Ok(upgraded) => {
                    Self::handle_connection(
                        context,
                        WebSocketStream::from_raw_socket(
                            TokioAdapter::new(TokioIo::new(upgraded)),
                            Role::Server,
                            None, // WebSocketConfig
                        )
                        .await,
                        addr,
                    ).await;
                }
                Err(error) => {
                    tracing::error!("Upgrade: Upgrade error: {}", error);
                }
            }
        });

        let mut response = Response::new(Body::default());

        // response with "101 Switching Protocols"
        *response.status_mut() = StatusCode::SWITCHING_PROTOCOLS;
        *response.version_mut() = http_version;
        response.headers_mut().append(CONNECTION, upgrade);
        response.headers_mut().append(UPGRADE, websocket);
        response.headers_mut().append(SEC_WEBSOCKET_ACCEPT, derived.unwrap().parse().unwrap());

        // Add custom response headers here
        response.headers_mut().append(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));

        Ok(response)
    }

    async fn handle_connection(
        context: WebSocketServerContext,
        websocket_stream: WebSocketStream<TokioAdapter<TokioIo<Upgraded>>>,
        addr: SocketAddr,
    ) {
        tracing::info!("WebSocket connection established: {}", addr);

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let rx = UnboundedReceiverStream::new(rx);

        context.peer_map.lock().unwrap().insert(addr, tx.clone());

        let (outgoing, incoming) = websocket_stream.split();

        let receive_from_incoming = incoming.try_for_each(|msg: Message | {
            if msg.is_close() {
                tracing::info!("WebSocket connection closing from client: {}", addr);
                tx.send(Message::Close(None)).ok();
            }
            future::ok(())
        });

        let receive_from_others = rx.map(Ok).forward(outgoing);

        pin_mut!(receive_from_incoming, receive_from_others);

        tokio::select! {
            _ = receive_from_incoming => {},

            _ = receive_from_others => (),

            // Handle CancellationToken
            _ = context.cancel_token.cancelled() => {
                let _ = tx.send(Message::Close(None));
            },
        }

        tracing::info!("WebSocket connection disconnected: {}", addr);
        context.peer_map.lock().unwrap().remove(&addr);
    }

}

fn make_400_bad_request(message: String) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .header("Content-Type", "text/plain")
        .body(Body::from(message))
        .unwrap()
}

fn make_404_not_found(message: String) -> Response<Full<Bytes>> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(CONTENT_TYPE, "text/plain")
        .body(Body::from(message))
        .unwrap()
}
