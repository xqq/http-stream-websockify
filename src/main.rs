use std::net::SocketAddr;
use std::sync::Arc;
use crate::http_upstream::{BasicAuthInfo, HttpUpstream};
use crate::stream_message::StreamMessage;
use crate::websocket_broadcast_server::WebSocketBroadcastServer;

mod http_upstream;
mod stream_message;
mod websocket_broadcast_server;


const UPSTREAM_URL: &str = "placeholder";
const BASIC_AUTH_USER: &str = "username";
const BASIC_AUTH_PASS: &str = "password";

const LISTEN_ADDR: &str = "127.0.0.1";
const LISTEN_PORT: &str = "8090";
const MOUNT_ENDPOINT: &str = "/stream/live.ts";

fn setup_tracing() {
    let subscriber = tracing_subscriber::FmtSubscriber::builder()
        .with_max_level(tracing::Level::TRACE)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup_tracing();

    let (tx, _) = tokio::sync::broadcast::channel::<StreamMessage>(16);
    let sender = tx;

    let basic_auth = BasicAuthInfo {
        username: BASIC_AUTH_USER.to_owned(),
        password: BASIC_AUTH_PASS.to_owned(),
    };

    let mut upstream = Arc::new(
        HttpUpstream::new(UPSTREAM_URL, Some(basic_auth), sender)
    );

    match Arc::get_mut(&mut upstream).unwrap().start_polling().await {
        Ok(_) => {
            println!("Upstream polling started");
        }
        Err(e) => {
            println!("Upstream request failed: {:#?}", e.as_ref());
            return Err(e);
        }
    }

    let upstream_clone = upstream.clone();

    let get_data_source: Arc<dyn Fn() -> tokio::sync::broadcast::Receiver<StreamMessage> + Send + Sync> = Arc::new(move || {
        let upstream = &upstream_clone;
        upstream.subscribe()
    });


    let listen_addr_port = format!("{}:{}", LISTEN_ADDR, LISTEN_PORT).parse::<SocketAddr>().unwrap();

    let mut ws_broadcast_server = Arc::new(
        WebSocketBroadcastServer::new(
            listen_addr_port,
            MOUNT_ENDPOINT.to_string(),
            get_data_source
        )
    );

    match Arc::get_mut(&mut ws_broadcast_server).unwrap().start().await {
        Ok(_) => {
            println!("WebSocket broadcast server listening started");
        }
        Err(e) => {
            println!("WebSocket broadcast server failed to start: {}", e);
            return Err(e.into());
        }
    }

    tokio::join!(upstream.join(), ws_broadcast_server.join());
    println!("Returned from tokio::join!()");
    Ok(())
}
