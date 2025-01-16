use std::net::SocketAddr;
use std::sync::Arc;
use bytes::Bytes;
use crate::bytes_merger::buffered_bytes_channel;
use crate::http_upstream::{BasicAuthInfo, HttpUpstream};
use crate::signal_waiter::wait_for_exit_signal;
use crate::websocket_broadcast_server::WebSocketBroadcastServer;

mod http_upstream;
mod websocket_broadcast_server;
mod signal_waiter;
mod bytes_merger;


const UPSTREAM_URL: &str = "placeholder";
const BASIC_AUTH_USER: &str = "username";
const BASIC_AUTH_PASS: &str = "password";

const LISTEN_ADDR: &str = "0.0.0.0";
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

    let (tx, rx) = buffered_bytes_channel(16, 32 * 1024);


    // Setup WebSocket broadcast server
    let listen_addr_port = format!("{}:{}", LISTEN_ADDR, LISTEN_PORT).parse::<SocketAddr>().unwrap();

    let mut ws_broadcast_server = Arc::new(
        WebSocketBroadcastServer::new(
            listen_addr_port,
            MOUNT_ENDPOINT.to_string(),
            rx,
        )
    );

    match Arc::get_mut(&mut ws_broadcast_server).unwrap().start().await {
        Ok(_) => {
            tracing::info!("WebSocket broadcast server listening started");
        }
        Err(e) => {
            tracing::error!("WebSocket broadcast server failed to start: {}", e);
            return Err(e.into());
        }
    }


    // Start upstream polling
    let basic_auth = BasicAuthInfo {
        username: BASIC_AUTH_USER.to_owned(),
        password: BASIC_AUTH_PASS.to_owned(),
    };

    let mut upstream = Arc::new(
        HttpUpstream::new(UPSTREAM_URL, Some(basic_auth), tx.clone())
    );

    match Arc::get_mut(&mut upstream).unwrap().start_polling().await {
        Ok(_) => {
            tracing::info!("Upstream polling started");
        }
        Err(e) => {
            tracing::error!("Upstream request failed: {:#?}", e.as_ref());
            return Err(e);
        }
    }


    // Wait for Ctrl-C, etc. signals
    wait_for_exit_signal().await;
    upstream.stop_polling();
    ws_broadcast_server.stop();

    tokio::join!(upstream.join(), ws_broadcast_server.join());
    tracing::trace!("Returned from tokio::join!()");
    Ok(())
}
