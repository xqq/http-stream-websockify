use std::sync::Arc;
use crate::http_upstream::{BasicAuthInfo, HttpUpstream};
use crate::stream_message::StreamMessage;

mod http_upstream;
mod stream_message;
mod websocket_broadcast_server;


const UPSTREAM_URL: &str = "placeholder";
const BASIC_AUTH_USER: &str = "username";
const BASIC_AUTH_PASS: &str = "password";


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, _) = tokio::sync::broadcast::channel::<StreamMessage>(8);
    let sender = tx;

    let basic_auth = BasicAuthInfo {
        username: BASIC_AUTH_USER.to_owned(),
        password: BASIC_AUTH_PASS.to_owned(),
    };

    let mut upstream = Arc::new(HttpUpstream::new(UPSTREAM_URL, Some(basic_auth), sender));

    match Arc::get_mut(&mut upstream).unwrap().start_polling().await {
        Ok(_) => {
            println!("Upstream polling started");
        }
        Err(e) => {
            println!("Upstream request failed: {:#?}", e.as_ref());
            return Err(e.into());
        }
    }

    let upstream_clone = upstream.clone();

    tokio::spawn(async move {
        let upstream = upstream_clone;

        const SECONDS: u64 = 5;
        println!("Will stop polling after {}s", SECONDS);
        tokio::time::sleep(std::time::Duration::from_secs(SECONDS)).await;

        println!("Attempt to stop polling");
        upstream.stop_polling();
    });

    upstream.join().await;
    println!("Returned from upstream.join()");
    Ok(())
}
