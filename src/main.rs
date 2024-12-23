use crate::http_upstream::{BasicAuthInfo, HttpUpstream};

mod http_upstream;


const UPSTREAM_URL: &str = "placeholder";
const BASIC_AUTH_USER: &str = "username";
const BASIC_AUTH_PASS: &str = "password";


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let basic_auth = BasicAuthInfo {
        username: BASIC_AUTH_USER.to_owned(),
        password: BASIC_AUTH_PASS.to_owned(),
    };
    let mut upstream = HttpUpstream::new(UPSTREAM_URL, Some(basic_auth));

    upstream.start_polling().await?;

    println!("Hello, world!");

    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    upstream.stop_polling();
    println!("Upstream polling stopped");
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    println!("Hello, world!");

    Ok(())
}
