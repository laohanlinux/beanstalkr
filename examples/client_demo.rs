//! Demo of the async beanstalk client.
//!
//! Run with: cargo run --example client_demo --features client
//!
//! Requires a beanstalkd server on 127.0.0.1:11300.

use std::time::Duration;

use beanstalkr::client::{Conn, Tube, TubeSet};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut conn = Conn::connect("127.0.0.1:11300").await?;

    // Put a job into default tube
    let id = conn
        .put(b"hello world", 1, Duration::ZERO, Duration::from_secs(60))
        .await?;
    println!("Put job {}", id);

    // Reserve from default tube
    let (id, body) = conn.reserve(Duration::from_secs(5)).await?;
    println!("Reserved job {}: {:?}", id, String::from_utf8_lossy(&body));
    conn.delete(id).await?;

    // Use a named tube
    let tube = Tube::named("mytube");
    let id = tube
        .put(&mut conn, b"myjob", 1, Duration::ZERO, Duration::from_secs(60))
        .await?;
    println!("Put job {} to mytube", id);

    // Reserve from multiple tubes
    let tube_set = TubeSet::with_tubes(&["default", "mytube"]);
    let (id, body) = tube_set
        .reserve(&mut conn, Duration::from_secs(5))
        .await?;
    println!("Reserved job {}: {:?}", id, String::from_utf8_lossy(&body));
    conn.delete(id).await?;

    let _ = conn.close().await;
    Ok(())
}
