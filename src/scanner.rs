use serde::Serialize;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::TcpStream;
use tokio::time::timeout;

// Represents a discovered network target
#[derive(Serialize, Debug, Clone)]
pub struct DiscoveredDevice {
    pub ip: String,
    pub hostname: String,
    pub os_name: String,
}

// Probes a single IP address on port 9090 with a strict timeout
async fn probe_target(ip: String, port: u16) -> Option<DiscoveredDevice> {
    let addr_str = format!("{}:{}", ip, port);
    let socket_addr: SocketAddr = addr_str.parse().ok()?;

    // Attempt connection with a short 300ms timeout to ensure fast scanning
    let stream_result = timeout(Duration::from_millis(300), TcpStream::connect(socket_addr)).await;

    if let Ok(Ok(mut stream)) = stream_result {
        let mut reader = BufReader::new(&mut stream);
        let mut line = String::new();

        // Read the initial JSON handshake sent by the target service
        if let Ok(Ok(_)) = timeout(Duration::from_millis(300), reader.read_line(&mut line)).await {
            // Parse JSON payload to confirm it's a valid qt-monitor-rust target
            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&line) {
                let hostname = value["hostname"].as_str().unwrap_or("Unknown").to_string();
                let os_name = value["os_name"].as_str().unwrap_or("Unknown OS").to_string();

                return Some(DiscoveredDevice {
                    ip,
                    hostname,
                    os_name,
                });
            }
        }
    }
    None
}

// Sweeps an entire /24 subnet range asynchronously across multiple threads
pub async fn scan_subnet(subnet_prefix: &str, port: u16) -> Vec<DiscoveredDevice> {
    let mut tasks = Vec::new();

    // Spawn 254 concurrent probe tasks across IPs .1 through .254
    for i in 1..255 {
        let target_ip = format!("{}.{}", subnet_prefix, i);
        tasks.push(tokio::spawn(probe_target(target_ip, port)));
    }

    let mut discovered = Vec::new();
    for task in tasks {
        if let Ok(Some(device)) = task.await {
            discovered.push(device);
        }
    }

    discovered
}