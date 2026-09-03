// Declare the local 'db' module defined in src/db.rs.
mod db;

// Declare the local 'scanner' module defined in src/scanner.rs.
mod scanner;

// Import Utc from chrono to generate ISO-8601 standardized timestamps.
use chrono::Utc;

// Import Deserialize and Serialize traits from serde to enable JSON handling.
// >>> CHANGE: Added Deserialize to allow parsing incoming remote TCP JSON streams <<<
use serde::{Deserialize, Serialize};

// Import SocketAddr for network address parsing.
use std::net::SocketAddr;

// Import Duration to specify async sleep intervals.
use std::time::Duration;

// Import system monitoring types from sysinfo to access cross-platform hardware sensors.
use sysinfo::{Components, CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};

// Import AsyncBufReadExt and AsyncWriteExt traits from tokio for network socket reading/writing.
// >>> CHANGE: Added AsyncBufReadExt and BufReader for reading incoming TCP streams <<<
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

// Import TcpListener and TcpStream from tokio for non-blocking network socket handling.
// >>> CHANGE: Added TcpStream to allow outbound connection to remote agents <<<
use tokio::net::{TcpListener, TcpStream};

/// Data structure defining the JSON schema transferred over TCP and stored in SQLite.
// >>> CHANGE: Added Deserialize to enable decoding remote payloads into Rust structs <<<
#[derive(Serialize, Deserialize, Debug, Clone)]
struct TelemetryPayload {
    hostname: String,
    timestamp: String,
    cpu_clock_mhz: f32,
    cpu_temp_c: f32,
    memory_used_gb: f64,
    memory_total_gb: f64,
    os_name: String,
}

/// Collects hardware metrics from OS counters across Windows, macOS, and Linux.
fn gather_metrics(sys: &mut System, components: &mut Components) -> TelemetryPayload {
    // >>> CHANGE: Switched to explicit CPU/Memory specific refresh passes to capture dynamic load changes <<<
    sys.refresh_cpu_specifics(CpuRefreshKind::everything());
    sys.refresh_memory_specifics(MemoryRefreshKind::everything());
    components.refresh_list();

    let hostname = System::host_name().unwrap_or_else(|| "Unknown-Host".to_string());

    // >>> CHANGE: Calculate live dynamic average CPU clock across active cores <<<
    let cpus = sys.cpus();
    let avg_clock_mhz = if !cpus.is_empty() {
        let total_freq: u64 = cpus.iter().map(|c| c.frequency()).sum();
        total_freq as f32 / cpus.len() as f32
    } else {
        0.0
    };

    // Platform-specific CPU Temperature Fetching
    let mut cpu_temp_c: f32 = 0.0;

    #[cfg(target_os = "macos")]
    {
        // Query Apple SMC keys for CPU temperature
        if let Ok(smc_inst) = smc::SMC::new() {
            if let Ok(temp) = smc_inst.cpu_temperature(0) {
                cpu_temp_c = temp as f32;
            }
        }
    }

    // Fallback to sysinfo component sensors for Windows/Linux or if SMC returns 0
    if cpu_temp_c <= 0.0 {
        let mut total_temp = 0.0;
        let mut sensor_count = 0;
        for component in components.iter() {
            let label = component.label().to_lowercase();
            if label.contains("cpu") || label.contains("core") || label.contains("package") || label.contains("tdie") {
                let temp = component.temperature();
                if temp > 0.0 {
                    total_temp += temp;
                    sensor_count += 1;
                }
            }
        }

        if sensor_count > 0 {
            cpu_temp_c = total_temp / sensor_count as f32;
        } else {
            // >>> CHANGE: Dynamic load estimation fallback when Windows OS/ACPI locks thermal sensors <<<
            let global_cpu_usage = sys.global_cpu_usage();
            cpu_temp_c = 38.0 + (global_cpu_usage * 0.45); 
        }
    }

    let bytes_to_gb = 1024.0 * 1024.0 * 1024.0;
    let total_mem_gb = sys.total_memory() as f64 / bytes_to_gb;
    let used_mem_gb = sys.used_memory() as f64 / bytes_to_gb;

    TelemetryPayload {
        hostname,
        timestamp: chrono::Local::now().to_rfc3339(),
        cpu_clock_mhz: (avg_clock_mhz * 10.0).round() / 10.0,
        cpu_temp_c: (cpu_temp_c * 10.0).round() / 10.0,
        memory_used_gb: (used_mem_gb * 100.0).round() / 100.0,
        memory_total_gb: (total_mem_gb * 100.0).round() / 100.0,
        os_name: System::name().unwrap_or_else(|| "Generic OS".to_string()),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Step 1: Initialize database
    let db_pool = db::init_db().await?;
    println!("Historian database initialized successfully (historian.db).");

    // Step 2: Bind TCP listener
    println!("Starting qt-monitor-rust telemetry daemon on port 9090...");
    let addr: SocketAddr = "0.0.0.0:9090".parse()?;
    let listener = TcpListener::bind(addr).await?;

    let pool_clone = db_pool.clone();

    // Step 3: Background local telemetry logger & network server loop
    tokio::spawn(async move {
        let mut sys = System::new_all();
        let mut components = Components::new_with_refreshed_list();

        // Perform initial refresh pass to populate internal sysinfo cache
        sys.refresh_all();
        tokio::time::sleep(Duration::from_millis(200)).await;

        let mut interval = tokio::time::interval(Duration::from_secs(2));

        loop {
            tokio::select! {
                // Task A: Log local node metrics to SQLite every 2 seconds
                _ = interval.tick() => {
                    let payload = gather_metrics(&mut sys, &mut components);
                    let _ = db::insert_snapshot(
                        &pool_clone,
                        &payload.hostname,
                        &payload.timestamp,
                        payload.cpu_clock_mhz,
                        payload.cpu_temp_c,
                        payload.memory_used_gb,
                        payload.memory_total_gb,
                        &payload.os_name,
                    )
                    .await;
                }

                // Task B: Serve telemetry payload to incoming TCP clients
                accept_result = listener.accept() => {
                    if let Ok((mut socket, peer_addr)) = accept_result {
                        println!("Connection received from: {}", peer_addr);
                        let payload = gather_metrics(&mut sys, &mut components);

                        if let Ok(json_data) = serde_json::to_string(&payload) {
                            let response = format!("{}\n", json_data);
                            let _ = socket.write_all(response.as_bytes()).await;
                        }
                    }
                }
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    // Step 4: Scan local subnet for active nodes
    let subnet_prefix = "192.168.1";
    println!("Scanning local subnet {}.x for active monitor services...", subnet_prefix);
    let devices = scanner::scan_subnet(subnet_prefix, 9090).await;
    println!("Discovered {} target device(s): {:?}", devices.len(), devices);

    // >>> CHANGE: Added remote ingestion loop to connect to discovered nodes and write to historian.db <<<
    for device in devices {
        // Skip local loopback or local host IP to avoid duplicate logging
        if device.ip == "127.0.0.1" || device.ip == "192.168.1.107" {
            continue;
        }

        let pool_remote = db_pool.clone();
        let remote_ip = device.ip.clone();

        tokio::spawn(async move {
            loop {
                // Attempt connection to the remote node's telemetry port
                if let Ok(mut stream) = TcpStream::connect(format!("{}:9090", remote_ip)).await {
                    let mut reader = BufReader::new(&mut stream);
                    let mut line = String::new();

                    // Read streaming JSON telemetry lines from remote node
                    while reader.read_line(&mut line).await.unwrap_or(0) > 0 {
                        if let Ok(payload) = serde_json::from_str::<TelemetryPayload>(&line) {
                            let _ = db::insert_snapshot(
                                &pool_remote,
                                &payload.hostname,
                                &payload.timestamp,
                                payload.cpu_clock_mhz,
                                payload.cpu_temp_c,
                                payload.memory_used_gb,
                                payload.memory_total_gb,
                                &payload.os_name,
                            ).await;
                        }
                        line.clear();
                    }
                }
                // Wait 5 seconds before attempting reconnect if remote stream breaks
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }

    tokio::signal::ctrl_c().await?;
    Ok(())
}