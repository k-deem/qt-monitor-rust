// Declare the local 'db' module defined in src/db.rs.
mod db;

// Declare the local 'scanner' module defined in src/scanner.rs.
mod scanner;

// Import Utc from chrono to generate ISO-8601 standardized timestamps.

// Import Serialize trait from serde to enable automatic struct-to-JSON serialization.
use serde::Serialize;

// Import SocketAddr for network address parsing.
use std::net::SocketAddr;

// Import Duration to specify async sleep intervals.
use std::time::Duration;

// Import system monitoring types from sysinfo to access cross-platform hardware sensors.
use sysinfo::{Components, CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};

// Import AsyncWriteExt trait from tokio to enable asynchronous stream writing over TCP sockets.
use tokio::io::AsyncWriteExt;

// Import TcpListener from tokio for non-blocking network socket handling.
use tokio::net::TcpListener;

/// Data structure defining the JSON schema transferred over TCP and stored in SQLite.
#[derive(Serialize, Debug, Clone)]
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
    sys.refresh_specifics(
        RefreshKind::new()
            .with_cpu(CpuRefreshKind::everything())
            .with_memory(MemoryRefreshKind::everything()),
    );
    components.refresh_list();

    let hostname = System::host_name().unwrap_or_else(|| "Unknown-Host".to_string());

    let cpus = sys.cpus();
    let avg_clock_mhz = if !cpus.is_empty() {
        let total_freq: u64 = cpus.iter().map(|c| c.frequency()).sum();
        total_freq as f32 / cpus.len() as f32
    } else {
        2400.0
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

        cpu_temp_c = if sensor_count > 0 {
            total_temp / sensor_count as f32
        } else {
            42.5 // Default baseline if restricted
        };
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
        os_name: System::name().unwrap_or_else(|| "Unknown OS".to_string()),
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

    // Step 3: Background telemetry logger & network server loop
    tokio::spawn(async move {
        let mut sys = System::new_all();
        let mut components = Components::new_with_refreshed_list();

        // Perform initial refresh pass to populate internal sysinfo cache
        sys.refresh_all();
        tokio::time::sleep(Duration::from_millis(200)).await;

        let mut interval = tokio::time::interval(Duration::from_secs(2));

        loop {
            tokio::select! {
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

    // Step 4: Scan local subnet
    let subnet_prefix = "192.168.1";
    println!("Scanning local subnet {}.x for active monitor services...", subnet_prefix);
    let devices = scanner::scan_subnet(subnet_prefix, 9090).await;
    println!("Discovered {} target device(s): {:?}", devices.len(), devices);

    tokio::signal::ctrl_c().await?;
    Ok(())
}