// Import the SqlitePoolOptions builder and Pool struct from sqlx for async connection handling.
use sqlx::{sqlite::SqlitePoolOptions, Pool, Sqlite};

// Import standard Error trait for flexible dynamic error handling across async boundaries.
use std::error::Error;

// Define a public type alias 'DbPool' to simplify references to our SQLite connection pool.
pub type DbPool = Pool<Sqlite>;

/// Initializes the SQLite database file and ensures required tables and indexes exist.
/// 
/// Returns a connected `DbPool` on success, or a boxed error if the file/connection fails.
pub async fn init_db() -> Result<DbPool, Box<dyn Error>> {
    // Configure and establish an async connection pool to 'historian.db'.
    // The 'mode=rwc' flag automatically creates the database file if it does not exist.
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect("sqlite://historian.db?mode=rwc")
        .await?;

    // Execute an SQL DDL script to construct the time-series telemetry storage table.
    // Includes an index on (hostname, timestamp) for rapid query performance during chart generation.
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS telemetry_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hostname TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            cpu_clock_mhz REAL NOT NULL,
            cpu_temp_c REAL NOT NULL,
            memory_used_gb REAL NOT NULL,
            memory_total_gb REAL NOT NULL,
            os_name TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_host_time ON telemetry_history(hostname, timestamp);
        "#,
    )
    .execute(&pool)
    .await?;

    // Return the active connection pool to the caller.
    Ok(pool)
}

/// Inserts a single telemetry snapshot into the 'telemetry_history' table.
/// 
/// Parameters match the metrics gathered by the cross-platform system monitors.
pub async fn insert_snapshot(
    pool: &DbPool,
    hostname: &str,
    timestamp: &str,
    cpu_clock_mhz: f32,
    cpu_temp_c: f32,
    memory_used_gb: f64,
    memory_total_gb: f64,
    os_name: &str,
) -> Result<(), Box<dyn Error>> {
    // Prepare a parameterized INSERT query to prevent SQL injection vulnerabilities.
    sqlx::query(
        r#"
        INSERT INTO telemetry_history 
        (hostname, timestamp, cpu_clock_mhz, cpu_temp_c, memory_used_gb, memory_total_gb, os_name)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    // Bind each value in order matching the positional placeholders (?) above.
    .bind(hostname)
    .bind(timestamp)
    .bind(cpu_clock_mhz)
    .bind(cpu_temp_c)
    .bind(memory_used_gb)
    .bind(memory_total_gb)
    .bind(os_name)
    // Asynchronously execute the statement against the database pool.
    .execute(pool)
    .await?;

    // Return Ok to signal a successful write.
    Ok(())
}