use axum::serve;
use std::env;
use std::time::Duration;
use tokio::net::TcpListener;
use nesso::server::{self, AppState};
use nesso::storage::engine::{GroupCommitConfig, SyncMode};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let default_workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let mut http_workers = default_workers;
    let mut batch_window_ms = 2u64;
    let mut max_batch_size = 64usize;
    let mut idle_commit_threshold_us = 0u64;
    let mut sync_mode = SyncMode::Standard;
    let mut data_dir = env::current_dir()?.join("nesso_data");
    let mut bind_addr = "127.0.0.1:8080".to_string();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--http-workers" => {
                if i + 1 < args.len() {
                    http_workers = args[i + 1].parse()?;
                    i += 2;
                } else {
                    eprintln!("Error: missing value for --http-workers");
                    std::process::exit(1);
                }
            }
            "--batch-window-ms" => {
                if i + 1 < args.len() {
                    batch_window_ms = args[i + 1].parse()?;
                    i += 2;
                } else {
                    eprintln!("Error: missing value for --batch-window-ms");
                    std::process::exit(1);
                }
            }
            "--max-batch-size" => {
                if i + 1 < args.len() {
                    max_batch_size = args[i + 1].parse()?;
                    i += 2;
                } else {
                    eprintln!("Error: missing value for --max-batch-size");
                    std::process::exit(1);
                }
            }
            "--idle-commit-threshold-us" => {
                if i + 1 < args.len() {
                    idle_commit_threshold_us = args[i + 1].parse()?;
                    i += 2;
                } else {
                    eprintln!("Error: missing value for --idle-commit-threshold-us");
                    std::process::exit(1);
                }
            }
            "--sync-mode" => {
                if i + 1 < args.len() {
                    match args[i + 1].to_lowercase().as_str() {
                        "standard" | "posix" => sync_mode = SyncMode::Standard,
                        "full" | "hardware" => sync_mode = SyncMode::FullHardware,
                        other => {
                            eprintln!("Error: unknown sync mode '{}'. Valid values: standard, full", other);
                            std::process::exit(1);
                        }
                    }
                    i += 2;
                } else {
                    eprintln!("Error: missing value for --sync-mode");
                    std::process::exit(1);
                }
            }
            "--data-dir" => {
                if i + 1 < args.len() {
                    data_dir = std::path::PathBuf::from(&args[i + 1]);
                    i += 2;
                } else {
                    eprintln!("Error: missing value for --data-dir");
                    std::process::exit(1);
                }
            }
            "--bind" => {
                if i + 1 < args.len() {
                    bind_addr = args[i + 1].clone();
                    i += 2;
                } else {
                    eprintln!("Error: missing value for --bind");
                    std::process::exit(1);
                }
            }
            "--help" | "-h" => {
                println!("Usage: nesso [OPTIONS]");
                println!("Options:");
                println!(
                    "  --http-workers <N>              Tokio runtime HTTP worker threads (default: available CPU cores, {})",
                    default_workers
                );
                println!("  --batch-window-ms <N>           Group commit batch window in milliseconds (default: 2)");
                println!("  --max-batch-size <N>            Group commit maximum batch size (default: 64)");
                println!("  --idle-commit-threshold-us <N>  Adaptive early commit idle threshold in microseconds (default: 0)");
                println!("  --sync-mode <standard|full>     Durability sync mode (default: standard [POSIX fsync])");
                println!("  --data-dir <PATH>               Directory for queue data (default: ./nesso_data)");
                println!("  --bind <ADDR>                   Address to bind to (default: 127.0.0.1:8080)");
                return Ok(());
            }
            _ => {
                i += 1;
            }
        }
    }

    // =========================================================================
    // TOKIO RUNTIME INITIALIZATION
    // =========================================================================
    // The Tokio worker pool handles network transport concurrency (accepting
    // sockets, HTTP request/response serialization, and event polling).
    //
    // ARCHITECTURAL NOTE:
    // This pool does NOT increase write throughput for any single queue beyond
    // the single-writer WAL engine limit. Instead, additional worker threads benefit:
    // 1. Concurrent I/O across MULTIPLE independent queues.
    // 2. High fan-out of concurrent consumers popping and acknowledging messages.
    // 3. Hundreds or thousands of concurrent long-polling connections without saturating the event loop.
    // =========================================================================
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(http_workers)
        .enable_all()
        .build()?;

    runtime.block_on(async move {
        let group_commit_config = GroupCommitConfig {
            batch_window: Duration::from_millis(batch_window_ms),
            max_batch_size,
            idle_commit_threshold: Duration::from_micros(idle_commit_threshold_us),
            sync_mode,
        };

        let state = AppState::new_with_config(data_dir, group_commit_config);
        let shutdown_token = state.shutdown_token.clone();
        let app = server::create_router_with_state(state.clone());

        let listener = TcpListener::bind(&bind_addr).await?;
        println!(
            "Nesso server running on {} (http_workers: {}, batch_window: {}ms, max_batch: {}, idle_threshold: {}us, sync_mode: {:?})",
            bind_addr, http_workers, batch_window_ms, max_batch_size, idle_commit_threshold_us, sync_mode
        );

        // Graceful shutdown:
        // 1. Signal received -> cancels shutdown_token (waking long-polling requests with 503)
        // 2. Axum stops accepting new connections and waits for active requests to finish
        serve(listener, app)
            .with_graceful_shutdown(server::shutdown_signal(shutdown_token))
            .await?;

        // 3. For every active Engine: force flush pending group commit batches, then stop expiration threads
        state.shutdown_all_queues().await?;

        // 4. Final confirmation log
        println!("Shutdown complete: all pending batches have been fsync-ed, expiration threads stopped.");

        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
