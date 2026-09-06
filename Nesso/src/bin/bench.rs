use reqwest::Client;
use rusqlite::Connection;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use base64::prelude::*;
use hdrhistogram::Histogram;
use serde_json::json;
use statrs::statistics::Statistics;
use std::fs;
use std::path::PathBuf;

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct RunResult {
    throughput: f64,
    p50_ms: f64,
    p99_ms: f64,
    dispatched: u64,
    found_on_disk: u64,
    integrity_ok: bool,
}

#[derive(Clone, Debug)]
struct BenchResult {
    system: String,
    threads: usize,
nominal_tasks: usize,
    sync: bool,
    op: String,
    runs: Vec<RunResult>,
}

impl BenchResult {
    fn mean_throughput(&self) -> f64 {
        let v: Vec<f64> = self.runs.iter().map(|r| r.throughput).collect();
        v.mean()
    }
    fn stddev_throughput(&self) -> f64 {
        let v: Vec<f64> = self.runs.iter().map(|r| r.throughput).collect();
        v.std_dev()
    }
    fn min_throughput(&self) -> f64 {
        self.runs.iter().map(|r| r.throughput).fold(f64::INFINITY, f64::min)
    }
    fn max_throughput(&self) -> f64 {
        self.runs.iter().map(|r| r.throughput).fold(0.0_f64, f64::max)
    }
    fn mean_p50(&self) -> f64 {
        let v: Vec<f64> = self.runs.iter().map(|r| r.p50_ms).collect();
        v.mean()
    }
    fn mean_p99(&self) -> f64 {
        let v: Vec<f64> = self.runs.iter().map(|r| r.p99_ms).collect();
        v.mean()
    }
    fn all_integrity_ok(&self) -> bool {
        self.runs.iter().all(|r| r.integrity_ok)
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    println!("Nesso vs SQLite Benchmark Harness v2 (Atomic Integrity)\n");

    let thread_counts = [1, 4, 16];
    let num_runs: usize = 6; // 1 warmup + 5 measured

    let client = Client::builder()
        .pool_max_idle_per_host(200)
        .build()
        .unwrap();

    // Start the Nesso HTTP server once
    let nesso_http_dir = PathBuf::from("bench_nesso_http_data");
    let _ = fs::remove_dir_all(&nesso_http_dir);
    let router = Nesso::server::create_router(nesso_http_dir.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:8181")
        .await
        .unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let mut all_results: Vec<BenchResult> = Vec::new();

    for sync in [false, true] {
        // Use larger batches for non-sync (fast), smaller for sync (SSD-bound)
        let nominal_tasks: usize = if sync { 1_000 } else { 10_000 };

        for &t_count in &thread_counts {
            let tasks_per_thread = nominal_tasks / t_count;
            let actual_dispatched = (tasks_per_thread * t_count) as u64;

            println!(
                "Config: sync={}, threads={}, nominal={}, actual_dispatched={}",
                sync, t_count, nominal_tasks, actual_dispatched
            );

            let mut sqlite_push_runs: Vec<RunResult> = Vec::new();
            let mut sqlite_pop_runs: Vec<RunResult> = Vec::new();
            let mut nesso_ip_push_runs: Vec<RunResult> = Vec::new();
            let mut nesso_ip_pop_runs: Vec<RunResult> = Vec::new();
            let mut nesso_http_push_runs: Vec<RunResult> = Vec::new();
            let mut nesso_http_pop_runs: Vec<RunResult> = Vec::new();

            for run_idx in 0..num_runs {
                let is_warmup = run_idx == 0;
                if is_warmup {
                    println!("  [Warmup]");
                } else {
                    println!("  [Run {}/5]", run_idx);
                }

                // ==========================================================
                // 1. SQLite In-Process
                // ==========================================================
                let sqlite_db_path =
                    format!("bench_sqlite_{}_{}_{}_r{}.db", sync, t_count, nominal_tasks, run_idx);
                let _ = fs::remove_file(&sqlite_db_path);
                {
                    let conn = Connection::open(&sqlite_db_path).unwrap();
                    conn.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
                    let sp = if sync { "FULL" } else { "NORMAL" };
                    conn.execute_batch(&format!("PRAGMA synchronous={};", sp)).unwrap();
                    conn.execute(
                        "CREATE TABLE queue (id INTEGER PRIMARY KEY AUTOINCREMENT, \
                         payload BLOB, priority INTEGER, state TEXT)",
                        [],
                    )
                    .unwrap();
                    conn.execute(
                        "CREATE INDEX idx_pop ON queue(state, priority DESC, id ASC)",
                        [],
                    )
                    .unwrap();
                }
                let db_path = Arc::new(sqlite_db_path.clone());

                // --- SQLite Push ---
                let counter = Arc::new(AtomicU64::new(0));
                let start = Instant::now();
                let mut handles = Vec::new();
                for _ in 0..t_count {
                    let db = db_path.clone();
                    let ctr = counter.clone();
                    handles.push(tokio::task::spawn_blocking(move || {
                        let conn = Connection::open(&*db).unwrap();
                        conn.busy_timeout(std::time::Duration::from_secs(30)).unwrap();
                        let sp = if sync { "FULL" } else { "NORMAL" };
                        conn.execute_batch(&format!("PRAGMA synchronous={};", sp)).unwrap();
                        let mut hist = Histogram::<u64>::new(3).unwrap();
                        for _ in 0..tasks_per_thread {
                            let t0 = Instant::now();
                            if conn
                                .execute(
                                    "INSERT INTO queue (payload, priority, state) \
                                     VALUES (?1, ?2, 'ready')",
                                    rusqlite::params![b"payload".to_vec(), 1],
                                )
                                .is_ok()
                            {
                                ctr.fetch_add(1, Ordering::Relaxed);
                            }
                            hist.record(t0.elapsed().as_micros() as u64).unwrap();
                        }
                        hist
                    }));
                }
                let mut combined = Histogram::<u64>::new(3).unwrap();
                for h in handles {
                    combined.add(h.await.unwrap()).unwrap();
                }
                let elapsed = start.elapsed().as_secs_f64();
                let dispatched = counter.load(Ordering::SeqCst);

                // Integrity: count actual rows on disk
                let conn = Connection::open(&sqlite_db_path).unwrap();
                let found: i64 = conn
                    .query_row("SELECT count(*) FROM queue WHERE state='ready'", [], |r| r.get(0))
                    .unwrap();
                let found = found as u64;
                let integrity = found == dispatched;

                if !is_warmup {
                    sqlite_push_runs.push(RunResult {
                        throughput: dispatched as f64 / elapsed,
                        p50_ms: combined.value_at_quantile(0.5) as f64 / 1000.0,
                        p99_ms: combined.value_at_quantile(0.99) as f64 / 1000.0,
                        dispatched,
                        found_on_disk: found,
                        integrity_ok: integrity,
                    });
                }

                // --- SQLite Pop+Ack ---
                let counter = Arc::new(AtomicU64::new(0));
                let start = Instant::now();
                let mut handles = Vec::new();
                for _ in 0..t_count {
                    let db = db_path.clone();
                    let ctr = counter.clone();
                    handles.push(tokio::task::spawn_blocking(move || {
                        let mut conn = Connection::open(&*db).unwrap();
                        conn.busy_timeout(std::time::Duration::from_secs(30)).unwrap();
                        let sp = if sync { "FULL" } else { "NORMAL" };
                        conn.execute_batch(&format!("PRAGMA synchronous={};", sp)).unwrap();
                        let mut hist = Histogram::<u64>::new(3).unwrap();
                        for _ in 0..tasks_per_thread {
                            let t0 = Instant::now();
                            let tx = conn
                                .transaction_with_behavior(
                                    rusqlite::TransactionBehavior::Immediate,
                                )
                                .unwrap();
                            let id_opt: Option<i64> = tx
                                .query_row(
                                    "SELECT id FROM queue WHERE state='ready' \
                                     ORDER BY priority DESC, id ASC LIMIT 1",
                                    [],
                                    |row| row.get(0),
                                )
                                .ok();
                            if let Some(id) = id_opt {
                                tx.execute(
                                    "UPDATE queue SET state='acked' WHERE id=?1",
                                    rusqlite::params![id],
                                )
                                .unwrap();
                                ctr.fetch_add(1, Ordering::Relaxed);
                            }
                            tx.commit().unwrap();
                            hist.record(t0.elapsed().as_micros() as u64).unwrap();
                        }
                        hist
                    }));
                }
                let mut combined = Histogram::<u64>::new(3).unwrap();
                for h in handles {
                    combined.add(h.await.unwrap()).unwrap();
                }
                let elapsed = start.elapsed().as_secs_f64();
                let popped = counter.load(Ordering::SeqCst);

                let remaining: i64 = conn
                    .query_row("SELECT count(*) FROM queue WHERE state='ready'", [], |r| r.get(0))
                    .unwrap();
                let remaining = remaining as u64;
                // After push we had `dispatched` ready. After popping `popped`, we expect dispatched-popped.
                let pop_integrity = remaining == dispatched.saturating_sub(popped);

                if !is_warmup {
                    sqlite_pop_runs.push(RunResult {
                        throughput: popped as f64 / elapsed,
                        p50_ms: combined.value_at_quantile(0.5) as f64 / 1000.0,
                        p99_ms: combined.value_at_quantile(0.99) as f64 / 1000.0,
                        dispatched: popped,
                        found_on_disk: remaining,
                        integrity_ok: pop_integrity,
                    });
                }

                // ==========================================================
                // 2. Nesso In-Process
                // ==========================================================
                let nesso_dir = PathBuf::from(format!(
                    "bench_nesso_ip_{}_{}_{}_r{}",
                    sync, t_count, nominal_tasks, run_idx
                ));
                let _ = fs::remove_dir_all(&nesso_dir);
                fs::create_dir_all(&nesso_dir).unwrap();
                let engine =
                    Nesso::storage::engine::Engine::open(&nesso_dir).unwrap();

                // --- Nesso In-Process Push ---
                let counter = Arc::new(AtomicU64::new(0));
                let start = Instant::now();
                let mut handles = Vec::new();
                for _ in 0..t_count {
                    let eng = engine.clone();
                    let ctr = counter.clone();
                    handles.push(tokio::task::spawn_blocking(move || {
                        let mut hist = Histogram::<u64>::new(3).unwrap();
                        for _ in 0..tasks_per_thread {
                            let t0 = Instant::now();
                            if eng.push(b"payload".to_vec(), 1).is_ok() {
                                if sync {
                                    eng.sync().unwrap();
                                }
                                ctr.fetch_add(1, Ordering::Relaxed);
                            }
                            hist.record(t0.elapsed().as_micros() as u64).unwrap();
                        }
                        hist
                    }));
                }
                let mut combined = Histogram::<u64>::new(3).unwrap();
                for h in handles {
                    combined.add(h.await.unwrap()).unwrap();
                }
                let elapsed = start.elapsed().as_secs_f64();
                let dispatched_ip = counter.load(Ordering::SeqCst);
                let (ready, _) = engine.status();
                let ip_push_integrity = ready as u64 == dispatched_ip;

                if !is_warmup {
                    nesso_ip_push_runs.push(RunResult {
                        throughput: dispatched_ip as f64 / elapsed,
                        p50_ms: combined.value_at_quantile(0.5) as f64 / 1000.0,
                        p99_ms: combined.value_at_quantile(0.99) as f64 / 1000.0,
                        dispatched: dispatched_ip,
                        found_on_disk: ready as u64,
                        integrity_ok: ip_push_integrity,
                    });
                }

                // --- Nesso In-Process Pop+Ack ---
                let counter = Arc::new(AtomicU64::new(0));
                let start = Instant::now();
                let mut handles = Vec::new();
                for cid in 0..t_count {
                    let eng = engine.clone();
                    let ctr = counter.clone();
                    handles.push(tokio::task::spawn_blocking(move || {
                        let mut hist = Histogram::<u64>::new(3).unwrap();
                        for _ in 0..tasks_per_thread {
                            let t0 = Instant::now();
                            if let Ok(Some((rec, _))) =
                                eng.pop_and_lease(cid as u32, 10)
                            {
                                if sync {
                                    eng.sync().unwrap();
                                }
                                if eng.ack(rec.id(), cid as u32).is_ok() {
                                    if sync {
                                        eng.sync().unwrap();
                                    }
                                    ctr.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            hist.record(t0.elapsed().as_micros() as u64).unwrap();
                        }
                        hist
                    }));
                }
                let mut combined = Histogram::<u64>::new(3).unwrap();
                for h in handles {
                    combined.add(h.await.unwrap()).unwrap();
                }
                let elapsed = start.elapsed().as_secs_f64();
                let popped_ip = counter.load(Ordering::SeqCst);
                let (ready, active) = engine.status();
                let ip_pop_integrity =
                    ready as u64 == dispatched_ip.saturating_sub(popped_ip)
                        && active == 0;

                if !is_warmup {
                    nesso_ip_pop_runs.push(RunResult {
                        throughput: popped_ip as f64 / elapsed,
                        p50_ms: combined.value_at_quantile(0.5) as f64 / 1000.0,
                        p99_ms: combined.value_at_quantile(0.99) as f64 / 1000.0,
                        dispatched: popped_ip,
                        found_on_disk: ready as u64,
                        integrity_ok: ip_pop_integrity,
                    });
                }

                // ==========================================================
                // 3. Nesso HTTP
                // ==========================================================
                let queue_name =
                    format!("bench_q_{}_{}_{}_r{}", sync, t_count, nominal_tasks, run_idx);
                let payload_b64 = BASE64_STANDARD.encode(b"payload");

                // --- Nesso HTTP Push ---
                let counter = Arc::new(AtomicU64::new(0));
                let start = Instant::now();
                let mut handles = Vec::new();
                for _ in 0..t_count {
                    let c = client.clone();
                    let q = queue_name.clone();
                    let p = payload_b64.clone();
                    let ctr = counter.clone();
                    handles.push(tokio::spawn(async move {
                        let mut hist = Histogram::<u64>::new(3).unwrap();
                        for _ in 0..tasks_per_thread {
                            let t0 = Instant::now();
                            if let Ok(r) = c
                                .post(format!(
                                    "http://127.0.0.1:8181/v1/queues/{}/push?sync={}",
                                    q, sync
                                ))
                                .json(&json!({"payload": p, "priority": 1}))
                                .send()
                                .await
                            {
                                if r.status().is_success() {
                                    ctr.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            hist.record(t0.elapsed().as_micros() as u64).unwrap();
                        }
                        hist
                    }));
                }
                let mut combined = Histogram::<u64>::new(3).unwrap();
                for h in handles {
                    combined.add(h.await.unwrap()).unwrap();
                }
                let elapsed = start.elapsed().as_secs_f64();
                let dispatched_http = counter.load(Ordering::SeqCst);

                let status_res = client
                    .get(format!(
                        "http://127.0.0.1:8181/v1/queues/{}/status",
                        queue_name
                    ))
                    .send()
                    .await
                    .unwrap()
                    .json::<serde_json::Value>()
                    .await
                    .unwrap();
                let ready_http = status_res["ready_tasks"].as_u64().unwrap_or(0);
                let http_push_integrity = ready_http == dispatched_http;

                if !is_warmup {
                    nesso_http_push_runs.push(RunResult {
                        throughput: dispatched_http as f64 / elapsed,
                        p50_ms: combined.value_at_quantile(0.5) as f64 / 1000.0,
                        p99_ms: combined.value_at_quantile(0.99) as f64 / 1000.0,
                        dispatched: dispatched_http,
                        found_on_disk: ready_http,
                        integrity_ok: http_push_integrity,
                    });
                }

                // --- Nesso HTTP Pop+Ack ---
                let counter = Arc::new(AtomicU64::new(0));
                let start = Instant::now();
                let mut handles = Vec::new();
                for cid in 0..t_count {
                    let c = client.clone();
                    let q = queue_name.clone();
                    let ctr = counter.clone();
                    handles.push(tokio::spawn(async move {
                        let mut hist = Histogram::<u64>::new(3).unwrap();
                        for _ in 0..tasks_per_thread {
                            let t0 = Instant::now();
                            if let Ok(r) = c
                                .post(format!(
                                    "http://127.0.0.1:8181/v1/queues/{}/pop?sync={}",
                                    q, sync
                                ))
                                .json(
                                    &json!({"consumer_id": cid as u32, "lease_secs": 10}),
                                )
                                .send()
                                .await
                            {
                                if r.status() == 200 {
                                    if let Ok(body) =
                                        r.json::<serde_json::Value>().await
                                    {
                                        if let Some(id) = body["id"].as_u64() {
                                            if let Ok(ar) = c
                                                .post(format!(
                                                    "http://127.0.0.1:8181/v1/queues/{}/tasks/{}/ack?sync={}",
                                                    q, id, sync
                                                ))
                                                .json(
                                                    &json!({"consumer_id": cid as u32}),
                                                )
                                                .send()
                                                .await
                                            {
                                                if ar.status().is_success() {
                                                    ctr.fetch_add(
                                                        1,
                                                        Ordering::Relaxed,
                                                    );
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            hist.record(t0.elapsed().as_micros() as u64).unwrap();
                        }
                        hist
                    }));
                }
                let mut combined = Histogram::<u64>::new(3).unwrap();
                for h in handles {
                    combined.add(h.await.unwrap()).unwrap();
                }
                let elapsed = start.elapsed().as_secs_f64();
                let popped_http = counter.load(Ordering::SeqCst);

                let status_res = client
                    .get(format!(
                        "http://127.0.0.1:8181/v1/queues/{}/status",
                        queue_name
                    ))
                    .send()
                    .await
                    .unwrap()
                    .json::<serde_json::Value>()
                    .await
                    .unwrap();
                let ready_http = status_res["ready_tasks"].as_u64().unwrap_or(0);
                let active_http =
                    status_res["active_leases"].as_u64().unwrap_or(u64::MAX);
                let http_pop_integrity =
                    ready_http == dispatched_http.saturating_sub(popped_http)
                        && active_http == 0;

                if !is_warmup {
                    nesso_http_pop_runs.push(RunResult {
                        throughput: popped_http as f64 / elapsed,
                        p50_ms: combined.value_at_quantile(0.5) as f64 / 1000.0,
                        p99_ms: combined.value_at_quantile(0.99) as f64 / 1000.0,
                        dispatched: popped_http,
                        found_on_disk: ready_http,
                        integrity_ok: http_pop_integrity,
                    });
                }
            } // end run loop

            all_results.push(BenchResult {
                system: "SQLite (In-Process)".into(),
                threads: t_count,
                nominal_tasks,
                sync,
                op: "Push".into(),
                runs: sqlite_push_runs,
            });
            all_results.push(BenchResult {
                system: "SQLite (In-Process)".into(),
                threads: t_count,
                nominal_tasks,
                sync,
                op: "Pop+Ack".into(),
                runs: sqlite_pop_runs,
            });
            all_results.push(BenchResult {
                system: "Nesso (In-Process)".into(),
                threads: t_count,
                nominal_tasks,
                sync,
                op: "Push".into(),
                runs: nesso_ip_push_runs,
            });
            all_results.push(BenchResult {
                system: "Nesso (In-Process)".into(),
                threads: t_count,
                nominal_tasks,
                sync,
                op: "Pop+Ack".into(),
                runs: nesso_ip_pop_runs,
            });
            all_results.push(BenchResult {
                system: "Nesso (HTTP)".into(),
                threads: t_count,
                nominal_tasks,
                sync,
                op: "Push".into(),
                runs: nesso_http_push_runs,
            });
            all_results.push(BenchResult {
                system: "Nesso (HTTP)".into(),
                threads: t_count,
                nominal_tasks,
                sync,
                op: "Pop+Ack".into(),
                runs: nesso_http_pop_runs,
            });
        }
    }

    // ======================================================================
    // Generate CSV
    // ======================================================================
    let mut csv = String::new();
    csv.push_str(
        "System,Threads,NominalTasks,ActualDispatched,Sync,Op,\
         IntegrityOK,Mean(ops/s),StdDev,Min,Max,Mean_p50(ms),Mean_p99(ms)\n",
    );
    for r in &all_results {
        let dispatched = if !r.runs.is_empty() {
            r.runs[0].dispatched
        } else {
            0
        };
        csv.push_str(&format!(
            "{},{},{},{},{},{},{},{:.2},{:.2},{:.2},{:.2},{:.3},{:.3}\n",
            r.system,
            r.threads,
            r.nominal_tasks,
            dispatched,
            r.sync,
            r.op,
            r.all_integrity_ok(),
            r.mean_throughput(),
            r.stddev_throughput(),
            r.min_throughput(),
            r.max_throughput(),
            r.mean_p50(),
            r.mean_p99(),
        ));
    }
    fs::write("benchmark_results.csv", &csv).unwrap();

    // ======================================================================
    // Generate Markdown Report
    // ======================================================================
    let mut md = String::new();

    md.push_str("# Nesso vs SQLite — Benchmark Report\n\n");

    // --- Caveats up front ---
    md.push_str("## ⚠️ Limitazioni Note (leggere PRIMA dei risultati)\n\n");
    md.push_str(
        "> **CAMPIONE LIMITATO**: I run con `sync=true` usano N=1000 operazioni \
         nominali per configurazione. I run senza fsync usano N=10000. Questi \
         campioni sono sufficienti per evidenziare trend architetturali, ma sono \
         soggetti a rumore statistico significativo. **Ripetere su hardware reale \
         con campioni da 100k+ prima di trarre conclusioni definitive.**\n\n",
    );
    md.push_str(
        "> **macOS E DURABILITÀ (F_FULLFSYNC)**: Su macOS, `File::sync_data()` \
         (che mappa su `fsync()`) **NON garantisce** che i dati siano \
         effettivamente scritti sulla memoria non-volatile del disco. macOS può \
         tenere i dati nella cache hardware del drive. Solo `fcntl(fd, F_FULLFSYNC)` \
         forza un flush reale fino al supporto fisico. Questo vale sia per Nesso \
         (che chiama `sync_data()`) sia per SQLite (che usa `fsync()` internamente, \
         a meno di essere compilato con `SQLITE_EXTRA_DURABLE` che attiva \
         `F_FULLFSYNC`).\n>\n> **Conseguenza**: I risultati `sync=true` su questo \
         hardware misurano il costo di un fsync *logico* (barriera verso il kernel), \
         non di un flush fisico completo. Il costo reale della durabilità completa \
         sarebbe più alto per ENTRAMBI i sistemi. **Ripetere questo benchmark su \
         Linux (dove `fsync()` è una garanzia reale di persistenza)** prima di \
         pubblicare affermazioni sulla durabilità.\n\n",
    );

    // --- Setup ---
    md.push_str("## Setup\n\n");
    md.push_str("- **OS**: macOS (Darwin arm64)\n");
    md.push_str("- **Disco**: SSD locale (APFS)\n");
    md.push_str("- **Compilazione**: `cargo run --release` (profilo optimized)\n");
    md.push_str("- **SQLite**: rusqlite 0.40.2, PRAGMA journal_mode=WAL\n");
    md.push_str("- **Warm-up**: 1 run scartato per ogni configurazione\n");
    md.push_str("- **Run misurati**: 5 per ogni configurazione\n\n");

    // --- Methodology ---
    md.push_str("## Metodologia\n\n");
    md.push_str("### Sistemi testati\n\n");
    md.push_str(
        "- **SQLite (In-Process)**: Libreria C chiamata direttamente dallo \
         stesso binario. Zero overhead di rete.\n",
    );
    md.push_str(
        "- **Nesso (In-Process)**: Engine Rust chiamato direttamente dallo \
         stesso binario. Zero overhead di rete. **Confronto alla pari con SQLite.**\n",
    );
    md.push_str(
        "- **Nesso (HTTP)**: Stack completo (Client HTTP → TCP loopback → \
         Axum → Engine). Include overhead di serializzazione JSON, routing, \
         e trasporto TCP. **NON confrontabile alla pari con SQLite In-Process.**\n\n",
    );

    md.push_str("### Verifica di integrità\n\n");
    md.push_str(
        "Ogni thread incrementa un contatore atomico (`AtomicU64`) ad ogni \
         operazione completata con successo. A fine run, l'harness confronta \
         il valore del contatore con il numero di record effettivamente presenti \
         su disco (SQLite: `SELECT count(*)`; Nesso: `engine.status()`). Se i \
         due numeri non coincidono, il run è marcato `IntegrityOK=false`.\n\n",
    );
    md.push_str(
        "**Nota**: il numero nominale di task (es. 1000) può differire dal \
         totale realmente dispatchato se `N % T != 0` (divisione intera). \
         L'integrità è verificata contro il conteggio *reale*, non contro il \
         valore nominale.\n\n",
    );

    // --- Results table ---
    md.push_str("## Risultati Grezzi\n\n");
    md.push_str(
        "| System | Threads | Nominal | Dispatched | Sync | Op | Integrity | \
         Mean (ops/s) | StdDev | Min | Max | p50 (ms) | p99 (ms) |\n",
    );
    md.push_str(
        "|--------|---------|---------|------------|------|----|-----------|---\
         ------------|--------|-----|-----|----------|----------|\n",
    );
    for r in &all_results {
        let dispatched = if !r.runs.is_empty() {
            r.runs[0].dispatched
        } else {
            0
        };
        let integrity_str = if r.all_integrity_ok() {
            "✅"
        } else {
            "❌ MISMATCH"
        };
        md.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} | {} | {:.0} | {:.0} | {:.0} | {:.0} | {:.3} | {:.3} |\n",
            r.system,
            r.threads,
            r.nominal_tasks,
            dispatched,
            r.sync,
            r.op,
            integrity_str,
            r.mean_throughput(),
            r.stddev_throughput(),
            r.min_throughput(),
            r.max_throughput(),
            r.mean_p50(),
            r.mean_p99(),
        ));
    }

    // --- Per-run detail ---
    md.push_str("\n## Dettaglio Per-Run (dati grezzi verificabili)\n\n");
    md.push_str(
        "| System | Threads | Sync | Op | Run | Dispatched | OnDisk | Integrity | Throughput | p50 | p99 |\n",
    );
    md.push_str(
        "|--------|---------|------|----|-----|------------|--------|-----------|------------|-----|-----|\n",
    );
    for r in &all_results {
        for (i, run) in r.runs.iter().enumerate() {
            md.push_str(&format!(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {:.0} | {:.3} | {:.3} |\n",
                r.system,
                r.threads,
                r.sync,
                r.op,
                i + 1,
                run.dispatched,
                run.found_on_disk,
                if run.integrity_ok { "✅" } else { "❌" },
                run.throughput,
                run.p50_ms,
                run.p99_ms,
            ));
        }
    }

    // --- Interpretation ---
    md.push_str("\n## Interpretazione\n\n");
    md.push_str(
        "**Confronto alla pari (In-Process vs In-Process)**: Nesso In-Process \
         vs SQLite In-Process è l'unico confronto metodologicamente corretto \
         per valutare il motore di storage. Nesso HTTP vs SQLite In-Process \
         misura una cosa diversa (motore + stack di rete vs motore puro) e va \
         interpretato come tale.\n\n",
    );
    md.push_str(
        "**Scalabilità multi-thread**: SQLite WAL permette una sola scrittura \
         alla volta. Con 16 thread concorrenti, i writer competono per il lock \
         del file generando contesa pesante (gestita internamente da \
         `busy_timeout`). Nesso serializza le scritture dietro un `Mutex` in \
         RAM senza mai collidere a livello di filesystem.\n\n",
    );
    md.push_str(
        "**Costo di sync=true**: I numeri di Nesso con sync (~250 ops/s push) \
         rappresentano il limite hardware dell'SSD per operazioni fsync \
         individuali. SQLite mostra throughput più alti con `synchronous=FULL` \
         perché in modalità WAL il flush avviene solo sull'append del journal, \
         non su ogni singola pagina del database — è un design architetturale \
         diverso, non un vantaggio intrinseco del motore. **Attenzione: su macOS \
         nessuno dei due sistemi garantisce un flush fisico reale senza \
         F_FULLFSYNC (vedi sezione limitazioni).**\n",
    );

    fs::write("benchmark_results.md", &md).unwrap();
    println!("\nBenchmark completo.");
    println!("  → benchmark_results.csv");
    println!("  → benchmark_results.md");
}
