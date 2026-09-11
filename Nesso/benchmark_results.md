# Nesso vs SQLite — Benchmark Report

## ⚠️ Limitazioni Note (leggere PRIMA dei risultati)

> **CAMPIONE LIMITATO**: I run con `sync=true` usano N=1000 operazioni nominali per configurazione. I run senza fsync usano N=10000. Questi campioni sono sufficienti per evidenziare trend architetturali, ma sono soggetti a rumore statistico significativo. **Ripetere su hardware reale con campioni da 100k+ prima di trarre conclusioni definitive.**

> **macOS E DURABILITÀ (F_FULLFSYNC)**: Su macOS, `File::sync_data()` (che mappa su `fsync()`) **NON garantisce** che i dati siano effettivamente scritti sulla memoria non-volatile del disco. macOS può tenere i dati nella cache hardware del drive. Solo `fcntl(fd, F_FULLFSYNC)` forza un flush reale fino al supporto fisico. Questo vale sia per Nesso (che chiama `sync_data()`) sia per SQLite (che usa `fsync()` internamente, a meno di essere compilato con `SQLITE_EXTRA_DURABLE` che attiva `F_FULLFSYNC`).
>
> **Conseguenza**: I risultati `sync=true` su questo hardware misurano il costo di un fsync *logico* (barriera verso il kernel), non di un flush fisico completo. Il costo reale della durabilità completa sarebbe più alto per ENTRAMBI i sistemi. **Ripetere questo benchmark su Linux (dove `fsync()` è una garanzia reale di persistenza)** prima di pubblicare affermazioni sulla durabilità.

## Setup

- **OS**: macOS (Darwin arm64)
- **Disco**: SSD locale (APFS)
- **Compilazione**: `cargo run --release` (profilo optimized)
- **SQLite**: rusqlite 0.40.2, PRAGMA journal_mode=WAL
- **Warm-up**: 1 run scartato per ogni configurazione
- **Run misurati**: 5 per ogni configurazione

## Metodologia

### Sistemi testati

- **SQLite (In-Process)**: Libreria C chiamata direttamente dallo stesso binario. Zero overhead di rete.
- **Nesso (In-Process)**: Engine Rust chiamato direttamente dallo stesso binario. Zero overhead di rete. **Confronto alla pari con SQLite.**
- **Nesso (HTTP)**: Stack completo (Client HTTP → TCP loopback → Axum → Engine). Include overhead di serializzazione JSON, routing, e trasporto TCP. **NON confrontabile alla pari con SQLite In-Process.**

### Verifica di integrità

Ogni thread incrementa un contatore atomico (`AtomicU64`) ad ogni operazione completata con successo. A fine run, l'harness confronta il valore del contatore con il numero di record effettivamente presenti su disco (SQLite: `SELECT count(*)`; Nesso: `engine.status()`). Se i due numeri non coincidono, il run è marcato `IntegrityOK=false`.

**Nota**: il numero nominale di task (es. 1000) può differire dal totale realmente dispatchato se `N % T != 0` (divisione intera). L'integrità è verificata contro il conteggio *reale*, non contro il valore nominale.

## Risultati Grezzi

| System | Threads | Nominal | Dispatched | Sync | Op | Integrity | Mean (ops/s) | StdDev | Min | Max | p50 (ms) | p99 (ms) |
|--------|---------|---------|------------|------|----|-----------|---------------|--------|-----|-----|----------|----------|
| SQLite (In-Process) | 1 | 10000 | 10000 | false | Push | ✅ | 58483 | 2320 | 54996 | 60551 | 0.013 | 0.037 |
| SQLite (In-Process) | 1 | 10000 | 10000 | false | Pop+Ack | ✅ | 65056 | 1459 | 62488 | 66110 | 0.012 | 0.021 |
| Nesso (In-Process) | 1 | 10000 | 10000 | false | Push | ✅ | 928892 | 17354 | 912450 | 952952 | 0.001 | 0.001 |
| Nesso (In-Process) | 1 | 10000 | 10000 | false | Pop+Ack | ✅ | 332406 | 6791 | 320651 | 338292 | 0.002 | 0.004 |
| Nesso (HTTP) | 1 | 10000 | 10000 | false | Push | ✅ | 22943 | 335 | 22453 | 23356 | 0.042 | 0.073 |
| Nesso (HTTP) | 1 | 10000 | 10000 | false | Pop+Ack | ✅ | 11539 | 73 | 11424 | 11593 | 0.085 | 0.116 |
| SQLite (In-Process) | 4 | 10000 | 10000 | false | Push | ✅ | 51909 | 1883 | 50424 | 54929 | 0.013 | 0.043 |
| SQLite (In-Process) | 4 | 10000 | 10000 | false | Pop+Ack | ✅ | 56234 | 5927 | 48371 | 62626 | 0.012 | 0.028 |
| Nesso (In-Process) | 4 | 10000 | 10000 | false | Push | ✅ | 367386 | 1171 | 366358 | 369246 | 0.002 | 0.100 |
| Nesso (In-Process) | 4 | 10000 | 10000 | false | Pop+Ack | ✅ | 124808 | 9582 | 111669 | 135013 | 0.013 | 0.166 |
| Nesso (HTTP) | 4 | 10000 | 10000 | false | Push | ✅ | 59523 | 1938 | 57413 | 61271 | 0.063 | 0.126 |
| Nesso (HTTP) | 4 | 10000 | 10000 | false | Pop+Ack | ✅ | 29093 | 880 | 27900 | 30090 | 0.130 | 0.251 |
| SQLite (In-Process) | 16 | 10000 | 10000 | false | Push | ✅ | 17784 | 5244 | 12447 | 25737 | 0.014 | 0.161 |
| SQLite (In-Process) | 16 | 10000 | 10000 | false | Pop+Ack | ✅ | 23397 | 9675 | 14303 | 38668 | 0.014 | 0.384 |
| Nesso (In-Process) | 16 | 10000 | 10000 | false | Push | ✅ | 277362 | 42410 | 232607 | 342343 | 0.003 | 0.658 |
| Nesso (In-Process) | 16 | 10000 | 10000 | false | Pop+Ack | ✅ | 132301 | 706 | 131189 | 133145 | 0.058 | 0.730 |
| Nesso (HTTP) | 16 | 10000 | 10000 | false | Push | ✅ | 107370 | 811 | 106392 | 108422 | 0.133 | 0.365 |
| Nesso (HTTP) | 16 | 10000 | 10000 | false | Pop+Ack | ✅ | 50390 | 697 | 49405 | 51199 | 0.301 | 0.570 |
| SQLite (In-Process) | 1 | 1000 | 1000 | true | Push | ✅ | 12485 | 494 | 12097 | 13150 | 0.063 | 0.224 |
| SQLite (In-Process) | 1 | 1000 | 1000 | true | Pop+Ack | ✅ | 15590 | 509 | 14993 | 16304 | 0.057 | 0.114 |
| Nesso (In-Process) | 1 | 1000 | 1000 | true | Push | ✅ | 247 | 1 | 245 | 249 | 4.000 | 5.381 |
| Nesso (In-Process) | 1 | 1000 | 1000 | true | Pop+Ack | ✅ | 124 | 0 | 124 | 125 | 8.002 | 9.249 |
| Nesso (HTTP) | 1 | 1000 | 1000 | true | Push | ✅ | 246 | 2 | 244 | 248 | 4.005 | 5.226 |
| Nesso (HTTP) | 1 | 1000 | 1000 | true | Pop+Ack | ✅ | 123 | 1 | 122 | 124 | 8.010 | 9.778 |
| SQLite (In-Process) | 4 | 1000 | 1000 | true | Push | ✅ | 8936 | 1985 | 7105 | 11962 | 0.068 | 0.604 |
| SQLite (In-Process) | 4 | 1000 | 1000 | true | Pop+Ack | ✅ | 12092 | 354 | 11792 | 12682 | 0.058 | 0.254 |
| Nesso (In-Process) | 4 | 1000 | 1000 | true | Push | ✅ | 970 | 6 | 962 | 977 | 4.001 | 5.147 |
| Nesso (In-Process) | 4 | 1000 | 1000 | true | Pop+Ack | ✅ | 496 | 1 | 495 | 499 | 8.001 | 9.673 |
| Nesso (HTTP) | 4 | 1000 | 1000 | true | Push | ✅ | 951 | 12 | 932 | 962 | 4.037 | 5.107 |
| Nesso (HTTP) | 4 | 1000 | 1000 | true | Pop+Ack | ✅ | 469 | 7 | 459 | 477 | 8.069 | 10.639 |
| SQLite (In-Process) | 16 | 1000 | 992 | true | Push | ✅ | 2151 | 928 | 1248 | 3696 | 0.090 | 75.327 |
| SQLite (In-Process) | 16 | 1000 | 992 | true | Pop+Ack | ✅ | 1722 | 380 | 1257 | 2106 | 0.101 | 91.941 |
| Nesso (In-Process) | 16 | 1000 | 992 | true | Push | ✅ | 3372 | 174 | 3141 | 3615 | 4.262 | 23.227 |
| Nesso (In-Process) | 16 | 1000 | 992 | true | Pop+Ack | ✅ | 1079 | 411 | 768 | 1679 | 15.404 | 36.169 |
| Nesso (HTTP) | 16 | 1000 | 992 | true | Push | ✅ | 2591 | 829 | 1512 | 3579 | 4.895 | 19.049 |
| Nesso (HTTP) | 16 | 1000 | 992 | true | Pop+Ack | ✅ | 984 | 348 | 669 | 1557 | 13.650 | 43.545 |

## Per-Run Detail (Raw Verifiable Data)

| System | Threads | Sync | Op | Run | Dispatched | OnDisk | Integrity | Throughput | p50 | p99 |
|--------|---------|------|----|-----|------------|--------|-----------|------------|-----|-----|
| SQLite (In-Process) | 1 | false | Push | 1 | 10000 | 10000 | ✅ | 57237 | 0.012 | 0.037 |
| SQLite (In-Process) | 1 | false | Push | 2 | 10000 | 10000 | ✅ | 60551 | 0.012 | 0.030 |
| SQLite (In-Process) | 1 | false | Push | 3 | 10000 | 10000 | ✅ | 59831 | 0.013 | 0.030 |
| SQLite (In-Process) | 1 | false | Push | 4 | 10000 | 10000 | ✅ | 59799 | 0.013 | 0.030 |
| SQLite (In-Process) | 1 | false | Push | 5 | 10000 | 10000 | ✅ | 54996 | 0.013 | 0.058 |
| SQLite (In-Process) | 1 | false | Pop+Ack | 1 | 10000 | 0 | ✅ | 65535 | 0.012 | 0.020 |
| SQLite (In-Process) | 1 | false | Pop+Ack | 2 | 10000 | 0 | ✅ | 66110 | 0.012 | 0.021 |
| SQLite (In-Process) | 1 | false | Pop+Ack | 3 | 10000 | 0 | ✅ | 65421 | 0.012 | 0.020 |
| SQLite (In-Process) | 1 | false | Pop+Ack | 4 | 10000 | 0 | ✅ | 65723 | 0.012 | 0.020 |
| SQLite (In-Process) | 1 | false | Pop+Ack | 5 | 10000 | 0 | ✅ | 62488 | 0.013 | 0.026 |
| Nesso (In-Process) | 1 | false | Push | 1 | 10000 | 10000 | ✅ | 926212 | 0.001 | 0.001 |
| Nesso (In-Process) | 1 | false | Push | 2 | 10000 | 10000 | ✅ | 952952 | 0.000 | 0.001 |
| Nesso (In-Process) | 1 | false | Push | 3 | 10000 | 10000 | ✅ | 913461 | 0.001 | 0.001 |
| Nesso (In-Process) | 1 | false | Push | 4 | 10000 | 10000 | ✅ | 939386 | 0.001 | 0.001 |
| Nesso (In-Process) | 1 | false | Push | 5 | 10000 | 10000 | ✅ | 912450 | 0.001 | 0.001 |
| Nesso (In-Process) | 1 | false | Pop+Ack | 1 | 10000 | 0 | ✅ | 338292 | 0.002 | 0.004 |
| Nesso (In-Process) | 1 | false | Pop+Ack | 2 | 10000 | 0 | ✅ | 334714 | 0.002 | 0.004 |
| Nesso (In-Process) | 1 | false | Pop+Ack | 3 | 10000 | 0 | ✅ | 334179 | 0.002 | 0.004 |
| Nesso (In-Process) | 1 | false | Pop+Ack | 4 | 10000 | 0 | ✅ | 334193 | 0.002 | 0.004 |
| Nesso (In-Process) | 1 | false | Pop+Ack | 5 | 10000 | 0 | ✅ | 320651 | 0.003 | 0.004 |
| Nesso (HTTP) | 1 | false | Push | 1 | 10000 | 10000 | ✅ | 22889 | 0.042 | 0.071 |
| Nesso (HTTP) | 1 | false | Push | 2 | 10000 | 10000 | ✅ | 22453 | 0.042 | 0.083 |
| Nesso (HTTP) | 1 | false | Push | 3 | 10000 | 10000 | ✅ | 23356 | 0.041 | 0.070 |
| Nesso (HTTP) | 1 | false | Push | 4 | 10000 | 10000 | ✅ | 23126 | 0.042 | 0.070 |
| Nesso (HTTP) | 1 | false | Push | 5 | 10000 | 10000 | ✅ | 22890 | 0.042 | 0.069 |
| Nesso (HTTP) | 1 | false | Pop+Ack | 1 | 10000 | 0 | ✅ | 11589 | 0.085 | 0.111 |
| Nesso (HTTP) | 1 | false | Pop+Ack | 2 | 10000 | 0 | ✅ | 11511 | 0.085 | 0.127 |
| Nesso (HTTP) | 1 | false | Pop+Ack | 3 | 10000 | 0 | ✅ | 11593 | 0.085 | 0.107 |
| Nesso (HTTP) | 1 | false | Pop+Ack | 4 | 10000 | 0 | ✅ | 11424 | 0.085 | 0.125 |
| Nesso (HTTP) | 1 | false | Pop+Ack | 5 | 10000 | 0 | ✅ | 11579 | 0.085 | 0.111 |
| SQLite (In-Process) | 4 | false | Push | 1 | 10000 | 10000 | ✅ | 50857 | 0.013 | 0.032 |
| SQLite (In-Process) | 4 | false | Push | 2 | 10000 | 10000 | ✅ | 50766 | 0.013 | 0.049 |
| SQLite (In-Process) | 4 | false | Push | 3 | 10000 | 10000 | ✅ | 54929 | 0.012 | 0.033 |
| SQLite (In-Process) | 4 | false | Push | 4 | 10000 | 10000 | ✅ | 52570 | 0.013 | 0.062 |
| SQLite (In-Process) | 4 | false | Push | 5 | 10000 | 10000 | ✅ | 50424 | 0.012 | 0.037 |
| SQLite (In-Process) | 4 | false | Pop+Ack | 1 | 10000 | 0 | ✅ | 57573 | 0.012 | 0.023 |
| SQLite (In-Process) | 4 | false | Pop+Ack | 2 | 10000 | 0 | ✅ | 52054 | 0.012 | 0.027 |
| SQLite (In-Process) | 4 | false | Pop+Ack | 3 | 10000 | 0 | ✅ | 60544 | 0.012 | 0.020 |
| SQLite (In-Process) | 4 | false | Pop+Ack | 4 | 10000 | 0 | ✅ | 48371 | 0.012 | 0.048 |
| SQLite (In-Process) | 4 | false | Pop+Ack | 5 | 10000 | 0 | ✅ | 62626 | 0.012 | 0.024 |
| Nesso (In-Process) | 4 | false | Push | 1 | 10000 | 10000 | ✅ | 366358 | 0.002 | 0.095 |
| Nesso (In-Process) | 4 | false | Push | 2 | 10000 | 10000 | ✅ | 367375 | 0.002 | 0.100 |
| Nesso (In-Process) | 4 | false | Push | 3 | 10000 | 10000 | ✅ | 366414 | 0.002 | 0.103 |
| Nesso (In-Process) | 4 | false | Push | 4 | 10000 | 10000 | ✅ | 369246 | 0.002 | 0.098 |
| Nesso (In-Process) | 4 | false | Push | 5 | 10000 | 10000 | ✅ | 367534 | 0.002 | 0.104 |
| Nesso (In-Process) | 4 | false | Pop+Ack | 1 | 10000 | 0 | ✅ | 111669 | 0.013 | 0.166 |
| Nesso (In-Process) | 4 | false | Pop+Ack | 2 | 10000 | 0 | ✅ | 133299 | 0.014 | 0.155 |
| Nesso (In-Process) | 4 | false | Pop+Ack | 3 | 10000 | 0 | ✅ | 120808 | 0.013 | 0.177 |
| Nesso (In-Process) | 4 | false | Pop+Ack | 4 | 10000 | 0 | ✅ | 135013 | 0.014 | 0.155 |
| Nesso (In-Process) | 4 | false | Pop+Ack | 5 | 10000 | 0 | ✅ | 123253 | 0.012 | 0.178 |
| Nesso (HTTP) | 4 | false | Push | 1 | 10000 | 10000 | ✅ | 57413 | 0.065 | 0.128 |
| Nesso (HTTP) | 4 | false | Push | 2 | 10000 | 10000 | ✅ | 60590 | 0.062 | 0.123 |
| Nesso (HTTP) | 4 | false | Push | 3 | 10000 | 10000 | ✅ | 60921 | 0.062 | 0.125 |
| Nesso (HTTP) | 4 | false | Push | 4 | 10000 | 10000 | ✅ | 57421 | 0.065 | 0.132 |
| Nesso (HTTP) | 4 | false | Push | 5 | 10000 | 10000 | ✅ | 61271 | 0.062 | 0.120 |
| Nesso (HTTP) | 4 | false | Pop+Ack | 1 | 10000 | 0 | ✅ | 29044 | 0.130 | 0.270 |
| Nesso (HTTP) | 4 | false | Pop+Ack | 2 | 10000 | 0 | ✅ | 30090 | 0.128 | 0.220 |
| Nesso (HTTP) | 4 | false | Pop+Ack | 3 | 10000 | 0 | ✅ | 27900 | 0.132 | 0.280 |
| Nesso (HTTP) | 4 | false | Pop+Ack | 4 | 10000 | 0 | ✅ | 29786 | 0.129 | 0.230 |
| Nesso (HTTP) | 4 | false | Pop+Ack | 5 | 10000 | 0 | ✅ | 28647 | 0.132 | 0.255 |
| SQLite (In-Process) | 16 | false | Push | 1 | 10000 | 10000 | ✅ | 25737 | 0.013 | 0.186 |
| SQLite (In-Process) | 16 | false | Push | 2 | 10000 | 10000 | ✅ | 14135 | 0.014 | 0.092 |
| SQLite (In-Process) | 16 | false | Push | 3 | 10000 | 10000 | ✅ | 19786 | 0.014 | 0.244 |
| SQLite (In-Process) | 16 | false | Push | 4 | 10000 | 10000 | ✅ | 12447 | 0.017 | 0.075 |
| SQLite (In-Process) | 16 | false | Push | 5 | 10000 | 10000 | ✅ | 16813 | 0.014 | 0.206 |
| SQLite (In-Process) | 16 | false | Pop+Ack | 1 | 10000 | 0 | ✅ | 20683 | 0.014 | 1.015 |
| SQLite (In-Process) | 16 | false | Pop+Ack | 2 | 10000 | 0 | ✅ | 14303 | 0.016 | 0.086 |
| SQLite (In-Process) | 16 | false | Pop+Ack | 3 | 10000 | 0 | ✅ | 38668 | 0.013 | 0.102 |
| SQLite (In-Process) | 16 | false | Pop+Ack | 4 | 10000 | 0 | ✅ | 16913 | 0.016 | 0.061 |
| SQLite (In-Process) | 16 | false | Pop+Ack | 5 | 10000 | 0 | ✅ | 26420 | 0.012 | 0.656 |
| Nesso (In-Process) | 16 | false | Push | 1 | 10000 | 10000 | ✅ | 283461 | 0.003 | 0.633 |
| Nesso (In-Process) | 16 | false | Push | 2 | 10000 | 10000 | ✅ | 232607 | 0.003 | 0.773 |
| Nesso (In-Process) | 16 | false | Push | 3 | 10000 | 10000 | ✅ | 342343 | 0.002 | 0.571 |
| Nesso (In-Process) | 16 | false | Push | 4 | 10000 | 10000 | ✅ | 247095 | 0.003 | 0.685 |
| Nesso (In-Process) | 16 | false | Push | 5 | 10000 | 10000 | ✅ | 281306 | 0.003 | 0.628 |
| Nesso (In-Process) | 16 | false | Pop+Ack | 1 | 10000 | 0 | ✅ | 132332 | 0.057 | 0.744 |
| Nesso (In-Process) | 16 | false | Pop+Ack | 2 | 10000 | 0 | ✅ | 131189 | 0.058 | 0.748 |
| Nesso (In-Process) | 16 | false | Pop+Ack | 3 | 10000 | 0 | ✅ | 132513 | 0.058 | 0.707 |
| Nesso (In-Process) | 16 | false | Pop+Ack | 4 | 10000 | 0 | ✅ | 132327 | 0.057 | 0.741 |
| Nesso (In-Process) | 16 | false | Pop+Ack | 5 | 10000 | 0 | ✅ | 133145 | 0.059 | 0.710 |
| Nesso (HTTP) | 16 | false | Push | 1 | 10000 | 10000 | ✅ | 106733 | 0.133 | 0.368 |
| Nesso (HTTP) | 16 | false | Push | 2 | 10000 | 10000 | ✅ | 107594 | 0.137 | 0.339 |
| Nesso (HTTP) | 16 | false | Push | 3 | 10000 | 10000 | ✅ | 108422 | 0.131 | 0.377 |
| Nesso (HTTP) | 16 | false | Push | 4 | 10000 | 10000 | ✅ | 107709 | 0.132 | 0.368 |
| Nesso (HTTP) | 16 | false | Push | 5 | 10000 | 10000 | ✅ | 106392 | 0.133 | 0.374 |
| Nesso (HTTP) | 16 | false | Pop+Ack | 1 | 10000 | 0 | ✅ | 50530 | 0.300 | 0.585 |
| Nesso (HTTP) | 16 | false | Pop+Ack | 2 | 10000 | 0 | ✅ | 51199 | 0.301 | 0.506 |
| Nesso (HTTP) | 16 | false | Pop+Ack | 3 | 10000 | 0 | ✅ | 50021 | 0.305 | 0.578 |
| Nesso (HTTP) | 16 | false | Pop+Ack | 4 | 10000 | 0 | ✅ | 50792 | 0.304 | 0.504 |
| Nesso (HTTP) | 16 | false | Pop+Ack | 5 | 10000 | 0 | ✅ | 49405 | 0.297 | 0.675 |
| SQLite (In-Process) | 1 | true | Push | 1 | 1000 | 1000 | ✅ | 12097 | 0.059 | 0.278 |
| SQLite (In-Process) | 1 | true | Push | 2 | 1000 | 1000 | ✅ | 12879 | 0.064 | 0.206 |
| SQLite (In-Process) | 1 | true | Push | 3 | 1000 | 1000 | ✅ | 12120 | 0.065 | 0.179 |
| SQLite (In-Process) | 1 | true | Push | 4 | 1000 | 1000 | ✅ | 13150 | 0.063 | 0.191 |
| SQLite (In-Process) | 1 | true | Push | 5 | 1000 | 1000 | ✅ | 12179 | 0.063 | 0.268 |
| SQLite (In-Process) | 1 | true | Pop+Ack | 1 | 1000 | 0 | ✅ | 15609 | 0.058 | 0.091 |
| SQLite (In-Process) | 1 | true | Pop+Ack | 2 | 1000 | 0 | ✅ | 15238 | 0.058 | 0.147 |
| SQLite (In-Process) | 1 | true | Pop+Ack | 3 | 1000 | 0 | ✅ | 15806 | 0.056 | 0.094 |
| SQLite (In-Process) | 1 | true | Pop+Ack | 4 | 1000 | 0 | ✅ | 16304 | 0.055 | 0.089 |
| SQLite (In-Process) | 1 | true | Pop+Ack | 5 | 1000 | 0 | ✅ | 14993 | 0.058 | 0.147 |
| Nesso (In-Process) | 1 | true | Push | 1 | 1000 | 1000 | ✅ | 248 | 3.999 | 5.811 |
| Nesso (In-Process) | 1 | true | Push | 2 | 1000 | 1000 | ✅ | 248 | 3.999 | 4.675 |
| Nesso (In-Process) | 1 | true | Push | 3 | 1000 | 1000 | ✅ | 246 | 3.999 | 4.987 |
| Nesso (In-Process) | 1 | true | Push | 4 | 1000 | 1000 | ✅ | 249 | 4.003 | 4.883 |
| Nesso (In-Process) | 1 | true | Push | 5 | 1000 | 1000 | ✅ | 245 | 4.001 | 6.551 |
| Nesso (In-Process) | 1 | true | Pop+Ack | 1 | 1000 | 0 | ✅ | 125 | 8.003 | 8.655 |
| Nesso (In-Process) | 1 | true | Pop+Ack | 2 | 1000 | 0 | ✅ | 124 | 8.003 | 10.095 |
| Nesso (In-Process) | 1 | true | Pop+Ack | 3 | 1000 | 0 | ✅ | 124 | 7.999 | 9.023 |
| Nesso (In-Process) | 1 | true | Pop+Ack | 4 | 1000 | 0 | ✅ | 124 | 8.003 | 9.463 |
| Nesso (In-Process) | 1 | true | Pop+Ack | 5 | 1000 | 0 | ✅ | 124 | 8.003 | 9.007 |
| Nesso (HTTP) | 1 | true | Push | 1 | 1000 | 1000 | ✅ | 244 | 4.011 | 5.047 |
| Nesso (HTTP) | 1 | true | Push | 2 | 1000 | 1000 | ✅ | 247 | 4.003 | 5.039 |
| Nesso (HTTP) | 1 | true | Push | 3 | 1000 | 1000 | ✅ | 245 | 4.003 | 6.027 |
| Nesso (HTTP) | 1 | true | Push | 4 | 1000 | 1000 | ✅ | 246 | 4.007 | 5.027 |
| Nesso (HTTP) | 1 | true | Push | 5 | 1000 | 1000 | ✅ | 248 | 3.999 | 4.991 |
| Nesso (HTTP) | 1 | true | Pop+Ack | 1 | 1000 | 0 | ✅ | 122 | 8.019 | 9.831 |
| Nesso (HTTP) | 1 | true | Pop+Ack | 2 | 1000 | 0 | ✅ | 123 | 8.011 | 9.231 |
| Nesso (HTTP) | 1 | true | Pop+Ack | 3 | 1000 | 0 | ✅ | 124 | 8.007 | 9.791 |
| Nesso (HTTP) | 1 | true | Pop+Ack | 4 | 1000 | 0 | ✅ | 123 | 8.007 | 10.031 |
| Nesso (HTTP) | 1 | true | Pop+Ack | 5 | 1000 | 0 | ✅ | 123 | 8.007 | 10.007 |
| SQLite (In-Process) | 4 | true | Push | 1 | 1000 | 1000 | ✅ | 7303 | 0.071 | 0.602 |
| SQLite (In-Process) | 4 | true | Push | 2 | 1000 | 1000 | ✅ | 9652 | 0.066 | 0.602 |
| SQLite (In-Process) | 4 | true | Push | 3 | 1000 | 1000 | ✅ | 11962 | 0.059 | 0.191 |
| SQLite (In-Process) | 4 | true | Push | 4 | 1000 | 1000 | ✅ | 7105 | 0.075 | 1.417 |
| SQLite (In-Process) | 4 | true | Push | 5 | 1000 | 1000 | ✅ | 8660 | 0.071 | 0.206 |
| SQLite (In-Process) | 4 | true | Pop+Ack | 1 | 1000 | 0 | ✅ | 12084 | 0.061 | 0.087 |
| SQLite (In-Process) | 4 | true | Pop+Ack | 2 | 1000 | 0 | ✅ | 11792 | 0.057 | 0.095 |
| SQLite (In-Process) | 4 | true | Pop+Ack | 3 | 1000 | 0 | ✅ | 12059 | 0.058 | 0.849 |
| SQLite (In-Process) | 4 | true | Pop+Ack | 4 | 1000 | 0 | ✅ | 12682 | 0.058 | 0.161 |
| SQLite (In-Process) | 4 | true | Pop+Ack | 5 | 1000 | 0 | ✅ | 11842 | 0.058 | 0.076 |
| Nesso (In-Process) | 4 | true | Push | 1 | 1000 | 1000 | ✅ | 969 | 4.001 | 4.979 |
| Nesso (In-Process) | 4 | true | Push | 2 | 1000 | 1000 | ✅ | 977 | 3.999 | 4.987 |
| Nesso (In-Process) | 4 | true | Push | 3 | 1000 | 1000 | ✅ | 972 | 3.999 | 5.927 |
| Nesso (In-Process) | 4 | true | Push | 4 | 1000 | 1000 | ✅ | 962 | 4.005 | 4.875 |
| Nesso (In-Process) | 4 | true | Push | 5 | 1000 | 1000 | ✅ | 970 | 4.001 | 4.967 |
| Nesso (In-Process) | 4 | true | Pop+Ack | 1 | 1000 | 0 | ✅ | 496 | 8.003 | 9.039 |
| Nesso (In-Process) | 4 | true | Pop+Ack | 2 | 1000 | 0 | ✅ | 495 | 7.999 | 12.367 |
| Nesso (In-Process) | 4 | true | Pop+Ack | 3 | 1000 | 0 | ✅ | 497 | 8.003 | 8.999 |
| Nesso (In-Process) | 4 | true | Pop+Ack | 4 | 1000 | 0 | ✅ | 495 | 8.003 | 8.991 |
| Nesso (In-Process) | 4 | true | Pop+Ack | 5 | 1000 | 0 | ✅ | 499 | 7.999 | 8.967 |
| Nesso (HTTP) | 4 | true | Push | 1 | 1000 | 1000 | ✅ | 958 | 4.031 | 5.071 |
| Nesso (HTTP) | 4 | true | Push | 2 | 1000 | 1000 | ✅ | 962 | 4.027 | 5.031 |
| Nesso (HTTP) | 4 | true | Push | 3 | 1000 | 1000 | ✅ | 949 | 4.031 | 5.095 |
| Nesso (HTTP) | 4 | true | Push | 4 | 1000 | 1000 | ✅ | 952 | 4.029 | 5.155 |
| Nesso (HTTP) | 4 | true | Push | 5 | 1000 | 1000 | ✅ | 932 | 4.065 | 5.183 |
| Nesso (HTTP) | 4 | true | Pop+Ack | 1 | 1000 | 0 | ✅ | 468 | 8.091 | 10.071 |
| Nesso (HTTP) | 4 | true | Pop+Ack | 2 | 1000 | 0 | ✅ | 477 | 8.043 | 10.063 |
| Nesso (HTTP) | 4 | true | Pop+Ack | 3 | 1000 | 0 | ✅ | 469 | 8.067 | 10.111 |
| Nesso (HTTP) | 4 | true | Pop+Ack | 4 | 1000 | 0 | ✅ | 459 | 8.095 | 12.847 |
| Nesso (HTTP) | 4 | true | Pop+Ack | 5 | 1000 | 0 | ✅ | 474 | 8.047 | 10.103 |
| SQLite (In-Process) | 16 | true | Push | 1 | 992 | 992 | ✅ | 2050 | 0.093 | 90.495 |
| SQLite (In-Process) | 16 | true | Push | 2 | 992 | 992 | ✅ | 2088 | 0.094 | 63.935 |
| SQLite (In-Process) | 16 | true | Push | 3 | 992 | 992 | ✅ | 1248 | 0.103 | 89.983 |
| SQLite (In-Process) | 16 | true | Push | 4 | 992 | 992 | ✅ | 3696 | 0.085 | 65.791 |
| SQLite (In-Process) | 16 | true | Push | 5 | 992 | 992 | ✅ | 1675 | 0.074 | 66.431 |
| SQLite (In-Process) | 16 | true | Pop+Ack | 1 | 992 | 0 | ✅ | 2106 | 0.093 | 94.655 |
| SQLite (In-Process) | 16 | true | Pop+Ack | 2 | 992 | 0 | ✅ | 1448 | 0.107 | 88.895 |
| SQLite (In-Process) | 16 | true | Pop+Ack | 3 | 992 | 0 | ✅ | 2094 | 0.098 | 91.135 |
| SQLite (In-Process) | 16 | true | Pop+Ack | 4 | 992 | 0 | ✅ | 1257 | 0.106 | 120.895 |
| SQLite (In-Process) | 16 | true | Pop+Ack | 5 | 992 | 0 | ✅ | 1707 | 0.100 | 64.127 |
| Nesso (In-Process) | 16 | true | Push | 1 | 992 | 992 | ✅ | 3296 | 4.067 | 32.927 |
| Nesso (In-Process) | 16 | true | Push | 2 | 992 | 992 | ✅ | 3615 | 4.147 | 5.195 |
| Nesso (In-Process) | 16 | true | Push | 3 | 992 | 992 | ✅ | 3141 | 4.947 | 29.375 |
| Nesso (In-Process) | 16 | true | Push | 4 | 992 | 992 | ✅ | 3390 | 4.059 | 25.855 |
| Nesso (In-Process) | 16 | true | Push | 5 | 992 | 992 | ✅ | 3420 | 4.091 | 22.783 |
| Nesso (In-Process) | 16 | true | Pop+Ack | 1 | 992 | 0 | ✅ | 768 | 20.015 | 39.839 |
| Nesso (In-Process) | 16 | true | Pop+Ack | 2 | 992 | 0 | ✅ | 1339 | 9.031 | 35.999 |
| Nesso (In-Process) | 16 | true | Pop+Ack | 3 | 992 | 0 | ✅ | 1679 | 8.071 | 25.903 |
| Nesso (In-Process) | 16 | true | Pop+Ack | 4 | 992 | 0 | ✅ | 839 | 19.903 | 38.911 |
| Nesso (In-Process) | 16 | true | Pop+Ack | 5 | 992 | 0 | ✅ | 770 | 19.999 | 40.191 |
| Nesso (HTTP) | 16 | true | Push | 1 | 992 | 992 | ✅ | 1512 | 5.999 | 31.855 |
| Nesso (HTTP) | 16 | true | Push | 2 | 992 | 992 | ✅ | 1984 | 4.259 | 31.967 |
| Nesso (HTTP) | 16 | true | Push | 3 | 992 | 992 | ✅ | 2934 | 5.047 | 12.927 |
| Nesso (HTTP) | 16 | true | Push | 4 | 992 | 992 | ✅ | 2944 | 5.079 | 11.719 |
| Nesso (HTTP) | 16 | true | Push | 5 | 992 | 992 | ✅ | 3579 | 4.089 | 6.779 |
| Nesso (HTTP) | 16 | true | Pop+Ack | 1 | 992 | 0 | ✅ | 857 | 12.071 | 47.967 |
| Nesso (HTTP) | 16 | true | Pop+Ack | 2 | 992 | 0 | ✅ | 1047 | 9.927 | 47.935 |
| Nesso (HTTP) | 16 | true | Pop+Ack | 3 | 992 | 0 | ✅ | 669 | 23.983 | 52.031 |
| Nesso (HTTP) | 16 | true | Pop+Ack | 4 | 992 | 0 | ✅ | 790 | 12.151 | 52.159 |
| Nesso (HTTP) | 16 | true | Pop+Ack | 5 | 992 | 0 | ✅ | 1557 | 10.119 | 17.631 |

## Analysis & Interpretation

**Apples-to-Apples Comparison (In-Process vs In-Process)**: Nesso In-Process vs SQLite In-Process is the primary methodologically sound comparison for evaluating storage engine performance. Nesso HTTP vs SQLite In-Process measures two distinct architectures (storage engine + asynchronous HTTP/TCP stack vs an embedded in-memory/file library) and should be interpreted accordingly.

**Multi-Threaded Scalability**: SQLite WAL allows only a single active writer at any given time. Under 16 concurrent threads, writers heavily compete for the database file lock (managed via `busy_timeout`). In contrast, Nesso serializes appends via an in-RAM mutex into an append-only WAL without filesystem-level lock contention.

**Durability Overhead (`sync=true`)**: Operations with individual fsync are strictly bounded by physical SSD capabilities. SQLite with `synchronous=FULL` shows higher throughput because WAL mode flushes the write-ahead log rather than individual b-tree database pages — an architectural design difference rather than an inherent engine speed difference. **Caveat on macOS**: Standard POSIX `fsync()` on macOS flushes to drive cache rather than guaranteeing a platter/NAND barrier without `fcntl(F_FULLFSYNC)`. See BENCHMARK.md for deep analysis.
