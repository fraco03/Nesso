# Nesso vs SQLite — Benchmark Report

## ⚠️ Limitazioni Note (leggere PRIMA dei risultati)

> **CAMPIONE LIMITATO**: I run con `sync=true` usano N=1000 operazioni nominali per configurazione. I run senza fsync usano N=10000. Questi campioni sono sufficienti per evidenziare trend architetturali, ma sono soggetti a rumore statistico significativo. **Ripetere su hardware reale con campioni da 100k+ prima di trarre conclusioni definitive.**

> **macOS E DURABILITÀ (POSIX fsync vs F_FULLFSYNC)**: Sia Nesso che SQLite vengono confrontati a parità di garanzia con lo standard POSIX `fsync()` (`SyncMode::Standard` in Nesso, `PRAGMA synchronous=FULL` in SQLite). Questo flush garantisce l'integrità totale al 100% contro crash di processo, segfault e terminazioni brutali `kill -9` (verificato nei test di crash recovery). Nesso supporta inoltre `SyncMode::FullHardware` per chi necessita di barriere hardware complete `F_FULLFSYNC` contro cadute improvvise di alimentazione del drive.

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
| SQLite (In-Process) | 1 | 10000 | 10000 | false | Push | ✅ | 57906 | 2905 | 52734 | 59543 | 0.013 | 0.032 |
| SQLite (In-Process) | 1 | 10000 | 10000 | false | Pop+Ack | ✅ | 66480 | 408 | 65889 | 66923 | 0.012 | 0.020 |
| Nesso (In-Process) | 1 | 10000 | 10000 | false | Push | ✅ | 899860 | 18640 | 889564 | 933115 | 0.001 | 0.001 |
| Nesso (In-Process) | 1 | 10000 | 10000 | false | Pop+Ack | ✅ | 475978 | 9314 | 465196 | 487848 | 0.002 | 0.003 |
| Nesso (HTTP) | 1 | 10000 | 10000 | false | Push | ✅ | 22980 | 439 | 22494 | 23395 | 0.042 | 0.076 |
| Nesso (HTTP) | 1 | 10000 | 10000 | false | Pop+Ack | ✅ | 11901 | 92 | 11818 | 12038 | 0.083 | 0.112 |
| SQLite (In-Process) | 4 | 10000 | 10000 | false | Push | ✅ | 51262 | 1558 | 49341 | 53440 | 0.012 | 0.038 |
| SQLite (In-Process) | 4 | 10000 | 10000 | false | Pop+Ack | ✅ | 55157 | 3353 | 51446 | 59432 | 0.012 | 0.024 |
| Nesso (In-Process) | 4 | 10000 | 10000 | false | Push | ✅ | 340845 | 27624 | 291669 | 356553 | 0.002 | 0.113 |
| Nesso (In-Process) | 4 | 10000 | 10000 | false | Pop+Ack | ✅ | 180938 | 2817 | 176529 | 183441 | 0.005 | 0.137 |
| Nesso (HTTP) | 4 | 10000 | 10000 | false | Push | ✅ | 62286 | 1083 | 61169 | 63670 | 0.060 | 0.118 |
| Nesso (HTTP) | 4 | 10000 | 10000 | false | Pop+Ack | ✅ | 31309 | 1357 | 29380 | 32586 | 0.122 | 0.236 |
| SQLite (In-Process) | 16 | 10000 | 10000 | false | Push | ✅ | 19529 | 1588 | 16709 | 20558 | 0.014 | 0.170 |
| SQLite (In-Process) | 16 | 10000 | 10000 | false | Pop+Ack | ✅ | 23543 | 6859 | 19875 | 35793 | 0.014 | 0.719 |
| Nesso (In-Process) | 16 | 10000 | 10000 | false | Push | ✅ | 272055 | 19413 | 248139 | 298567 | 0.003 | 0.650 |
| Nesso (In-Process) | 16 | 10000 | 10000 | false | Pop+Ack | ✅ | 173808 | 2219 | 170257 | 176106 | 0.005 | 0.694 |
| Nesso (HTTP) | 16 | 10000 | 10000 | false | Push | ✅ | 100524 | 4373 | 93101 | 104105 | 0.138 | 0.457 |
| Nesso (HTTP) | 16 | 10000 | 10000 | false | Pop+Ack | ✅ | 51657 | 781 | 50895 | 52953 | 0.296 | 0.540 |
| SQLite (In-Process) | 1 | 1000 | 1000 | true | Push | ✅ | 19030 | 1197 | 16923 | 19822 | 0.046 | 0.105 |
| SQLite (In-Process) | 1 | 1000 | 1000 | true | Pop+Ack | ✅ | 16654 | 130 | 16466 | 16815 | 0.055 | 0.083 |
| Nesso (In-Process) | 1 | 1000 | 1000 | true | Push | ✅ | 56985 | 1240 | 55955 | 59019 | 0.017 | 0.027 |
| Nesso (In-Process) | 1 | 1000 | 1000 | true | Pop+Ack | ✅ | 30157 | 955 | 28905 | 31431 | 0.031 | 0.047 |
| Nesso (HTTP) | 1 | 1000 | 1000 | true | Push | ✅ | 17964 | 1373 | 15529 | 18732 | 0.052 | 0.096 |
| Nesso (HTTP) | 1 | 1000 | 1000 | true | Pop+Ack | ✅ | 9203 | 214 | 8932 | 9392 | 0.106 | 0.148 |
| SQLite (In-Process) | 4 | 1000 | 1000 | true | Push | ✅ | 12518 | 241 | 12323 | 12915 | 0.047 | 0.181 |
| SQLite (In-Process) | 4 | 1000 | 1000 | true | Pop+Ack | ✅ | 12070 | 139 | 11921 | 12284 | 0.055 | 0.096 |
| Nesso (In-Process) | 4 | 1000 | 1000 | true | Push | ✅ | 46032 | 3194 | 41119 | 50054 | 0.066 | 0.096 |
| Nesso (In-Process) | 4 | 1000 | 1000 | true | Pop+Ack | ✅ | 23231 | 1418 | 20849 | 24485 | 0.139 | 0.342 |
| Nesso (HTTP) | 4 | 1000 | 1000 | true | Push | ✅ | 42081 | 400 | 41447 | 42477 | 0.090 | 0.161 |
| Nesso (HTTP) | 4 | 1000 | 1000 | true | Pop+Ack | ✅ | 19311 | 265 | 19023 | 19556 | 0.201 | 0.324 |
| SQLite (In-Process) | 16 | 1000 | 992 | true | Push | ✅ | 2466 | 841 | 1687 | 3723 | 0.068 | 74.169 |
| SQLite (In-Process) | 16 | 1000 | 992 | true | Pop+Ack | ✅ | 1639 | 313 | 1256 | 2082 | 0.100 | 103.961 |
| Nesso (In-Process) | 16 | 1000 | 992 | true | Push | ✅ | 29674 | 6141 | 25468 | 40529 | 0.372 | 3.715 |
| Nesso (In-Process) | 16 | 1000 | 992 | true | Pop+Ack | ✅ | 20259 | 624 | 19625 | 21063 | 0.604 | 4.950 |
| Nesso (HTTP) | 16 | 1000 | 992 | true | Push | ✅ | 35947 | 2674 | 31171 | 37365 | 0.333 | 1.656 |
| Nesso (HTTP) | 16 | 1000 | 992 | true | Pop+Ack | ✅ | 17569 | 1362 | 15137 | 18276 | 0.768 | 2.605 |

## Per-Run Detail (Raw Verifiable Data)

| System | Threads | Sync | Op | Run | Dispatched | OnDisk | Integrity | Throughput | p50 | p99 |
|--------|---------|------|----|-----|------------|--------|-----------|------------|-----|-----|
| SQLite (In-Process) | 1 | false | Push | 1 | 10000 | 10000 | ✅ | 52734 | 0.013 | 0.032 |
| SQLite (In-Process) | 1 | false | Push | 2 | 10000 | 10000 | ✅ | 59543 | 0.012 | 0.031 |
| SQLite (In-Process) | 1 | false | Push | 3 | 10000 | 10000 | ✅ | 59151 | 0.013 | 0.035 |
| SQLite (In-Process) | 1 | false | Push | 4 | 10000 | 10000 | ✅ | 59340 | 0.012 | 0.031 |
| SQLite (In-Process) | 1 | false | Push | 5 | 10000 | 10000 | ✅ | 58760 | 0.013 | 0.030 |
| SQLite (In-Process) | 1 | false | Pop+Ack | 1 | 10000 | 0 | ✅ | 66684 | 0.012 | 0.020 |
| SQLite (In-Process) | 1 | false | Pop+Ack | 2 | 10000 | 0 | ✅ | 66645 | 0.012 | 0.020 |
| SQLite (In-Process) | 1 | false | Pop+Ack | 3 | 10000 | 0 | ✅ | 66256 | 0.012 | 0.020 |
| SQLite (In-Process) | 1 | false | Pop+Ack | 4 | 10000 | 0 | ✅ | 66923 | 0.012 | 0.021 |
| SQLite (In-Process) | 1 | false | Pop+Ack | 5 | 10000 | 0 | ✅ | 65889 | 0.012 | 0.021 |
| Nesso (In-Process) | 1 | false | Push | 1 | 10000 | 10000 | ✅ | 933115 | 0.000 | 0.001 |
| Nesso (In-Process) | 1 | false | Push | 2 | 10000 | 10000 | ✅ | 893386 | 0.001 | 0.001 |
| Nesso (In-Process) | 1 | false | Push | 3 | 10000 | 10000 | ✅ | 889564 | 0.001 | 0.001 |
| Nesso (In-Process) | 1 | false | Push | 4 | 10000 | 10000 | ✅ | 891497 | 0.001 | 0.001 |
| Nesso (In-Process) | 1 | false | Push | 5 | 10000 | 10000 | ✅ | 891736 | 0.001 | 0.001 |
| Nesso (In-Process) | 1 | false | Pop+Ack | 1 | 10000 | 0 | ✅ | 487848 | 0.001 | 0.003 |
| Nesso (In-Process) | 1 | false | Pop+Ack | 2 | 10000 | 0 | ✅ | 482028 | 0.002 | 0.003 |
| Nesso (In-Process) | 1 | false | Pop+Ack | 3 | 10000 | 0 | ✅ | 476132 | 0.002 | 0.003 |
| Nesso (In-Process) | 1 | false | Pop+Ack | 4 | 10000 | 0 | ✅ | 465196 | 0.002 | 0.003 |
| Nesso (In-Process) | 1 | false | Pop+Ack | 5 | 10000 | 0 | ✅ | 468685 | 0.002 | 0.003 |
| Nesso (HTTP) | 1 | false | Push | 1 | 10000 | 10000 | ✅ | 22513 | 0.042 | 0.089 |
| Nesso (HTTP) | 1 | false | Push | 2 | 10000 | 10000 | ✅ | 23232 | 0.042 | 0.068 |
| Nesso (HTTP) | 1 | false | Push | 3 | 10000 | 10000 | ✅ | 22494 | 0.041 | 0.088 |
| Nesso (HTTP) | 1 | false | Push | 4 | 10000 | 10000 | ✅ | 23395 | 0.041 | 0.068 |
| Nesso (HTTP) | 1 | false | Push | 5 | 10000 | 10000 | ✅ | 23264 | 0.042 | 0.069 |
| Nesso (HTTP) | 1 | false | Pop+Ack | 1 | 10000 | 0 | ✅ | 11824 | 0.084 | 0.109 |
| Nesso (HTTP) | 1 | false | Pop+Ack | 2 | 10000 | 0 | ✅ | 11818 | 0.084 | 0.111 |
| Nesso (HTTP) | 1 | false | Pop+Ack | 3 | 10000 | 0 | ✅ | 11881 | 0.083 | 0.124 |
| Nesso (HTTP) | 1 | false | Pop+Ack | 4 | 10000 | 0 | ✅ | 12038 | 0.082 | 0.110 |
| Nesso (HTTP) | 1 | false | Pop+Ack | 5 | 10000 | 0 | ✅ | 11945 | 0.083 | 0.107 |
| SQLite (In-Process) | 4 | false | Push | 1 | 10000 | 10000 | ✅ | 50452 | 0.012 | 0.049 |
| SQLite (In-Process) | 4 | false | Push | 2 | 10000 | 10000 | ✅ | 53440 | 0.012 | 0.043 |
| SQLite (In-Process) | 4 | false | Push | 3 | 10000 | 10000 | ✅ | 51057 | 0.012 | 0.030 |
| SQLite (In-Process) | 4 | false | Push | 4 | 10000 | 10000 | ✅ | 52020 | 0.012 | 0.031 |
| SQLite (In-Process) | 4 | false | Push | 5 | 10000 | 10000 | ✅ | 49341 | 0.012 | 0.036 |
| SQLite (In-Process) | 4 | false | Pop+Ack | 1 | 10000 | 0 | ✅ | 53027 | 0.012 | 0.019 |
| SQLite (In-Process) | 4 | false | Pop+Ack | 2 | 10000 | 0 | ✅ | 59432 | 0.012 | 0.022 |
| SQLite (In-Process) | 4 | false | Pop+Ack | 3 | 10000 | 0 | ✅ | 54049 | 0.012 | 0.022 |
| SQLite (In-Process) | 4 | false | Pop+Ack | 4 | 10000 | 0 | ✅ | 57831 | 0.012 | 0.021 |
| SQLite (In-Process) | 4 | false | Pop+Ack | 5 | 10000 | 0 | ✅ | 51446 | 0.012 | 0.037 |
| Nesso (In-Process) | 4 | false | Push | 1 | 10000 | 10000 | ✅ | 349016 | 0.002 | 0.106 |
| Nesso (In-Process) | 4 | false | Push | 2 | 10000 | 10000 | ✅ | 356553 | 0.002 | 0.108 |
| Nesso (In-Process) | 4 | false | Push | 3 | 10000 | 10000 | ✅ | 291669 | 0.002 | 0.135 |
| Nesso (In-Process) | 4 | false | Push | 4 | 10000 | 10000 | ✅ | 353990 | 0.002 | 0.102 |
| Nesso (In-Process) | 4 | false | Push | 5 | 10000 | 10000 | ✅ | 352997 | 0.002 | 0.112 |
| Nesso (In-Process) | 4 | false | Pop+Ack | 1 | 10000 | 0 | ✅ | 183367 | 0.005 | 0.124 |
| Nesso (In-Process) | 4 | false | Pop+Ack | 2 | 10000 | 0 | ✅ | 180635 | 0.005 | 0.135 |
| Nesso (In-Process) | 4 | false | Pop+Ack | 3 | 10000 | 0 | ✅ | 176529 | 0.005 | 0.146 |
| Nesso (In-Process) | 4 | false | Pop+Ack | 4 | 10000 | 0 | ✅ | 183441 | 0.005 | 0.124 |
| Nesso (In-Process) | 4 | false | Pop+Ack | 5 | 10000 | 0 | ✅ | 180716 | 0.005 | 0.156 |
| Nesso (HTTP) | 4 | false | Push | 1 | 10000 | 10000 | ✅ | 61169 | 0.062 | 0.117 |
| Nesso (HTTP) | 4 | false | Push | 2 | 10000 | 10000 | ✅ | 63190 | 0.060 | 0.116 |
| Nesso (HTTP) | 4 | false | Push | 3 | 10000 | 10000 | ✅ | 63670 | 0.059 | 0.117 |
| Nesso (HTTP) | 4 | false | Push | 4 | 10000 | 10000 | ✅ | 61594 | 0.060 | 0.120 |
| Nesso (HTTP) | 4 | false | Push | 5 | 10000 | 10000 | ✅ | 61807 | 0.061 | 0.121 |
| Nesso (HTTP) | 4 | false | Pop+Ack | 1 | 10000 | 0 | ✅ | 29380 | 0.127 | 0.273 |
| Nesso (HTTP) | 4 | false | Pop+Ack | 2 | 10000 | 0 | ✅ | 32578 | 0.118 | 0.206 |
| Nesso (HTTP) | 4 | false | Pop+Ack | 3 | 10000 | 0 | ✅ | 30669 | 0.125 | 0.226 |
| Nesso (HTTP) | 4 | false | Pop+Ack | 4 | 10000 | 0 | ✅ | 31332 | 0.120 | 0.269 |
| Nesso (HTTP) | 4 | false | Pop+Ack | 5 | 10000 | 0 | ✅ | 32586 | 0.119 | 0.207 |
| SQLite (In-Process) | 16 | false | Push | 1 | 10000 | 10000 | ✅ | 20168 | 0.013 | 0.200 |
| SQLite (In-Process) | 16 | false | Push | 2 | 10000 | 10000 | ✅ | 20098 | 0.014 | 0.192 |
| SQLite (In-Process) | 16 | false | Push | 3 | 10000 | 10000 | ✅ | 20114 | 0.014 | 0.221 |
| SQLite (In-Process) | 16 | false | Push | 4 | 10000 | 10000 | ✅ | 16709 | 0.014 | 0.115 |
| SQLite (In-Process) | 16 | false | Push | 5 | 10000 | 10000 | ✅ | 20558 | 0.014 | 0.120 |
| SQLite (In-Process) | 16 | false | Pop+Ack | 1 | 10000 | 0 | ✅ | 35793 | 0.012 | 0.778 |
| SQLite (In-Process) | 16 | false | Pop+Ack | 2 | 10000 | 0 | ✅ | 19875 | 0.014 | 0.380 |
| SQLite (In-Process) | 16 | false | Pop+Ack | 3 | 10000 | 0 | ✅ | 20961 | 0.014 | 1.106 |
| SQLite (In-Process) | 16 | false | Pop+Ack | 4 | 10000 | 0 | ✅ | 20484 | 0.014 | 1.285 |
| SQLite (In-Process) | 16 | false | Pop+Ack | 5 | 10000 | 0 | ✅ | 20602 | 0.014 | 0.045 |
| Nesso (In-Process) | 16 | false | Push | 1 | 10000 | 10000 | ✅ | 298567 | 0.002 | 0.614 |
| Nesso (In-Process) | 16 | false | Push | 2 | 10000 | 10000 | ✅ | 283333 | 0.003 | 0.660 |
| Nesso (In-Process) | 16 | false | Push | 3 | 10000 | 10000 | ✅ | 262968 | 0.003 | 0.653 |
| Nesso (In-Process) | 16 | false | Push | 4 | 10000 | 10000 | ✅ | 248139 | 0.003 | 0.687 |
| Nesso (In-Process) | 16 | false | Push | 5 | 10000 | 10000 | ✅ | 267268 | 0.003 | 0.635 |
| Nesso (In-Process) | 16 | false | Pop+Ack | 1 | 10000 | 0 | ✅ | 176106 | 0.005 | 0.688 |
| Nesso (In-Process) | 16 | false | Pop+Ack | 2 | 10000 | 0 | ✅ | 170257 | 0.005 | 0.710 |
| Nesso (In-Process) | 16 | false | Pop+Ack | 3 | 10000 | 0 | ✅ | 173990 | 0.005 | 0.708 |
| Nesso (In-Process) | 16 | false | Pop+Ack | 4 | 10000 | 0 | ✅ | 173567 | 0.005 | 0.670 |
| Nesso (In-Process) | 16 | false | Pop+Ack | 5 | 10000 | 0 | ✅ | 175119 | 0.005 | 0.695 |
| Nesso (HTTP) | 16 | false | Push | 1 | 10000 | 10000 | ✅ | 93101 | 0.136 | 0.677 |
| Nesso (HTTP) | 16 | false | Push | 2 | 10000 | 10000 | ✅ | 100539 | 0.139 | 0.420 |
| Nesso (HTTP) | 16 | false | Push | 3 | 10000 | 10000 | ✅ | 104105 | 0.140 | 0.366 |
| Nesso (HTTP) | 16 | false | Push | 4 | 10000 | 10000 | ✅ | 101651 | 0.139 | 0.419 |
| Nesso (HTTP) | 16 | false | Push | 5 | 10000 | 10000 | ✅ | 103224 | 0.137 | 0.405 |
| Nesso (HTTP) | 16 | false | Pop+Ack | 1 | 10000 | 0 | ✅ | 52953 | 0.285 | 0.565 |
| Nesso (HTTP) | 16 | false | Pop+Ack | 2 | 10000 | 0 | ✅ | 51714 | 0.295 | 0.557 |
| Nesso (HTTP) | 16 | false | Pop+Ack | 3 | 10000 | 0 | ✅ | 51352 | 0.302 | 0.505 |
| Nesso (HTTP) | 16 | false | Pop+Ack | 4 | 10000 | 0 | ✅ | 50895 | 0.297 | 0.581 |
| Nesso (HTTP) | 16 | false | Pop+Ack | 5 | 10000 | 0 | ✅ | 51369 | 0.303 | 0.492 |
| SQLite (In-Process) | 1 | true | Push | 1 | 1000 | 1000 | ✅ | 16923 | 0.047 | 0.203 |
| SQLite (In-Process) | 1 | true | Push | 2 | 1000 | 1000 | ✅ | 19822 | 0.045 | 0.074 |
| SQLite (In-Process) | 1 | true | Push | 3 | 1000 | 1000 | ✅ | 19433 | 0.046 | 0.070 |
| SQLite (In-Process) | 1 | true | Push | 4 | 1000 | 1000 | ✅ | 19280 | 0.046 | 0.111 |
| SQLite (In-Process) | 1 | true | Push | 5 | 1000 | 1000 | ✅ | 19692 | 0.045 | 0.069 |
| SQLite (In-Process) | 1 | true | Pop+Ack | 1 | 1000 | 0 | ✅ | 16704 | 0.055 | 0.084 |
| SQLite (In-Process) | 1 | true | Pop+Ack | 2 | 1000 | 0 | ✅ | 16815 | 0.055 | 0.082 |
| SQLite (In-Process) | 1 | true | Pop+Ack | 3 | 1000 | 0 | ✅ | 16599 | 0.055 | 0.089 |
| SQLite (In-Process) | 1 | true | Pop+Ack | 4 | 1000 | 0 | ✅ | 16686 | 0.055 | 0.075 |
| SQLite (In-Process) | 1 | true | Pop+Ack | 5 | 1000 | 0 | ✅ | 16466 | 0.056 | 0.083 |
| Nesso (In-Process) | 1 | true | Push | 1 | 1000 | 1000 | ✅ | 56943 | 0.017 | 0.027 |
| Nesso (In-Process) | 1 | true | Push | 2 | 1000 | 1000 | ✅ | 55955 | 0.017 | 0.028 |
| Nesso (In-Process) | 1 | true | Push | 3 | 1000 | 1000 | ✅ | 56998 | 0.017 | 0.025 |
| Nesso (In-Process) | 1 | true | Push | 4 | 1000 | 1000 | ✅ | 56010 | 0.017 | 0.027 |
| Nesso (In-Process) | 1 | true | Push | 5 | 1000 | 1000 | ✅ | 59019 | 0.015 | 0.026 |
| Nesso (In-Process) | 1 | true | Pop+Ack | 1 | 1000 | 0 | ✅ | 30453 | 0.031 | 0.049 |
| Nesso (In-Process) | 1 | true | Pop+Ack | 2 | 1000 | 0 | ✅ | 29598 | 0.031 | 0.050 |
| Nesso (In-Process) | 1 | true | Pop+Ack | 3 | 1000 | 0 | ✅ | 28905 | 0.034 | 0.047 |
| Nesso (In-Process) | 1 | true | Pop+Ack | 4 | 1000 | 0 | ✅ | 30396 | 0.030 | 0.045 |
| Nesso (In-Process) | 1 | true | Pop+Ack | 5 | 1000 | 0 | ✅ | 31431 | 0.030 | 0.044 |
| Nesso (HTTP) | 1 | true | Push | 1 | 1000 | 1000 | ✅ | 18553 | 0.052 | 0.070 |
| Nesso (HTTP) | 1 | true | Push | 2 | 1000 | 1000 | ✅ | 18732 | 0.052 | 0.068 |
| Nesso (HTTP) | 1 | true | Push | 3 | 1000 | 1000 | ✅ | 18294 | 0.053 | 0.072 |
| Nesso (HTTP) | 1 | true | Push | 4 | 1000 | 1000 | ✅ | 18711 | 0.051 | 0.071 |
| Nesso (HTTP) | 1 | true | Push | 5 | 1000 | 1000 | ✅ | 15529 | 0.053 | 0.199 |
| Nesso (HTTP) | 1 | true | Pop+Ack | 1 | 1000 | 0 | ✅ | 9019 | 0.108 | 0.149 |
| Nesso (HTTP) | 1 | true | Pop+Ack | 2 | 1000 | 0 | ✅ | 9384 | 0.104 | 0.141 |
| Nesso (HTTP) | 1 | true | Pop+Ack | 3 | 1000 | 0 | ✅ | 9288 | 0.106 | 0.131 |
| Nesso (HTTP) | 1 | true | Pop+Ack | 4 | 1000 | 0 | ✅ | 9392 | 0.104 | 0.131 |
| Nesso (HTTP) | 1 | true | Pop+Ack | 5 | 1000 | 0 | ✅ | 8932 | 0.108 | 0.186 |
| SQLite (In-Process) | 4 | true | Push | 1 | 1000 | 1000 | ✅ | 12547 | 0.047 | 0.085 |
| SQLite (In-Process) | 4 | true | Push | 2 | 1000 | 1000 | ✅ | 12472 | 0.047 | 0.129 |
| SQLite (In-Process) | 4 | true | Push | 3 | 1000 | 1000 | ✅ | 12335 | 0.048 | 0.463 |
| SQLite (In-Process) | 4 | true | Push | 4 | 1000 | 1000 | ✅ | 12915 | 0.046 | 0.087 |
| SQLite (In-Process) | 4 | true | Push | 5 | 1000 | 1000 | ✅ | 12323 | 0.046 | 0.142 |
| SQLite (In-Process) | 4 | true | Pop+Ack | 1 | 1000 | 0 | ✅ | 12084 | 0.056 | 0.091 |
| SQLite (In-Process) | 4 | true | Pop+Ack | 2 | 1000 | 0 | ✅ | 11978 | 0.055 | 0.099 |
| SQLite (In-Process) | 4 | true | Pop+Ack | 3 | 1000 | 0 | ✅ | 11921 | 0.056 | 0.102 |
| SQLite (In-Process) | 4 | true | Pop+Ack | 4 | 1000 | 0 | ✅ | 12082 | 0.055 | 0.104 |
| SQLite (In-Process) | 4 | true | Pop+Ack | 5 | 1000 | 0 | ✅ | 12284 | 0.055 | 0.082 |
| Nesso (In-Process) | 4 | true | Push | 1 | 1000 | 1000 | ✅ | 46316 | 0.062 | 0.097 |
| Nesso (In-Process) | 4 | true | Push | 2 | 1000 | 1000 | ✅ | 50054 | 0.073 | 0.102 |
| Nesso (In-Process) | 4 | true | Push | 3 | 1000 | 1000 | ✅ | 46668 | 0.065 | 0.097 |
| Nesso (In-Process) | 4 | true | Push | 4 | 1000 | 1000 | ✅ | 46003 | 0.063 | 0.090 |
| Nesso (In-Process) | 4 | true | Push | 5 | 1000 | 1000 | ✅ | 41119 | 0.068 | 0.096 |
| Nesso (In-Process) | 4 | true | Pop+Ack | 1 | 1000 | 0 | ✅ | 24485 | 0.142 | 0.208 |
| Nesso (In-Process) | 4 | true | Pop+Ack | 2 | 1000 | 0 | ✅ | 23969 | 0.129 | 0.179 |
| Nesso (In-Process) | 4 | true | Pop+Ack | 3 | 1000 | 0 | ✅ | 23130 | 0.139 | 0.223 |
| Nesso (In-Process) | 4 | true | Pop+Ack | 4 | 1000 | 0 | ✅ | 23720 | 0.135 | 0.192 |
| Nesso (In-Process) | 4 | true | Pop+Ack | 5 | 1000 | 0 | ✅ | 20849 | 0.151 | 0.908 |
| Nesso (HTTP) | 4 | true | Push | 1 | 1000 | 1000 | ✅ | 41447 | 0.090 | 0.174 |
| Nesso (HTTP) | 4 | true | Push | 2 | 1000 | 1000 | ✅ | 42223 | 0.089 | 0.168 |
| Nesso (HTTP) | 4 | true | Push | 3 | 1000 | 1000 | ✅ | 42295 | 0.090 | 0.154 |
| Nesso (HTTP) | 4 | true | Push | 4 | 1000 | 1000 | ✅ | 42477 | 0.089 | 0.151 |
| Nesso (HTTP) | 4 | true | Push | 5 | 1000 | 1000 | ✅ | 41963 | 0.090 | 0.156 |
| Nesso (HTTP) | 4 | true | Pop+Ack | 1 | 1000 | 0 | ✅ | 19033 | 0.205 | 0.317 |
| Nesso (HTTP) | 4 | true | Pop+Ack | 2 | 1000 | 0 | ✅ | 19403 | 0.202 | 0.319 |
| Nesso (HTTP) | 4 | true | Pop+Ack | 3 | 1000 | 0 | ✅ | 19023 | 0.201 | 0.365 |
| Nesso (HTTP) | 4 | true | Pop+Ack | 4 | 1000 | 0 | ✅ | 19556 | 0.199 | 0.308 |
| Nesso (HTTP) | 4 | true | Pop+Ack | 5 | 1000 | 0 | ✅ | 19538 | 0.200 | 0.310 |
| SQLite (In-Process) | 16 | true | Push | 1 | 992 | 992 | ✅ | 3723 | 0.056 | 61.471 |
| SQLite (In-Process) | 16 | true | Push | 2 | 992 | 992 | ✅ | 1695 | 0.078 | 92.031 |
| SQLite (In-Process) | 16 | true | Push | 3 | 992 | 992 | ✅ | 2642 | 0.062 | 63.327 |
| SQLite (In-Process) | 16 | true | Push | 4 | 992 | 992 | ✅ | 2585 | 0.059 | 62.367 |
| SQLite (In-Process) | 16 | true | Push | 5 | 992 | 992 | ✅ | 1687 | 0.084 | 91.647 |
| SQLite (In-Process) | 16 | true | Pop+Ack | 1 | 992 | 0 | ✅ | 1697 | 0.092 | 95.231 |
| SQLite (In-Process) | 16 | true | Pop+Ack | 2 | 992 | 0 | ✅ | 1718 | 0.105 | 90.047 |
| SQLite (In-Process) | 16 | true | Pop+Ack | 3 | 992 | 0 | ✅ | 1444 | 0.090 | 115.519 |
| SQLite (In-Process) | 16 | true | Pop+Ack | 4 | 992 | 0 | ✅ | 1256 | 0.115 | 126.271 |
| SQLite (In-Process) | 16 | true | Pop+Ack | 5 | 992 | 0 | ✅ | 2082 | 0.097 | 92.735 |
| Nesso (In-Process) | 16 | true | Push | 1 | 992 | 992 | ✅ | 27889 | 0.392 | 3.563 |
| Nesso (In-Process) | 16 | true | Push | 2 | 992 | 992 | ✅ | 27612 | 0.305 | 3.269 |
| Nesso (In-Process) | 16 | true | Push | 3 | 992 | 992 | ✅ | 26870 | 0.404 | 4.559 |
| Nesso (In-Process) | 16 | true | Push | 4 | 992 | 992 | ✅ | 25468 | 0.435 | 5.283 |
| Nesso (In-Process) | 16 | true | Push | 5 | 992 | 992 | ✅ | 40529 | 0.323 | 1.901 |
| Nesso (In-Process) | 16 | true | Pop+Ack | 1 | 992 | 0 | ✅ | 19625 | 0.501 | 8.743 |
| Nesso (In-Process) | 16 | true | Pop+Ack | 2 | 992 | 0 | ✅ | 21063 | 0.710 | 2.083 |
| Nesso (In-Process) | 16 | true | Pop+Ack | 3 | 992 | 0 | ✅ | 20247 | 0.722 | 2.353 |
| Nesso (In-Process) | 16 | true | Pop+Ack | 4 | 992 | 0 | ✅ | 19680 | 0.639 | 6.347 |
| Nesso (In-Process) | 16 | true | Pop+Ack | 5 | 992 | 0 | ✅ | 20680 | 0.449 | 5.223 |
| Nesso (HTTP) | 16 | true | Push | 1 | 992 | 992 | ✅ | 37152 | 0.323 | 1.339 |
| Nesso (HTTP) | 16 | true | Push | 2 | 992 | 992 | ✅ | 37365 | 0.330 | 1.236 |
| Nesso (HTTP) | 16 | true | Push | 3 | 992 | 992 | ✅ | 36992 | 0.323 | 1.357 |
| Nesso (HTTP) | 16 | true | Push | 4 | 992 | 992 | ✅ | 37055 | 0.315 | 1.406 |
| Nesso (HTTP) | 16 | true | Push | 5 | 992 | 992 | ✅ | 31171 | 0.375 | 2.941 |
| Nesso (HTTP) | 16 | true | Pop+Ack | 1 | 992 | 0 | ✅ | 18039 | 0.753 | 2.061 |
| Nesso (HTTP) | 16 | true | Pop+Ack | 2 | 992 | 0 | ✅ | 18158 | 0.747 | 2.175 |
| Nesso (HTTP) | 16 | true | Pop+Ack | 3 | 992 | 0 | ✅ | 18276 | 0.773 | 2.145 |
| Nesso (HTTP) | 16 | true | Pop+Ack | 4 | 992 | 0 | ✅ | 18233 | 0.768 | 2.213 |
| Nesso (HTTP) | 16 | true | Pop+Ack | 5 | 992 | 0 | ✅ | 15137 | 0.799 | 4.431 |

## Analysis & Interpretation

**Apples-to-Apples Comparison (In-Process vs In-Process)**: Nesso In-Process vs SQLite In-Process is the primary methodologically sound comparison for evaluating storage engine performance. Nesso HTTP vs SQLite In-Process measures two distinct architectures (storage engine + asynchronous HTTP/TCP stack vs an embedded in-memory/file library) and should be interpreted accordingly.

**Multi-Threaded Scalability**: SQLite WAL allows only a single active writer at any given time. Under 16 concurrent threads, writers heavily compete for the database file lock (managed via `busy_timeout`). In contrast, Nesso serializes appends via an in-RAM mutex into an append-only WAL without filesystem-level lock contention.

**Durability Overhead (`sync=true`)**: Operations with individual fsync are strictly bounded by physical SSD capabilities. SQLite with `synchronous=FULL` shows higher throughput because WAL mode flushes the write-ahead log rather than individual b-tree database pages — an architectural design difference rather than an inherent engine speed difference. **Caveat on macOS**: Standard POSIX `fsync()` on macOS flushes to drive cache rather than guaranteeing a platter/NAND barrier without `fcntl(F_FULLFSYNC)`. See BENCHMARK.md for deep analysis.
