# ECU Telemetry TCP Server

A multithreaded TCP server in C++ (POSIX sockets) that receives simulated ECU telemetry data over a custom 16-byte binary protocol.

## Highlights

1. **Thread pool architecture** — 8 worker threads with a bounded connection queue (mutex + condition variable) handle concurrent ECU clients efficiently.
2. **16-byte binary protocol** — Custom wire format with network byte order (`htons`/`htonl`) and XOR checksum validation for ECU telemetry (RPM, speed, timestamp).
3. **Partial TCP read handling** — `read_exact()` loops until a full 16-byte frame arrives; tested with a load generator that intentionally splits frames across multiple `send()` calls.
4. **Load-tested** — 50 simulated ECU clients sending 50,000 packets total; no data races reported by ThreadSanitizer.

## Frame Layout

```
Offset  Size  Field          Notes
──────  ────  ─────          ─────
 0       1    magic          0xAA — frame start marker
 1       1    type           0x01 = telemetry
 2       2    payload_len    Network order, fixed = 10
 4       2    ecu_id         Network order
 6       4    timestamp_ms   Network order (ms since ECU boot)
10       2    rpm            Network order
12       2    speed          Network order (km/h)
14       1    _reserved      Padding (0x00)
15       1    checksum       XOR of bytes 0–14
──────  ────
Total: 16 bytes
```

## Build

```bash
# Standard build
g++ -O2 -pthread server.cpp -o server
g++ -O2 -pthread load_test.cpp -o load_test

# ThreadSanitizer build (for verification)
g++ -O2 -pthread -fsanitize=thread server.cpp -o server_tsan
g++ -O2 -pthread -fsanitize=thread load_test.cpp -o load_test_tsan
```

## Run

```bash
# Terminal 1: start the server
./server

# Terminal 2: run the load test
./load_test
```

The server listens on port 9000 by default.

## Architecture

```
                  ┌──────────────┐
  ECU clients ──▶ │  accept()    │
                  │  main thread │
                  └──────┬───────┘
                         │ push fd
                  ┌──────▼───────────────────┐
                  │  bounded queue (max 100)  │
                  │  std::mutex + cond_var    │
                  └──────┬───────────────────┘
                         │ pop fd
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
     ┌─────────┐   ┌─────────┐   ┌─────────┐
     │ Worker 0│   │ Worker 1│   │  ... 7  │
     │ read_   │   │ read_   │   │ read_   │
     │ exact() │   │ exact() │   │ exact() │
     └─────────┘   └─────────┘   └─────────┘
```

## Known Limitations

These are intentional scoping decisions, not oversights:

- **No epoll / non-blocking I/O.** The server uses blocking `recv()` in each worker. With only 8 workers, it can handle 8 concurrent clients; additional clients wait in the queue. For thousands of concurrent connections, `epoll` + event-driven design would be needed.
- **No frame resync.** If bytes shift (e.g. a partial frame is received due to a bug), all subsequent frames on that connection will fail checksum validation. The server drops bad frames silently; there's no scan-for-magic-byte recovery.
- **No idle timeout.** A connected client that stops sending will hold a worker thread indefinitely. Production servers would use `poll()`/`select()` with a timeout.
- **No graceful shutdown.** Ctrl+C terminates the process immediately. Implementing clean shutdown (drain queue, finish in-flight connections, join threads) adds complexity that doesn't support the 4 resume points.
- **XOR checksum is weak.** It can't detect reordered bytes or pairs of bit flips that cancel out. CRC-16/CRC-32 would be stronger but adds complexity.

## Project Structure

```
├── protocol.h      # 16-byte packed struct, byte-order conversion, XOR checksum
├── server.cpp      # Thread pool server: accept → queue → workers → read_exact
├── load_test.cpp   # 50-client load tester with split-send validation
└── README.md       # This file
```
