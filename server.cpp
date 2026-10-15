/*
 * server.cpp — Multithreaded TCP server with a fixed pool of 8 workers
 *
 * Architecture:
 *
 *   ┌──────────┐      ┌──────────────────────┐      ┌──────────────┐
 *   │  accept  │─────▶│  bounded queue (100)  │─────▶│  8 workers   │
 *   │  loop    │      │  mutex + cond_var     │      │  (threads)   │
 *   └──────────┘      └──────────────────────┘      └──────────────┘
 *
 *   Main thread: accepts TCP connections and pushes client file descriptors
 *   onto a std::queue protected by a mutex.  If the queue already has 100
 *   entries, the new connection is rejected (backpressure).
 *
 *   Worker threads: wait on a condition variable, pop a client fd, then
 *   read 16-byte frames in a loop until the client disconnects.
 *
 * Why a fixed pool instead of one-thread-per-client?
 *   Creating a thread per connection doesn't scale — 10,000 clients would
 *   mean 10,000 threads fighting for CPU time.  A fixed pool caps resource
 *   usage.  For higher concurrency, epoll + non-blocking I/O would be better,
 *   but that's beyond the scope of this project.
 *
 * Why a bounded queue?
 *   An unbounded queue could grow without limit if clients connect faster
 *   than workers can drain.  The bound (100) provides backpressure: once
 *   full, new connections are rejected, preventing memory exhaustion.
 *
 * Why condition_variable, not a spin loop?
 *   A spin loop burns CPU while waiting.  condition_variable puts the
 *   thread to sleep in the OS scheduler until signaled — zero CPU cost
 *   while idle.
 */

#include "protocol.h"

#include <iostream>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <unordered_map>
#include <atomic>
#include <csignal>
#include <cstring>
#include <cstdlib>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

// ─── Configuration ───────────────────────────────────────────────────────────

static constexpr int PORT           = 9000;
static constexpr int NUM_WORKERS    = 8;
static constexpr int MAX_QUEUE_SIZE = 100;
static constexpr int LISTEN_BACKLOG = 128;

// ─── Shared state: connection queue ──────────────────────────────────────────
// Protected by queue_mtx.  Workers wait on queue_cv.

static std::mutex              queue_mtx;
static std::condition_variable queue_cv;
static std::queue<int>         conn_queue;       // pending client fds
static bool                    server_running = true;

// ─── Shared state: per-ECU stats ─────────────────────────────────────────────
// Protected by stats_mtx.  The critical section is tiny: one map lookup
// and one integer increment.  Keeping the lock duration short minimises
// contention across workers.

static std::mutex                             stats_mtx;
static std::unordered_map<uint16_t, uint64_t> ecu_packet_count;
static uint64_t                               total_good = 0;
static uint64_t                               total_bad  = 0;

// ─── read_exact ──────────────────────────────────────────────────────────────
// TCP is a byte stream, not a message stream.  A single recv() might return
// fewer bytes than requested (e.g. if the sender did two 8-byte send() calls,
// or if the kernel's receive buffer was partially filled).
//
// read_exact() keeps calling recv() in a loop, advancing a buffer pointer,
// until exactly `len` bytes have arrived — or the connection closes / errors.
//
// This is point 3 on the resume.

static ssize_t read_exact(int fd, void* buf, size_t len) {
    size_t   total = 0;
    uint8_t* ptr   = static_cast<uint8_t*>(buf);

    while (total < len) {
        ssize_t n = recv(fd, ptr + total, len - total, 0);
        if (n <= 0) {
            // n == 0 → client closed the connection gracefully
            // n <  0 → error (ECONNRESET, etc.)
            return static_cast<ssize_t>(total);
        }
        total += static_cast<size_t>(n);
    }
    return static_cast<ssize_t>(total);  // == len on success
}

// ─── Worker thread function ──────────────────────────────────────────────────
// Each worker runs this loop forever:
//   1. Lock the queue mutex and wait for a connection (cond_var).
//   2. Pop the client fd.
//   3. Read 16-byte frames until the client disconnects.
//   4. For each valid frame: update stats, send ACK.
//   5. Close the socket and go back to step 1.
//
// The worker_id is only for logging; there's no per-worker state.

static void worker_func(int worker_id) {
    (void)worker_id;  // suppress unused warning; useful for debug prints

    while (true) {
        int client_fd = -1;

        // ── Step 1 & 2: wait for work, then pop ──
        {
            std::unique_lock<std::mutex> lock(queue_mtx);
            queue_cv.wait(lock, [] {
                return !conn_queue.empty() || !server_running;
            });

            if (!server_running && conn_queue.empty()) {
                return;  // clean shutdown path (not used in this version)
            }

            client_fd = conn_queue.front();
            conn_queue.pop();
        }
        // Mutex released here — other workers can pop while we handle this client.

        // ── Step 3: read frames until disconnect ──
        TelemetryPacket pkt;

        while (true) {
            ssize_t n = read_exact(client_fd, &pkt, FRAME_SIZE);
            if (n < static_cast<ssize_t>(FRAME_SIZE)) {
                // Incomplete read → client disconnected mid-frame or cleanly.
                break;
            }

            // ── Step 4: validate and process ──
            if (validate_packet(pkt)) {
                uint16_t ecu_id, rpm, speed;
                uint32_t ts;
                decode_packet(pkt, ecu_id, ts, rpm, speed);

                // Update stats — lock scope is just the map update.
                {
                    std::lock_guard<std::mutex> slock(stats_mtx);
                    ecu_packet_count[ecu_id]++;
                    total_good++;
                }

                // Send 1-byte ACK back to the client.
                uint8_t ack = ACK_OK;
                // If send fails (broken pipe), the next recv will return 0/-1
                // and we'll exit the loop.  SIGPIPE is already ignored.
                send(client_fd, &ack, 1, 0);
            } else {
                // Bad frame: either wrong magic or checksum mismatch.
                // We DROP it silently — there's no resync logic.
                // Known limitation: if bytes shift, all subsequent frames
                // will also fail until the client reconnects.
                std::lock_guard<std::mutex> slock(stats_mtx);
                total_bad++;
            }
        }

        close(client_fd);
    }
}

// ─── Print stats ─────────────────────────────────────────────────────────────

static void print_stats() {
    std::lock_guard<std::mutex> lock(stats_mtx);
    std::cout << "\n[server] === Statistics ===\n"
              << "  Good frames : " << total_good << "\n"
              << "  Bad frames  : " << total_bad  << "\n"
              << "  ECU breakdown:\n";
    for (auto& [id, count] : ecu_packet_count) {
        std::cout << "    ECU " << id << ": " << count << " packets\n";
    }
    std::cout << std::flush;
}

// ─── Signal handler for SIGUSR1 (print stats on demand) ─────────────────────

static void sigusr1_handler(int) {
    // Not async-signal-safe to call cout here, but acceptable for a demo.
    // In production, you'd set a flag and check it in the main loop.
    print_stats();
}

// ─── main ────────────────────────────────────────────────────────────────────

int main() {
    // Ignore SIGPIPE: if a client disconnects while we're writing, we don't
    // want the default behavior (process termination).  Instead, send() will
    // return -1 with errno == EPIPE, which we handle gracefully.
    signal(SIGPIPE, SIG_IGN);

    // Optional: print stats with `kill -USR1 <pid>`.
    signal(SIGUSR1, sigusr1_handler);

    // ── 1. Create a TCP socket ──
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        return 1;
    }

    // ── 2. Set SO_REUSEADDR ──
    // Without this, binding after a recent shutdown fails with EADDRINUSE
    // because the OS keeps the port in TIME_WAIT state for ~60 seconds.
    int opt = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt");
        close(server_fd);
        return 1;
    }

    // ── 3. Bind to 0.0.0.0:PORT ──
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;   // listen on all interfaces
    addr.sin_port        = htons(PORT);

    if (bind(server_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("bind");
        close(server_fd);
        return 1;
    }

    // ── 4. Listen ──
    // LISTEN_BACKLOG is the OS-level queue for completed TCP handshakes
    // waiting to be accept()ed.  This is separate from our conn_queue.
    if (listen(server_fd, LISTEN_BACKLOG) < 0) {
        perror("listen");
        close(server_fd);
        return 1;
    }

    std::cout << "[server] Listening on port " << PORT
              << " with " << NUM_WORKERS << " worker threads\n"
              << "[server] Queue capacity: " << MAX_QUEUE_SIZE << " connections\n"
              << "[server] PID: " << getpid() << "  (kill -USR1 " << getpid()
              << " to print stats)\n" << std::flush;

    // ── 5. Spawn the worker thread pool ──
    std::vector<std::thread> workers;
    workers.reserve(NUM_WORKERS);
    for (int i = 0; i < NUM_WORKERS; ++i) {
        workers.emplace_back(worker_func, i);
    }

    // ── 6. Accept loop ──
    // The main thread's only job: accept incoming connections and push
    // them onto the queue for workers.
    while (true) {
        sockaddr_in client_addr{};
        socklen_t   client_len = sizeof(client_addr);

        int client_fd = accept(server_fd,
                               reinterpret_cast<sockaddr*>(&client_addr),
                               &client_len);
        if (client_fd < 0) {
            perror("accept");
            continue;
        }

        {
            std::lock_guard<std::mutex> lock(queue_mtx);
            if (static_cast<int>(conn_queue.size()) >= MAX_QUEUE_SIZE) {
                // Backpressure: reject the connection instead of growing
                // memory without bound.
                std::cerr << "[server] Queue full (" << MAX_QUEUE_SIZE
                          << ") — rejecting connection\n";
                close(client_fd);
                continue;
            }
            conn_queue.push(client_fd);
        }
        // Wake ONE sleeping worker to handle this connection.
        queue_cv.notify_one();
    }

    // ── Cleanup (unreachable in this design) ──
    // Ctrl+C invokes the default SIGINT handler, which terminates the
    // process.  We deliberately don't implement graceful shutdown because
    // it adds complexity we can't defend in an interview.  Known limitation.
    close(server_fd);
    server_running = false;
    queue_cv.notify_all();
    for (auto& t : workers) {
        t.join();
    }

    print_stats();
    return 0;
}
