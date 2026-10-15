/*
 * load_test.cpp — Load tester: 50 simulated ECU clients, 1000 frames each
 *
 * Strategy:
 *   • 50 clients, each sending 1,000 frames = 50,000 total packets.
 *   • Clients launch in waves of 8 (matching the 8 server workers).
 *   • Each client connects, sends all frames (waiting for ACK after each),
 *     then disconnects.
 *   • Every 10th frame is intentionally split into two 8-byte send() calls
 *     to exercise the server's read_exact() partial-read handling.
 *
 * Why waves of 8?
 *   The server has 8 workers.  Launching all 50 clients at once would queue
 *   42 of them, which works but doesn't test steady-state worker utilisation
 *   as well.  Waves of 8 keep all workers busy without overwhelming the queue.
 *
 * The split-send test:
 *   TCP is a byte stream.  If the client does send(8 bytes) then send(8 bytes),
 *   the server might receive all 16 in one recv(), or just 8, or even 3 + 13.
 *   The usleep(100) after the first half-send increases the chance that the
 *   kernel delivers the halves separately, forcing read_exact to loop.
 */

#include "protocol.h"

#include <iostream>
#include <iomanip>
#include <vector>
#include <thread>
#include <chrono>
#include <atomic>
#include <cstring>
#include <cstdlib>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

// ─── Configuration ───────────────────────────────────────────────────────────

static constexpr int  SERVER_PORT       = 9000;
static constexpr char SERVER_IP[]       = "127.0.0.1";
static constexpr int  TOTAL_CLIENTS     = 50;
static constexpr int  FRAMES_PER_CLIENT = 1000;
static constexpr int  WAVE_SIZE         = 8;

// ─── Atomic counters (no mutex needed for simple counters) ───────────────────

static std::atomic<uint64_t> total_acks{0};
static std::atomic<uint64_t> total_sent{0};
static std::atomic<uint64_t> total_split_sends{0};
static std::atomic<uint64_t> total_connect_failures{0};

// ─── Single client simulation ────────────────────────────────────────────────

static void client_func(int client_id) {
    // ── Connect ──
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("client: socket");
        total_connect_failures++;
        return;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(SERVER_PORT);
    inet_pton(AF_INET, SERVER_IP, &addr.sin_addr);

    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        perror("client: connect");
        close(fd);
        total_connect_failures++;
        return;
    }

    uint16_t ecu_id = static_cast<uint16_t>(client_id);

    // ── Send frames ──
    for (int i = 0; i < FRAMES_PER_CLIENT; ++i) {
        TelemetryPacket pkt = build_packet(
            ecu_id,
            static_cast<uint32_t>(i * 10),            // simulated timestamp
            static_cast<uint16_t>(3000 + (i % 500)),   // RPM varies 3000-3499
            static_cast<uint16_t>(60  + (i % 100))     // speed varies 60-159
        );

        bool send_ok = true;

        if (i % 10 == 0) {
            // ── Split send: two 8-byte halves ──
            // This tests the server's read_exact() loop.
            const uint8_t* raw = reinterpret_cast<const uint8_t*>(&pkt);

            if (send(fd, raw, 8, 0) != 8) { send_ok = false; }
            else {
                // Tiny delay so the kernel is more likely to deliver the
                // halves as separate recv() calls on the server side.
                usleep(100);
                if (send(fd, raw + 8, 8, 0) != 8) { send_ok = false; }
            }
            total_split_sends++;
        } else {
            // ── Normal single send ──
            if (send(fd, &pkt, FRAME_SIZE, 0) != FRAME_SIZE) {
                send_ok = false;
            }
        }

        if (!send_ok) {
            // Connection broke — stop this client.
            break;
        }
        total_sent++;

        // ── Wait for ACK ──
        uint8_t ack = 0;
        ssize_t n = recv(fd, &ack, 1, 0);
        if (n == 1 && ack == ACK_OK) {
            total_acks++;
        } else {
            // No ACK → server dropped the frame or connection died.
            // Either way, stop sending.
            break;
        }
    }

    close(fd);
}

// ─── main ────────────────────────────────────────────────────────────────────

int main() {
    std::cout << "╔══════════════════════════════════════╗\n"
              << "║       ECU Telemetry Load Tester      ║\n"
              << "╠══════════════════════════════════════╣\n"
              << "║  Clients        : " << std::setw(6) << TOTAL_CLIENTS
              << "             ║\n"
              << "║  Frames/client  : " << std::setw(6) << FRAMES_PER_CLIENT
              << "             ║\n"
              << "║  Wave size      : " << std::setw(6) << WAVE_SIZE
              << "             ║\n"
              << "║  Expected total : " << std::setw(6)
              << TOTAL_CLIENTS * FRAMES_PER_CLIENT << " frames      ║\n"
              << "╚══════════════════════════════════════╝\n\n";

    auto t_start = std::chrono::steady_clock::now();

    // ── Launch clients in waves ──
    int launched = 0;
    int wave_num = 0;

    while (launched < TOTAL_CLIENTS) {
        wave_num++;
        int batch = std::min(WAVE_SIZE, TOTAL_CLIENTS - launched);
        std::vector<std::thread> wave;
        wave.reserve(batch);

        for (int i = 0; i < batch; ++i) {
            wave.emplace_back(client_func, launched + i);
        }
        for (auto& t : wave) {
            t.join();
        }

        launched += batch;
        std::cout << "  Wave " << std::setw(2) << wave_num
                  << " complete — " << launched << "/" << TOTAL_CLIENTS
                  << " clients finished"
                  << "  (acks so far: " << total_acks.load() << ")\n";
    }

    auto t_end = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(t_end - t_start).count();

    uint64_t sent    = total_sent.load();
    uint64_t acks    = total_acks.load();
    uint64_t splits  = total_split_sends.load();
    uint64_t failures = total_connect_failures.load();
    double   pps     = (elapsed > 0.0) ? static_cast<double>(sent) / elapsed : 0.0;

    std::cout << "\n╔══════════════════════════════════════╗\n"
              << "║            Test Results               ║\n"
              << "╠══════════════════════════════════════╣\n"
              << "║  Frames sent     : " << std::setw(8) << sent    << "         ║\n"
              << "║  ACKs received   : " << std::setw(8) << acks    << "         ║\n"
              << "║  Split sends     : " << std::setw(8) << splits  << "         ║\n"
              << "║  Connect failures: " << std::setw(8) << failures << "         ║\n"
              << "║  Elapsed time    : " << std::setw(8) << std::fixed
              << std::setprecision(2) << elapsed << " s       ║\n"
              << "║  Throughput      : " << std::setw(8)
              << static_cast<uint64_t>(pps) << " pkt/s    ║\n"
              << "╚══════════════════════════════════════╝\n";

    if (acks == sent && sent == static_cast<uint64_t>(TOTAL_CLIENTS) * FRAMES_PER_CLIENT) {
        std::cout << "\n  ✓ ALL " << sent << " frames acknowledged — PASS\n\n";
        return 0;
    } else {
        std::cout << "\n  ✗ MISMATCH: sent=" << sent << " acks=" << acks
                  << " expected=" << TOTAL_CLIENTS * FRAMES_PER_CLIENT
                  << " — FAIL\n\n";
        return 1;
    }
}
