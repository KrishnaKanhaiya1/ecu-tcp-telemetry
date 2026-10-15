/*
 * protocol.h — 16-byte binary telemetry protocol
 *
 * Wire format (all multi-byte fields in network / big-endian order):
 *
 *   Offset  Size  Field
 *   ------  ----  -----
 *    0       1    magic         0xAA — frame start marker
 *    1       1    type          0x01 = telemetry
 *    2       2    payload_len   fixed value 10 (bytes 4–13)
 *    4       2    ecu_id        ECU identifier
 *    6       4    timestamp_ms  milliseconds since ECU boot
 *   10       2    rpm           engine RPM
 *   12       2    speed         vehicle speed in km/h
 *   14       1    _reserved     reserved / padding (0x00)
 *   15       1    checksum      XOR of bytes 0–14
 *
 * Why packed?
 *   We need the struct layout to match the wire format exactly —
 *   no compiler-inserted padding between fields.  Without __packed__,
 *   the compiler might align payload_len (uint16_t at offset 2) or
 *   timestamp_ms (uint32_t at offset 6) to 4-byte boundaries, blowing
 *   up the struct beyond 16 bytes.
 *
 * Why network byte order?
 *   TCP doesn't guarantee both ends use the same CPU endianness.
 *   A little-endian x86 client talking to a big-endian ARM server
 *   would misinterpret multi-byte integers.  htons/htonl give us a
 *   canonical wire order (big-endian) that both sides agree on.
 *
 * What can XOR checksum NOT catch?
 *   • Two bit-flips that cancel out (same bit position in two bytes).
 *   • Byte reordering (XOR is commutative, so swapped bytes
 *     produce the same checksum).
 *   • It's NOT a cryptographic hash — it detects accidental
 *     corruption, not intentional tampering.
 *   For production, CRC-16 or CRC-32 would be stronger.
 */

#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <cstdint>
#include <cstring>
#include <arpa/inet.h>   // htons, htonl, ntohs, ntohl

// ─── Constants ───────────────────────────────────────────────────────────────

constexpr uint8_t  PROTOCOL_MAGIC    = 0xAA;
constexpr uint8_t  MSG_TYPE_TELEMETRY = 0x01;
constexpr uint8_t  ACK_OK            = 0x06;   // sent back on valid frame
constexpr size_t   FRAME_SIZE        = 16;

// ─── Packet structure ────────────────────────────────────────────────────────

struct __attribute__((packed)) TelemetryPacket {
    uint8_t  magic;          // byte  0   — always 0xAA
    uint8_t  type;           // byte  1   — message type
    uint16_t payload_len;    // bytes 2-3 — network byte order
    uint16_t ecu_id;         // bytes 4-5 — network byte order
    uint32_t timestamp_ms;   // bytes 6-9 — network byte order
    uint16_t rpm;            // bytes 10-11 — network byte order
    uint16_t speed;          // bytes 12-13 — network byte order
    uint8_t  _reserved;      // byte  14  — padding
    uint8_t  checksum;       // byte  15  — XOR of bytes 0–14
};

// Compile-time guarantee: the struct is exactly 16 bytes on the wire.
// If a compiler ever pads it, this fires and we know immediately.
static_assert(sizeof(TelemetryPacket) == 16,
              "TelemetryPacket must be exactly 16 bytes");

// ─── XOR checksum ────────────────────────────────────────────────────────────
// XOR every byte in the frame EXCEPT the checksum field itself (byte 15).
// The result is stored in byte 15.  To verify, XOR bytes 0-14 and compare.
inline uint8_t compute_xor(const void* data, size_t len = FRAME_SIZE - 1) {
    const uint8_t* p = static_cast<const uint8_t*>(data);
    uint8_t result = 0;
    for (size_t i = 0; i < len; ++i) {
        result ^= p[i];
    }
    return result;
}

// ─── Build a packet (host values → wire format) ─────────────────────────────
// Caller passes plain host-order integers; this function converts them to
// network byte order and computes the checksum.
inline TelemetryPacket build_packet(uint16_t ecu_id,
                                     uint32_t timestamp_ms,
                                     uint16_t rpm,
                                     uint16_t speed) {
    TelemetryPacket pkt{};
    pkt.magic        = PROTOCOL_MAGIC;
    pkt.type         = MSG_TYPE_TELEMETRY;
    pkt.payload_len  = htons(10);            // payload = bytes 4–13 = 10 bytes
    pkt.ecu_id       = htons(ecu_id);
    pkt.timestamp_ms = htonl(timestamp_ms);
    pkt.rpm          = htons(rpm);
    pkt.speed        = htons(speed);
    pkt._reserved    = 0x00;
    pkt.checksum     = compute_xor(&pkt);    // XOR of bytes 0–14
    return pkt;
}

// ─── Validate a received frame ───────────────────────────────────────────────
// Check magic byte and verify XOR checksum.  Returns false if either fails.
inline bool validate_packet(const TelemetryPacket& pkt) {
    if (pkt.magic != PROTOCOL_MAGIC) return false;
    return pkt.checksum == compute_xor(&pkt);
}

// ─── Decode a validated frame (wire format → host values) ────────────────────
// After validate_packet() succeeds, call this to get usable integers.
inline void decode_packet(const TelemetryPacket& pkt,
                          uint16_t& ecu_id,
                          uint32_t& timestamp_ms,
                          uint16_t& rpm,
                          uint16_t& speed) {
    ecu_id       = ntohs(pkt.ecu_id);
    timestamp_ms = ntohl(pkt.timestamp_ms);
    rpm          = ntohs(pkt.rpm);
    speed        = ntohs(pkt.speed);
}

#endif // PROTOCOL_H
