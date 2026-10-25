# WARP: Wireless Activation Ring Protocol

High-Speed User-Space Reliability Layer over Custom UDP for Local Wi-Fi Networks.

---

## 1. Overview
**WARP** is a high-performance, lightweight transport protocol designed for maximum-speed file transfer and data streaming across local Wi-Fi networks. Operating entirely in user-space inside standard UDP datagrams (`SOCK_DGRAM`), it bypasses the heavy state machine, connection handshake delays, and slow-start congestion control penalties of TCP while traversing NAT routers and OS kernels without requiring root privileges.

### Core Architectural Features
* **7-Byte Custom Header**: Zero unnecessary overhead ($97.66\%$ payload efficiency on standard 1500-byte MTU).
* **Sliding Window with Piggybacked Retransmissions**: The sender never stops to wait for retransmissions. Missing packets are injected into the ongoing stream of the next batch, keeping the Wi-Fi radio at 100% duty cycle.
* **Deferred Range-Encoded NACKs**: Upstream channel remains completely silent during batch transmission. Missing packets are encoded as `(start_seq, count)` ranges, allowing massive burst drops to fit inside a single 60-byte UDP datagram.
* **Dropped NACK Recovery (20ms Silence Watchdog)**: If a NACK drops in transit, the receiver's async silence watchdog detects radio silence $\ge 20\text{ms}$ and automatically re-emits the missing gap set. Mid-stream, missing packets self-heal on subsequent batch boundaries.
* **Cumulative Memory Pruning**: The 4-byte Sequence Number in NACKs functions as a cumulative ACK (`ack_seq`), safely evicting confirmed packets from sender RAM.
* **Cryptographic Verification**: End-to-end SHA-256 integrity verification.

---

## 2. The 7-Byte Wire Format

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|            App ID             |     Flags     |   Sequence /  |
|           (16 bits)           |    (8 bits)   | Cumulative ACK|
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|           Sequence / Cumulative ACK (cont.)   |  Payload...   |
|                       (32 bits)               | (Data / NACK) |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|<---------------------- EXACTLY 7 BYTES ---------------------->|
```

### 1-Byte Flag Breakdown (Bitwise Map)
| Bit | Name | Hex | Semantics |
|---|---|:---:|---|
| **Bit 0** | `TYPE` | `0x01` | `0` = Application Data, `1` = NACK Packet |
| **Bit 1** | `PIGGYBACK` | `0x02` | `0` = Fresh Data, `1` = Piggybacked Retransmission |
| **Bit 2** | `END_OF_BATCH` | `0x04` | `0` = Mid-Batch Packet, `1` = Batch Boundary Check |
| **Bit 3** | `FIN` | `0x08` | `1` = Transfer Complete (EOF) |
| **Bit 4** | `SYN_CTRL` | `0x10` | `1` = Session Metadata (file size, filename, checksum) |
| **Bit 5** | `COMPRESSED` | `0x20` | `1` = Payload compressed with LZ4 / zlib |
| **Bit 6** | `HEARTBEAT` | `0x40` | `1` = Silence Watchdog Ping |
| **Bit 7** | `RANGE_NACK` | `0x80` | `1` = Range-Encoded NACK `(start, count)` |

---

## 3. Quick Start

### Receiving a File
```bash
python -m warp.cli receive --port 9999 --output-dir ./downloads
```

### Sending a File
```bash
python -m warp.cli send <path_to_file> --host <receiver_ip> --port 9999
```

### Running the Loss Benchmark
Simulate a transfer under heavy 5% packet loss and 10% dropped NACKs:
```bash
python -m warp.cli benchmark --size-mb 10 --loss 0.05 --nack-loss 0.10
```

---

## 4. Running Tests

Run the complete test suite:
```bash
python -m unittest discover -s tests
```
* `test_protocol.py`: Verifies 7-byte header packing, bitwise state flags, range encoding, and metadata serialization.
* `test_buffer.py`: Verifies sender memory pruning and receiver slotted reassembly.
* `test_transfer.py`: Tests end-to-end file transfer over UDP with simulated packet loss and dropped NACKs.
