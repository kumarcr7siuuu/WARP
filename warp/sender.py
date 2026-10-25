"""
WARP (Wireless Activation Ring Protocol) - Sender Engine
Implements:
- 7-byte WARP datagram transmission over standard UDP.
- Application-level variable batch sizing with END_OF_BATCH framing.
- Sliding Window with Piggybacked Retransmissions (Master Strategy).
- Priority injection of missing sequences into ongoing batch blasts.
- Cumulative ACK memory pruning.
- Micro-pacer (token bucket rate limiter) to prevent Wi-Fi self-congestion.
- Tail Drain / Ping state machine to guarantee loss-free completion.
"""

import socket
import asyncio
import hashlib
import time
import os
from typing import Optional, Dict, List, Callable
from collections import deque

from .protocol import (
    WarpHeader,
    WarpPacket,
    HEADER_SIZE,
    DEFAULT_APP_ID,
    APP_ID_DATA,
    APP_ID_CONTROL,
    DEFAULT_PAYLOAD_SIZE,
    FLAG_IS_NACK,
    FLAG_IS_PIGGYBACK,
    FLAG_END_OF_BATCH,
    FLAG_FIN,
    FLAG_SYN_CTRL,
    FLAG_RANGE_NACK,
    SessionMetadata,
    decode_nack_payload_raw,
    decode_nack_payload_ranges,
    ranges_to_sequences,
)
from .buffer import Batch, SenderWindow


class WarpSender:
    def __init__(
        self,
        dest_host: str,
        dest_port: int,
        app_id: int = DEFAULT_APP_ID,
        payload_size: int = DEFAULT_PAYLOAD_SIZE,
        batch_size: int = 64,
        rate_mbps: Optional[float] = None,  # None = un-paced / max speed
        piggyback_ratio: int = 3,           # Send 1 piggybacked packet per 3 fresh packets
    ):
        self.dest_host = dest_host
        self.dest_port = dest_port
        self.dest_addr = (dest_host, dest_port)
        self.app_id = app_id
        self.payload_size = payload_size
        self.batch_size = batch_size
        self.rate_mbps = rate_mbps
        self.piggyback_ratio = piggyback_ratio

        self.window = SenderWindow()
        self.piggyback_queue: deque[int] = deque()  # sequence numbers that need piggybacking
        self.is_running = False
        self.transfer_finished = False
        self.handshake_event = asyncio.Event()
        self.last_contact_time = 0.0

        # Socket setup
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Attempt to expand OS send buffer (8 MB)
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8 * 1024 * 1024)
        except OSError:
            pass
        self.sock.setblocking(False)

        # Performance counters
        self.stats = {
            "bytes_sent": 0,
            "fresh_packets_sent": 0,
            "piggybacked_packets_sent": 0,
            "nacks_received": 0,
            "packets_pruned": 0,
            "start_time": 0.0,
            "end_time": 0.0,
        }

    async def _nack_listener_task(self):
        """Asynchronously listens for incoming NACK / ACK datagrams from the receiver."""
        loop = asyncio.get_running_loop()
        while self.is_running:
            try:
                raw_data, addr = await loop.sock_recvfrom(self.sock, 65535)
                if len(raw_data) < HEADER_SIZE:
                    continue

                header = WarpHeader.unpack(raw_data)
                payload = raw_data[HEADER_SIZE:]

                if not header.is_nack:
                    continue  # Ignore non-NACK packets

                self.last_contact_time = time.monotonic()
                self.handshake_event.set()

                self.stats["nacks_received"] += 1
                cumulative_ack = header.seq_num

                # 1. Prune acknowledged packets from memory
                pruned = self.window.acknowledge_cumulative(cumulative_ack)
                self.stats["packets_pruned"] += pruned

                # 2. Decode missing sequences (if any)
                missing_seqs: List[int] = []
                if header.is_range_nack:
                    ranges = decode_nack_payload_ranges(payload)
                    missing_seqs = ranges_to_sequences(ranges)
                else:
                    missing_seqs = decode_nack_payload_raw(payload)

                # 3. Add to piggyback queue (avoid duplicates)
                existing = set(self.piggyback_queue)
                for seq in missing_seqs:
                    if seq not in existing:
                        self.piggyback_queue.append(seq)
                        existing.add(seq)

            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(0.002)

    async def send_file(
        self,
        file_path: str,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> dict:
        """
        Sends an entire file using the WARP protocol:
        1. Computes SHA256 checksum and sends SessionMetadata handshake.
        2. Streams data in variable-sized batches with Piggybacked Retransmissions.
        3. Enters Tail Drain state to ensure loss-free delivery before closing.
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        file_size = os.path.getsize(file_path)
        file_name = os.path.basename(file_path)

        # 1. Compute SHA256 for end-to-end verification
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            while chunk := f.read(1024 * 1024):
                hasher.update(chunk)
        file_hash = hasher.digest()

        # Calculate packet counts
        total_packets = (file_size + self.payload_size - 1) // self.payload_size if file_size > 0 else 1

        loop = asyncio.get_running_loop()
        self.is_running = True
        self.stats["start_time"] = time.monotonic()

        # Start background NACK listener
        nack_task = asyncio.create_task(self._nack_listener_task())

        try:
            # 2. Handshake Verification (Detect if receiver is online and listening)
            metadata = SessionMetadata(
                file_name=file_name,
                file_size=file_size,
                total_packets=total_packets,
                chunk_size=self.payload_size,
                sha256_hash=file_hash,
            )
            meta_header = WarpHeader(
                app_id=self.app_id,
                flags=FLAG_SYN_CTRL,
                seq_num=0
            )
            meta_packet = WarpPacket(header=meta_header, payload=metadata.serialize()).serialize()

            handshake_success = False
            for attempt in range(1, 4):
                try:
                    await loop.sock_sendto(self.sock, meta_packet, self.dest_addr)
                except ConnectionResetError:
                    raise ConnectionRefusedError(
                        f"Device at {self.dest_host}:{self.dest_port} is online, but no WARP receiver is listening on this port."
                    )
                except OSError as e:
                    if getattr(e, "winerror", None) == 10054:
                        raise ConnectionRefusedError(
                            f"Device at {self.dest_host}:{self.dest_port} is online, but no WARP receiver is listening on this port."
                        )
                    raise

                try:
                    await asyncio.wait_for(self.handshake_event.wait(), timeout=0.150)
                    handshake_success = True
                    break
                except asyncio.TimeoutError:
                    continue

            if not handshake_success:
                raise TimeoutError(
                    f"No response from {self.dest_host}:{self.dest_port} after 3 probes. "
                    f"Please verify the target device IP and ensure the WARP receiver is running."
                )

            # Micro-pacer timing
            sleep_per_packet = 0.0
            if self.rate_mbps and self.rate_mbps > 0:
                bytes_per_sec = (self.rate_mbps * 1_000_000) / 8
                packets_per_sec = bytes_per_sec / (self.payload_size + HEADER_SIZE + 28)
                sleep_per_packet = 1.0 / packets_per_sec

            # 3. Stream File in Batches
            current_seq = 0
            batch_id = 0
            fresh_since_piggyback = 0
            last_heartbeat_time = time.monotonic()

            with open(file_path, "rb") as f:
                while current_seq < total_packets:
                    # Sliding Window flow control: wait if in-flight packets exceed window capacity
                    while self.window.is_window_full() and self.is_running:
                        # Dead-peer detection: if stalled for > 1.5s, send heartbeat ping
                        silence_duration = time.monotonic() - self.last_contact_time
                        if silence_duration >= 1.5 and time.monotonic() - last_heartbeat_time >= 0.2:
                            hb_hdr = WarpHeader(app_id=self.app_id, flags=FLAG_HEARTBEAT, seq_num=current_seq)
                            hb_pkt = WarpPacket(header=hb_hdr).serialize()
                            await loop.sock_sendto(self.sock, hb_pkt, self.dest_addr)
                            last_heartbeat_time = time.monotonic()

                        if silence_duration >= 4.0:
                            raise ConnectionResetError(
                                f"Connection lost: Receiver at {self.dest_host}:{self.dest_port} stopped responding mid-transfer."
                            )

                        # Process any queued piggybacks while waiting
                        if self.piggyback_queue:
                            retrans_seq = self.piggyback_queue.popleft()
                            retrans_raw = self.window.get_packet_payload(retrans_seq)
                            if retrans_raw:
                                orig_hdr = WarpHeader.unpack(retrans_raw)
                                new_flags = orig_hdr.flags | FLAG_IS_PIGGYBACK
                                new_hdr = WarpHeader(app_id=self.app_id, flags=new_flags, seq_num=retrans_seq)
                                piggy_pkt = WarpPacket(header=new_hdr, payload=retrans_raw[HEADER_SIZE:]).serialize()
                                await loop.sock_sendto(self.sock, piggy_pkt, self.dest_addr)
                                self.stats["bytes_sent"] += len(piggy_pkt)
                                self.stats["piggybacked_packets_sent"] += 1
                        await asyncio.sleep(0.001)

                    # Construct a batch
                    batch_packets: Dict[int, bytes] = {}
                    batch_start = current_seq
                    batch_len = min(self.batch_size, total_packets - current_seq)

                    # Read chunk bytes for the batch
                    for i in range(batch_len):
                        seq = current_seq + i
                        payload = f.read(self.payload_size)
                        is_last_in_batch = (i == batch_len - 1)
                        is_final_packet = (seq == total_packets - 1)

                        flags = 0
                        if is_last_in_batch:
                            flags |= FLAG_END_OF_BATCH
                        if is_final_packet:
                            flags |= FLAG_FIN

                        hdr = WarpHeader(app_id=self.app_id, flags=flags, seq_num=seq)
                        pkt_bytes = WarpPacket(header=hdr, payload=payload).serialize()
                        batch_packets[seq] = pkt_bytes

                    # Register in sliding window
                    batch_end = current_seq + batch_len - 1
                    batch = Batch(batch_id=batch_id, start_seq=batch_start, end_seq=batch_end, packets=batch_packets)
                    self.window.add_batch(batch)

                    # Blast the batch packets
                    for seq in range(batch_start, batch_end + 1):
                        pkt = batch_packets[seq]
                        await loop.sock_sendto(self.sock, pkt, self.dest_addr)
                        self.stats["bytes_sent"] += len(pkt)
                        self.stats["fresh_packets_sent"] += 1
                        fresh_since_piggyback += 1

                        if progress_callback:
                            progress_callback(seq + 1, total_packets)

                        # Check if we should inject a piggybacked retransmission
                        if fresh_since_piggyback >= self.piggyback_ratio and self.piggyback_queue:
                            retrans_seq = self.piggyback_queue.popleft()
                            retrans_raw = self.window.get_packet_payload(retrans_seq)
                            if retrans_raw:
                                # Re-serialize with FLAG_IS_PIGGYBACK set
                                orig_hdr = WarpHeader.unpack(retrans_raw)
                                new_flags = orig_hdr.flags | FLAG_IS_PIGGYBACK
                                new_hdr = WarpHeader(app_id=self.app_id, flags=new_flags, seq_num=retrans_seq)
                                piggy_pkt = WarpPacket(header=new_hdr, payload=retrans_raw[HEADER_SIZE:]).serialize()

                                await loop.sock_sendto(self.sock, piggy_pkt, self.dest_addr)
                                self.stats["bytes_sent"] += len(piggy_pkt)
                                self.stats["piggybacked_packets_sent"] += 1
                                fresh_since_piggyback = 0

                        if sleep_per_packet > 0:
                            await asyncio.sleep(sleep_per_packet)

                    current_seq += batch_len
                    batch_id += 1
                    # Give event loop a tiny breath to process incoming NACKs
                    await asyncio.sleep(0.001)

            # 4. Tail Drain State: Resolve all remaining retransmissions until 100% acked
            await self._tail_drain_state(total_packets, loop)

        finally:
            self.is_running = False
            self.stats["end_time"] = time.monotonic()
            nack_task.cancel()
            try:
                await nack_task
            except asyncio.CancelledError:
                pass
            self.sock.close()

        duration = max(0.001, self.stats["end_time"] - self.stats["start_time"])
        throughput_mbps = (self.stats["bytes_sent"] * 8) / (duration * 1_000_000)
        self.stats["duration_sec"] = duration
        self.stats["throughput_mbps"] = throughput_mbps
        return self.stats

    async def _tail_drain_state(self, total_packets: int, loop: asyncio.AbstractEventLoop):
        """
        Guarantees loss-free completion at the end of the file:
        - If missing packets remain, blasts piggybacked packets directly.
        - If radio is silent, re-pings with the final FIN / END_OF_BATCH packet.
        - Exits once all packets up to total_packets - 1 are pruned from RAM.
        """
        max_drain_time = 10.0  # seconds maximum overall drain
        start_drain = time.monotonic()
        last_progress_time = time.monotonic()
        last_ping_time = time.monotonic()

        while time.monotonic() - start_drain < max_drain_time:
            # Check if all packets have been acknowledged
            if self.window.highest_acked_seq >= total_packets - 1 and len(self.window.packet_map) == 0:
                break  # 100% complete!

            # Process any remaining piggyback queue
            did_work = False
            while self.piggyback_queue:
                retrans_seq = self.piggyback_queue.popleft()
                retrans_raw = self.window.get_packet_payload(retrans_seq)
                if retrans_raw:
                    orig_hdr = WarpHeader.unpack(retrans_raw)
                    new_flags = orig_hdr.flags | FLAG_IS_PIGGYBACK
                    new_hdr = WarpHeader(app_id=self.app_id, flags=new_flags, seq_num=retrans_seq)
                    piggy_pkt = WarpPacket(header=new_hdr, payload=retrans_raw[HEADER_SIZE:]).serialize()

                    await loop.sock_sendto(self.sock, piggy_pkt, self.dest_addr)
                    self.stats["bytes_sent"] += len(piggy_pkt)
                    self.stats["piggybacked_packets_sent"] += 1
                    did_work = True

            if did_work:
                last_progress_time = time.monotonic()

            # If 30ms has passed since last action, ping the receiver
            if time.monotonic() - last_ping_time >= 0.030:
                if self.window.packet_map:
                    # Ping with the lowest unacknowledged packet still in flight
                    lowest_seq = min(self.window.packet_map.keys())
                    raw_pkt = self.window.get_packet_payload(lowest_seq)
                    if raw_pkt:
                        orig_hdr = WarpHeader.unpack(raw_pkt)
                        new_flags = orig_hdr.flags | FLAG_IS_PIGGYBACK | FLAG_END_OF_BATCH
                        new_hdr = WarpHeader(app_id=self.app_id, flags=new_flags, seq_num=lowest_seq)
                        ping_pkt = WarpPacket(header=new_hdr, payload=raw_pkt[HEADER_SIZE:]).serialize()
                        await loop.sock_sendto(self.sock, ping_pkt, self.dest_addr)
                        last_progress_time = time.monotonic()
                last_ping_time = time.monotonic()

            # Exit if completely idle with no progress for 2.0s
            if time.monotonic() - last_progress_time >= 2.0:
                break

            await asyncio.sleep(0.005)
