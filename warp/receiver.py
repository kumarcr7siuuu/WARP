"""
WARP (Wireless Activation Ring Protocol) - Receiver Engine
Implements:
- 7-byte custom WARP header decoding.
- Slotted memory buffer that places fresh and piggybacked packets directly by sequence.
- Immediate gap detection on END_OF_BATCH.
- 20ms Radio Silence Watchdog with NACK re-transmission loop.
- Cumulative ACK pruning feedback to clear sender RAM.
- Streaming file write with end-to-end SHA-256 verification.
"""

import socket
import asyncio
import hashlib
import time
import os
from typing import Optional, Callable, Tuple

from .protocol import (
    WarpHeader,
    WarpPacket,
    HEADER_SIZE,
    DEFAULT_APP_ID,
    FLAG_IS_NACK,
    FLAG_IS_PIGGYBACK,
    FLAG_END_OF_BATCH,
    FLAG_FIN,
    FLAG_SYN_CTRL,
    FLAG_RANGE_NACK,
    SessionMetadata,
    encode_nack_payload_ranges,
    sequences_to_ranges,
)
from .buffer import ReceiverSlottedBuffer


class WarpReceiver:
    def __init__(
        self,
        listen_host: str = "0.0.0.0",
        listen_port: int = 9999,
        app_id: int = DEFAULT_APP_ID,
        output_dir: str = ".",
        silence_timeout_ms: float = 20.0,
        max_nack_retries: int = 50,
    ):
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.app_id = app_id
        self.output_dir = output_dir
        self.silence_timeout_sec = silence_timeout_ms / 1000.0
        self.max_nack_retries = max_nack_retries
        self.timed_out = False

        self.buffer = ReceiverSlottedBuffer(start_seq=0)
        self.metadata: Optional[SessionMetadata] = None
        self.output_file_path: Optional[str] = None
        self.file_handle = None
        self.hasher = hashlib.sha256()

        self.sender_addr: Optional[Tuple[str, int]] = None
        self.last_packet_time: float = 0.0
        self.is_running = False
        self.nack_retry_count = 0
        self.transfer_completed = False

        # Socket setup
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 16 * 1024 * 1024)
        except OSError:
            pass
        self.sock.bind((self.listen_host, self.listen_port))
        self.sock.setblocking(False)

        # Performance stats
        self.stats = {
            "bytes_received": 0,
            "fresh_packets_received": 0,
            "piggybacked_packets_received": 0,
            "nacks_sent": 0,
            "watchdog_triggers": 0,
            "verified": False,
            "start_time": 0.0,
            "end_time": 0.0,
            "file_path": "",
        }

    async def _send_nack(self, loop: asyncio.AbstractEventLoop):
        """Builds and transmits a NACK packet with cumulative ACK and missing gap ranges."""
        if not self.sender_addr:
            return

        gaps = self.buffer.get_unresolved_gaps()
        cum_ack = self.buffer.get_cumulative_ack()

        # If gaps exist, encode as range NACK. If clean, empty payload.
        if gaps:
            ranges = sequences_to_ranges(gaps)
            payload = encode_nack_payload_ranges(ranges)
            flags = FLAG_IS_NACK | FLAG_RANGE_NACK
        else:
            payload = b""
            flags = FLAG_IS_NACK

        header = WarpHeader(
            app_id=self.app_id,
            flags=flags,
            seq_num=0xFFFFFFFF if cum_ack < 0 else cum_ack
        )
        packet = WarpPacket(header=header, payload=payload).serialize()
        await loop.sock_sendto(self.sock, packet, self.sender_addr)
        self.stats["nacks_sent"] += 1

    async def _silence_watchdog_task(self):
        """
        Runs continuously in the background.
        If radio silence >= 20ms and there are unresolved gaps, re-fires the NACK.
        This provides automatic recovery when a previous NACK was dropped in transit!
        """
        loop = asyncio.get_running_loop()
        while self.is_running and not self.transfer_completed:
            await asyncio.sleep(0.005)
            if self.last_packet_time == 0.0:
                continue

            elapsed = time.monotonic() - self.last_packet_time
            if elapsed >= self.silence_timeout_sec:
                gaps = self.buffer.get_unresolved_gaps()
                if gaps:
                    if self.nack_retry_count < self.max_nack_retries:
                        self.stats["watchdog_triggers"] += 1
                        await self._send_nack(loop)
                        self.nack_retry_count += 1
                        self.last_packet_time = time.monotonic()
                    else:
                        self.timed_out = True
                        break

    async def receive_file(
        self,
        custom_output_name: Optional[str] = None,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> dict:
        """
        Runs the receiver loop until the entire file is received, assembled, and verified.
        """
        loop = asyncio.get_running_loop()
        self.is_running = True
        self.stats["start_time"] = time.monotonic()

        watchdog_task = asyncio.create_task(self._silence_watchdog_task())

        try:
            while not self.transfer_completed and not self.timed_out:
                try:
                    raw_data, addr = await asyncio.wait_for(loop.sock_recvfrom(self.sock, 65535), timeout=2.0)
                except asyncio.TimeoutError:
                    if self.buffer.is_complete():
                        self.transfer_completed = True
                        break
                    if self.timed_out:
                        break
                    continue

                self.last_packet_time = time.monotonic()
                self.sender_addr = addr

                if len(raw_data) < HEADER_SIZE:
                    continue

                header = WarpHeader.unpack(raw_data)
                payload = raw_data[HEADER_SIZE:]

                # Protocol guard: drop stray noise packets
                if header.app_id != self.app_id and not header.is_syn_ctrl:
                    continue

                # 1. Handle Session Metadata Handshake
                if header.is_syn_ctrl:
                    if self.metadata is None:
                        self.metadata = SessionMetadata.deserialize(payload)
                        self.buffer.total_packets_expected = self.metadata.total_packets

                        target_name = custom_output_name or self.metadata.file_name
                        os.makedirs(self.output_dir, exist_ok=True)
                        self.output_file_path = os.path.join(self.output_dir, target_name)
                        self.file_handle = open(self.output_file_path, "wb")
                        self.stats["file_path"] = self.output_file_path

                    # Send back an immediate confirmation ACK
                    await self._send_nack(loop)
                    continue

                # Handle Heartbeat Ping (probed by sender during silence)
                if header.is_heartbeat:
                    await self._send_nack(loop)
                    continue

                # 2. Handle Data Packet (Fresh or Piggybacked)
                if not header.is_nack:
                    seq = header.seq_num
                    inserted = self.buffer.insert(seq, payload)
                    if inserted:
                        self.stats["bytes_received"] += len(payload)
                        if header.is_piggyback:
                            self.stats["piggybacked_packets_received"] += 1
                        else:
                            self.stats["fresh_packets_received"] += 1

                        # Reset watchdog retry count on successful packet arrival
                        self.nack_retry_count = 0

                    # Flush any contiguous completed sequences to disk
                    flushed = self.buffer.flush_contiguous()
                    for seq_flushed, chunk in flushed:
                        if self.file_handle:
                            self.file_handle.write(chunk)
                            self.hasher.update(chunk)
                        if progress_callback and self.metadata:
                            progress_callback(seq_flushed + 1, self.metadata.total_packets)

                    # 3. Check for Batch Boundary
                    if header.is_end_of_batch or header.is_fin:
                        await self._send_nack(loop)

                    # 4. Check for Total Completion
                    if self.buffer.is_complete():
                        self.transfer_completed = True
                        break

            # End of transfer verification
            self.stats["end_time"] = time.monotonic()
            if self.file_handle:
                self.file_handle.flush()
                self.file_handle.close()
                self.file_handle = None

            # Verify cryptographic SHA-256
            if self.metadata:
                computed_hash = self.hasher.digest()
                self.stats["verified"] = (computed_hash == self.metadata.sha256_hash)

            # Send multiple clean ACKs to help the sender exit its tail drain state
            for _ in range(3):
                await self._send_nack(loop)
                await asyncio.sleep(0.005)

        finally:
            self.is_running = False
            watchdog_task.cancel()
            try:
                await watchdog_task
            except asyncio.CancelledError:
                pass
            if self.file_handle:
                self.file_handle.close()
            self.sock.close()

        duration = max(0.001, self.stats["end_time"] - self.stats["start_time"])
        throughput_mbps = (self.stats["bytes_received"] * 8) / (duration * 1_000_000)
        self.stats["duration_sec"] = duration
        self.stats["throughput_mbps"] = throughput_mbps
        return self.stats
