"""
Integration tests for end-to-end file transfer using WARP over UDP,
including testing packet loss and dropped NACK recovery.
"""

import unittest
import asyncio
import os
import tempfile
import hashlib
from warp.sender import WarpSender
from warp.receiver import WarpReceiver
from warp.channel import SimulatedLossyChannel


class TestWarpTransfer(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.src_file = os.path.join(self.temp_dir.name, "source.bin")
        self.dst_dir = os.path.join(self.temp_dir.name, "received")
        os.makedirs(self.dst_dir, exist_ok=True)

        # Generate 200 KB test binary file with random pattern
        self.file_size = 200 * 1024
        self.original_hash = hashlib.sha256()
        with open(self.src_file, "wb") as f:
            chunk = os.urandom(4096)
            for _ in range(self.file_size // 4096):
                f.write(chunk)
                self.original_hash.update(chunk)
        self.expected_digest = self.original_hash.digest()

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_loss_free_transfer(self):
        """Test direct transfer over localhost with 0% packet loss."""
        port = 10001

        async def run_test():
            receiver = WarpReceiver(
                listen_host="127.0.0.1",
                listen_port=port,
                output_dir=self.dst_dir,
                silence_timeout_ms=15.0
            )
            sender = WarpSender(
                dest_host="127.0.0.1",
                dest_port=port,
                batch_size=32
            )

            # Start receiver task
            recv_task = asyncio.create_task(receiver.receive_file())
            await asyncio.sleep(0.02)  # Allow socket to bind

            # Send file
            send_stats = await sender.send_file(self.src_file)
            recv_stats = await recv_task

            # Verify stats and file content
            self.assertTrue(recv_stats["verified"])
            self.assertTrue(os.path.exists(recv_stats["file_path"]))

            with open(recv_stats["file_path"], "rb") as f:
                received_digest = hashlib.sha256(f.read()).digest()
            self.assertEqual(received_digest, self.expected_digest)

        asyncio.run(run_test())

    def test_transfer_with_packet_and_nack_loss(self):
        """
        Test transfer through a simulated lossy channel with:
        - 5% Data Packet Drop
        - 15% NACK Packet Drop (verifies that dropped NACK recovery works!)
        """
        recv_port = 10002
        proxy_port = 10003

        async def run_test():
            # Setup lossy channel
            channel = SimulatedLossyChannel(
                listen_host="127.0.0.1",
                listen_port=proxy_port,
                target_host="127.0.0.1",
                target_port=recv_port,
                packet_loss_rate=0.06,  # 6% forward loss
                nack_loss_rate=0.15,    # 15% reverse NACK drop
            )
            channel_task = asyncio.create_task(channel.run())

            receiver = WarpReceiver(
                listen_host="127.0.0.1",
                listen_port=recv_port,
                output_dir=self.dst_dir,
                silence_timeout_ms=15.0
            )
            sender = WarpSender(
                dest_host="127.0.0.1",
                dest_port=proxy_port,   # Send via lossy channel
                batch_size=32,
                piggyback_ratio=2
            )

            recv_task = asyncio.create_task(receiver.receive_file())
            await asyncio.sleep(0.02)

            send_stats = await sender.send_file(self.src_file)
            recv_stats = await recv_task

            await channel.stop()
            channel_task.cancel()
            try:
                await channel_task
            except asyncio.CancelledError:
                pass

            # Verify that despite packet loss and dropped NACKs:
            # 1. Piggybacked retransmissions occurred
            self.assertGreater(recv_stats["piggybacked_packets_received"], 0)
            # 2. SHA-256 is 100% verified bit-for-bit identical
            self.assertTrue(recv_stats["verified"])

            with open(recv_stats["file_path"], "rb") as f:
                received_digest = hashlib.sha256(f.read()).digest()
            self.assertEqual(received_digest, self.expected_digest)

        asyncio.run(run_test())

    def test_offline_receiver_detection(self):
        """Verifies that sending to an offline host/port triggers TimeoutError or ConnectionRefusedError."""
        unused_port = 19999
        sender = WarpSender(dest_host="127.0.0.1", dest_port=unused_port)

        async def run_test():
            with self.assertRaises((TimeoutError, ConnectionRefusedError)):
                await sender.send_file(self.src_file)

        asyncio.run(run_test())


if __name__ == "__main__":
    unittest.main()
