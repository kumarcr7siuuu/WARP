"""
Unit tests for SenderWindow and ReceiverSlottedBuffer.
"""

import unittest
from warp.buffer import Batch, SenderWindow, ReceiverSlottedBuffer


class TestWarpBuffer(unittest.TestCase):
    def test_sender_window_pruning(self):
        window = SenderWindow()
        # Add Batch 1: seqs 0..9
        pkts1 = {s: f"pkt_{s}".encode() for s in range(0, 10)}
        batch1 = Batch(batch_id=1, start_seq=0, end_seq=9, packets=pkts1)
        window.add_batch(batch1)

        # Add Batch 2: seqs 10..19
        pkts2 = {s: f"pkt_{s}".encode() for s in range(10, 20)}
        batch2 = Batch(batch_id=2, start_seq=10, end_seq=19, packets=pkts2)
        window.add_batch(batch2)

        self.assertEqual(window.in_flight_packet_count(), 20)
        self.assertEqual(window.in_flight_batch_count(), 2)

        # Cumulative ACK for seq 9 (Batch 1 fully completed)
        evicted = window.acknowledge_cumulative(9)
        self.assertEqual(evicted, 10)
        self.assertEqual(window.in_flight_packet_count(), 10)
        self.assertEqual(window.in_flight_batch_count(), 1)
        self.assertIsNone(window.get_packet_payload(5))
        self.assertIsNotNone(window.get_packet_payload(15))

    def test_receiver_slotted_buffer_gap_and_piggyback(self):
        buf = ReceiverSlottedBuffer(start_seq=0)
        buf.total_packets_expected = 5

        # Packet 0 arrives
        buf.insert(0, b"data_0")
        self.assertEqual(buf.get_unresolved_gaps(), [])
        self.assertEqual(buf.get_cumulative_ack(), 0)

        # Packet 1 is DROPPED! Packet 2 arrives:
        buf.insert(2, b"data_2")
        self.assertEqual(buf.get_unresolved_gaps(), [1])
        self.assertEqual(buf.get_cumulative_ack(), 0)

        # Packet 3 arrives:
        buf.insert(3, b"data_3")
        self.assertEqual(buf.get_unresolved_gaps(), [1])

        # Flush contiguous: should only flush packet 0 because 1 is missing
        flushed = buf.flush_contiguous()
        self.assertEqual(len(flushed), 1)
        self.assertEqual(flushed[0], (0, b"data_0"))

        # Now, piggybacked packet 1 arrives!
        buf.insert(1, b"data_1")
        self.assertEqual(buf.get_unresolved_gaps(), [])  # Gap resolved!
        self.assertEqual(buf.get_cumulative_ack(), 3)

        # Now flush contiguous should return packets 1, 2, 3 in exact order
        flushed2 = buf.flush_contiguous()
        self.assertEqual(len(flushed2), 3)
        self.assertEqual([s for s, _ in flushed2], [1, 2, 3])

        # Finally packet 4 arrives
        buf.insert(4, b"data_4")
        flushed3 = buf.flush_contiguous()
        self.assertEqual(len(flushed3), 1)
        self.assertEqual(flushed3[0], (4, b"data_4"))

        self.assertTrue(buf.is_complete())


if __name__ == "__main__":
    unittest.main()
