"""
WARP (Wireless Activation Ring Protocol) - Buffer & Sliding Window Architecture
Implements:
1. SenderWindow: Retains active batches in memory until cumulative ACK or clean batch confirmation frees them.
2. ReceiverSlottedBuffer: Pre-allocated / slotted reassembly buffer that maps out-of-order and piggybacked
   packets directly into sequence offsets, tracks missing gaps, and flushes contiguous completed batches.
"""

from typing import Dict, List, Set, Tuple, Optional
from collections import deque
from .protocol import WarpPacket


class Batch:
    """Represents a logical batch of packets sent by the sender."""
    __slots__ = ("batch_id", "start_seq", "end_seq", "packets")

    def __init__(self, batch_id: int, start_seq: int, end_seq: int, packets: Dict[int, bytes]):
        self.batch_id = batch_id
        self.start_seq = start_seq
        self.end_seq = end_seq
        self.packets = packets  # seq_num -> raw serialized packet bytes (ready to blast)


class SenderWindow:
    """
    Manages in-flight batches in sender RAM.
    Supports:
    - Adding active batches.
    - Sliding Window flow control (limits maximum unacknowledged in-flight packets).
    - Fetching specific missing packets for piggybacking into the next batch.
    - Cumulative ACK memory pruning: when a NACK or confirmation arrives with seq_num = X,
      all stored packets with seq_num <= X are safely evicted from RAM.
    """
    def __init__(self, max_in_flight_packets: int = 512):
        self.max_in_flight_packets = max_in_flight_packets
        self.batches: Dict[int, Batch] = {}  # batch_id -> Batch
        self.packet_map: Dict[int, bytes] = {}  # seq_num -> raw packet bytes
        self.highest_acked_seq: int = -1

    def is_window_full(self) -> bool:
        """Returns True if in-flight packets exceed the sliding window capacity."""
        return len(self.packet_map) >= self.max_in_flight_packets

    def add_batch(self, batch: Batch):
        self.batches[batch.batch_id] = batch
        for seq, raw_pkt in batch.packets.items():
            self.packet_map[seq] = raw_pkt

    def get_packet_payload(self, seq_num: int) -> Optional[bytes]:
        """Retrieves raw packet for retransmission/piggybacking."""
        return self.packet_map.get(seq_num)

    def acknowledge_cumulative(self, ack_seq: int) -> int:
        """
        Prunes all packets with seq_num <= ack_seq.
        0xFFFFFFFF indicates no packets have been acknowledged yet.
        Returns the number of packets evicted from RAM.
        """
        if ack_seq == 0xFFFFFFFF or ack_seq <= self.highest_acked_seq:
            return 0

        evicted = 0
        self.highest_acked_seq = ack_seq

        # Evict packets from packet_map
        keys_to_delete = [s for s in list(self.packet_map.keys()) if s <= ack_seq]
        for s in keys_to_delete:
            del self.packet_map[s]
            evicted += 1

        # Evict batches whose end_seq <= ack_seq
        batch_ids_to_delete = [b_id for b_id, b in self.batches.items() if b.end_seq <= ack_seq]
        for b_id in batch_ids_to_delete:
            del self.batches[b_id]

        return evicted

    def in_flight_packet_count(self) -> int:
        return len(self.packet_map)

    def in_flight_batch_count(self) -> int:
        return len(self.batches)


class ReceiverSlottedBuffer:
    """
    Pre-allocated slotted buffer on the receiver side.
    Maps out-of-order and piggybacked packets directly to their sequence slots.
    Maintains a live `missing_set` of unresolved gaps for immediate or timeout NACK generation.
    Flushes contiguous byte streams directly to the application layer / disk.
    """
    def __init__(self, start_seq: int = 0):
        self.expected_seq: int = start_seq
        self.highest_received_seq: int = start_seq - 1
        self.slots: Dict[int, bytes] = {}  # seq_num -> payload bytes
        self.missing_set: Set[int] = set()  # set of unreceived seq numbers in the active window
        self.total_packets_expected: Optional[int] = None

    def insert(self, seq_num: int, payload: bytes) -> bool:
        """
        Inserts a packet into its sequence slot.
        Returns True if this was a new packet, False if duplicate/already committed.
        """
        if seq_num < self.expected_seq:
            # Duplicate / already written to disk
            return False

        if seq_num in self.slots:
            # Already received in this window
            return False

        # Slot payload
        self.slots[seq_num] = payload

        # If it was in missing_set, resolve it!
        self.missing_set.discard(seq_num)

        # If seq_num advanced beyond highest_received_seq, record intervening gaps
        if seq_num > self.highest_received_seq:
            # Mark all skipped sequences as missing
            for s in range(self.highest_received_seq + 1, seq_num):
                if s not in self.slots:
                    self.missing_set.add(s)
            self.highest_received_seq = seq_num

        return True

    def get_unresolved_gaps(self) -> List[int]:
        """Returns sorted list of missing sequence numbers in the active window."""
        return sorted(self.missing_set)

    def get_cumulative_ack(self) -> int:
        """
        Returns the highest contiguous sequence number received without any gaps.
        This is safe for the sender to clear from RAM.
        If no packets received yet, returns expected_seq - 1.
        """
        curr = self.expected_seq
        while curr in self.slots:
            curr += 1
        return curr - 1

    def flush_contiguous(self) -> List[Tuple[int, bytes]]:
        """
        Pulls out contiguous sequence chunks ready to be written to disk/stream.
        Advances expected_seq and evicts flushed chunks from RAM.
        """
        flushed = []
        while self.expected_seq in self.slots:
            payload = self.slots.pop(self.expected_seq)
            flushed.append((self.expected_seq, payload))
            self.expected_seq += 1
        return flushed

    def is_complete(self) -> bool:
        """Returns True if all expected packets have arrived and flushed."""
        if self.total_packets_expected is None:
            return False
        return self.expected_seq >= self.total_packets_expected and not self.missing_set and not self.slots
