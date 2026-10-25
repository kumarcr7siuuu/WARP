"""
WARP (Wireless Activation Ring Protocol) - Wire Protocol & Codec
Specification: 7-Byte Custom Header following standard UDP header.
Wire Format:
    - App ID: 2 Bytes (16-bit uint, big-endian)
    - Flags: 1 Byte (8-bit bitfield)
    - Sequence Number / Cumulative Ack: 4 Bytes (32-bit uint, big-endian)
"""

import struct
from dataclasses import dataclass
from enum import IntEnum
from typing import List, Tuple, Optional

# Constants
HEADER_SIZE = 7  # Exactly 7 bytes
HEADER_STRUCT = struct.Struct("!HBI")  # uint16 (App ID), uint8 (Flags), uint32 (Seq)

DEFAULT_APP_ID = 0x5750      # ASCII 'WP' for WARP
APP_ID_CONTROL = 0x5751      # Control stream
APP_ID_DATA    = 0x5752      # Bulk data stream

# 1500 MTU - 20 (IPv4) - 8 (UDP) - 7 (WARP) = 1465 bytes max payload
DEFAULT_MTU = 1500
DEFAULT_PAYLOAD_SIZE = 1465

# 1-Byte Flag Breakdown (Bitwise Map)
FLAG_IS_NACK       = 0x01  # Bit 0: 0 = Data Packet, 1 = NACK Packet
FLAG_IS_PIGGYBACK  = 0x02  # Bit 1: 0 = Fresh Data, 1 = Piggybacked Retransmission
FLAG_END_OF_BATCH  = 0x04  # Bit 2: 0 = Mid-Batch, 1 = END_OF_BATCH
FLAG_FIN           = 0x08  # Bit 3: 1 = Transfer Complete (EOF)
FLAG_SYN_CTRL      = 0x10  # Bit 4: 1 = Session Init / Metadata Control Packet
FLAG_COMPRESSED    = 0x20  # Bit 5: 1 = Payload compressed with LZ4 / zlib
FLAG_HEARTBEAT     = 0x40  # Bit 6: 1 = Silence Ping / Heartbeat probe
FLAG_RANGE_NACK    = 0x80  # Bit 7: 1 = Range-Encoded NACK (start_seq, count)


class PacketType(IntEnum):
    DATA = 0
    NACK = 1


@dataclass
class WarpHeader:
    app_id: int
    flags: int
    seq_num: int  # For DATA: packet seq. For NACK: cumulative ACK high-water mark

    @property
    def is_nack(self) -> bool:
        return bool(self.flags & FLAG_IS_NACK)

    @property
    def is_piggyback(self) -> bool:
        return bool(self.flags & FLAG_IS_PIGGYBACK)

    @property
    def is_end_of_batch(self) -> bool:
        return bool(self.flags & FLAG_END_OF_BATCH)

    @property
    def is_fin(self) -> bool:
        return bool(self.flags & FLAG_FIN)

    @property
    def is_syn_ctrl(self) -> bool:
        return bool(self.flags & FLAG_SYN_CTRL)

    @property
    def is_compressed(self) -> bool:
        return bool(self.flags & FLAG_COMPRESSED)

    @property
    def is_heartbeat(self) -> bool:
        return bool(self.flags & FLAG_HEARTBEAT)

    @property
    def is_range_nack(self) -> bool:
        return bool(self.flags & FLAG_RANGE_NACK)

    def pack(self) -> bytes:
        return HEADER_STRUCT.pack(self.app_id, self.flags, self.seq_num)

    @classmethod
    def unpack(cls, data: bytes) -> "WarpHeader":
        if len(data) < HEADER_SIZE:
            raise ValueError(f"Packet too small for WARP header: {len(data)} < {HEADER_SIZE}")
        app_id, flags, seq_num = HEADER_STRUCT.unpack_from(data, 0)
        return cls(app_id=app_id, flags=flags, seq_num=seq_num)


@dataclass
class WarpPacket:
    header: WarpHeader
    payload: bytes = b""

    def serialize(self) -> bytes:
        return self.header.pack() + self.payload

    @classmethod
    def deserialize(cls, raw: bytes) -> "WarpPacket":
        header = WarpHeader.unpack(raw)
        payload = raw[HEADER_SIZE:]
        return cls(header=header, payload=payload)


# --- Payload Encoders and Decoders ---

def encode_nack_payload_raw(missing_sequences: List[int]) -> bytes:
    """Encodes missing sequences as raw 4-byte big-endian integers."""
    return struct.pack(f"!{len(missing_sequences)}I", *missing_sequences)


def decode_nack_payload_raw(payload: bytes) -> List[int]:
    """Decodes raw 4-byte sequence list from NACK payload."""
    count = len(payload) // 4
    if count == 0:
        return []
    return list(struct.unpack(f"!{count}I", payload[:count * 4]))


def encode_nack_payload_ranges(ranges: List[Tuple[int, int]]) -> bytes:
    """
    Encodes missing gaps as (start_seq: u32, count: u16) pairs.
    Each range takes only 6 bytes, allowing hundreds of lost packets
    to fit inside a single unfragmented 60-byte UDP datagram.
    """
    out = bytearray()
    for start_seq, count in ranges:
        out.extend(struct.pack("!IH", start_seq, count))
    return bytes(out)


def decode_nack_payload_ranges(payload: bytes) -> List[Tuple[int, int]]:
    """Decodes (start_seq, count) ranges from NACK payload."""
    ranges = []
    offset = 0
    range_struct = struct.Struct("!IH")
    while offset + 6 <= len(payload):
        start_seq, count = range_struct.unpack_from(payload, offset)
        ranges.append((start_seq, count))
        offset += 6
    return ranges


def sequences_to_ranges(sequences: List[int]) -> List[Tuple[int, int]]:
    """Helper to convert a sorted list of missing sequences into contiguous (start, count) ranges."""
    if not sequences:
        return []
    sorted_seqs = sorted(sequences)
    ranges = []
    start = sorted_seqs[0]
    count = 1

    for s in sorted_seqs[1:]:
        if s == start + count:
            count += 1
        else:
            ranges.append((start, count))
            start = s
            count = 1
    ranges.append((start, count))
    return ranges


def ranges_to_sequences(ranges: List[Tuple[int, int]]) -> List[int]:
    """Helper to expand (start, count) ranges back into individual sequence numbers."""
    seqs = []
    for start, count in ranges:
        seqs.extend(range(start, start + count))
    return seqs


# --- Metadata / Session Control Payload ---
# Control format:
# [Total File Size: 8B (uint64)] [Total Packets: 4B (uint32)] [Chunk Size: 2B (uint16)]
# [SHA256 Hash: 32B] [Filename Length: 2B (uint16)] [Filename: UTF-8]
METADATA_HEADER_STRUCT = struct.Struct("!QIH32sH")

@dataclass
class SessionMetadata:
    file_name: str
    file_size: int
    total_packets: int
    chunk_size: int
    sha256_hash: bytes  # 32 bytes

    def serialize(self) -> bytes:
        fname_bytes = self.file_name.encode("utf-8")
        header = METADATA_HEADER_STRUCT.pack(
            self.file_size,
            self.total_packets,
            self.chunk_size,
            self.sha256_hash,
            len(fname_bytes)
        )
        return header + fname_bytes

    @classmethod
    def deserialize(cls, data: bytes) -> "SessionMetadata":
        if len(data) < METADATA_HEADER_STRUCT.size:
            raise ValueError("Data too small for session metadata")
        file_size, total_packets, chunk_size, sha256_hash, fname_len = METADATA_HEADER_STRUCT.unpack_from(data, 0)
        fname_bytes = data[METADATA_HEADER_STRUCT.size: METADATA_HEADER_STRUCT.size + fname_len]
        return cls(
            file_name=fname_bytes.decode("utf-8", errors="replace"),
            file_size=file_size,
            total_packets=total_packets,
            chunk_size=chunk_size,
            sha256_hash=sha256_hash
        )
