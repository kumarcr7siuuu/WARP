"""
Unit tests for WARP wire protocol, 7-byte header codec, and flag bit manipulation.
"""

import unittest
from warp.protocol import (
    WarpHeader,
    WarpPacket,
    HEADER_SIZE,
    FLAG_IS_NACK,
    FLAG_IS_PIGGYBACK,
    FLAG_END_OF_BATCH,
    FLAG_FIN,
    FLAG_RANGE_NACK,
    sequences_to_ranges,
    ranges_to_sequences,
    encode_nack_payload_ranges,
    decode_nack_payload_ranges,
    encode_nack_payload_raw,
    decode_nack_payload_raw,
    SessionMetadata,
)


class TestWarpProtocol(unittest.TestCase):
    def test_header_size(self):
        """Header must be EXACTLY 7 bytes."""
        hdr = WarpHeader(app_id=0x5750, flags=0, seq_num=100)
        packed = hdr.pack()
        self.assertEqual(len(packed), 7)
        self.assertEqual(HEADER_SIZE, 7)

    def test_header_roundtrip(self):
        """Test pack and unpack roundtrip with boundary values."""
        hdr = WarpHeader(
            app_id=0xBEEF,
            flags=FLAG_IS_NACK | FLAG_END_OF_BATCH,
            seq_num=0xDEADBEEF
        )
        raw = hdr.pack()
        unpacked = WarpHeader.unpack(raw)

        self.assertEqual(unpacked.app_id, 0xBEEF)
        self.assertEqual(unpacked.flags, FLAG_IS_NACK | FLAG_END_OF_BATCH)
        self.assertEqual(unpacked.seq_num, 0xDEADBEEF)
        self.assertTrue(unpacked.is_nack)
        self.assertFalse(unpacked.is_piggyback)
        self.assertTrue(unpacked.is_end_of_batch)

    def test_flag_properties(self):
        """Test bitwise flag getters."""
        hdr = WarpHeader(
            app_id=1,
            flags=FLAG_IS_PIGGYBACK | FLAG_FIN | FLAG_RANGE_NACK,
            seq_num=42
        )
        self.assertFalse(hdr.is_nack)
        self.assertTrue(hdr.is_piggyback)
        self.assertFalse(hdr.is_end_of_batch)
        self.assertTrue(hdr.is_fin)
        self.assertTrue(hdr.is_range_nack)

    def test_packet_serialization(self):
        """Test full WarpPacket serialize and deserialize."""
        hdr = WarpHeader(app_id=0x5750, flags=FLAG_END_OF_BATCH, seq_num=500)
        payload = b"Hello, WARP protocol!"
        pkt = WarpPacket(header=hdr, payload=payload)

        raw = pkt.serialize()
        self.assertEqual(len(raw), 7 + len(payload))

        deserialized = WarpPacket.deserialize(raw)
        self.assertEqual(deserialized.header.seq_num, 500)
        self.assertTrue(deserialized.header.is_end_of_batch)
        self.assertEqual(deserialized.payload, payload)

    def test_range_nack_encoding(self):
        """Test run-length range encoding for missing sequence gaps."""
        # Consecutive gaps: 10..15, 20..22, 50
        missing = [10, 11, 12, 13, 14, 15, 20, 21, 22, 50]
        ranges = sequences_to_ranges(missing)
        expected_ranges = [(10, 6), (20, 3), (50, 1)]
        self.assertEqual(ranges, expected_ranges)

        # Convert back
        recovered_seqs = ranges_to_sequences(ranges)
        self.assertEqual(recovered_seqs, missing)

        # Encode to bytes and decode
        encoded_bytes = encode_nack_payload_ranges(ranges)
        self.assertEqual(len(encoded_bytes), 3 * 6)  # 3 ranges * 6 bytes = 18 bytes
        decoded_ranges = decode_nack_payload_ranges(encoded_bytes)
        self.assertEqual(decoded_ranges, expected_ranges)

    def test_session_metadata_roundtrip(self):
        """Test SessionMetadata serialize and deserialize."""
        meta = SessionMetadata(
            file_name="test_dataset.tar.gz",
            file_size=104857600,
            total_packets=71576,
            chunk_size=1465,
            sha256_hash=b"\xab" * 32
        )
        data = meta.serialize()
        decoded = SessionMetadata.deserialize(data)

        self.assertEqual(decoded.file_name, "test_dataset.tar.gz")
        self.assertEqual(decoded.file_size, 104857600)
        self.assertEqual(decoded.total_packets, 71576)
        self.assertEqual(decoded.chunk_size, 1465)
        self.assertEqual(decoded.sha256_hash, b"\xab" * 32)


if __name__ == "__main__":
    unittest.main()
