// Custom UDP Protocol Architecture: 7-byte Header Specification
// Reference: Custom UDP Protocol Architecture.pdf

pub const DEFAULT_APP_ID: u16 = 0x5741; // 'WA' in ASCII
pub const HEADER_SIZE: usize = 7;
pub const MAX_PACKET_SIZE: usize = 1472; // Standard Ethernet MTU (1500) - IP (20) - UDP (8)
pub const MAX_PAYLOAD_SIZE: usize = MAX_PACKET_SIZE - HEADER_SIZE; // 1465 bytes

// Bitwise flags (1 byte)
pub const FLAG_NACK: u8 = 0x01;          // Bit 0: 0 = Data, 1 = NACK Payload
pub const FLAG_PIGGYBACK: u8 = 0x02;     // Bit 1: 0 = Fresh Data, 1 = Piggybacked Retransmission
pub const FLAG_END_OF_BATCH: u8 = 0x04;  // Bit 2: 0 = Mid-Batch, 1 = END_OF_BATCH
pub const FLAG_EOF: u8 = 0x08;           // Bit 3: End of entire stream / file

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub app_id: u16,
    pub flags: u8,
    pub sequence: u32,
}

impl Header {
    pub fn new(app_id: u16, flags: u8, sequence: u32) -> Self {
        Self {
            app_id,
            flags,
            sequence,
        }
    }

    pub fn is_nack(&self) -> bool {
        (self.flags & FLAG_NACK) != 0
    }

    pub fn is_piggyback(&self) -> bool {
        (self.flags & FLAG_PIGGYBACK) != 0
    }

    pub fn is_end_of_batch(&self) -> bool {
        (self.flags & FLAG_END_OF_BATCH) != 0
    }

    pub fn is_eof(&self) -> bool {
        (self.flags & FLAG_EOF) != 0
    }

    pub fn encode(&self, dest: &mut [u8]) -> usize {
        assert!(dest.len() >= HEADER_SIZE);
        dest[0..2].copy_from_slice(&self.app_id.to_be_bytes());
        dest[2] = self.flags;
        dest[3..7].copy_from_slice(&self.sequence.to_be_bytes());
        HEADER_SIZE
    }

    pub fn parse(src: &[u8]) -> Option<(Self, &[u8])> {
        if src.len() < HEADER_SIZE {
            return None;
        }

        let app_id = u16::from_be_bytes([src[0], src[1]]);
        let flags = src[2];
        let sequence = u32::from_be_bytes([src[3], src[4], src[5], src[6]]);

        Some((Self { app_id, flags, sequence }, &src[HEADER_SIZE..]))
    }
}

// NACK Payloads: Packed 32-bit sequence numbers representing gaps
pub fn encode_nack_payload(missing_seqs: &[u32]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(missing_seqs.len() * 4);
    for &seq in missing_seqs {
        payload.extend_from_slice(&seq.to_be_bytes());
    }
    payload
}

pub fn decode_nack_payload(payload: &[u8]) -> Vec<u32> {
    let count = payload.len() / 4;
    let mut missing = Vec::with_capacity(count);
    for chunk in payload.chunks_exact(4) {
        let seq = u32::from_be_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        missing.push(seq);
    }
    missing
}
