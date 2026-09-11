use crc32fast::Hasher;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpType {
    Created = 0,
    Leased = 1,
    Acked = 2,
    Nacked = 3,
    Expired = 4,
    DeadLettered = 5,
}

#[derive(Debug, Clone)]
pub struct Record {
    id: u64,
    op_type: OpType,
    priority: u8,
    payload: Vec<u8>,
}

pub const MAGIC_BYTE: u8 = 0x4E; // 'N'
// Header size: Magic(1) + Checksum(4) + OpType(1) + Priority(1) + ID(8) + PayloadLen(4) = 19 bytes
pub const HEADER_SIZE: usize = 19;

impl Record {
    pub fn new(id: u64, op_type: OpType, priority: u8, payload: Vec<u8>) -> Self {
        Self { id, op_type, priority, payload }
    }

    pub fn id(&self) -> u64 { self.id }
    pub fn op_type(&self) -> OpType { self.op_type }
    pub fn priority(&self) -> u8 { self.priority }
    pub fn payload(&self) -> &[u8] { &self.payload }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(HEADER_SIZE + self.payload.len());
        buf.push(MAGIC_BYTE);
        buf.extend_from_slice(&[0, 0, 0, 0]); // CRC32 checksum placeholder

        let start_of_data = buf.len();
        buf.push(self.op_type as u8);
        buf.push(self.priority);
        buf.extend_from_slice(&self.id.to_be_bytes());
        buf.extend_from_slice(&(self.payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.payload);

        let mut hasher = Hasher::new();
        hasher.update(&buf[start_of_data..]);
        let checksum = hasher.finalize();

        buf[1..5].copy_from_slice(&checksum.to_be_bytes());
        buf
    }

    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < HEADER_SIZE {
            return None;
        }
        if data[0] != MAGIC_BYTE {
            return None;
        }

        let expected_checksum = u32::from_be_bytes(data[1..5].try_into().unwrap());
        
        let mut hasher = Hasher::new();
        hasher.update(&data[5..]);
        if hasher.finalize() != expected_checksum {
            return None;
        }

        let op_type = match data[5] {
            0 => OpType::Created,
            1 => OpType::Leased,
            2 => OpType::Acked,
            3 => OpType::Nacked,
            4 => OpType::Expired,
            5 => OpType::DeadLettered,
            _ => return None,
        };

        let priority = data[6];
        let id = u64::from_be_bytes(data[7..15].try_into().unwrap());
        let payload_len = u32::from_be_bytes(data[15..19].try_into().unwrap()) as usize;

        if data.len() < HEADER_SIZE + payload_len {
            return None;
        }

        let payload = data[19..19 + payload_len].to_vec();

        Some(Self { id, op_type, priority, payload })
    }
}
