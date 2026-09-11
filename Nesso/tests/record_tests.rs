use nesso::storage::record::{OpType, Record};

#[test]
fn test_roundtrip_basic() {
    let rec = Record::new(42, OpType::Created, 10, b"hello".to_vec());
    let encoded = rec.encode();
    let decoded = Record::decode(&encoded).expect("Decode failed");
    
    assert_eq!(decoded.id(), 42);
    assert_eq!(decoded.op_type(), OpType::Created);
    assert_eq!(decoded.priority(), 10);
    assert_eq!(decoded.payload(), b"hello");
}

#[test]
fn test_roundtrip_empty_payload() {
    let rec = Record::new(99, OpType::Acked, 0, vec![]);
    let encoded = rec.encode();
    let decoded = Record::decode(&encoded).expect("Decode failed");
    
    assert_eq!(decoded.id(), 99);
    assert_eq!(decoded.op_type(), OpType::Acked);
    assert_eq!(decoded.payload().len(), 0);
}

#[test]
fn test_roundtrip_large_payload() {
    let large_payload = vec![0x42; 5000]; // 5KB
    let rec = Record::new(1, OpType::Created, 5, large_payload.clone());
    let encoded = rec.encode();
    let decoded = Record::decode(&encoded).expect("Decode failed");
    
    assert_eq!(decoded.payload(), large_payload.as_slice());
}

#[test]
fn test_invalid_checksum() {
    let rec = Record::new(1, OpType::Created, 1, b"test".to_vec());
    let mut encoded = rec.encode();
    
    // Corrupt payload byte
    let len = encoded.len();
    encoded[len - 1] ^= 0xFF;
    
    assert!(Record::decode(&encoded).is_none(), "Should fail checksum validation");
}

#[test]
fn test_invalid_magic_byte() {
    let rec = Record::new(1, OpType::Created, 1, b"test".to_vec());
    let mut encoded = rec.encode();
    
    // Corrupt magic byte
    encoded[0] = 0x00;
    
    assert!(Record::decode(&encoded).is_none(), "Should fail magic byte validation");
}
