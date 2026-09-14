use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use super::record::{Record, HEADER_SIZE, MAGIC_BYTE};
use super::group_commit::{perform_sync, SyncMode};

pub const DEFAULT_MAX_SEGMENT_SIZE: u64 = 64 * 1024 * 1024; // 64 MB
const MAX_LRU_CACHE_SIZE: usize = 8;

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub struct CompactionStats {
    pub closed_segments_compacted: usize,
    pub surviving_records: usize,
    pub new_segment_id: u64,
}

#[inline]
fn extract_payload_len(header: &[u8]) -> usize {
    u32::from_be_bytes(header[15..19].try_into().unwrap()) as usize
}

pub struct WalReader {
    dir: PathBuf,
    read_cache: Mutex<Vec<(u64, File)>>,
}

impl WalReader {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            dir,
            read_cache: Mutex::new(Vec::with_capacity(MAX_LRU_CACHE_SIZE)),
        }
    }

    pub fn clear_cache(&self) {
        let mut cache = self.read_cache.lock().unwrap();
        cache.clear();
    }

    pub fn read_at(&self, segment_id: u64, offset: u64) -> io::Result<Option<Record>> {
        let mut cache = self.read_cache.lock().unwrap();

        let file_idx = cache.iter().position(|(id, _)| *id == segment_id);
        let file = if let Some(idx) = file_idx {
            let entry = cache.remove(idx);
            cache.push(entry);
            &mut cache.last_mut().unwrap().1
        } else {
            let path = self.dir.join(format!("nesso.{:05}.wal", segment_id));
            let f = File::open(&path)?;
            if cache.len() >= MAX_LRU_CACHE_SIZE {
                cache.remove(0);
            }
            cache.push((segment_id, f));
            &mut cache.last_mut().unwrap().1
        };

        file.seek(SeekFrom::Start(offset))?;

        let mut header = vec![0u8; HEADER_SIZE];
        if let Err(e) = file.read_exact(&mut header) {
            if e.kind() == io::ErrorKind::UnexpectedEof { return Ok(None); }
            return Err(e);
        }

        if header[0] != MAGIC_BYTE {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "Corrupted magic byte"));
        }

        let payload_len = extract_payload_len(&header);
        let mut payload = vec![0u8; payload_len];

        if let Err(e) = file.read_exact(&mut payload) {
            if e.kind() == io::ErrorKind::UnexpectedEof { return Ok(None); }
            return Err(e);
        }

        let mut full_record = Vec::with_capacity(HEADER_SIZE + payload_len);
        full_record.extend_from_slice(&header);
        full_record.extend_from_slice(&payload);

        Ok(Record::decode(&full_record))
    }
}

pub struct Wal {
    dir: PathBuf,
    active_segment_id: u64,
    active_file: Arc<File>,
    current_size: u64,
    max_segment_size: u64,
}

impl Wal {
    pub fn open(dir: impl AsRef<Path>, max_segment_size: Option<u64>) -> io::Result<Self> {
        let dir = dir.as_ref();
        
        if !dir.exists() {
            fs::create_dir_all(dir)?;
        }

        let mut max_id = 0;
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if name.ends_with(".compacting") || name.ends_with(".tmp") {
                        let _ = fs::remove_file(&path);
                        continue;
                    }
                    if let Some(id_str) = name.strip_prefix("nesso.").and_then(|s| s.strip_suffix(".wal")) {
                        if let Ok(id) = id_str.parse::<u64>() {
                            if id > max_id { max_id = id; }
                        }
                    }
                }
            }
        }

        if max_id == 0 { max_id = 1; }

        let active_path = dir.join(format!("nesso.{:05}.wal", max_id));
        let mut active_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&active_path)?;

        let current_size = active_file.seek(SeekFrom::End(0))?;

        Ok(Self {
            dir: dir.to_path_buf(),
            active_segment_id: max_id,
            active_file: Arc::new(active_file),
            current_size,
            max_segment_size: max_segment_size.unwrap_or(DEFAULT_MAX_SEGMENT_SIZE),
        })
    }

    fn rotate(&mut self) -> io::Result<()> {
        perform_sync(&self.active_file, SyncMode::Standard)?;
        self.active_segment_id += 1;
        
        let active_path = self.dir.join(format!("nesso.{:05}.wal", self.active_segment_id));
        let new_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(true)
            .open(&active_path)?;
            
        self.active_file = Arc::new(new_file);
        self.current_size = 0;
        Ok(())
    }

    pub fn append(&mut self, record: &Record) -> io::Result<(u64, u64)> {
        if self.current_size >= self.max_segment_size {
            self.rotate()?;
        }

        let bytes = record.encode();
        let offset_in_segment = self.current_size;

        if let Err(e) = (&*self.active_file).write_all(&bytes) {
            let _ = self.active_file.set_len(offset_in_segment);
            return Err(e);
        }

        self.current_size += bytes.len() as u64;
        Ok((self.active_segment_id, offset_in_segment))
    }

    pub fn sync(&self) -> io::Result<()> {
        perform_sync(&self.active_file, SyncMode::Standard)
    }

    pub fn active_file_arc(&self) -> Arc<File> {
        Arc::clone(&self.active_file)
    }

    pub fn active_file_clone(&self) -> io::Result<File> {
        self.active_file.try_clone()
    }

    pub fn active_segment_id(&self) -> u64 {
        self.active_segment_id
    }

    pub fn current_size(&self) -> u64 {
        self.current_size
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn compact(&mut self) -> io::Result<Option<CompactionStats>> {
        let res = self.compact_with_offsets()?;
        Ok(res.map(|(stats, _)| stats))
    }

    /// Discovers all closed segments with id < active_segment_id.
    pub fn plan_compaction(dir: &Path, active_segment_id: u64) -> io::Result<Option<Vec<u64>>> {
        let mut closed_segments = Vec::new();
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if let Some(id_str) = name.strip_prefix("nesso.").and_then(|s| s.strip_suffix(".wal")) {
                        if let Ok(id) = id_str.parse::<u64>() {
                            if id < active_segment_id {
                                closed_segments.push(id);
                            }
                        }
                    }
                }
            }
        }
        closed_segments.sort_unstable();

        if closed_segments.is_empty() {
            Ok(None)
        } else {
            Ok(Some(closed_segments))
        }
    }

    /// Phase 1 (Non-Blocking I/O): Reads closed segments, performs global deduplication,
    /// writes surviving records to atomic temporary file `nesso.00001.compacting`,
    /// and issues `sync_data()`. Can be executed without holding any locks.
    pub fn execute_compaction_phase1(dir: &Path, closed_segments: &[u64]) -> io::Result<(CompactionStats, HashMap<u64, u64>)> {
        let mut latest_records: HashMap<u64, Record> = HashMap::new();

        for &seg_id in closed_segments {
            let path = dir.join(format!("nesso.{:05}.wal", seg_id));
            let mut file = File::open(&path)?;

            loop {
                let mut header = [0u8; HEADER_SIZE];
                match file.read_exact(&mut header) {
                    Ok(()) => {}
                    Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e),
                }

                if header[0] != MAGIC_BYTE {
                    break;
                }

                let payload_len = extract_payload_len(&header);
                let mut payload = vec![0u8; payload_len];
                if let Err(e) = file.read_exact(&mut payload) {
                    if e.kind() == io::ErrorKind::UnexpectedEof { break; }
                    return Err(e);
                }

                let mut full_record = Vec::with_capacity(HEADER_SIZE + payload_len);
                full_record.extend_from_slice(&header);
                full_record.extend_from_slice(&payload);

                if let Some(record) = Record::decode(&full_record) {
                    match record.op_type() {
                        super::record::OpType::Acked | super::record::OpType::DeadLettered => {
                            latest_records.remove(&record.id());
                        }
                        _ => {
                            latest_records.insert(record.id(), record);
                        }
                    }
                }
            }
        }

        // Write to atomic temporary file: nesso.00001.compacting
        let temp_path = dir.join("nesso.00001.compacting");
        let mut temp_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&temp_path)?;

        let mut surviving_ids: Vec<u64> = latest_records.keys().copied().collect();
        surviving_ids.sort_unstable();

        let mut new_offsets = HashMap::new();
        let mut current_offset = 0u64;

        for id in &surviving_ids {
            let record = &latest_records[id];
            let bytes = record.encode();
            temp_file.write_all(&bytes)?;
            new_offsets.insert(*id, current_offset);
            current_offset += bytes.len() as u64;
        }

        perform_sync(&temp_file, SyncMode::Standard)?;
        drop(temp_file);

        let stats = CompactionStats {
            closed_segments_compacted: closed_segments.len(),
            surviving_records: surviving_ids.len(),
            new_segment_id: 1,
        };

        Ok((stats, new_offsets))
    }

    /// Phase 2 (Atomic Swap): Atomically renames `nesso.00001.compacting` over `nesso.00001.wal`
    /// and unlinks obsolete closed segments. Must be executed under lock alongside in-memory index updates.
    pub fn execute_compaction_phase2(dir: &Path, closed_segments: &[u64]) -> io::Result<()> {
        let temp_path = dir.join("nesso.00001.compacting");
        let dest_path = dir.join("nesso.00001.wal");
        fs::rename(&temp_path, &dest_path)?;

        for &seg_id in closed_segments {
            if seg_id > 1 {
                let p = dir.join(format!("nesso.{:05}.wal", seg_id));
                let _ = fs::remove_file(p);
            }
        }

        Ok(())
    }

    pub fn compact_with_offsets(&mut self) -> io::Result<Option<(CompactionStats, HashMap<u64, u64>)>> {
        let closed_segments = match Self::plan_compaction(&self.dir, self.active_segment_id)? {
            Some(segs) => segs,
            None => return Ok(None),
        };

        let (stats, new_offsets) = Self::execute_compaction_phase1(&self.dir, &closed_segments)?;
        Self::execute_compaction_phase2(&self.dir, &closed_segments)?;

        Ok(Some((stats, new_offsets)))
    }

    pub fn iter_all(&self) -> io::Result<WalIteratorAll> {
        let mut segment_ids = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if let Some(id_str) = name.strip_prefix("nesso.").and_then(|s| s.strip_suffix(".wal")) {
                        if let Ok(id) = id_str.parse::<u64>() {
                            segment_ids.push(id);
                        }
                    }
                }
            }
        }
        segment_ids.sort_unstable();

        Ok(WalIteratorAll {
            dir: self.dir.clone(),
            segment_ids,
            current_segment_idx: 0,
            current_file: None,
            current_segment_id: 0,
            current_offset: 0,
        })
    }
}

pub struct WalIteratorAll {
    dir: PathBuf,
    segment_ids: Vec<u64>,
    current_segment_idx: usize,
    current_file: Option<File>,
    current_segment_id: u64,
    current_offset: u64,
}

impl Iterator for WalIteratorAll {
    type Item = io::Result<(u64, u64, Record)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.current_file.is_none() {
                if self.current_segment_idx >= self.segment_ids.len() {
                    return None;
                }
                self.current_segment_id = self.segment_ids[self.current_segment_idx];
                let path = self.dir.join(format!("nesso.{:05}.wal", self.current_segment_id));
                match File::open(&path) {
                    Ok(f) => {
                        self.current_file = Some(f);
                        self.current_offset = 0;
                    }
                    Err(e) => return Some(Err(e)),
                }
            }

            let file = self.current_file.as_mut().unwrap();
            let mut header = [0u8; HEADER_SIZE];
            let offset_in_segment = self.current_offset;

            match file.read_exact(&mut header) {
                Ok(()) => {}
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    self.current_file = None;
                    self.current_segment_idx += 1;
                    continue;
                }
                Err(e) => return Some(Err(e)),
            }

            if header[0] != MAGIC_BYTE {
                self.current_file = None;
                self.current_segment_idx += 1;
                continue;
            }

            let payload_len = extract_payload_len(&header);
            let mut payload = vec![0u8; payload_len];

            if let Err(e) = file.read_exact(&mut payload) {
                if e.kind() == io::ErrorKind::UnexpectedEof {
                    self.current_file = None;
                    self.current_segment_idx += 1;
                    continue;
                }
                return Some(Err(e));
            }

            let mut full_record = Vec::with_capacity(HEADER_SIZE + payload_len);
            full_record.extend_from_slice(&header);
            full_record.extend_from_slice(&payload);

            self.current_offset += HEADER_SIZE as u64 + payload_len as u64;

            match Record::decode(&full_record) {
                Some(record) => return Some(Ok((self.current_segment_id, offset_in_segment, record))),
                None => continue,
            }
        }
    }
}
