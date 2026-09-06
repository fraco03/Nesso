use std::collections::{HashMap, BinaryHeap};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use std::io;
use std::thread;
use std::time::Duration;
use std::path::Path;

use super::record::{OpType, Record};
use super::wal::{Wal, WalReader};

#[derive(Debug, Clone)]
pub struct TaskRef {
    pub id: u64,
    pub priority: u8,
    pub retries: u8,
}

impl PartialEq for TaskRef {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.priority == other.priority
    }
}
impl Eq for TaskRef {}

impl PartialOrd for TaskRef {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TaskRef {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority.cmp(&other.priority)
            .then_with(|| other.id.cmp(&self.id))
    }
}

#[derive(Debug, Clone)]
pub struct LeaseInfo {
    pub consumer_id: u32,
    pub expire_timestamp: u64,
    pub retries: u8,
    pub priority: u8,
}

const MAX_RETRIES: u8 = 3;

pub struct EngineState {
    pub wal: Wal,
    pub next_id: u64,
    pub index: HashMap<u64, (u64, u64)>,      // ID -> (segment_id, offset)
    pub data_index: HashMap<u64, (u64, u64)>, // ID -> (segment_id, offset)
    pub ready_queue: BinaryHeap<TaskRef>,
    pub leased: HashMap<u64, LeaseInfo>,
}

#[derive(Clone)]
pub struct Engine {
    pub inner: Arc<Mutex<EngineState>>,
    pub reader: Arc<WalReader>,
}

impl Engine {
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref();
        let wal = Wal::open(dir, None)?;
        let reader = Arc::new(WalReader::new(dir.to_path_buf()));
        
        let mut state = EngineState {
            wal,
            next_id: 1,
            index: HashMap::new(),
            data_index: HashMap::new(),
            ready_queue: BinaryHeap::new(),
            leased: HashMap::new(),
        };

        state.recover(&reader)?;

        let engine = Self {
            inner: Arc::new(Mutex::new(state)),
            reader,
        };

        engine.start_expiration_thread();
        Ok(engine)
    }

    pub fn push(&self, payload: Vec<u8>, priority: u8) -> io::Result<u64> {
        let mut state = self.inner.lock().unwrap();
        let id = state.next_id;
        state.next_id += 1;

        let record = Record::new(id, OpType::Created, priority, payload);
        let (seg, off) = state.wal.append(&record)?;

        state.index.insert(id, (seg, off));
        state.data_index.insert(id, (seg, off));
        state.ready_queue.push(TaskRef { id, priority, retries: 0 });

        Ok(id)
    }

    pub fn pop_and_lease(&self, consumer_id: u32, lease_ttl_secs: u64) -> io::Result<Option<(Record, u8)>> {
        let id;
        let priority;
        let retries;
        let data_seg;
        let data_off;
        
        {
            let mut state = self.inner.lock().unwrap();
            
            let task_ref = match state.ready_queue.pop() {
                Some(t) => t,
                None => return Ok(None),
            };

            id = task_ref.id;
            priority = task_ref.priority;
            retries = task_ref.retries;

            let expire_timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() + lease_ttl_secs;

            let lease_info = LeaseInfo {
                consumer_id,
                expire_timestamp,
                retries,
                priority,
            };

            let mut payload = Vec::with_capacity(13);
            payload.extend_from_slice(&consumer_id.to_be_bytes());
            payload.extend_from_slice(&expire_timestamp.to_be_bytes());
            payload.push(retries);

            let leased_record = Record::new(id, OpType::Leased, priority, payload);
            let (seg, off) = state.wal.append(&leased_record)?;
            
            state.index.insert(id, (seg, off));
            state.leased.insert(id, lease_info);

            let &(seg_data, off_data) = state.data_index.get(&id)
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Missing data offset"))?;
                
            data_seg = seg_data;
            data_off = off_data;
        } 

        // I/O read-only eseguito concorrentemente fuori dal lock
        let original_record = self.reader.read_at(data_seg, data_off)?
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Record not found at offset"))?;

        Ok(Some((original_record, retries)))
    }

    pub fn ack(&self, id: u64, consumer_id: u32) -> io::Result<()> {
        let mut state = self.inner.lock().unwrap();

        if let Some(lease) = state.leased.get(&id) {
            if lease.consumer_id != consumer_id {
                return Err(io::Error::new(io::ErrorKind::PermissionDenied, "Task leased to another consumer"));
            }
            
            let ack_record = Record::new(id, OpType::Acked, lease.priority, vec![]);
            let (seg, off) = state.wal.append(&ack_record)?;
            
            state.index.insert(id, (seg, off));
            state.leased.remove(&id);
            state.data_index.remove(&id);
            Ok(())
        } else {
            Err(io::Error::new(io::ErrorKind::NotFound, "Task not found or not leased"))
        }
    }

    pub fn nack(&self, id: u64, consumer_id: u32) -> io::Result<()> {
        let mut state = self.inner.lock().unwrap();

        let lease = state.leased.get(&id)
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Task not found or not leased"))?;

        if lease.consumer_id != consumer_id {
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, "Task leased to another consumer"));
        }

        Self::process_task_failure(&mut state, id, lease.priority, lease.retries, OpType::Nacked)
    }

    fn process_task_failure(state: &mut EngineState, id: u64, priority: u8, current_retries: u8, failure_type: OpType) -> io::Result<()> {
        let new_retries = current_retries + 1;
        let payload = vec![new_retries];
        
        let op = if new_retries >= MAX_RETRIES {
            OpType::DeadLettered
        } else {
            failure_type
        };

        let record = Record::new(id, op, priority, payload);
        let (seg, off) = state.wal.append(&record)?;
        
        state.index.insert(id, (seg, off));
        state.leased.remove(&id);

        if op == OpType::DeadLettered {
            state.data_index.remove(&id);
        } else {
            state.ready_queue.push(TaskRef { id, priority, retries: new_retries });
        }

        Ok(())
    }

    fn start_expiration_thread(&self) {
        let engine = Arc::clone(&self.inner);
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(1));
                
                let mut expired_tasks = Vec::new();
                
                {
                    let state = match engine.lock() {
                        Ok(guard) => guard,
                        Err(poisoned) => {
                            eprintln!("Warning: Engine Mutex was poisoned, attempting recovery for background expiration.");
                            poisoned.into_inner()
                        }
                    };

                    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

                    for (&id, lease) in state.leased.iter() {
                        if now >= lease.expire_timestamp {
                            expired_tasks.push((id, lease.priority, lease.retries));
                        }
                    }
                }
                
                if !expired_tasks.is_empty() {
                    let mut state = engine.lock().unwrap();
                    for (id, priority, retries) in expired_tasks {
                        let _ = Self::process_task_failure(&mut state, id, priority, retries, OpType::Expired);
                    }
                }
            }
        });
    }

    pub fn status(&self) -> (usize, usize) {
        let state = self.inner.lock().unwrap();
        (state.ready_queue.len(), state.leased.len())
    }

    pub fn sync(&self) -> io::Result<()> {
        let state = self.inner.lock().unwrap();
        state.wal.sync()
    }
}

impl EngineState {
    fn recover(&mut self, reader: &WalReader) -> io::Result<()> {
        for result in self.wal.iter_all()? {
            let (segment_id, offset, record) = result?;
            
            if record.id() >= self.next_id {
                self.next_id = record.id() + 1;
            }

            self.index.insert(record.id(), (segment_id, offset));
            
            if record.op_type() == OpType::Created {
                self.data_index.insert(record.id(), (segment_id, offset));
            }
        }

        let index_copy = self.index.clone();
        for (id, &(segment, offset)) in index_copy.iter() {
            if let Some(record) = reader.read_at(segment, offset)? {
                match record.op_type() {
                    OpType::Created | OpType::Nacked | OpType::Expired => {
                        let retries = if record.op_type() == OpType::Created {
                            0
                        } else {
                            record.payload().first().copied().unwrap_or(0)
                        };
                        self.ready_queue.push(TaskRef { id: *id, priority: record.priority(), retries });
                    }
                    OpType::Leased => {
                        let payload = record.payload();
                        if payload.len() >= 13 {
                            let consumer_id = u32::from_be_bytes(payload[0..4].try_into().unwrap());
                            let expire_timestamp = u64::from_be_bytes(payload[4..12].try_into().unwrap());
                            let retries = payload[12];
                            
                            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                            if expire_timestamp > now {
                                self.leased.insert(*id, LeaseInfo {
                                    consumer_id,
                                    expire_timestamp,
                                    retries,
                                    priority: record.priority(),
                                });
                            } else {
                                self.ready_queue.push(TaskRef { id: *id, priority: record.priority(), retries });
                            }
                        } else {
                            self.ready_queue.push(TaskRef { id: *id, priority: record.priority(), retries: 0 });
                        }
                    }
                    OpType::Acked | OpType::DeadLettered => {
                        self.data_index.remove(id);
                        self.index.remove(id);
                    }
                }
            }
        }
        Ok(())
    }
}
