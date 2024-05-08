use std::sync::{Arc, Mutex};
use std::collections::VecDeque;
use anyhow::{Result, Error};

pub struct ReplicationLog {
    id: String,
    log_entries: Arc<Mutex<VecDeque<Vec<u8>>>>, // Shared log entries
    offset: usize,
}

impl ReplicationLog {
    pub fn new() -> Self {
        ReplicationLog {
            id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
            log_entries: Arc::new(Mutex::new(VecDeque::new())),
            offset: 0,
        }
    }

    pub fn push(&mut self, request: &[u8]) -> Result<(), Error> {
        let mut log = self.log_entries.lock().unwrap();
        log.push_back(request.to_vec());
        self.offset+=1;
        Ok(())
    }

    pub fn replication_id(&self) -> &str {
        &self.id
    }

    pub fn current_offset(&self) -> usize {
        self.offset
    }

    pub fn iterator(&self) -> ReplicationLogIterator {
        ReplicationLogIterator::new(Arc::clone(&self.log_entries))
    }
}

// A separate struct for traversing the log
pub struct ReplicationLogIterator {
    log_handle: Arc<Mutex<VecDeque<Vec<u8>>>>,
    offset: usize,
}

impl ReplicationLogIterator {
    pub fn new(log_handle: Arc<Mutex<VecDeque<Vec<u8>>>>) -> Self {
        ReplicationLogIterator {
            log_handle,
            offset: 0,
        }
    }

    pub fn next(&mut self) -> Option<Vec<u8>> {
        let log = self.log_handle.lock().unwrap();
        if self.offset < log.len() {
            let entry = log[self.offset].clone();
            self.offset += 1;
            Some(entry)
        } else {
            None
        }
    }

    // Optionally, if you want to support resetting the iterator
    pub fn reset(&mut self) {
        self.offset = 0;
    }
}