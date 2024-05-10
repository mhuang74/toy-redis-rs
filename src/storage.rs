use std::collections::HashMap;
use std::time::{Duration, SystemTime};

pub struct Storage {
    store: HashMap<Vec<u8>, (Vec<u8>, Option<SystemTime>)>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            store: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: Vec<u8>, value: Vec<u8>, ttl: Option<Duration>) {
        let expiry = ttl.map(|duration| {
            SystemTime::now()
                .checked_add(duration)
                .expect("Failed to set expiry time")
        });
        self.store.insert(key, (value, expiry));
    }

    pub fn get(&mut self, key: &Vec<u8>) -> Option<Vec<u8>> {
        if let Some((value, expiry)) = self.store.get(key) {
            if let Some(expiry_time) = expiry {
                if SystemTime::now() > *expiry_time {
                    self.store.remove(key);
                    return None;
                }
            }
            Some(value.clone())
        } else {
            None
        }
    }
}
