use std::collections::HashMap;
use std::time::SystemTime;

pub struct Storage {
    store: HashMap<Vec<u8>, (Vec<u8>, Option<SystemTime>)>,
}

impl Storage {
    pub fn new() -> Self {
        Storage {
            store: HashMap::new(),
        }
    }
    // ... methods to interact with the key-value store ...
}