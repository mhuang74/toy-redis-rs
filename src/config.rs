pub struct ReplicaMaster {
    pub hostname: String,
    pub port: usize,
}

pub struct AppConfig {
    pub replicaof: Option<ReplicaMaster>,
}

impl AppConfig {
    pub fn new() -> Self {
        AppConfig {
            replicaof: None,
        }
    }
}