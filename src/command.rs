#[derive(Clone)]
pub struct Command {
    pub request: Vec<Vec<u8>>,
    pub raw_request_bytes_length: usize,
}


