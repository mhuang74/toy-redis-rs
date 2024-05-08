use anyhow::Error;
use redis_protocol_parser::{RedisProtocolParser, RESP};

pub struct RESPParser;

impl RESPParser {
    pub fn new() -> Self {
        RESPParser {}
    }

    pub fn parse_resp(request: &[u8]) -> Result<RESP, Error> {
        Ok(RESP::Nil)
    }
}