use crate::replication_log::ReplicationLog;
use crate::resp_protocol::RESPParser;
use crate::storage::Storage;
use anyhow::{anyhow, Error, Result};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// Used when Redis is started in Replication Mode.
/// * Only handles WRITE requests and does not respond.
pub struct Replica {
    storage: Arc<Mutex<Storage>>,
}

impl Replica {
    pub fn new(storage: Arc<Mutex<Storage>>) -> Self {
        Replica { storage }
    }

    pub async fn handle_connection(
        &self,
        mut stream: TcpStream,
        address: &str,
    ) -> Result<(), Error> {
        const PING: &str = "*1\r\n$4\r\nPING\r\n";
        const REPL_CONF_PORT: &str =
            "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n";
        const REPL_CONF_CAPABILITY: &str = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
        const REPL_CONF_PSYNC: &str = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";

        // PING
        send_and_wait_for_response(&mut stream, PING).await?;

        // REPLCONF to set listening port
        send_and_wait_for_response(&mut stream, REPL_CONF_PORT).await?;

        // REPLCONF to set capability
        send_and_wait_for_response(&mut stream, REPL_CONF_CAPABILITY).await?;

        // PSYNC
        send_and_wait_for_response(&mut stream, REPL_CONF_PSYNC).await?;

        // enter listening loop
        let mut buffer = [0; 1024];
        loop {
            let bytes_read = stream.read(&mut buffer).await?;

            if bytes_read == 0 {
                eprintln!("Disconncted from Replication Master at {}", address);
                break;
            }

            // only convert part of buffer with data read in!
            let request = &buffer[..bytes_read];
            println!(
                "Master[{}]: {}",
                address,
                RESPParser::bytes_to_escaped_string(request)
            );

            match RESPParser::parse(request) {
                Ok(req_vec) => {
                    println!("Parsed request: {:?}", req_vec);
                }
                Err(e) => {
                    eprintln!("Error parsing RESP: {}", e);
                }
            }

            // TODO: write to storage
        }

        Ok(())
    }
}

async fn send_and_wait_for_response(stream: &mut TcpStream, message: &str) -> Result<()> {
    println!(
        "[Replica] Sending: {}",
        RESPParser::bytes_to_escaped_string(message.as_bytes())
    );

    let mut buffer: [u8; 1024] = [0; 1024];

    stream
        .write_all(message.as_bytes())
        .await
        .expect("Failed to send message to the master");

    let bytes_read = stream.read(&mut buffer).await?;

    // only convert part of buffer with data read in!
    let reply = &buffer[..bytes_read];
    println!(
        "[Replica] Received: {}",
        RESPParser::bytes_to_escaped_string(reply)
    );

    Ok(())
}
