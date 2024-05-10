use crate::config::ReplicaMaster;

use crate::resp_protocol::RESPParser;
use crate::storage::Storage;
use crate::write_response;
use anyhow::{Error, Result};

use std::sync::{Arc, Mutex};
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// Used when Redis is started in Replication Mode.
/// * Only handles WRITE requests and does not respond.
pub struct Replica {
    _master: ReplicaMaster,
    storage: Arc<Mutex<Storage>>,
}

impl Replica {
    pub fn new(master: ReplicaMaster, storage: Arc<Mutex<Storage>>) -> Self {
        Replica {
            _master: master,
            storage,
        }
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

        let mut buffer = [0; 1024];

        // PING
        send_and_wait_for_response(&mut stream, PING).await?;

        // REPLCONF to set listening port
        send_and_wait_for_response(&mut stream, REPL_CONF_PORT).await?;

        // REPLCONF to set capability
        send_and_wait_for_response(&mut stream, REPL_CONF_CAPABILITY).await?;

        // PSYNC
        send_and_wait_for_response(&mut stream, REPL_CONF_PSYNC).await?;
        // read in RDB file after PSYNC response
        let bytes_read = stream.read(&mut buffer).await?;
        println!("Received {} bytes of RDB file", bytes_read);

        // enter listening loop
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

            let request_vector = match RESPParser::parse(request) {
                Ok(req_vec) => {
                    // println!("Parsed request: {:?}", req_vec);
                    req_vec
                }
                Err(e) => {
                    eprintln!("Error parsing RESP: {}", e);
                    continue; // Skip this iteration if there's an error
                }
            };

            if let Some(command) = request_vector.first() {
                match command.as_slice() {
                    b"SET" | b"set" | b"Set" => {
                        let set_args: [Option<&[u8]>; 4] = [
                            request_vector.get(1).map(|x| x.as_slice()),
                            request_vector.get(2).map(|x| x.as_slice()),
                            request_vector.get(3).map(|x| x.as_slice()),
                            request_vector.get(4).map(|x| x.as_slice()),
                        ];
                        match set_args {
                            [Some(var), Some(val), None, None] => {
                                let mut storage = self.storage.lock().unwrap();
                                storage.set(var.to_vec(), val.to_vec(), None);
                            }
                            [Some(var), Some(val), Some(b"PX" | b"px"), Some(expiry)] => {
                                use std::time::Duration;
                                let expiry_duration = Duration::from_millis(
                                    String::from_utf8_lossy(expiry)
                                        .parse::<u64>()
                                        .expect("Invalid expiry format"),
                                );
                                {
                                    let mut storage = self.storage.lock().unwrap();
                                    storage.set(var.to_vec(), val.to_vec(), Some(expiry_duration));
                                }
                            }
                            _ => {}
                        }
                    }
                    b"INFO" | b"info" | b"Info" => {
                        match request_vector.get(1).unwrap().as_slice() {
                            b"REPLICATION" | b"replication" | b"Replication" => {
                                let mut response_buffer = Vec::new();
                                write_response!(
                                    &mut stream,
                                    &mut response_buffer,
                                    vec!["role:slave".as_bytes()]
                                );
                            }
                            _ => {
                                eprintln!("Unsupported INFO subcommand");
                            }
                        }
                    }
                    _ => {
                        eprintln!("Unsupported command: {}", String::from_utf8_lossy(command));
                    }
                }
            } else {
                eprintln!("Received an empty RESP request");
            }
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
