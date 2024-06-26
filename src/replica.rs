use crate::resp_protocol::RESPParser;
use crate::storage::Storage;
use crate::write_response;
use anyhow::{Error, Result};

use std::sync::{Arc, Mutex};
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// Handles connection to Replication Master
/// * Only handles WRITE requests and does not respond.
pub struct Replica {
    storage: Arc<Mutex<Storage>>,
    offset: usize,
}

impl Replica {
    pub fn new(storage: Arc<Mutex<Storage>>) -> Self {
        Replica { storage, offset: 0}
    }

    /// Increments the offset of the replica by the given amount.
    ///
    /// # Arguments
    ///
    /// * `amount` - The amount to increment the offset by.
    pub fn increment_offset(&mut self, amount: usize) {
        let old_offset = self.offset;
        self.offset += amount;
        println!("Incrementing offset from {} to {}", old_offset, self.offset);
    }

    /// Returns the current offset of the replica as a byte array.
    pub fn get_offset_as_bytes(&self) -> Vec<u8> {
        self.offset.to_string().into_bytes()
    }

    pub async fn handle_connection(
        &mut self,
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

        println!("[Replica] Handshake complete. Entering replication listen loop");

        let mut response_buffer = Vec::new();

        // enter listening loop
        loop {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

            let bytes_read: usize;

            if buffer.iter().all(|&x| x == 0) {
                // buffer empty, read from from stream
                bytes_read = stream.read(&mut buffer).await?;
                if bytes_read == 0 {
                    eprintln!("[Replica] Disconnected from Replication Master {}", address);
                    break;
                }
            } else {
                // reprocess entire buffer, except for the nulls at end
                let zero_count = buffer.iter().rev().take_while(|&&x| x == 0).count();
                bytes_read = buffer.len() - zero_count;
            }

            // only convert part of buffer with data read in!
            let request = &buffer[..bytes_read];
            println!(
                "[Replica] Received from {}: {}",
                address,
                RESPParser::bytes_to_escaped_string(request)
            );

            let commands = match RESPParser::parse(request) {
                Ok(commands) => {
                    // println!("Parsed request: {:?}", req_vec);
                    // clear entire buffer
                    buffer.fill(0);
                    commands
                }
                Err(e) => {
                    eprintln!(
                        "[Replica] {}. Req: {}",
                        e,
                        RESPParser::bytes_to_escaped_string(request)
                    );
                    buffer.fill(0);
                    continue; // Skip this iteration if there's an error
                }
            };

            for command in commands {
                let request_parts = command.request;

                let request_string = request_parts
                    .iter()
                    .map(|slice| String::from_utf8_lossy(slice).to_string())
                    .collect::<Vec<String>>()
                    .join(" ");
                println!("Processing command of {} raw bytes: {}", command.raw_request_bytes_length, request_string);

                if let Some(action) = request_parts.first() {
                    // clear response buffer

                    match action.as_slice() {
                        b"PING" | b"ping" | b"Ping" => {
                            self.increment_offset(command.raw_request_bytes_length);
                        }
                        b"SET" | b"set" | b"Set" => {
                            let set_args: [Option<&[u8]>; 4] = [
                                request_parts.get(1).map(|x| x.as_slice()),
                                request_parts.get(2).map(|x| x.as_slice()),
                                request_parts.get(3).map(|x| x.as_slice()),
                                request_parts.get(4).map(|x| x.as_slice()),
                            ];
                            match set_args {
                                [Some(var), Some(val), None, None] => {
                                    {
                                        let mut storage = self.storage.lock().unwrap();
                                        storage.set(var.to_vec(), val.to_vec(), None);
                                    }
                                    eprintln!(
                                        "[Replica] {} set to {}",
                                        String::from_utf8_lossy(var),
                                        String::from_utf8_lossy(val)
                                    );
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
                                        storage.set(
                                            var.to_vec(),
                                            val.to_vec(),
                                            Some(expiry_duration),
                                        );
                                    }
                                    eprintln!(
                                        "[Replica] {} set to {} with expiry {:?}",
                                        String::from_utf8_lossy(var),
                                        String::from_utf8_lossy(val),
                                        expiry_duration
                                    );
                                }
                                _ => {
                                    eprintln!("[Replica] Unsupported SET subcommand");
                                }
                            }

                            self.increment_offset(command.raw_request_bytes_length);

                        }
                        b"REPLCONF" | b"replconf" | b"ReplConf" => {

                            let set_args: [Option<&[u8]>; 2] = [
                                request_parts.get(1).map(|x| x.as_slice()),
                                request_parts.get(2).map(|x| x.as_slice()),
                            ];
                            match set_args {
                                [Some(b"GETACK"), Some(b"*")] => {
                                    let current_offset = self.get_offset_as_bytes();
                                    write_response!(
                                        &mut stream,
                                        &mut response_buffer,
                                        vec!["REPLCONF".as_bytes(),
                                             "ACK".as_bytes(),
                                             current_offset.as_slice()
                                        ]
                                    );
                                }
                                _ => {
                                    eprintln!("[Replica] Unsupported REPLCONF subcommand");
                                }
                            }

                            // update offset AFTER responding to REPLCONF command
                            self.increment_offset(command.raw_request_bytes_length);
                            
                        }
                        _ => {
                            // Handle other commands
                            eprintln!(
                                "[Replica] Unsupported command: {}",
                                String::from_utf8_lossy(action)
                            );
                        }
                    }
                } else {
                    eprintln!("[Replica] Received an empty RESP request");
                }
            }
        }
        Ok(())
    }
}

async fn send_and_wait_for_response(stream: &mut TcpStream, message: &str) -> Result<()> {
    println!(
        "[Replica Handshake] Sending: {}",
        RESPParser::bytes_to_escaped_string(message.as_bytes())
    );

    stream
        .write_all(message.as_bytes())
        .await
        .expect("Failed to send message to the master");

    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

    // let mut buffer: [u8; 1024] = [0; 1024];

    // // read response
    // let bytes_read = stream.read(&mut buffer).await?;

    // // only convert part of buffer with data read in!
    // let reply = &buffer[..bytes_read];
    // println!(
    //     "[Replica Handshake] Received: {}",
    //     RESPParser::bytes_to_escaped_string(reply)
    // );

    Ok(())
}
