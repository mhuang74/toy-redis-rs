use crate::replication_log::ReplicationLog;
use crate::replication_log::ReplicationLogIterator;
use crate::resp_protocol::RESPParser;
use crate::storage::Storage;
use crate::write_response;
use anyhow::{Error, Result};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

/// Used when Redis is started in Normal/Master Mode.
/// * For Client connections, handles both READ and WRITE requests
/// * For Replication connections, forward WRITE requests from Replication Log
pub struct Server {
    replication_log: Arc<Mutex<ReplicationLog>>,
    storage: Arc<Mutex<Storage>>,
}

impl Server {
    pub fn new(replication_log: Arc<Mutex<ReplicationLog>>, storage: Arc<Mutex<Storage>>) -> Self {
        Server {
            replication_log,
            storage,
        }
    }

    pub async fn handle_connection(
        &self,
        addr: SocketAddr,
        mut stream: TcpStream,
    ) -> Result<(), Error> {
        let mut replica: bool = false;
        let mut buffer = [0; 1024];
        let mut response_buffer = Vec::new();

        loop {
            let bytes_read = stream.read(&mut buffer).await?;

            if bytes_read == 0 {
                eprintln!("[{}]: Disconncted", addr);
                break;
            }

            // only convert part of buffer with data read in!
            let request = &buffer[..bytes_read];
            println!(
                "[{}]: {}",
                addr,
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
                // clear response buffer
                response_buffer.clear();

                match command.as_slice() {
                    b"PING" | b"ping" | b"Ping" => {
                        write_response!(&mut stream, &mut response_buffer, vec!["PONG".as_bytes()]);
                    }
                    b"ECHO" | b"echo" | b"Echo" => {
                        let args = request_vector[1..].to_vec();
                        write_response!(&mut stream, &mut response_buffer, args);
                    }
                    b"REPLCONF" | b"replconf" | b"Replconf" => {
                        write_response!(&mut stream, &mut response_buffer, vec!["OK".as_bytes()]);
                    }
                    b"GET" | b"get" | b"Get" => {
                        if let Some(var_bytes) = request_vector.get(1) {
                            let key = var_bytes.to_vec();
                            let val: Option<Vec<u8>>;
                            {
                                let mut storage = self.storage.lock().unwrap();
                                val = storage.get(&key);
                            }
                            if val.is_some() {
                                let response_val = val.unwrap().to_owned();
                                write_response!(
                                    &mut stream,
                                    &mut response_buffer,
                                    vec![&response_val]
                                );
                            }
                        }
                    }
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
                            _ => {
                                eprintln!("Unsupported SET subcommand");
                            }
                        }

                        // propagate SET via replication log
                        {
                            let mut repl_log_guard = self.replication_log.lock().unwrap();
                            repl_log_guard.push(request)?;
                        }

                        // respond OK
                        write_response!(&mut stream, &mut response_buffer, vec!["OK".as_bytes()]);
                    }
                    b"INFO" | b"info" | b"Info" => {
                        match request_vector.get(1).unwrap().as_slice() {
                            b"REPLICATION" | b"replication" | b"Replication" => {
                                let repl_id;
                                let repl_offset;
                                {
                                    let repl_log_guard = self.replication_log.lock().unwrap();
                                    repl_id = repl_log_guard.replication_id().to_string();
                                    repl_offset = repl_log_guard.current_offset();
                                }
                                let response = format!(
                                    "role:master\nmaster_replid:{}\nmaster_repl_offset:{}",
                                    repl_id, repl_offset
                                );
                                write_response!(
                                    &mut stream,
                                    &mut response_buffer,
                                    vec![response.as_bytes()]
                                );
                            }
                            _ => {
                                eprintln!("Unsupported INFO subcommand");
                            }
                        }
                    }
                    b"PSYNC" | b"psync" | b"Psync" => {
                        const EMPTY_RDB_FILE_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

                        // respond with FULLSYNC
                        let repl_id;
                        let repl_offset;
                        {
                            let repl_log_guard = self.replication_log.lock().unwrap();
                            repl_id = repl_log_guard.replication_id().to_string();
                            repl_offset = repl_log_guard.current_offset();
                        }
                        let response = format!("+FULLRESYNC {} {}\r\n", repl_id, repl_offset);
                        write_response!(
                            &mut stream,
                            &mut response_buffer,
                            vec![response.as_bytes()]
                        );

                        // Convert the hex string to bytes
                        let rdb_bytes = hex::decode(EMPTY_RDB_FILE_HEX).expect("Can't decode hex");

                        let mut rdb_response: Vec<u8> = Vec::new();
                        rdb_response.push(b'$');
                        rdb_response.extend_from_slice(rdb_bytes.len().to_string().as_bytes());
                        rdb_response.push(b'\r');
                        rdb_response.push(b'\n');
                        rdb_response.extend_from_slice(&rdb_bytes);

                        // send RDB file
                        stream.write_all(&rdb_response).await?;

                        replica = true;
                    }
                    _ => {
                        // Handle other commands
                        eprintln!("Unsupported command: {}", String::from_utf8_lossy(command));
                    }
                }
            } else {
                eprintln!("Received an empty RESP request");
            }

            // if replication handshake is confirmed, then enter into relay

            if replica {
                println!("Connection {} converted into Replica Relay Mode", addr);

                let mut repl_iter: ReplicationLogIterator;
                {
                    let log_guard = self.replication_log.lock().unwrap();
                    repl_iter = log_guard.iterator();
                }

                let mut entry: Option<Vec<u8>>;

                loop {
                    entry = repl_iter.next();

                    if let Some(request) = entry {
                        println!(
                            "relaying repl log request: {}",
                            RESPParser::bytes_to_escaped_string(&request)
                        );
                        stream.write_all(&request).await?;
                    }

                    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
                }
            }
        }

        Ok(())
    }
}
