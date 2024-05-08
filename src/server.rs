use std::sync::{Arc, Mutex};
use crate::replication_log::ReplicationLog;
use crate::replication_log::ReplicationLogIterator;
use crate::storage::Storage;
use tokio::net::TcpStream;
use anyhow::{Result,Error,anyhow};
use std::net::SocketAddr;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;


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

    pub async fn handle_connection(&self, addr: SocketAddr, mut stream: TcpStream) -> Result<(), Error> {

        let mut buffer = [0; 1024];
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
                String::from_utf8_lossy(request)
                    .replace('\r', "\\r")
                    .replace('\n', "\\n")
            );

            // mock: response
            stream
                .write_all("+PONG\r\n".as_bytes())
                .await
                .expect("Failed to send message to the master");

            // mock: write all requests to repl log
            // limit lock this small block
            {
                let mut repl_log_guard = self.replication_log.lock().unwrap();
                repl_log_guard.push(request)?;
            }
    
            // if replication handshake is confirmed, then enter into relay 
            
            if setup_replication_relay(addr, &stream)? {
                println!("Connection {} converted into Replica Relay Mode", addr);

                let mut repl_iter: ReplicationLogIterator;
                {
                    let log_guard = self.replication_log.lock().unwrap();
                    repl_iter = log_guard.iterator();
                }

                let mut entry: Option<Vec<u8>>;

                loop {
                    entry = repl_iter.next();

                    if let Some(message) = entry {
                        println!("relaying log: {:?}", String::from_utf8_lossy(&message).replace('\r', "\\r").replace('\n', "\\n"));
                        stream.write_all(&message).await?;
                    }

                    tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;

                }
            }
        }
    
        Ok(())
    }


}

fn setup_replication_relay(addr: SocketAddr, stream: &TcpStream) -> Result<bool, Error> {
    let is_port_over_five = addr.port() % 10 > 5;
    Ok(is_port_over_five)
}