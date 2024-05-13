mod args;
mod command;
mod config;
mod replica;
mod replication_log;
mod resp_protocol;
mod server;
mod storage;
mod util;

use config::{AppConfig, ReplicaMaster};
use replica::Replica;
use replication_log::ReplicationLog;
use server::Server;
use std::sync::{Arc, Mutex};
use storage::Storage;

use tokio::net::TcpListener;
use tokio::net::TcpStream;

#[tokio::main]
async fn main() {
    let args = args::parse();

    // Parse the replicaof argument into a ReplicaMaster if provided
    let replica_master = args.replicaof.map(|(hostname, port)| ReplicaMaster {
        hostname,
        port: port as usize,
    });

    let _app_config = Arc::new(Mutex::new(AppConfig::new()));
    // app_config.lock().unwrap().replicaof = replica_master;

    // storaged shared by main server handler and replica handler
    let storage = Arc::new(Mutex::new(Storage::new()));

    // Startup in Replica Mode
    if let Some(master) = &replica_master {
        // connect to Replication Master
        let address = format!("{}:{}", master.hostname, master.port);
        let stream = TcpStream::connect(&address)
            .await
            .expect("Failed to connect to the master");

        println!(
            "Connected to Replica Master at {}:{}. Setting up replication...",
            master.hostname, master.port
        );

        let mut replica = Replica::new(storage.clone());

        let _handle = tokio::spawn(async move {
            replica
                .handle_connection(stream, &address)
                .await
                .expect("Error handling input from connection");
        });
    }

    // Always listen on normal Server port

    // bind to listening port
    let server_address = format!("127.0.0.1:{}", args.port);

    let listener = TcpListener::bind(&server_address)
        .await
        .unwrap_or_else(|_| panic!("Unable to bind to server address: {}", &server_address));

    println!("Main handler started at address: {}", &server_address);

    // TODO: this is shared between Server threads that handles WRITE, and Replica threads that propgate WRITE to replicas
    let replication_log = Arc::new(Mutex::new(ReplicationLog::new()));

    // start Redis server listening loop
    loop {
        let (stream, addr) = listener.accept().await.unwrap();
        println!("accepted new connection from: {:?}", addr);

        let repl_log = replication_log.clone();
        let stor = storage.clone();
        let master = replica_master.clone();

        tokio::spawn(async move {
            let server = Server::new(master, repl_log, stor);

            server
                .handle_connection(addr, stream)
                .await
                .expect("Error handling input from connection");
        });
    }
}
