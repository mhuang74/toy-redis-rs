mod args;
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
    let replica_master = args.replicaof.map(|values| {
        if values.len() == 2 {
            let hostname = values[0].clone();
            let port = values[1].parse::<usize>().expect("Invalid port number");
            ReplicaMaster { hostname, port }
        } else {
            panic!("Expected hostname and port for --replicaof");
        }
    });

    let _app_config = Arc::new(Mutex::new(AppConfig::new()));
    // app_config.lock().unwrap().replicaof = replica_master;

    // Startup in Replica Mode
    if let Some(master) = &replica_master {
        println!(
            "Starting as a Replica of {}:{}",
            master.hostname, master.port
        );

        // connect to Replication Master
        let address = format!("{}:{}", master.hostname, master.port);
        let stream = TcpStream::connect(&address)
            .await
            .expect("Failed to connect to the master");

        println!(
            "Connected to Replica Master at {}:{}. Setting up replication...",
            master.hostname, master.port
        );

        let storage = Arc::new(Mutex::new(Storage::new()));
        let replica = Replica::new(storage.clone());

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

    println!("Server started at address: {}", &server_address);

    // start Redis server listening loop
    let replication_log = Arc::new(Mutex::new(ReplicationLog::new()));
    let storage = Arc::new(Mutex::new(Storage::new()));

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
