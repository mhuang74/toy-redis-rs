use anyhow::{anyhow, Result};
use clap::Parser;
use redis_protocol_parser::{RedisProtocolParser, RESP};
use std::collections::HashMap;
use std::ops::Add;
use std::time::Duration;
use std::time::SystemTime;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Turn debugging information on
    #[arg(short, long)]
    verbose: bool,

    /// Replicaof in the format 'hostname port'
    #[arg(long, number_of_values = 2)]
    replicaof: Option<Vec<String>>,

    /// Port number
    #[arg(short, long, default_value_t = 6379)]
    port: usize,
}
#[derive(Clone)]
struct ReplicaMaster {
    hostname: String,
    port: usize,
}

// use String to pass Context across async boundaries
#[derive(Clone)]
struct Context {
    replicaof: Option<ReplicaMaster>,
    master_replid: Option<String>,
    master_repl_offset: Option<usize>,
    store: HashMap<Vec<u8>, (Vec<u8>, Option<SystemTime>)>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Parse the replicaof argument into a ReplicaMaster if provided
    let replica_master = cli.replicaof.map(|values| {
        if values.len() == 2 {
            let hostname = values[0].clone();
            let port = values[1].parse::<usize>().expect("Invalid port number");
            ReplicaMaster { hostname, port }
        } else {
            panic!("Expected hostname and port for --replicaof");
        }
    });

    let context = Context {
        replicaof: replica_master,
        master_replid: Some("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string()),
        master_repl_offset: Some(0),
        store: HashMap::new(),
    };

    // set up replication
    setup_replication(context.clone())
        .await
        .expect("Error setting up replication");

    // bind to listening port
    let server_address = format!("127.0.0.1:{}", cli.port);

    let listener = TcpListener::bind(&server_address)
        .await
        .unwrap_or_else(|_| panic!("Unable to bind to server address: {}", &server_address));

    println!("Server started at address: {}", &server_address);

    // start main loop
    loop {
        let (stream, addr) = listener.accept().await.unwrap();
        let context = context.clone();
        tokio::spawn(async move {
            println!("accepted new connection from: {:?}", addr);

            handle_connection(context, stream)
                .await
                .expect("Error handling input from connection");
        });
    }
}

async fn setup_replication(context: Context) -> Result<()> {
    if let Some(master) = context.replicaof {
        const PING: &str = "*1\r\n$4\r\nPING\r\n";
        const REPL_CONF_PORT: &str =
            "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n6380\r\n";
        const REPL_CONF_CAPABILITY: &str = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
        const REPL_CONF_PSYNC: &str = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";

        let mut master_stream = TcpStream::connect(format!("{}:{}", master.hostname, master.port))
            .await
            .expect("Failed to connect to the master");

        println!(
            "Connected to replica master at {}:{}. Setting up replication...",
            master.hostname, master.port
        );

        async fn send_master_and_wait_for_response(
            stream: &mut TcpStream,
            message: &str,
        ) -> Result<()> {
            let mut buffer: [u8; 1024] = [0; 1024];

            stream
                .write_all(message.as_bytes())
                .await
                .expect("Failed to send message to the master");

            let bytes_read = stream.read(&mut buffer).await?;

            // only convert part of buffer with data read in!
            let reply = &buffer[..bytes_read];
            println!("Received reply: {}", String::from_utf8_lossy(reply));

            Ok(())
        }

        // PING
        send_master_and_wait_for_response(&mut master_stream, PING).await?;

        // REPLCONF to set listening port
        send_master_and_wait_for_response(&mut master_stream, REPL_CONF_PORT).await?;

        // REPLCONF to set capability
        send_master_and_wait_for_response(&mut master_stream, REPL_CONF_CAPABILITY).await?;

        // PSYNC
        send_master_and_wait_for_response(&mut master_stream, REPL_CONF_PSYNC).await?;
    }

    Ok(())
}

async fn handle_connection(mut context: Context, mut stream: TcpStream) -> Result<()> {
    let mut buffer = [0; 1024];
    loop {
        let bytes_read = stream.read(&mut buffer).await?;

        if bytes_read == 0 {
            eprintln!("Connection reached EOF");
            break;
        }

        // only convert part of buffer with data read in!
        let request = &buffer[..bytes_read];
        println!(
            "Received request: {}",
            String::from_utf8_lossy(request)
                .replace('\r', "\\r")
                .replace('\n', "\\n")
        );

        let (resp, _) = RedisProtocolParser::parse_resp(request)
            .map_err(|e| anyhow!("Error parsing RESP: {}", e))?;

        let responses = handle_resp(&mut context, resp);

        for response in responses {
            println!(
                "Server Response: {}",
                String::from_utf8_lossy(&response)
                    .replace('\r', "\\r")
                    .replace('\n', "\\n")
            );

            stream.write_all(&response).await?;
            stream.flush().await?;

            tokio::time::sleep(tokio::time::Duration::from_millis(20)).await;
        }
    }
    Ok(())
}

fn handle_resp(context: &mut Context, resp: RESP) -> Vec<Vec<u8>> {
    println!("handling resp: {:?}", &resp);

    match resp {
        RESP::String(cmd) | RESP::BulkString(cmd) => handle_command(context, cmd, vec![None]),
        RESP::Array(v) => {
            println!("Got RESP Vector of {} elements", v.len());

            let mut resp_vec_iter = v.iter();

            let command: Option<&[u8]> = match resp_vec_iter.next().unwrap() {
                RESP::String(cmd) | RESP::BulkString(cmd) => Some(cmd),
                _ => {
                    eprintln!("Only expecting RESP String/BulkString as command");
                    None
                }
            };

            // assuming remaining are arguments
            let arguments: Vec<Option<&[u8]>> = resp_vec_iter
                .map(|resp_arg| match resp_arg {
                    RESP::String(cmd) | RESP::BulkString(cmd) => Some(*cmd),
                    _ => {
                        eprintln!("Only expecting RESP String/BulkString as argument");
                        None
                    }
                })
                .collect();

            if let Some(cmd) = command {
                handle_command(context, cmd, arguments)
            } else {
                vec!["-ERR missing command\r\n".to_string().into_bytes()]
            }
        }
        _ => {
            println!("Unsupported RESP type");
            vec!["-ERR Unsupported RESP type\r\n".to_string().into_bytes()]
        }
    }
}

fn handle_command<'a>(
    context: &mut Context,
    command: &'a [u8],
    arguments: Vec<Option<&'a [u8]>>,
) -> Vec<Vec<u8>> {
    match command {
        b"PING" | b"ping" | b"Ping" => vec!["+PONG\r\n".to_string().into_bytes()],
        b"ECHO" | b"echo" | b"Echo" => {
            // respond via BulkString
            let arg = arguments
                .first()
                .expect("Expecting arg")
                .expect("Missing message for ECHO");
            vec![format!("${}\r\n{}\r\n", arg.len(), String::from_utf8_lossy(arg)).into_bytes()]
        }
        b"SET" | b"set" | b"Set" => {
            let key = arguments
                .first()
                .expect("Expecting KEY for SET command")
                .expect("Invalid KEY for SET");
            let val = arguments
                .get(1)
                .expect("Expecting VAL for SET command")
                .expect("Invalid VAL for SET");

            if let Some(Some(expirary_arg)) = arguments.get(2) {
                let duration = match expirary_arg.to_vec().as_slice() {
                    b"PX" | b"px" | b"Px" => arguments
                        .get(3)
                        .and_then(|&x| x)
                        .and_then(|x| String::from_utf8_lossy(x).parse::<u64>().ok())
                        .map(Duration::from_millis),
                    b"EX" | b"ex" | b"Ex" => arguments
                        .get(3)
                        .and_then(|&x| x)
                        .and_then(|x| String::from_utf8_lossy(x).parse::<u64>().ok())
                        .map(Duration::from_secs),
                    _ => None,
                };
                let expiry = duration.map(|dur| SystemTime::now().add(dur));

                println!(
                    "Adding key '{}' with expiration duration {:?} expiring at {:?}",
                    String::from_utf8_lossy(key),
                    duration,
                    expiry
                );

                context.store.insert(key.to_vec(), (val.to_vec(), expiry));
            } else {
                context.store.insert(key.to_vec(), (val.to_vec(), None));
            }

            vec!["+OK\r\n".to_string().into_bytes()]
        }
        b"GET" | b"get" | b"Get" => {
            let key = arguments
                .first()
                .expect("Expecting KEY for GET command")
                .expect("Invalid KEY for GET");

            match context.store.get(key) {
                Some((val, None)) => {
                    vec![
                        format!("${}\r\n{}\r\n", val.len(), String::from_utf8_lossy(val))
                            .into_bytes(),
                    ]
                }
                Some((val, Some(expiry))) => {
                    if SystemTime::now() > *expiry {
                        println!(
                            "KEY '{}' expired at {:?}",
                            String::from_utf8_lossy(key),
                            expiry
                        );
                        context.store.remove(key);
                        vec!["$-1\r\n".to_string().into_bytes()]
                    } else {
                        vec![
                            format!("${}\r\n{}\r\n", val.len(), String::from_utf8_lossy(val))
                                .to_string()
                                .into_bytes(),
                        ]
                    }
                }
                None => vec!["$-1\r\n".to_string().into_bytes()],
            }
        }
        b"INFO" | b"info" | b"Info" => {
            if let Some(Some(category_arg)) = arguments.first() {
                match category_arg.to_vec().as_slice() {
                    b"REPLICATION" | b"replication" | b"Replication" => {
                        let bulk_str: String = if let Some(m) = &context.replicaof {
                            // this is a replica
                            println!("This is a replica to: {}:{}", m.hostname, m.port);
                            "role:slave".to_string()
                        } else {
                            // not a replica
                            format!(
                                "role:master\nmaster_replid:{}\nmaster_repl_offset:{}",
                                context
                                    .master_replid
                                    .as_ref()
                                    .expect("Missiing master repl id"),
                                context
                                    .master_repl_offset
                                    .as_ref()
                                    .expect("Missing master repl offset")
                            )
                        };

                        vec![format!("${}\r\n{}\r\n", bulk_str.len(), bulk_str).into_bytes()]
                    }
                    _ => vec!["-ERR unknown command\r\n".to_string().into_bytes()],
                }
            } else {
                vec!["-ERR unknown command\r\n".to_string().into_bytes()]
            }
        }
        b"REPLCONF" | b"replconf" | b"Replconf" => vec!["+OK\r\n".to_string().into_bytes()],
        b"PSYNC" | b"psync" | b"Psync" => {
            const EMPTY_RDB_FILE_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

            // Convert the hex string to bytes
            let rdb_bytes = hex::decode(EMPTY_RDB_FILE_HEX).expect("Can't decode hex");
            let mut rdb_response: Vec<u8> = Vec::new();
            rdb_response.push(b'$');
            rdb_response.extend_from_slice(rdb_bytes.len().to_string().as_bytes());
            rdb_response.push(b'\r');
            rdb_response.push(b'\n');
            rdb_response.extend_from_slice(&rdb_bytes);

            vec![
                format!(
                    "+FULLRESYNC {} {}\r\n",
                    context
                        .master_replid
                        .as_ref()
                        .expect("Missing master repl id"),
                    context
                        .master_repl_offset
                        .as_ref()
                        .expect("Missing master repl offset")
                )
                .into_bytes(),
                rdb_response,
            ]
        }
        // Add more commands and their respective handling here
        _ => {
            eprintln!(
                "No implementation for resp request: {}",
                String::from_utf8_lossy(command)
            );
            vec!["-ERR unknown command\r\n".to_string().into_bytes()]
        }
    }
}
