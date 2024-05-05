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
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let cli = Cli::parse();

    println!("Listening on port: {}", cli.port);

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

    let listener = TcpListener::bind(format!("127.0.0.1:{}", cli.port))
        .await
        .unwrap();

    loop {
        let (socket, addr) = listener.accept().await.unwrap();
        let mut context = context.clone();
        tokio::spawn(async move {
            println!("accepted new connection from: {:?}", addr);

            handle_connection(&mut context, socket).await;
        });
    }
}

async fn handle_connection(context: &mut Context, mut stream: TcpStream) {
    let mut buffer = [0; 1024];
    loop {
        let response = match stream.read(&mut buffer).await {
            Ok(bytes_read) => {
                // if nothing read, then reached EOF
                if bytes_read == 0 {
                    eprintln!("Connection reached EOF");
                    break;
                }

                // only convert part of buffer with data read in!
                let request = &buffer[..bytes_read];
                println!("Received request: {:?}", request);

                match RedisProtocolParser::parse_resp(request) {
                    Ok((resp, left)) => {
                        println!("Parsed request. resp: {:?}, left: {:?}", resp, left);

                        handle_resp(context, resp)
                    }
                    Err(e) => {
                        eprintln!("Error parsing RESP: {}", e);
                        "".to_string()
                    }
                }
            }
            Err(e) => {
                println!("Failed to read from connection: {}", e);

                "-ERR connection error\r\n".to_string()
            }
        };

        println!("Server Response: {}", response);

        stream.write_all(response.as_bytes()).await.unwrap();
        stream.flush().await.unwrap();
    }
}

fn handle_resp(context: &mut Context, resp: RESP) -> String {
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
                "-ERR missing command\r\n".to_string()
            }
        }
        _ => {
            println!("Unsupported RESP type");
            "-ERR Unsupported RESP type\r\n".to_string()
        }
    }
}

fn handle_command(context: &mut Context, command: &[u8], arguments: Vec<Option<&[u8]>>) -> String {
    match command {
        b"PING" | b"ping" | b"Ping" => "+PONG\r\n".to_string(),
        b"ECHO" | b"echo" | b"Echo" => {
            // respond via BulkString
            let arg = arguments
                .first()
                .expect("Expecting arg")
                .expect("Missing message for ECHO");
            format!("${}\r\n{}\r\n", arg.len(), String::from_utf8_lossy(arg))
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

            "+OK\r\n".to_string()
        }
        b"GET" | b"get" | b"Get" => {
            let key = arguments
                .first()
                .expect("Expecting KEY for GET command")
                .expect("Invalid KEY for GET");

            match context.store.get(key) {
                Some((val, None)) => {
                    format!("${}\r\n{}\r\n", val.len(), String::from_utf8_lossy(val))
                }
                Some((val, Some(expiry))) => {
                    if SystemTime::now() > *expiry {
                        println!(
                            "KEY '{}' expired at {:?}",
                            String::from_utf8_lossy(key),
                            expiry
                        );
                        context.store.remove(key);
                        "$-1\r\n".to_string()
                    } else {
                        format!("${}\r\n{}\r\n", val.len(), String::from_utf8_lossy(val))
                    }
                }
                None => "$-1\r\n".to_string(),
            }
        }
        b"INFO" | b"info" | b"Info" => {
            if let Some(Some(category_arg)) = arguments.first() {
                match category_arg.to_vec().as_slice() {
                    b"REPLICATION" | b"replication" | b"Replication" => {
                        let bulk_str = if let Some(m) = &context.replicaof {
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

                        format!("${}\r\n{}\r\n", bulk_str.len(), bulk_str)
                    }
                    _ => "-ERR unknown command\r\n".to_string(),
                }
            } else {
                "-ERR unknown command\r\n".to_string()
            }
        }
        // Add more commands and their respective handling here
        _ => {
            eprintln!(
                "No implementation for resp request: {}",
                String::from_utf8_lossy(command)
            );
            "-ERR unknown command\r\n".to_string()
        }
    }
}
