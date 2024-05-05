use redis_protocol_parser::{RedisProtocolParser, RESP};
use std::collections::HashMap;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

// use String to pass Context across async boundaries
#[derive(Clone)]
struct Context {
    namespace: Option<String>,
    store: HashMap<Vec<u8>, Vec<u8>>,
}

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let context = Context {
        namespace: None,
        store: HashMap::new(),
    };

    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

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
    println!("Processing in namespace: {:?}", context.namespace);

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
        b"PING" => "+PONG\r\n".to_string(),
        b"ECHO" => {
            // respond via BulkString
            let arg = arguments
                .first()
                .expect("Expecting arg")
                .expect("Missing message for ECHO");
            format!("${}\r\n{}\r\n", arg.len(), String::from_utf8_lossy(arg))
        }
        b"SET" => {
            let key = arguments
                .first()
                .expect("Expecting KEY for SET command")
                .expect("Invalid KEY for SET");
            let val = arguments
                .get(1)
                .expect("Expecting VAL for SET command")
                .expect("Invalid VAL for SET");

            context.store.insert(key.to_vec(), val.to_vec());

            "+OK\r\n".to_string()
        }
        b"GET" => {
            let key = arguments
                .first()
                .expect("Expecting KEY for GET command")
                .expect("Invalid KEY for GET");

            match context.store.get(key) {
                Some(val) => format!("${}\r\n{}\r\n", val.len(), String::from_utf8_lossy(val)),
                None => "$-1\r\n".to_string(),
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
