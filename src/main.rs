use redis_protocol_parser::{RedisProtocolParser, RESP};
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::net::TcpStream;

// use String to pass Context across async boundaries
#[derive(Clone)]
struct Context {
    namespace: Option<String>,
}

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    let context = Context { namespace: None };

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

fn resp_to_string(resp: RESP) -> String {
    match resp {
        RESP::String(s) => {
            println!("Got RESP String");
            String::from_utf8_lossy(s).to_string()
        }
        RESP::BulkString(bs) => {
            println!("Got RESP BulkString");
            String::from_utf8_lossy(bs).to_string()
        }
        RESP::Error(e) => {
            println!("Got RESP Error");
            String::from_utf8_lossy(e).to_string()
        }
        RESP::Array(v) => {
            println!("Got RESP Vector of {} elements", v.len());
            let mut vector_resp_strings: Vec<String> = Vec::new();
            for resp in v {
                vector_resp_strings.push(resp_to_string(resp));
            }
            let result: String = vector_resp_strings.join(" ");
            result
        }
        _ => {
            println!("Unsupported RESP type");
            "".to_string()
        }
    }
}

fn handle_resp(resp_str: &str) -> String {
    match resp_str {
        "PING" => "+PONG\r\n".to_string(),
        _ => "".to_string(),
    }
}

async fn handle_connection(context: &mut Context, mut stream: TcpStream) {
    println!("Processing in namespace: {:?}", context.namespace);

    let mut buffer = [0; 1024];
    let response = match stream.read(&mut buffer).await {
        Ok(bytes_read) => {
            // only convert part of buffer with data read in!
            let request = &buffer[..bytes_read];
            println!("Received request: {:?}", request);

            match RedisProtocolParser::parse_resp(request) {
                Ok((resp, left)) => {
                    println!("Parsed request. resp: {:?}, left: {:?}", resp, left);

                    let resp_str = resp_to_string(resp);

                    println!("RESP String: {}", resp_str);

                    handle_resp(&resp_str)
                }
                Err(e) => {
                    eprintln!("Error parsing RESP: {}", e);
                    "".to_string()
                }
            }
        }
        Err(e) => {
            println!("Failed to read from connection: {}", e);

            "".to_string()
        }
    };

    println!("Server Response: {}", response);

    stream.write_all(response.as_bytes()).await.unwrap();
    stream.flush().await.unwrap();
}
