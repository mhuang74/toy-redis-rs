use clap::Parser;
// use clap::builder::ValueParser;
// use std::str::FromStr;

fn parse_host_port(s: &str) -> Result<(String, u16), String> {
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() != 2 {
        return Err("Please provide a host and port in the format 'hostname port'".to_string());
    }
    let host = parts[0].to_string();
    let port = parts[1]
        .parse::<u16>()
        .map_err(|_| "Invalid port number".to_string())?;
    Ok((host, port))
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Cli {
    /// Turn debugging information on
    #[arg(short, long)]
    pub verbose: bool,

    /// Replicaof in the format 'hostname port'
    #[arg(long, value_parser = parse_host_port)]
    pub replicaof: Option<(String, u16)>,

    /// Port number
    #[arg(short, long, default_value_t = 6379)]
    pub port: usize,
}

pub fn parse() -> Cli {
    Cli::parse()
}
