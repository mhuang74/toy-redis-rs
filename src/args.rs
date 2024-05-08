use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct Cli {
    /// Turn debugging information on
    #[arg(short, long)]
    pub verbose: bool,

    /// Replicaof in the format 'hostname port'
    #[arg(long, number_of_values = 2)]
    pub replicaof: Option<Vec<String>>,

    /// Port number
    #[arg(short, long, default_value_t = 6379)]
    pub port: usize,
}

pub fn parse() -> Cli {
    Cli::parse()
}
