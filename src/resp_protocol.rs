use crate::command::Command;
use anyhow::{anyhow, Error};

pub struct RESPParser;

impl RESPParser {
    /// parse RESP encoded bytes into Vector of UTF strings
    pub fn parse(raw_request_bytes: &[u8]) -> Result<Vec<Command>, Error> {
        // println!("About to parse: {}", RESPParser::bytes_to_escaped_string(request));

        let mut leftover: &[u8] = raw_request_bytes;
        let mut results: Vec<Command> = Vec::new();

        let mut count = 0;
        while (!leftover.is_empty()) & (count < 10) {
            count += 1;
            match parse_resp(leftover) {
                Ok((resp, unparsed)) => {
                    println!("Parsed into RESP of len {} with unparsed of len {}: {:?}", resp.len(), unparsed.len(), resp);
                    // println!("unparsed: {}", RESPParser::bytes_to_escaped_string(unparsed));

                    match RESPParser::resp_to_decoded_string(resp) {
                        Ok(request_contents_bytes) => results.push(
                            // leftover still contains what's parsed
                            Command { 
                                request: request_contents_bytes,
                                raw_request_bytes_length: (leftover.len() - unparsed.len())
                             }
                        ),
                        Err(e) => {
                            eprintln!("{}", e);
                        }
                    }

                    // update leftover to what's unparsed
                    leftover = unparsed;
                }
                Err(e) => {
                    eprintln!(
                        "{}. Error parsing '{}'",
                        e,
                        RESPParser::bytes_to_escaped_string(leftover)
                    );
                    break;
                }
            }
        }

        println!("Parsed into {} commands", { results.len() });

        Ok(results)
    }

    pub fn resp_to_decoded_string(resp: Resp) -> Result<Vec<Vec<u8>>, Error> {
        match resp {
            Resp::String(s) | Resp::BulkString(s) | Resp::Integer(s) => Ok(vec![s.to_vec()]),
            Resp::Array(v) => {
                // println!("Got RESP Vector of {} elements", v.len());

                let nested_vecs = v
                    .into_iter()
                    .map(RESPParser::resp_to_decoded_string)
                    .collect::<Result<Vec<Vec<Vec<u8>>>, Error>>()?;

                Ok(nested_vecs.into_iter().flatten().collect())
            }
            Resp::Error(s) => Err(anyhow!(
                "RESP Error: {}",
                RESPParser::bytes_to_escaped_string(s)
            )),
            _ => Err(anyhow!("Unsupported RESP type: {:?}", resp)),
        }
    }

    pub fn bytes_to_escaped_string(resp_message: &[u8]) -> String {
        String::from_utf8_lossy(resp_message)
            .replace('\r', "\\r")
            .replace('\n', "\\n")
            .to_string()
    }
}

use std::io::Write;

type RespResult<'a> = std::result::Result<(Resp<'a>, &'a [u8]), RespError>;

const CR: u8 = b'\r';
const LF: u8 = b'\n';

#[derive(Debug, Eq, PartialEq)]
pub enum Resp<'a> {
    String(&'a [u8]),
    Error(&'a [u8]),
    Integer(&'a [u8]),
    BulkString(&'a [u8]),
    NilBulk,
    Array(Vec<Resp<'a>>),
    NilArray,
}

impl<'a> std::fmt::Display for Resp<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Resp::String(bytes)
            | Resp::Error(bytes)
            | Resp::Integer(bytes)
            | Resp::BulkString(bytes) => {
                write!(f, "{}", hex::encode(bytes))
            }
            Resp::NilBulk => write!(f, "(nil bulk string)"),
            Resp::Array(array) => {
                let strings: Vec<String> = array.iter().map(|resp| format!("{}", resp)).collect();
                write!(f, "[{}]", strings.join(", "))
            }
            Resp::NilArray => write!(f, "(nil array)"),
        }
    }
}

impl<'a> Resp<'a> {
    pub fn len(&self) -> usize {
        match self {
            Resp::String(s) => s.len() + 1,
            Resp::Error(s) => s.len() + 1,
            Resp::Integer(s) => s.len() + 1,
            Resp::BulkString(s) => s.len() + 1,
            Resp::NilBulk => 5, // $-1\r\n
            Resp::Array(a) => a.iter().map(|s| s.len()).sum(),
            Resp::NilArray => 5, // *-1\r\n
        }
    }

    pub fn write_to_writer<W>(&self, writer: &mut W) -> Result<(), RespError>
    where
        W: Write,
    {
        match self {
            Resp::String(s) => {
                writer.write_all(&[b'+'])?;
                writer.write_all(s)?;
                writer.write_all(&[b'\r', b'\n'])?;
            }
            Resp::Error(s) => {
                writer.write_all(&[b'-'])?;
                writer.write_all(s)?;
                writer.write_all(&[b'\r', b'\n'])?;
            }
            Resp::Integer(s) => {
                writer.write_all(&[b':'])?;
                writer.write_all(s)?;
                writer.write_all(&[b'\r', b'\n'])?;
            }
            Resp::BulkString(s) => {
                writer.write_all(&[b'$'])?;
                writer.write_all(format!("{}", s.len()).as_bytes())?;
                writer.write_all(&[b'\r', b'\n'])?;
                writer.write_all(s)?;
                writer.write_all(&[b'\r', b'\n'])?;
            }
            Resp::NilBulk => writer.write_all(&[b'$', b'-', b'1', b'\r', b'\n'])?,
            Resp::Array(a) => {
                writer.write_all(&[b'*'])?;
                writer.write_all(format!("{}", a.len()).as_bytes())?;
                writer.write_all(&[b'\r', b'\n'])?;
                for s in a {
                    s.write_to_writer(writer)?
                }
                // writer.write_all(&[b'\r', b'\n'])?;
            }
            Resp::NilArray => writer.write_all(&[b'*', b'-', b'1', b'\r', b'\n'])?,
        };
        Ok(())
    }
}

#[derive(Debug)]
pub enum RespError {
    // Cannot find CRLF at index
    NotEnoughBytes,
    // Incorrect format detected
    IncorrectFormat,
    Other(Box<dyn std::error::Error>),
}

impl std::fmt::Display for RespError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RespError::NotEnoughBytes => write!(f, "No enough bytes"),
            RespError::IncorrectFormat => write!(f, "Incorrect format"),
            RespError::Other(err) => write!(f, "{}", err),
        }
    }
}

impl std::error::Error for RespError {}

impl From<std::str::Utf8Error> for RespError {
    fn from(from: std::str::Utf8Error) -> Self {
        Self::Other(Box::new(from))
    }
}

impl From<std::num::ParseIntError> for RespError {
    fn from(from: std::num::ParseIntError) -> Self {
        Self::Other(Box::new(from))
    }
}

impl From<std::io::Error> for RespError {
    fn from(from: std::io::Error) -> Self {
        Self::Other(Box::new(from))
    }
}

pub fn parse_resp(input: &[u8]) -> RespResult {
    if input.is_empty() {
        Err(RespError::NotEnoughBytes)
    } else {
        let (resp, leftover) = match input[0] {
            b'+' => parse_simple_string(&input[1..])?,
            b':' => parse_integers(&input[1..])?,
            b'$' => parse_bulk_strings(&input[1..])?,
            b'*' => parse_arrays(&input[1..])?,
            b'-' => parse_errors(&input[1..])?,
            _ => parse_simple_string(input)?,
        };
        Ok((resp, leftover))
    }
}

fn parse_everything_until_crlf(input: &[u8]) -> std::result::Result<(&[u8], &[u8]), RespError> {
    for (index, (first, second)) in input.iter().zip(input.iter().skip(1)).enumerate() {
        if first == &CR && second == &LF {
            return Ok((&input[0..index], &input[index + 2..]));
        }
    }
    Err(RespError::NotEnoughBytes)
}

fn parse_everything_until_index(input: &[u8], index: usize) -> Result<(&[u8], &[u8]), RespError> {
    if input.len() <= index {
        Err(RespError::NotEnoughBytes)
    } else if input[index] == b'\r' && input[index + 1] == b'\n' {
        return Ok((&input[..index], &input[index + 2..]));
    } else {
        return Err(RespError::IncorrectFormat);
    }
}

pub fn parse_simple_string(input: &[u8]) -> RespResult {
    parse_everything_until_crlf(input).map(|(x, y)| (Resp::String(x), y))
}

pub fn parse_errors(input: &[u8]) -> RespResult {
    parse_everything_until_crlf(input).map(|(x, y)| (Resp::Error(x), y))
}

pub fn parse_integers(input: &[u8]) -> RespResult {
    parse_everything_until_crlf(input).map(|(x, y)| (Resp::Integer(x), y))
}

pub fn parse_bulk_strings(input: &[u8]) -> RespResult {
    let (size_str, leftover) = parse_everything_until_crlf(input)?;
    let size = std::str::from_utf8(size_str)?.parse::<i64>()?;

    if size < 0 {
        Ok((Resp::NilBulk, leftover))
    } else {
        let size = size as usize;
        if leftover.starts_with(b"REDIS") {
            let (result, leftover) = (&leftover[..size], &leftover[size..]);
            Ok((Resp::BulkString(result), leftover))
        } else {
            let (result, leftover) = parse_everything_until_index(leftover, size)?;
            Ok((Resp::BulkString(result), leftover))
        }
    }
}

pub fn parse_arrays(input: &[u8]) -> RespResult {
    let (size_str, leftover) = parse_everything_until_crlf(input)?;
    let size = std::str::from_utf8(size_str)?.parse::<i64>()?;

    if size < 0 {
        Ok((Resp::NilArray, leftover))
    } else {
        let sizes = size as usize;
        let mut left = leftover;
        let mut result = Vec::with_capacity(sizes);
        for _ in 0..sizes {
            let (element, tmp) = parse_resp(left)?;
            result.push(element);
            left = tmp;
        }
        return Ok((Resp::Array(result), left));
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn test_simple_string() {
        let input = "+hello\r\n".as_bytes();
        let (resp, left) = parse_resp(input).unwrap();
        assert_eq!(resp, Resp::String("hello".as_bytes()));
        assert!(left.is_empty());
    }

    #[test]
    pub fn test_errors() {
        let input = "+hello".as_bytes();
        let err = parse_resp(input).unwrap_err();
        assert!(matches!(err, RespError::NotEnoughBytes));
        let input = "*2\r\n$3\r\nfoo\r\n)hello".as_bytes();
        let err = parse_resp(input).unwrap_err();
        assert!(matches!(err, RespError::NotEnoughBytes));
        let input = "".as_bytes();
        let err = parse_resp(input).unwrap_err();
        assert!(matches!(err, RespError::NotEnoughBytes));
        let input = "$4\r\nfoo\r\n".as_bytes();
        let err = parse_resp(input).unwrap_err();
        assert!(matches!(err, RespError::IncorrectFormat));
        let input = "*2\r\n$3\r\nfoo+hello\r\n".as_bytes();
        let err = parse_resp(input).unwrap_err();
        assert!(matches!(err, RespError::IncorrectFormat));
    }

    #[test]
    pub fn test_nil() {
        let input = "$-1\r\n".as_bytes();
        let (resp, left) = parse_resp(input).unwrap();
        assert_eq!(resp, Resp::NilBulk);
        assert!(left.is_empty());
        let input = "*-1\r\n".as_bytes();
        let (resp, left) = parse_resp(input).unwrap();
        assert_eq!(resp, Resp::NilArray);
        assert!(left.is_empty());
    }

    #[test]
    pub fn test_bulk_string() {
        let input = "$6\r\nfoobar\r\n".as_bytes();
        let (resp, left) = parse_resp(input).unwrap();
        assert_eq!(resp, Resp::BulkString("foobar".as_bytes()));
        assert!(left.is_empty());
        let input = "$0\r\n\r\n".as_bytes();
        let (resp, left) = parse_resp(input).unwrap();
        assert_eq!(resp, Resp::BulkString("".as_bytes()));
        assert!(left.is_empty());
    }

    #[test]
    pub fn test_rdb_file() {
        const EMPTY_RDB_FILE_HEX: &str = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        let rdb_bytes = hex::decode(EMPTY_RDB_FILE_HEX).expect("Can't decode hex");
        let mut rdb_response: Vec<u8> = Vec::new();
        rdb_response.push(b'$');
        rdb_response.extend_from_slice(rdb_bytes.len().to_string().as_bytes());
        rdb_response.push(b'\r');
        rdb_response.push(b'\n');
        rdb_response.extend_from_slice(&rdb_bytes);

        let (resp, left) = parse_resp(&rdb_response).unwrap();
        println!("rdb len {}: {:?}", resp.len(), resp);
        println!("left: {:?}", left);
    }

    #[test]
    pub fn test_arrays() {
        let input = "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n".as_bytes();
        let (resp, left) = parse_resp(input).unwrap();
        assert_eq!(
            resp,
            Resp::Array(vec![
                Resp::BulkString("foo".as_bytes()),
                Resp::BulkString("bar".as_bytes())
            ])
        );
        assert!(left.is_empty());
        let input = "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$6\r\nfoobar\r\n".as_bytes();
        let (resp, left) = parse_resp(input).unwrap();
        assert_eq!(
            resp,
            Resp::Array(vec![
                Resp::Integer("1".as_bytes()),
                Resp::Integer("2".as_bytes()),
                Resp::Integer("3".as_bytes()),
                Resp::Integer("4".as_bytes()),
                Resp::BulkString("foobar".as_bytes()),
            ])
        );
        assert!(left.is_empty());
    }

    #[test]
    pub fn test_array_of_arrays() {
        let input = b"*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Foo\r\n-Bar\r\n";
        let (resp, left) = parse_resp(input).unwrap();
        assert_eq!(
            resp,
            Resp::Array(vec![
                Resp::Array(vec![
                    Resp::Integer("1".as_bytes()),
                    Resp::Integer("2".as_bytes()),
                    Resp::Integer("3".as_bytes()),
                ]),
                Resp::Array(vec![
                    Resp::String("Foo".as_bytes()),
                    Resp::Error("Bar".as_bytes()),
                ]),
            ])
        );
        assert!(left.is_empty());
    }

    #[test]
    pub fn test_info_command_output() {
        let input = b"$5180\r\n# Server\r\nredis_version:255.255.255\r\nredis_git_sha1:f36eb5a1\r\nredis_git_dirty:0\r\nredis_build_id:f219bc9a3885f906\r\nredis_mode:standalone\r\nos:Linux 5.15.0-53-generic x86_64\r\narch_bits:64\r\nmonotonic_clock:POSIX clock_gettime\r\nmultiplexing_api:epoll\r\natomicvar_api:c11-builtin\r\ngcc_version:11.3.0\r\nprocess_id:44314\r\nprocess_supervised:no\r\nrun_id:91b15383dedb3acb3991ee89c50dc2e3ea637986\r\ntcp_port:6379\r\nserver_time_usec:1669247775474011\r\nuptime_in_seconds:32726\r\nuptime_in_days:0\r\nhz:10\r\nconfigured_hz:10\r\nlru_clock:8303391\r\nexecutable:/home/hbina/git/redis/./src/redis-server\r\nconfig_file:/home/hbina/git/redis/./redis.conf\r\nio_threads_active:0\r\nlistener0:name=tcp,bind=127.0.0.1,bind=-::1,port=6379\r\n\r\n# Clients\r\nconnected_clients:1\r\ncluster_connections:0\r\nmaxclients:10000\r\nclient_recent_max_input_buffer:8\r\nclient_recent_max_output_buffer:0\r\nblocked_clients:0\r\ntracking_clients:0\r\nclients_in_timeout_table:0\r\n\r\n# Memory\r\nused_memory:1063504\r\nused_memory_human:1.01M\r\nused_memory_rss:8257536\r\nused_memory_rss_human:7.88M\r\nused_memory_peak:1236840\r\nused_memory_peak_human:1.18M\r\nused_memory_peak_perc:85.99%\r\nused_memory_overhead:867224\r\nused_memory_startup:865168\r\nused_memory_dataset:196280\r\nused_memory_dataset_perc:98.96%\r\nallocator_allocated:1341384\r\nallocator_active:1740800\r\nallocator_resident:6275072\r\ntotal_system_memory:33048694784\r\ntotal_system_memory_human:30.78G\r\nused_memory_lua:31744\r\nused_memory_vm_eval:31744\r\nused_memory_lua_human:31.00K\r\nused_memory_scripts_eval:0\r\nnumber_of_cached_scripts:0\r\nnumber_of_functions:0\r\nnumber_of_libraries:0\r\nused_memory_vm_functions:32768\r\nused_memory_vm_total:64512\r\nused_memory_vm_total_human:63.00K\r\nused_memory_functions:184\r\nused_memory_scripts:184\r\nused_memory_scripts_human:184B\r\nmaxmemory:0\r\nmaxmemory_human:0B\r\nmaxmemory_policy:noeviction\r\nallocator_frag_ratio:1.30\r\nallocator_frag_bytes:399416\r\nallocator_rss_ratio:3.60\r\nallocator_rss_bytes:4534272\r\nrss_overhead_ratio:1.32\r\nrss_overhead_bytes:1982464\r\nmem_fragmentation_ratio:7.93\r\nmem_fragmentation_bytes:7216328\r\nmem_not_counted_for_evict:0\r\nmem_replication_backlog:0\r\nmem_total_replication_buffers:0\r\nmem_clients_slaves:0\r\nmem_clients_normal:1800\r\nmem_cluster_links:0\r\nmem_aof_buffer:0\r\nmem_allocator:jemalloc-5.2.1\r\nactive_defrag_running:0\r\nlazyfree_pending_objects:0\r\nlazyfreed_objects:0\r\n\r\n# Persistence\r\nloading:0\r\nasync_loading:0\r\ncurrent_cow_peak:0\r\ncurrent_cow_size:0\r\ncurrent_cow_size_age:0\r\ncurrent_fork_perc:0.00\r\ncurrent_save_keys_processed:0\r\ncurrent_save_keys_total:0\r\nrdb_changes_since_last_save:0\r\nrdb_bgsave_in_progress:0\r\nrdb_last_save_time:1669247076\r\nrdb_last_bgsave_status:ok\r\nrdb_last_bgsave_time_sec:0\r\nrdb_current_bgsave_time_sec:-1\r\nrdb_saves:1\r\nrdb_last_cow_size:225280\r\nrdb_last_load_keys_expired:0\r\nrdb_last_load_keys_loaded:0\r\naof_enabled:0\r\naof_rewrite_in_progress:0\r\naof_rewrite_scheduled:0\r\naof_last_rewrite_time_sec:-1\r\naof_current_rewrite_time_sec:-1\r\naof_last_bgrewrite_status:ok\r\naof_rewrites:0\r\naof_rewrites_consecutive_failures:0\r\naof_last_write_status:ok\r\naof_last_cow_size:0\r\nmodule_fork_in_progress:0\r\nmodule_fork_last_cow_size:0\r\n\r\n# Stats\r\ntotal_connections_received:13\r\ntotal_commands_processed:21\r\ninstantaneous_ops_per_sec:0\r\ntotal_net_input_bytes:431\r\ntotal_net_output_bytes:1136345\r\ntotal_net_repl_input_bytes:0\r\ntotal_net_repl_output_bytes:0\r\ninstantaneous_input_kbps:0.00\r\ninstantaneous_output_kbps:0.00\r\ninstantaneous_input_repl_kbps:0.00\r\ninstantaneous_output_repl_kbps:0.00\r\nrejected_connections:0\r\nsync_full:0\r\nsync_partial_ok:0\r\nsync_partial_err:0\r\nexpired_keys:0\r\nexpired_stale_perc:0.00\r\nexpired_time_cap_reached_count:0\r\nexpire_cycle_cpu_milliseconds:1046\r\nevicted_keys:0\r\nevicted_clients:0\r\ntotal_eviction_exceeded_time:0\r\ncurrent_eviction_exceeded_time:0\r\nkeyspace_hits:0\r\nkeyspace_misses:0\r\npubsub_channels:0\r\npubsub_patterns:0\r\npubsubshard_channels:0\r\nlatest_fork_usec:295\r\ntotal_forks:1\r\nmigrate_cached_sockets:0\r\nslave_expires_tracked_keys:0\r\nactive_defrag_hits:0\r\nactive_defrag_misses:0\r\nactive_defrag_key_hits:0\r\nactive_defrag_key_misses:0\r\ntotal_active_defrag_time:0\r\ncurrent_active_defrag_time:0\r\ntracking_total_keys:0\r\ntracking_total_items:0\r\ntracking_total_prefixes:0\r\nunexpected_error_replies:0\r\ntotal_error_replies:1\r\ndump_payload_sanitizations:0\r\ntotal_reads_processed:35\r\ntotal_writes_processed:33\r\nio_threaded_reads_processed:0\r\nio_threaded_writes_processed:0\r\nreply_buffer_shrinks:23\r\nreply_buffer_expands:10\r\nacl_access_denied_auth:0\r\nacl_access_denied_cmd:0\r\nacl_access_denied_key:0\r\nacl_access_denied_channel:0\r\n\r\n# Replication\r\nrole:master\r\nconnected_slaves:0\r\nmaster_failover_state:no-failover\r\nmaster_replid:b47d5da0e4b42b52640f5e086a4b24d4a6cb6c5f\r\nmaster_replid2:0000000000000000000000000000000000000000\r\nmaster_repl_offset:0\r\nsecond_repl_offset:-1\r\nrepl_backlog_active:0\r\nrepl_backlog_size:1048576\r\nrepl_backlog_first_byte_offset:0\r\nrepl_backlog_histlen:0\r\n\r\n# CPU\r\nused_cpu_sys:39.159292\r\nused_cpu_user:24.101233\r\nused_cpu_sys_children:0.000000\r\nused_cpu_user_children:0.002011\r\nused_cpu_sys_main_thread:39.154828\r\nused_cpu_user_main_thread:24.102692\r\n\r\n# Modules\r\n\r\n# Errorstats\r\nerrorstat_ERR:count=1\r\n\r\n# Cluster\r\ncluster_enabled:0\r\n\r\n# Keyspace\r\ndb0:keys=1,expires=0,avg_ttl=0\r\n\r\n";
        let (resp, left) = parse_resp(input).unwrap();
        assert_eq!(
            resp,
            Resp::BulkString(b"# Server\r\nredis_version:255.255.255\r\nredis_git_sha1:f36eb5a1\r\nredis_git_dirty:0\r\nredis_build_id:f219bc9a3885f906\r\nredis_mode:standalone\r\nos:Linux 5.15.0-53-generic x86_64\r\narch_bits:64\r\nmonotonic_clock:POSIX clock_gettime\r\nmultiplexing_api:epoll\r\natomicvar_api:c11-builtin\r\ngcc_version:11.3.0\r\nprocess_id:44314\r\nprocess_supervised:no\r\nrun_id:91b15383dedb3acb3991ee89c50dc2e3ea637986\r\ntcp_port:6379\r\nserver_time_usec:1669247775474011\r\nuptime_in_seconds:32726\r\nuptime_in_days:0\r\nhz:10\r\nconfigured_hz:10\r\nlru_clock:8303391\r\nexecutable:/home/hbina/git/redis/./src/redis-server\r\nconfig_file:/home/hbina/git/redis/./redis.conf\r\nio_threads_active:0\r\nlistener0:name=tcp,bind=127.0.0.1,bind=-::1,port=6379\r\n\r\n# Clients\r\nconnected_clients:1\r\ncluster_connections:0\r\nmaxclients:10000\r\nclient_recent_max_input_buffer:8\r\nclient_recent_max_output_buffer:0\r\nblocked_clients:0\r\ntracking_clients:0\r\nclients_in_timeout_table:0\r\n\r\n# Memory\r\nused_memory:1063504\r\nused_memory_human:1.01M\r\nused_memory_rss:8257536\r\nused_memory_rss_human:7.88M\r\nused_memory_peak:1236840\r\nused_memory_peak_human:1.18M\r\nused_memory_peak_perc:85.99%\r\nused_memory_overhead:867224\r\nused_memory_startup:865168\r\nused_memory_dataset:196280\r\nused_memory_dataset_perc:98.96%\r\nallocator_allocated:1341384\r\nallocator_active:1740800\r\nallocator_resident:6275072\r\ntotal_system_memory:33048694784\r\ntotal_system_memory_human:30.78G\r\nused_memory_lua:31744\r\nused_memory_vm_eval:31744\r\nused_memory_lua_human:31.00K\r\nused_memory_scripts_eval:0\r\nnumber_of_cached_scripts:0\r\nnumber_of_functions:0\r\nnumber_of_libraries:0\r\nused_memory_vm_functions:32768\r\nused_memory_vm_total:64512\r\nused_memory_vm_total_human:63.00K\r\nused_memory_functions:184\r\nused_memory_scripts:184\r\nused_memory_scripts_human:184B\r\nmaxmemory:0\r\nmaxmemory_human:0B\r\nmaxmemory_policy:noeviction\r\nallocator_frag_ratio:1.30\r\nallocator_frag_bytes:399416\r\nallocator_rss_ratio:3.60\r\nallocator_rss_bytes:4534272\r\nrss_overhead_ratio:1.32\r\nrss_overhead_bytes:1982464\r\nmem_fragmentation_ratio:7.93\r\nmem_fragmentation_bytes:7216328\r\nmem_not_counted_for_evict:0\r\nmem_replication_backlog:0\r\nmem_total_replication_buffers:0\r\nmem_clients_slaves:0\r\nmem_clients_normal:1800\r\nmem_cluster_links:0\r\nmem_aof_buffer:0\r\nmem_allocator:jemalloc-5.2.1\r\nactive_defrag_running:0\r\nlazyfree_pending_objects:0\r\nlazyfreed_objects:0\r\n\r\n# Persistence\r\nloading:0\r\nasync_loading:0\r\ncurrent_cow_peak:0\r\ncurrent_cow_size:0\r\ncurrent_cow_size_age:0\r\ncurrent_fork_perc:0.00\r\ncurrent_save_keys_processed:0\r\ncurrent_save_keys_total:0\r\nrdb_changes_since_last_save:0\r\nrdb_bgsave_in_progress:0\r\nrdb_last_save_time:1669247076\r\nrdb_last_bgsave_status:ok\r\nrdb_last_bgsave_time_sec:0\r\nrdb_current_bgsave_time_sec:-1\r\nrdb_saves:1\r\nrdb_last_cow_size:225280\r\nrdb_last_load_keys_expired:0\r\nrdb_last_load_keys_loaded:0\r\naof_enabled:0\r\naof_rewrite_in_progress:0\r\naof_rewrite_scheduled:0\r\naof_last_rewrite_time_sec:-1\r\naof_current_rewrite_time_sec:-1\r\naof_last_bgrewrite_status:ok\r\naof_rewrites:0\r\naof_rewrites_consecutive_failures:0\r\naof_last_write_status:ok\r\naof_last_cow_size:0\r\nmodule_fork_in_progress:0\r\nmodule_fork_last_cow_size:0\r\n\r\n# Stats\r\ntotal_connections_received:13\r\ntotal_commands_processed:21\r\ninstantaneous_ops_per_sec:0\r\ntotal_net_input_bytes:431\r\ntotal_net_output_bytes:1136345\r\ntotal_net_repl_input_bytes:0\r\ntotal_net_repl_output_bytes:0\r\ninstantaneous_input_kbps:0.00\r\ninstantaneous_output_kbps:0.00\r\ninstantaneous_input_repl_kbps:0.00\r\ninstantaneous_output_repl_kbps:0.00\r\nrejected_connections:0\r\nsync_full:0\r\nsync_partial_ok:0\r\nsync_partial_err:0\r\nexpired_keys:0\r\nexpired_stale_perc:0.00\r\nexpired_time_cap_reached_count:0\r\nexpire_cycle_cpu_milliseconds:1046\r\nevicted_keys:0\r\nevicted_clients:0\r\ntotal_eviction_exceeded_time:0\r\ncurrent_eviction_exceeded_time:0\r\nkeyspace_hits:0\r\nkeyspace_misses:0\r\npubsub_channels:0\r\npubsub_patterns:0\r\npubsubshard_channels:0\r\nlatest_fork_usec:295\r\ntotal_forks:1\r\nmigrate_cached_sockets:0\r\nslave_expires_tracked_keys:0\r\nactive_defrag_hits:0\r\nactive_defrag_misses:0\r\nactive_defrag_key_hits:0\r\nactive_defrag_key_misses:0\r\ntotal_active_defrag_time:0\r\ncurrent_active_defrag_time:0\r\ntracking_total_keys:0\r\ntracking_total_items:0\r\ntracking_total_prefixes:0\r\nunexpected_error_replies:0\r\ntotal_error_replies:1\r\ndump_payload_sanitizations:0\r\ntotal_reads_processed:35\r\ntotal_writes_processed:33\r\nio_threaded_reads_processed:0\r\nio_threaded_writes_processed:0\r\nreply_buffer_shrinks:23\r\nreply_buffer_expands:10\r\nacl_access_denied_auth:0\r\nacl_access_denied_cmd:0\r\nacl_access_denied_key:0\r\nacl_access_denied_channel:0\r\n\r\n# Replication\r\nrole:master\r\nconnected_slaves:0\r\nmaster_failover_state:no-failover\r\nmaster_replid:b47d5da0e4b42b52640f5e086a4b24d4a6cb6c5f\r\nmaster_replid2:0000000000000000000000000000000000000000\r\nmaster_repl_offset:0\r\nsecond_repl_offset:-1\r\nrepl_backlog_active:0\r\nrepl_backlog_size:1048576\r\nrepl_backlog_first_byte_offset:0\r\nrepl_backlog_histlen:0\r\n\r\n# CPU\r\nused_cpu_sys:39.159292\r\nused_cpu_user:24.101233\r\nused_cpu_sys_children:0.000000\r\nused_cpu_user_children:0.002011\r\nused_cpu_sys_main_thread:39.154828\r\nused_cpu_user_main_thread:24.102692\r\n\r\n# Modules\r\n\r\n# Errorstats\r\nerrorstat_ERR:count=1\r\n\r\n# Cluster\r\ncluster_enabled:0\r\n\r\n# Keyspace\r\ndb0:keys=1,expires=0,avg_ttl=0\r\n")
        );
        assert!(left.is_empty());
    }
}
