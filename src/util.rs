// utils

#[macro_export]
macro_rules! write_response {
    ($stream:expr, $response_buffer:expr, $vector_of_byte_arrays:expr) => {{
        use $crate::resp_protocol::Resp;
        let response = if $vector_of_byte_arrays.len() > 1 {
            let bulk_strings: Vec<Resp> = $vector_of_byte_arrays
                .iter()
                .map(|s| Resp::BulkString(s))
                .collect();
            Resp::Array(bulk_strings)
        } else {
            Resp::BulkString($vector_of_byte_arrays.first().unwrap())
        };
        $response_buffer.clear();
        if response.write_to_writer(&mut $response_buffer).is_ok() {
            $stream.write_all(&$response_buffer).await?;
            println!("Sent: {}", RESPParser::bytes_to_escaped_string(&$response_buffer));
        }
    }};
}

#[macro_export]
macro_rules! empty_response {
    ($stream:expr, $response_buffer:expr) => {{
        use $crate::resp_protocol::Resp;
        let response = Resp::NilBulk;
        $response_buffer.clear();
        if response.write_to_writer(&mut $response_buffer).is_ok() {
            $stream.write_all(&$response_buffer).await?;
        }
    }};
}
