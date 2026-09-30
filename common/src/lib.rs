use std::io::{self, Read, Write};
use std::net::{TcpStream};
use woonsocket_work::Work;
use serde::{Deserialize, Serialize};



#[derive(Serialize, Deserialize, Debug)]
pub struct LatencyRecord {
    pub latency: u64,
    pub send_timestamp: u64,
    pub server_processing_time: u64,
    pub recv_timestamp: u64,
}


#[derive(Serialize, Deserialize, Debug)]
pub struct Request {
    pub work: Work,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Response {
    pub result: Option<Vec<u8>>,
    pub server_processing_time: u64,
}

pub fn serialize<T: Serialize>(
    value: &T,
) -> Result<Vec<u8>, bincode::error::EncodeError> {
    bincode::serde::encode_to_vec(
        value, 
        bincode::config::standard(),
    )
}


pub fn deserialize<T: for <'a> Deserialize<'a>>(
    bytes: &[u8],
) -> Result<T, bincode::error::DecodeError> {
    let (value, _) = bincode::serde::decode_from_slice(
        bytes, 
        bincode::config::standard(),
    )?;

    Ok(value)
}

pub fn send_message(
    stream: &mut TcpStream,
    message: &[u8],
) -> io::Result<()> {
    let length = message.len() as u32;
    stream.write_all(&length.to_be_bytes())?;
    stream.write_all(message)?;

    Ok(())
}

pub fn recv_message(
    stream: &mut TcpStream,
) -> io::Result<Vec<u8>> {
    let mut length_bytes = [0u8; 4];

    // read_exact is blocking
    stream.read_exact(&mut length_bytes)?;
    let length = u32::from_be_bytes(length_bytes) as usize;

    let mut message = vec![0u8; length];
    stream.read_exact(&mut message)?;

    Ok(message)
}