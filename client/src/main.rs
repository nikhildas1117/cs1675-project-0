use std::net::TcpStream;
use std::num::NonZeroU64;

use common::{
    deserialize,
    recv_message,
    serialize,
    send_message,
    Request,
    Response,
};

use woonsocket_work::Work;

fn main() {
    let mut stream = TcpStream::connect(
        "127.0.0.1:8080").expect("failed to connect");
    
    // Determine Work to perform
    let request = Request {
        work: Work::Poisson(
            NonZeroU64::new(100).unwrap()
        ),
    };

    println!("Sending Work");

    // Send Work
    let bytes = serialize(&request).expect("failed to serialize");
    send_message(&mut stream, &bytes).expect("failed to write");

    // Receive response
    let bytes = recv_message(&mut stream).expect("failed to read");
    let response: Response = deserialize(&bytes).expect("failed to deserialize");

    println!("Received response: {:?}", response);
}
