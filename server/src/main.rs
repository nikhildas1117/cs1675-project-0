use std::net::{TcpListener};

use common::{
    deserialize,
    recv_message,
    serialize,
    send_message,
    Request,
    Response,
};

fn main() {
    let listener = TcpListener::bind("127.0.0.1:8080")
        .expect("Failed to bind to address");

    println!("Server listening on 127.0.0.1:8080");

    let (mut stream, addr) = listener.accept().expect("failed to accept connection");

    println!("Client connected: {addr}");

    // Receive request
    let bytes = recv_message(&mut stream).expect("failed to read");

    let request: Request = deserialize(&bytes).expect("failed to deserialize");

    println!("Received Work: {:?}", request.work);

    let work_result = request.work.perform();

    println!("Work Performed");

    let response = Response {
        result: work_result,
    };

    let bytes = serialize(&response).expect("failed to serialize");

    send_message(&mut stream, &bytes).expect("failed to write");
}
