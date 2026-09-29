use std::fs::{OpenOptions, File};
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use woonsocket_work::args::WoonsocketServerOpt;

use common::{
    deserialize,
    recv_message,
    send_message,
    serialize,
    Request,
    Response,
};

fn main() {
    let opt = WoonsocketServerOpt::parse();

    let port = opt.port;
    let runtime = opt.runtime_secs;
    let outpath = opt.outpath;

    let listener = TcpListener::bind(
        format!("127.0.0.1:{}", port)
    ).expect("Failed to bind to address");

    listener.set_nonblocking(true).expect("failed to set nonblocking");
    println!("Server listening");

    // Logs
    let output_path = outpath.join("server.log");
    let file = OpenOptions::new().create(true).append(true).open(&output_path).expect("failed to open");
    let file = Arc::new(Mutex::new(file));

    let start = Instant::now();

    while start.elapsed() < Duration::from_secs(runtime) {
        match listener.accept() {
            Ok((stream, addr)) => {
                let file = Arc::clone(&file);
                println!("Client connected: {addr}");

                stream.set_nonblocking(false).expect("failed to set stream nonblocking");

                thread::spawn(move || {
                    handle_connection(stream, file);
                });
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(e) => {
                eprintln!("failed to accept connection: {e}");
            }
        }
    }
    println!("Server runtime complete")
}


fn handle_connection(mut stream: TcpStream, file: Arc<Mutex<File>>){
    loop{
        // Receive request
        let bytes = match recv_message(&mut stream) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                println!("Client disconnected");
                break;
            }
            Err(e) => {
                eprintln!("Failed to receive request: {e}");
                break;
            }
        };

        let request: Request = match deserialize(&bytes) {
            Ok(request) => request,
            Err(e) => {
                eprintln!("Failed to receive request: {e}");
                break;
            }
        };

        let start = Instant::now();

        let work_result = request.work.perform();

        let server_processing_time = start.elapsed().as_nanos() as u64;

        let response = Response {
            result: work_result,
            server_processing_time,
        };

        let bytes = serialize(&response).expect("failed to serialize");

        send_message(&mut stream, &bytes).expect("send failed");

        {
            let mut file = file.lock().expect("failed to lock server log");

            writeln!(
                file,
                "processing_time_ns={}",
                server_processing_time,
            ).expect("failed to write server log");
        }
    }
}
