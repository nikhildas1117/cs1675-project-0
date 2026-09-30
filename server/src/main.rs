use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use clap::Parser;
use woonsocket_work::args::WoonsocketServerOpt;

use common::{deserialize, recv_message, send_message, serialize, Request, Response};


fn main() {
    // Get Parameters
    let opt = WoonsocketServerOpt::parse();
    let port = opt.port;
    let runtime = opt.runtime_secs;
    let outpath = opt.outpath;

    // Create output directory
    std::fs::create_dir_all(&outpath).expect("failed to create outpath");


    // Bind Listener
    let listener = TcpListener::bind(("0.0.0.0", port)).expect("failed to bind");
    listener.set_nonblocking(true).expect("failed to set listener nonblocking");
    println!("Server listening on 0.0.0.0:{port}");

    // Initialize Time
    let deadline = Duration::from_secs(runtime);
    let start = Instant::now();

    // Handles return each connection's per-request processing times.
    let mut handles: Vec<JoinHandle<Vec<u64>>> = Vec::new();
    // Clones used to unblock reader threads at shutdown.
    let mut closers: Vec<TcpStream> = Vec::new();

    while start.elapsed() < deadline {
        match listener.accept() {
            Ok((stream, _addr)) => {
                // Accepted sockets may inherit non-blocking mode; force blocking.
                // if stream.set_nonblocking(false).is_err() {
                //     continue;
                // }
                stream.set_nodelay(true).ok();
                if let Ok(c) = stream.try_clone() {
                    closers.push(c);
                }
                handles.push(thread::spawn(move || handle_connection(stream)));
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_micros(500));
            }
            Err(e) => {
                eprintln!("failed to accept connection: {e}");
                thread::sleep(Duration::from_millis(1));
            }
        }
    }

    // Time's up: unblock any threads still waiting on a read, then collect.
    for c in &closers {
        let _ = c.shutdown(Shutdown::Both);
    }

    let mut times: Vec<u64> = Vec::new();
    for h in handles {
        if let Ok(mut t) = h.join() {
            times.append(&mut t);
        }
    }

    write_log(&outpath, &times);
    println!("Server runtime complete: {} requests handled", times.len());
}


// Serve one client connection until it disconnects or errors.
fn handle_connection(mut stream: TcpStream) -> Vec<u64> {
    let mut times = Vec::new();

    loop {
        let bytes = match recv_message(&mut stream) {
            Ok(b) => b,
            Err(_) => break, // EOF, reset, or shutdown at exit
        };

        let request: Request = match deserialize(&bytes) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("failed to deserialize request: {e}");
                break;
            }
        };

        let t0 = Instant::now();
        let result = request.work.perform();
        let server_processing_time = t0.elapsed().as_nanos() as u64;

        let response = Response {
            result,
            server_processing_time,
        };

        let bytes = match serialize(&response) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("failed to serialize response: {e}");
                break;
            }
        };

        if send_message(&mut stream, &bytes).is_err() {
            break;
        }

        times.push(server_processing_time);
    }

    times
}


//  Write everything once at the end, buffered.
fn write_log(outpath: &std::path::Path, times: &[u64]) {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(outpath.join("server.log"))
        .expect("failed to open server log");
    let mut w = BufWriter::new(file);

    for t in times {
        writeln!(w, "processing_time_ns={t}").expect("failed to write server log");
    }
    w.flush().expect("failed to flush server log");
}