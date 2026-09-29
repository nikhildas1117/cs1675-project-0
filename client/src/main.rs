use std::fs::{OpenOptions, File};
use std::io::Write;
use std::net::{Ipv4Addr, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use rand::thread_rng;
use rand_distr::{Distribution, Exp};
use woonsocket_work::args::{
    ClientMode,
    OpenLoopKind,
    WoonsocketClientOpt,
};
use woonsocket_work::Work;

use common::{
    deserialize,
    recv_message,
    send_message,
    serialize,
    LatencyRecord,
    Request,
    Response,
};


fn main() {
    // Define Parameters
    let opt = WoonsocketClientOpt::parse();
    let runtime = opt.runtime_secs;
    let ip = opt.ip;
    let port = opt.port;
    let work = opt.work;
    let outpath = opt.outpath;

    // Setup Outpath
    let output_path = outpath.join("latency.csv");

    let mut file = OpenOptions::new().create(true).append(true).open(&output_path).expect("failed to open");
    
    if file.metadata().expect("failed to read metadata").len() == 0 {
        writeln!(
            file,
            "latency,send_timestamp,server_processing_time,recv_timestamp"
        ).expect("failed to write header");
    }
    let file = Arc::new(Mutex::new(file));
    
    // Match Mode
    match opt.mode {
        ClientMode::ClosedLoop { num_threads } => {
            println!("Running in Closed Loop Mode");
            closed_loop(num_threads, ip, port, work, runtime, file);
        },
        ClientMode::OpenLoop { interval_us, kind, } => {
            println!("Running in Open Loop Mode");
            open_loop(interval_us, kind, ip, port, work, runtime, file);
        },
    }
}


fn closed_loop(
    num_threads: u64,
    ip: Ipv4Addr,
    port: u16,
    work: Work,
    runtime: u64,
    file: Arc<Mutex<File>>,
) {
    let mut handles = Vec::new();

    for _ in 0..num_threads {
        let file = Arc::clone(&file);

        let handle = thread::spawn(move || {
            let mut stream = TcpStream::connect(
                format!("{}:{}", ip, port)).expect("failed to connect");
            
            let start = Instant::now();

            while start.elapsed() < Duration::from_secs(runtime) {
                let request = Request { work };
                let bytes = serialize(&request).expect("failed to serialize");
                let send_timestamp = start.elapsed().as_nanos() as u64;
                send_message(&mut stream, &bytes).expect("failed to write");

                let bytes = recv_message(&mut stream).expect("failed to read");
                let recv_timestamp = start.elapsed().as_nanos() as u64;
                let response: Response = deserialize(&bytes).expect("failed to deserialize");

                let latency = recv_timestamp - send_timestamp;

                let record = LatencyRecord {
                    latency,
                    send_timestamp,
                    server_processing_time: response.server_processing_time,
                    recv_timestamp,
                };

                write_record(&file, &record)
            }
        });

        handles.push(handle);
    }
    
    // Collect Threads
    for handle in handles {
        handle.join().expect("Thread panicked");
    }
}




fn open_loop(
    interval_us: u64,
    kind: OpenLoopKind,
    ip: Ipv4Addr,
    port: u16,
    work: Work,
    runtime: u64,
    file: Arc<Mutex<File>>,
) {
    let mut rng = thread_rng();
    let poisson = Exp::new(1.0 / interval_us as f64).expect("invalid exponential distribution");

    let start = Instant::now();

    let mut handles = Vec::new();

    while start.elapsed() < Duration::from_secs(runtime) {
        let request_ip = ip;
        let request_work = work;
        let request_file = Arc::clone(&file);

        let handle = thread::spawn(move || {
            let mut stream = TcpStream::connect(
                format!("{}:{}", ip, port)).expect("failed to connect");
            let request = Request { work };
            let bytes = serialize(&request).expect("failed to serialize");
            let send_timestamp = start.elapsed().as_nanos() as u64;
            send_message(&mut stream, &bytes).expect("failed to write");

            let bytes = recv_message(&mut stream).expect("failed to read");
            let recv_timestamp = start.elapsed().as_nanos() as u64;
            let response: Response = deserialize(&bytes).expect("failed to deserialize");
            let latency = recv_timestamp - send_timestamp;

            let record = LatencyRecord {
                latency,
                send_timestamp,
                server_processing_time: response.server_processing_time,
                recv_timestamp,
            };

            write_record(&request_file, &record)
        });

        handles.push(handle);
        
        let interval = match kind {
            OpenLoopKind::Constant => interval_us,
            OpenLoopKind::Poisson => {
                poisson.sample(&mut rng) as u64
            }
        };
        thread::sleep(Duration::from_micros(interval))
    }

    for handle in handles {
        handle.join().expect("thread panicked");
    }
}

fn write_record(
    file: &Arc<Mutex<std::fs::File>>,
    record: &LatencyRecord,
) {
    let mut file = file.lock().expect("failed to lock output file");
    writeln!(
        file,
        "{},{},{},{}",
        record.latency,
        record.send_timestamp,
        record.server_processing_time,
        record.recv_timestamp
    ).expect("failed to write latency record")
}
