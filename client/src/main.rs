use std::fs::OpenOptions;
use std::io::{BufWriter, Write};
use std::net::{Ipv4Addr, Shutdown, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use rand::thread_rng;
use rand_distr::{Distribution, Exp};
use woonsocket_work::args::{ClientMode, OpenLoopKind, WoonsocketClientOpt};
use woonsocket_work::Work;

use common::{deserialize, 
    recv_message, 
    send_message, 
    serialize, 
    LatencyRecord, 
    Request, 
    Response
};

/// Number of persistent connections used by open-loop generator.
const OPEN_LOOP_CONNECTIONS: usize = 64;
/// Extra time after the runtime to wait for outstanding responses.
const DRAIN_GRACE: Duration = Duration::from_secs(2);


fn main() {
    // Define Parameters
    let opt = WoonsocketClientOpt::parse();
    let runtime = opt.runtime_secs;
    let ip = opt.ip;
    let port = opt.port;
    let work = opt.work;
    let outpath = opt.outpath;

    // Create output directory
    std::fs::create_dir_all(&outpath).expect("failed to create outpath");

    // One shared time origin for every thread.
    let start = Instant::now();

    let records = match opt.mode {
        ClientMode::ClosedLoop { num_threads } => {
            println!("Running in Closed Loop Mode");
            closed_loop(num_threads, ip, port, work, runtime, start)
        }
        ClientMode::OpenLoop { interval_us, kind } => {
            println!("Running in Open Loop Mode");
            open_loop(interval_us, kind, ip, port, work, runtime, start)
        }
    };

    write_records(&outpath.join("latency.csv"), &records);
    println!("Client finished: {} responses recorded", records.len());
}


// CLOSED LOOP
fn closed_loop(
    num_threads: u64,
    ip: Ipv4Addr,
    port: u16,
    work: Work,
    runtime: u64,
    start: Instant,
) -> Vec<LatencyRecord> {
    let deadline = Duration::from_secs(runtime);
    let mut handles = Vec::new();

    for _ in 0..num_threads {
        handles.push(thread::spawn(move || {
            // Each thread gets its own records
            let mut records = Vec::new();
            let mut stream = TcpStream::connect((ip, port)).expect("failed to connect");
            stream.set_nodelay(true).except("set_nodedelay failed");
        
            //
            while start.elapsed() < deadline {
                let send_timestamp = start.elapsed().as_nanos() as u64;
                let request = Request { work };
                let bytes = serialize(&request).expect("failed to serialize");

                if send_message(&mut stream, &bytes).is_err() { 
                    println!("Thread failed to send")
                    break; 
                }

                // recv_message blocking TODO: double check
                let bytes = match recv_message(&mut stream) {
                    Ok(b) => b,
                    Err(_) => 
                    println!("Thread failed to recv");
                    break;
                };
                let recv_timestamp = start.elapsed().as_nanos() as u64;
                let response: Response = match deserialize(&bytes) {
                    Ok(r) => r,
                    Err(_) => 
                    println!("Thread failed to deserialize");
                    break;
                };

                records.push(
                    make_record(send_timestamp, recv_timestamp, &response)
                );
            }

            records
        }));
    }

    let mut all = Vec::new();
    for h in handles {
        let records = h.join().expect("closed-loop thread panicked");
        all.extend(records);
    }
    all
}



// OPEN LOOP
fn open_loop(
    interval_us: u64,
    kind: OpenLoopKind,
    ip: Ipv4Addr,
    port: u16,
    work: Work,
    runtime: u64,
    start: Instant,
) -> Vec<LatencyRecord> {
    let deadline = Duration::from_secs(runtime);

    // One TCP connection + reader thread per connection.
    let mut conns = Vec::new();
    let mut readers = Vec::new();

    for _ in 0..OPEN_LOOP_CONNECTIONS {
        let stream = TcpStream::connect((ip, port)).expect("failed to connect");
        stream.set_nodelay(true).ok();
        
        // One stream for sending, the cloned stream for receiving
        let mut reader_stream = stream.try_clone().expect("clone failed");

        // Send timestamps to reader thread
        let (ts_tx, ts_rx) = channel::<u64>();

        
        readers.push(thread::spawn(move || {
            let mut records = Vec::new();

            loop {
                let bytes = match recv_message(&mut reader_stream) {
                    Ok(b) => b,
                    Err(_) => break,
                };

                let recv_timestamp = start.elapsed().as_nanos() as u64;
                let send_timestamp = match ts_rx.recv() {
                    Ok(t) => t,
                    Err(_) => break,
                };

                let response: Response = match deserialize(&bytes) {
                    Ok(r) => r,
                    Err(_) => break,
                };
                records.push(
                    make_record(send_timestamp, recv_timestamp, &response)
                );
            }
            records
        }));

        conns.push((stream, ts_tx)));
    }

    // Generate Timings
    let mut rng = thread_rng();
    let exp = match kind {
        OpenLoopKind::Poisson => Some(
            Exp::new(1.0 / (interval_us.max(1) as f64)).expect("invalid distribution"),
        ),
        OpenLoopKind::Constant => None,
    };

    let mut next = Duration::ZERO;
    let mut connection = 0;

    while next < deadline {
        let now = start.elapsed();
        if next > now {
            thread::sleep(next - now);
        }

        // Choose connection 
        let i = connection % conns.len();
        connection += 1;

        let (stream, ts_tx) = &mut conns[i];

        // Use the scheduled time as the request timestamp.
        let send_timestamp = next.as_nanos() as u64;

        let request = Request { work };
        let bytes = serialize(&request).expect("failed to serialize");

        // Tell the reader when this request was scheduled.
        if ts_tx.send(send_timestamp).is_err() {
            break;
        }

        // Send without waiting for the response.
        if send_message(stream, &bytes).is_err() {
            break;
        }

        // Schedule the next request.
        let gap_us = match (&kind, &exp) {
            (OpenLoopKind::Poisson, Some(exp)) => {
                exp.sample(&mut rng)
            }
            _ => interval_us.max(1) as f64,
        };

        next += Duration::from_secs_f64(
            gap_us / 1_000_000.0
        );
    }

    drop(conns);

    readers
        .into_iter()
        .flat_map(|h| h.join().expect("reader thread panicked"))
        .collect()
}

// LOGGING
fn make_record(send_timestamp: u64, recv_timestamp: u64, response: &Response) -> LatencyRecord {
    LatencyRecord {
        latency: recv_timestamp.saturating_sub(send_timestamp),
        send_timestamp,
        server_processing_time: response.server_processing_time,
        recv_timestamp,
    }
}


fn write_records(path: &std::path::Path, records: &[LatencyRecord]) {
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("failed to open latency file");
    let is_empty = file.metadata().map(|m| m.len() == 0).unwrap_or(true);
    let mut w = BufWriter::new(file);

    if is_empty {
        writeln!(w, "latency,send_timestamp,server_processing_time,recv_timestamp")
            .expect("failed to write header");
    }
    for r in records {
        writeln!(
            w,
            "{},{},{},{}",
            r.latency, r.send_timestamp, r.server_processing_time, r.recv_timestamp
        )
        .expect("failed to write record");
    }
    w.flush().expect("failed to flush");
}
