use core_engine::protocol::{Frame, Opcode};
use std::env;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

fn main() -> std::io::Result<()> {
    let config = Config::from_env()?;
    let started = Instant::now();

    let handles: Vec<_> = (0..config.clients)
        .map(|index| {
            let addr = config.addr;
            let payload_size = config.payload_size;
            thread::spawn(move || run_client(addr, index, payload_size))
        })
        .collect();

    let mut failures = 0;
    for handle in handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                failures += 1;
                eprintln!("client failed: {error}");
            }
            Err(_) => {
                failures += 1;
                eprintln!("client panicked");
            }
        }
    }

    let elapsed = started.elapsed();
    let successes = config.clients - failures;
    let requests_per_second = successes as f64 / elapsed.as_secs_f64();

    println!(
        "clients={} successes={} failures={} elapsed_ms={} throughput_rps={:.2}",
        config.clients,
        successes,
        failures,
        elapsed.as_millis(),
        requests_per_second
    );

    if failures > 0 {
        std::process::exit(1);
    }

    Ok(())
}

fn run_client(addr: SocketAddr, index: usize, payload_size: usize) -> std::io::Result<()> {
    let mut stream = connect_with_retry(addr, Duration::from_secs(5))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    let mut payload = format!("task-{index}:").into_bytes();
    payload.resize(payload_size.max(payload.len()), b'x');
    let request = Frame::new(Opcode::AppendTask, payload)
        .encode()
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;

    stream.write_all(&request)?;

    let mut response = [0; 5];
    stream.read_exact(&mut response)?;
    let frame = Frame::decode(&response)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    if frame.opcode() != Opcode::Ack {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("expected ACK, got {:?}", frame.opcode()),
        ));
    }

    Ok(())
}

fn connect_with_retry(addr: SocketAddr, timeout: Duration) -> std::io::Result<TcpStream> {
    let started = Instant::now();
    let mut last_error = None;

    while started.elapsed() < timeout {
        match TcpStream::connect(addr) {
            Ok(stream) => return Ok(stream),
            Err(error) => {
                last_error = Some(error);
                thread::sleep(Duration::from_millis(25));
            }
        }
    }

    Err(last_error
        .unwrap_or_else(|| std::io::Error::new(std::io::ErrorKind::TimedOut, "connect timed out")))
}

struct Config {
    addr: SocketAddr,
    clients: usize,
    payload_size: usize,
}

impl Config {
    fn from_env() -> std::io::Result<Self> {
        let mut addr = "127.0.0.1:7000".parse::<SocketAddr>().unwrap();
        let mut clients = 100;
        let mut payload_size = 32;
        let mut args = env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--addr" => {
                    let value = args
                        .next()
                        .ok_or_else(|| invalid_arg("missing --addr value"))?;
                    addr = value
                        .parse()
                        .map_err(|_| invalid_arg("invalid --addr value"))?;
                }
                "--clients" => {
                    let value = args
                        .next()
                        .ok_or_else(|| invalid_arg("missing --clients value"))?;
                    clients = value
                        .parse()
                        .map_err(|_| invalid_arg("invalid --clients value"))?;
                }
                "--payload-size" => {
                    let value = args
                        .next()
                        .ok_or_else(|| invalid_arg("missing --payload-size value"))?;
                    payload_size = value
                        .parse()
                        .map_err(|_| invalid_arg("invalid --payload-size value"))?;
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                _ => return Err(invalid_arg("unknown argument")),
            }
        }

        Ok(Self {
            addr,
            clients,
            payload_size,
        })
    }
}

fn invalid_arg(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
}

fn print_usage() {
    println!("usage: tcp_benchmark [--addr 127.0.0.1:7000] [--clients 100] [--payload-size 32]");
}
