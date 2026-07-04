#[cfg(target_os = "macos")]
use core_engine::net::tcp::AppendServer;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;

fn main() -> std::io::Result<()> {
    #[cfg(not(target_os = "macos"))]
    {
        eprintln!("core-engine TCP server currently requires macOS kqueue");
        std::process::exit(1);
    }

    #[cfg(target_os = "macos")]
    {
        let config = Config::from_env()?;
        let mut server = AppendServer::bind(config.addr, &config.wal_path)?;
        println!(
            "core-engine listening on {} with WAL {}",
            server.local_addr()?,
            config.wal_path.display()
        );
        server.run()
    }
}

#[cfg(target_os = "macos")]
struct Config {
    addr: SocketAddr,
    wal_path: PathBuf,
}

#[cfg(target_os = "macos")]
impl Config {
    fn from_env() -> std::io::Result<Self> {
        let mut addr = "127.0.0.1:7000".parse::<SocketAddr>().unwrap();
        let mut wal_path = PathBuf::from("data/node-1.log");
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
                "--wal" => {
                    let value = args
                        .next()
                        .ok_or_else(|| invalid_arg("missing --wal value"))?;
                    wal_path = PathBuf::from(value);
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                _ => return Err(invalid_arg("unknown argument")),
            }
        }

        if let Some(parent) = wal_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        Ok(Self { addr, wal_path })
    }
}

#[cfg(target_os = "macos")]
fn invalid_arg(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
}

#[cfg(target_os = "macos")]
fn print_usage() {
    println!("usage: core-engine [--addr 127.0.0.1:7000] [--wal data/node-1.log]");
}
