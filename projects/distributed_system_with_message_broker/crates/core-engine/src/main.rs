#[cfg(target_os = "macos")]
use core_engine::log;
#[cfg(target_os = "macos")]
use core_engine::net::raft::RaftPeer;
#[cfg(target_os = "macos")]
use core_engine::net::tcp::AppendServer;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
#[cfg(target_os = "macos")]
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(target_os = "macos")]
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

fn main() -> std::io::Result<()> {
    #[cfg(not(target_os = "macos"))]
    {
        eprintln!("core-engine TCP server currently requires macOS kqueue");
        std::process::exit(1);
    }

    #[cfg(target_os = "macos")]
    {
        install_signal_handlers();
        let config = Config::from_env()?;
        let mut server = AppendServer::bind(config.addr, &config.wal_path)?
            .with_node_id(config.node_id.clone())
            .with_membership(
                config.node_id.clone(),
                config.membership_addr,
                config.join_membership_addrs.clone(),
            )?;
        server = if let Some(raft_log_path) = &config.raft_log_path {
            server.with_raft_and_log(
                config.node_id.clone(),
                config.raft_peers.clone(),
                raft_log_path,
            )?
        } else {
            server.with_raft(config.node_id.clone(), config.raft_peers.clone())
        };

        let addr = server.local_addr()?;
        let membership = server
            .membership_addr()
            .transpose()?
            .expect("membership is enabled");
        log::info(
            "core-engine starting",
            &[
                ("node_id", config.node_id.clone()),
                ("addr", addr.to_string()),
                ("membership_addr", membership.to_string()),
                ("wal", config.wal_path.display().to_string()),
                (
                    "raft_log",
                    config
                        .raft_log_path
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "none".to_string()),
                ),
            ],
        );

        let result = server.run_until(&SHUTDOWN);
        log::info(
            "core-engine stopped",
            &[("node_id", config.node_id.clone())],
        );
        result
    }
}

#[cfg(target_os = "macos")]
fn install_signal_handlers() {
    unsafe {
        libc::signal(libc::SIGINT, handle_signal as libc::sighandler_t);
        libc::signal(libc::SIGTERM, handle_signal as libc::sighandler_t);
    }
}

#[cfg(target_os = "macos")]
extern "C" fn handle_signal(_sig: libc::c_int) {
    SHUTDOWN.store(true, Ordering::Relaxed);
}

#[cfg(target_os = "macos")]
struct Config {
    node_id: String,
    addr: SocketAddr,
    membership_addr: SocketAddr,
    join_membership_addrs: Vec<SocketAddr>,
    raft_peers: Vec<RaftPeer>,
    wal_path: PathBuf,
    raft_log_path: Option<PathBuf>,
}

#[cfg(target_os = "macos")]
impl Config {
    fn from_env() -> std::io::Result<Self> {
        let mut node_id = env::var("NODE_ID").unwrap_or_else(|_| "node-1".to_string());
        let mut addr = env::var("ADDR")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| "127.0.0.1:7000".parse::<SocketAddr>().unwrap());
        let mut membership_addr = env::var("MEMBERSHIP_ADDR")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(|| "127.0.0.1:7100".parse::<SocketAddr>().unwrap());
        let mut join_membership_addrs = Vec::new();
        let mut raft_peers = Vec::new();
        let mut data_dir: Option<PathBuf> = env::var_os("DATA_DIR").map(PathBuf::from);
        let mut wal_path: Option<PathBuf> = env::var_os("WAL_PATH").map(PathBuf::from);
        let mut raft_log_path: Option<PathBuf> = env::var_os("RAFT_LOG_PATH").map(PathBuf::from);
        let mut args = env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--node-id" => {
                    node_id = args
                        .next()
                        .ok_or_else(|| invalid_arg("missing --node-id value"))?;
                }
                "--addr" => {
                    let value = args
                        .next()
                        .ok_or_else(|| invalid_arg("missing --addr value"))?;
                    addr = value
                        .parse()
                        .map_err(|_| invalid_arg("invalid --addr value"))?;
                }
                "--membership-addr" => {
                    let value = args
                        .next()
                        .ok_or_else(|| invalid_arg("missing --membership-addr value"))?;
                    membership_addr = value
                        .parse()
                        .map_err(|_| invalid_arg("invalid --membership-addr value"))?;
                }
                "--join" => {
                    let value = args
                        .next()
                        .ok_or_else(|| invalid_arg("missing --join value"))?;
                    join_membership_addrs.push(
                        value
                            .parse()
                            .map_err(|_| invalid_arg("invalid --join value"))?,
                    );
                }
                "--raft-peer" => {
                    let value = args
                        .next()
                        .ok_or_else(|| invalid_arg("missing --raft-peer value"))?;
                    raft_peers.push(parse_raft_peer(&value)?);
                }
                "--data-dir" => {
                    let value = args
                        .next()
                        .ok_or_else(|| invalid_arg("missing --data-dir value"))?;
                    data_dir = Some(PathBuf::from(value));
                }
                "--wal" => {
                    let value = args
                        .next()
                        .ok_or_else(|| invalid_arg("missing --wal value"))?;
                    wal_path = Some(PathBuf::from(value));
                }
                "--raft-log" => {
                    let value = args
                        .next()
                        .ok_or_else(|| invalid_arg("missing --raft-log value"))?;
                    raft_log_path = Some(PathBuf::from(value));
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                _ => return Err(invalid_arg("unknown argument")),
            }
        }

        if let Some(dir) = &data_dir {
            std::fs::create_dir_all(dir)?;
            if wal_path.is_none() {
                wal_path = Some(dir.join("wal.log"));
            }
            if raft_log_path.is_none() {
                raft_log_path = Some(dir.join("raft.log"));
            }
        }

        let wal_path = wal_path.unwrap_or_else(|| PathBuf::from("data/node-1.log"));
        if let Some(parent) = wal_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        if let Some(path) = &raft_log_path {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
        }

        Ok(Self {
            node_id,
            addr,
            membership_addr,
            join_membership_addrs,
            raft_peers,
            wal_path,
            raft_log_path,
        })
    }
}

#[cfg(target_os = "macos")]
fn parse_raft_peer(value: &str) -> std::io::Result<RaftPeer> {
    let Some((id, addr)) = value.split_once('=') else {
        return Err(invalid_arg("invalid --raft-peer value"));
    };
    if id.is_empty() {
        return Err(invalid_arg("invalid --raft-peer value"));
    }
    Ok(RaftPeer {
        id: id.to_string(),
        addr: addr
            .parse()
            .map_err(|_| invalid_arg("invalid --raft-peer value"))?,
    })
}

#[cfg(target_os = "macos")]
fn invalid_arg(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
}

#[cfg(target_os = "macos")]
fn print_usage() {
    println!(
        "usage: core-engine [--node-id node-1] [--addr 127.0.0.1:7000] \
         [--membership-addr 127.0.0.1:7100] [--join 127.0.0.1:7100] \
         [--raft-peer node-2=127.0.0.1:7001] [--data-dir data/node-1] \
         [--wal data/node-1.log] [--raft-log data/node-1.raft.log]"
    );
    println!("env: NODE_ID ADDR MEMBERSHIP_ADDR DATA_DIR WAL_PATH RAFT_LOG_PATH");
}
