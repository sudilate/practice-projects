use core_engine::storage::Wal;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn main() -> std::io::Result<()> {
    let config = Config::from_env()?;
    let path = config
        .path
        .clone()
        .unwrap_or_else(|| temp_wal_path("wal-bench"));

    if path.exists() {
        fs::remove_file(&path)?;
    }

    let mut wal = Wal::open(&path)?;
    let payload = vec![b'x'; config.payload_size];

    let started = Instant::now();
    for _ in 0..config.records {
        wal.append(&payload)?;
    }
    let elapsed = started.elapsed();

    let records_per_second = config.records as f64 / elapsed.as_secs_f64().max(f64::EPSILON);
    let bytes = config.records as u64 * (4 + config.payload_size as u64);
    let mib_per_second = (bytes as f64 / (1024.0 * 1024.0)) / elapsed.as_secs_f64().max(f64::EPSILON);

    println!(
        "records={} payload_size={} elapsed_ms={} throughput_rps={:.2} throughput_mib_s={:.2}",
        config.records,
        config.payload_size,
        elapsed.as_millis(),
        records_per_second,
        mib_per_second
    );

    if config.keep {
        println!("wal_path={}", path.display());
    } else {
        let _ = fs::remove_file(&path);
    }

    Ok(())
}

struct Config {
    records: usize,
    payload_size: usize,
    path: Option<PathBuf>,
    keep: bool,
}

impl Config {
    fn from_env() -> std::io::Result<Self> {
        let mut records = 10_000;
        let mut payload_size = 64;
        let mut path = None;
        let mut keep = false;
        let mut args = env::args().skip(1);

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--records" => {
                    let value = args
                        .next()
                        .ok_or_else(|| invalid_arg("missing --records value"))?;
                    records = value
                        .parse()
                        .map_err(|_| invalid_arg("invalid --records value"))?;
                }
                "--payload-size" => {
                    let value = args
                        .next()
                        .ok_or_else(|| invalid_arg("missing --payload-size value"))?;
                    payload_size = value
                        .parse()
                        .map_err(|_| invalid_arg("invalid --payload-size value"))?;
                }
                "--path" => {
                    let value = args
                        .next()
                        .ok_or_else(|| invalid_arg("missing --path value"))?;
                    path = Some(PathBuf::from(value));
                }
                "--keep" => keep = true,
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                _ => return Err(invalid_arg("unknown argument")),
            }
        }

        Ok(Self {
            records,
            payload_size,
            path,
            keep,
        })
    }
}

fn temp_wal_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos();
    env::temp_dir().join(format!("core-engine-{name}-{nanos}.log"))
}

fn invalid_arg(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
}

fn print_usage() {
    println!(
        "usage: wal_benchmark [--records 10000] [--payload-size 64] [--path /tmp/wal.log] [--keep]"
    );
}
