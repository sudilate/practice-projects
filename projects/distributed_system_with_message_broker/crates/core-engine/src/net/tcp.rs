use crate::net::kqueue::{EventFilter, Kqueue};
use crate::net::swim::MembershipRuntime;
use crate::protocol::{ErrorResponse, Frame, Opcode, StreamingDecoder};
use crate::storage::Wal;
use std::collections::{HashMap, VecDeque};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::fd::{AsRawFd, RawFd};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

pub struct AppendServer {
    listener: TcpListener,
    wal: Wal,
    membership: Option<MembershipRuntime>,
}

impl AppendServer {
    pub fn bind(addr: SocketAddr, wal_path: impl AsRef<Path>) -> io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true)?;

        Ok(Self {
            listener,
            wal: Wal::open(wal_path)?,
            membership: None,
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub fn with_membership(
        mut self,
        node_id: String,
        membership_addr: SocketAddr,
        join_peers: Vec<SocketAddr>,
    ) -> io::Result<Self> {
        let mut membership = MembershipRuntime::bind(node_id, membership_addr)?;
        membership.send_join_requests(&join_peers)?;
        self.membership = Some(membership);
        Ok(self)
    }

    pub fn membership_addr(&self) -> Option<io::Result<SocketAddr>> {
        self.membership.as_ref().map(MembershipRuntime::local_addr)
    }

    pub fn run(&mut self) -> io::Result<()> {
        self.run_until(&AtomicBool::new(false))
    }

    pub fn run_until(&mut self, shutdown: &AtomicBool) -> io::Result<()> {
        let kqueue = Kqueue::new()?;
        kqueue.register_read(self.listener.as_raw_fd())?;
        if let Some(membership) = &self.membership {
            membership.register_read(&kqueue)?;
        }
        let mut connections = HashMap::new();

        while !shutdown.load(Ordering::Relaxed) {
            for event in kqueue.wait(Some(Duration::from_millis(50)))? {
                if event.fd == self.listener.as_raw_fd() && event.filter == EventFilter::Read {
                    self.accept_ready(&kqueue, &mut connections)?;
                    continue;
                }
                if let Some(membership) = self.membership.as_mut() {
                    if event.fd == membership.fd() && event.filter == EventFilter::Read {
                        membership.receive_ready()?;
                        continue;
                    }
                }

                match event.filter {
                    EventFilter::Read => self.read_ready(&kqueue, &mut connections, event.fd)?,
                    EventFilter::Write => write_ready(&kqueue, &mut connections, event.fd)?,
                }
            }
            if let Some(membership) = self.membership.as_mut() {
                membership.tick(Instant::now())?;
            }
        }

        Ok(())
    }

    fn accept_ready(
        &self,
        kqueue: &Kqueue,
        connections: &mut HashMap<RawFd, Connection>,
    ) -> io::Result<()> {
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    stream.set_nonblocking(true)?;
                    let fd = stream.as_raw_fd();
                    kqueue.register_read(fd)?;
                    connections.insert(fd, Connection::new(stream));
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) => return Err(error),
            }
        }
    }

    fn read_ready(
        &mut self,
        kqueue: &Kqueue,
        connections: &mut HashMap<RawFd, Connection>,
        fd: RawFd,
    ) -> io::Result<()> {
        let Some(connection) = connections.get_mut(&fd) else {
            return Ok(());
        };

        let mut buffer = [0; 4096];
        loop {
            match connection.stream.read(&mut buffer) {
                Ok(0) => {
                    connections.remove(&fd);
                    return Ok(());
                }
                Ok(read) => match connection.decoder.push(&buffer[..read]) {
                    Ok(frames) => {
                        for frame in frames {
                            connection
                                .write_queue
                                .push_back(handle_frame(&mut self.wal, frame));
                        }
                        if !connection.write_queue.is_empty() {
                            kqueue.register_write(fd)?;
                        }
                    }
                    Err(error) => {
                        connection
                            .write_queue
                            .push_back(error_frame(400, &error.to_string()));
                        kqueue.register_write(fd)?;
                    }
                },
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) => {
                    connections.remove(&fd);
                    return Err(error);
                }
            }
        }
    }
}

struct Connection {
    stream: TcpStream,
    decoder: StreamingDecoder,
    write_queue: VecDeque<Vec<u8>>,
}

impl Connection {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            decoder: StreamingDecoder::new(),
            write_queue: VecDeque::new(),
        }
    }
}

fn write_ready(
    kqueue: &Kqueue,
    connections: &mut HashMap<RawFd, Connection>,
    fd: RawFd,
) -> io::Result<()> {
    let Some(connection) = connections.get_mut(&fd) else {
        return Ok(());
    };

    while let Some(bytes) = connection.write_queue.front_mut() {
        match connection.stream.write(bytes) {
            Ok(0) => break,
            Ok(written) if written == bytes.len() => {
                connection.write_queue.pop_front();
            }
            Ok(written) => {
                bytes.drain(..written);
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(error) => {
                connections.remove(&fd);
                return Err(error);
            }
        }
    }

    if connections
        .get(&fd)
        .is_some_and(|connection| connection.write_queue.is_empty())
    {
        kqueue.unregister_write(fd)?;
    }

    Ok(())
}

fn handle_frame(wal: &mut Wal, frame: Frame) -> Vec<u8> {
    match frame.opcode() {
        Opcode::AppendTask => match wal.append(frame.payload()) {
            Ok(_) => Frame::new(Opcode::Ack, Vec::new())
                .encode()
                .expect("ACK frame encodes"),
            Err(error) => error_frame(500, &error.to_string()),
        },
        opcode => error_frame(400, &format!("unsupported opcode: {opcode:?}")),
    }
}

fn error_frame(code: u16, message: &str) -> Vec<u8> {
    let payload = ErrorResponse {
        code,
        message: message.to_string(),
    }
    .encode()
    .unwrap_or_else(|_| {
        ErrorResponse {
            code,
            message: "error message too large".to_string(),
        }
        .encode()
        .expect("fallback error payload encodes")
    });

    Frame::new(Opcode::Error, payload)
        .encode()
        .expect("error frame encodes")
}

#[cfg(test)]
mod tests {
    use super::AppendServer;
    use crate::protocol::{Frame, Opcode};
    use crate::storage::Wal;
    use std::fs;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpStream};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    #[test]
    fn appends_tcp_frame_to_wal_and_acks() {
        let wal_path = test_wal_path("tcp-append");
        let mut server =
            AppendServer::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap(), &wal_path)
                .expect("server binds");
        let addr = server.local_addr().expect("server addr is available");
        let shutdown = Arc::new(AtomicBool::new(false));
        let server_shutdown = Arc::clone(&shutdown);

        let handle = thread::spawn(move || {
            server
                .run_until(&server_shutdown)
                .expect("server exits cleanly");
        });

        let mut stream = TcpStream::connect(addr).expect("client connects");
        let request = Frame::new(Opcode::AppendTask, b"task".to_vec())
            .encode()
            .expect("request encodes");
        stream.write_all(&request).expect("request writes");

        let mut response = [0; 5];
        stream.read_exact(&mut response).expect("ACK reads");
        let frame = Frame::decode(&response).expect("ACK decodes");

        shutdown.store(true, Ordering::Relaxed);
        handle.join().expect("server joins");

        assert_eq!(frame.opcode(), Opcode::Ack);
        let mut wal = Wal::open(&wal_path).expect("WAL reopens");
        assert_eq!(wal.get(0).expect("WAL reads"), Some(b"task".to_vec()));

        let _ = fs::remove_file(wal_path);
    }

    #[test]
    fn handles_one_hundred_concurrent_clients() {
        const CLIENTS: usize = 100;

        let wal_path = test_wal_path("tcp-100-clients");
        let mut server =
            AppendServer::bind("127.0.0.1:0".parse::<SocketAddr>().unwrap(), &wal_path)
                .expect("server binds");
        let addr = server.local_addr().expect("server addr is available");
        let shutdown = Arc::new(AtomicBool::new(false));
        let server_shutdown = Arc::clone(&shutdown);

        let server_handle = thread::spawn(move || {
            server
                .run_until(&server_shutdown)
                .expect("server exits cleanly");
        });

        let client_handles: Vec<_> = (0..CLIENTS)
            .map(|index| {
                thread::spawn(move || {
                    let mut stream = TcpStream::connect(addr).expect("client connects");
                    stream
                        .set_read_timeout(Some(Duration::from_secs(5)))
                        .expect("read timeout sets");
                    stream
                        .set_write_timeout(Some(Duration::from_secs(5)))
                        .expect("write timeout sets");
                    let payload = format!("task-{index}").into_bytes();
                    let request = Frame::new(Opcode::AppendTask, payload)
                        .encode()
                        .expect("request encodes");
                    stream.write_all(&request).expect("request writes");

                    let mut response = [0; 5];
                    stream.read_exact(&mut response).expect("ACK reads");
                    let frame = Frame::decode(&response).expect("ACK decodes");

                    assert_eq!(frame.opcode(), Opcode::Ack);
                })
            })
            .collect();

        for handle in client_handles {
            handle.join().expect("client joins");
        }

        shutdown.store(true, Ordering::Relaxed);
        server_handle.join().expect("server joins");

        let wal = Wal::open(&wal_path).expect("WAL reopens");
        assert_eq!(wal.len(), CLIENTS);

        let _ = fs::remove_file(wal_path);
    }

    fn test_wal_path(name: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos();
        std::env::temp_dir().join(format!("core-engine-{name}-{nanos}.log"))
    }
}
