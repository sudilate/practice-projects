use crate::net::kqueue::Kqueue;
use std::io;
use std::net::{SocketAddr, UdpSocket};
use std::os::fd::{AsRawFd, RawFd};

pub const MAX_DATAGRAM_SIZE: usize = 65_507;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Datagram {
    pub source: SocketAddr,
    pub bytes: Vec<u8>,
}

#[derive(Debug)]
pub struct UdpTransport {
    socket: UdpSocket,
}

impl UdpTransport {
    pub fn bind(addr: SocketAddr) -> io::Result<Self> {
        let socket = UdpSocket::bind(addr)?;
        socket.set_nonblocking(true)?;
        Ok(Self { socket })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    pub fn fd(&self) -> RawFd {
        self.socket.as_raw_fd()
    }

    pub fn register_read(&self, kqueue: &Kqueue) -> io::Result<()> {
        kqueue.register_read(self.fd())
    }

    pub fn send_to(&self, target: SocketAddr, bytes: &[u8]) -> io::Result<usize> {
        self.socket.send_to(bytes, target)
    }

    pub fn receive_ready(&self) -> io::Result<Vec<Datagram>> {
        let mut datagrams = Vec::new();
        let mut buffer = [0; MAX_DATAGRAM_SIZE];

        loop {
            match self.socket.recv_from(&mut buffer) {
                Ok((read, source)) => datagrams.push(Datagram {
                    source,
                    bytes: buffer[..read].to_vec(),
                }),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(datagrams),
                Err(error) => return Err(error),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::UdpTransport;
    use crate::net::kqueue::{EventFilter, Kqueue};
    use std::net::SocketAddr;
    use std::time::{Duration, Instant};

    #[test]
    fn sends_and_receives_datagram() {
        let receiver = UdpTransport::bind(addr(0)).expect("receiver binds");
        let sender = UdpTransport::bind(addr(0)).expect("sender binds");
        let receiver_addr = receiver.local_addr().expect("receiver addr is available");
        let sender_addr = sender.local_addr().expect("sender addr is available");
        let kqueue = Kqueue::new().expect("kqueue creates");
        receiver.register_read(&kqueue).expect("receiver registers");

        sender
            .send_to(receiver_addr, b"membership")
            .expect("datagram sends");

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            let events = kqueue
                .wait(Some(Duration::from_millis(25)))
                .expect("kqueue waits");
            if events
                .iter()
                .any(|event| event.fd == receiver.fd() && event.filter == EventFilter::Read)
            {
                let datagrams = receiver.receive_ready().expect("datagrams receive");

                assert_eq!(datagrams.len(), 1);
                assert_eq!(datagrams[0].source, sender_addr);
                assert_eq!(datagrams[0].bytes, b"membership");
                return;
            }

            assert!(
                Instant::now() < deadline,
                "timed out waiting for UDP readiness"
            );
        }
    }

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }
}
