use crate::membership::{Member, MembershipMessage, SwimState};
use crate::net::kqueue::Kqueue;
use crate::net::udp::UdpTransport;
use crate::protocol::Frame;
use std::io;
use std::net::SocketAddr;
use std::os::fd::RawFd;

#[derive(Debug)]
pub struct MembershipRuntime {
    transport: UdpTransport,
    state: SwimState,
}

impl MembershipRuntime {
    pub fn bind(local_id: String, local_addr: SocketAddr) -> io::Result<Self> {
        let transport = UdpTransport::bind(local_addr)?;
        let bound_addr = transport.local_addr()?;
        Ok(Self {
            transport,
            state: SwimState::new(local_id, bound_addr),
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.transport.local_addr()
    }

    pub fn fd(&self) -> RawFd {
        self.transport.fd()
    }

    pub fn members(&self) -> &std::collections::HashMap<String, Member> {
        self.state.members()
    }

    pub fn register_read(&self, kqueue: &Kqueue) -> io::Result<()> {
        self.transport.register_read(kqueue)
    }

    pub fn send_join_requests(&self, peers: &[SocketAddr]) -> io::Result<()> {
        let local = self
            .state
            .member(self.state.local_id())
            .expect("local member is always present")
            .clone();
        for peer in peers {
            self.send_message(*peer, &MembershipMessage::Join(local.clone()))?;
        }
        Ok(())
    }

    pub fn receive_ready(&mut self) -> io::Result<()> {
        for datagram in self.transport.receive_ready()? {
            self.handle_datagram(datagram.source, &datagram.bytes)?;
        }
        Ok(())
    }

    fn handle_datagram(&mut self, source: SocketAddr, bytes: &[u8]) -> io::Result<()> {
        let frame = Frame::decode(bytes).map_err(invalid_data)?;
        let message =
            MembershipMessage::decode(frame.opcode(), frame.payload()).map_err(invalid_data)?;
        self.handle_message(source, message)
    }

    fn handle_message(&mut self, source: SocketAddr, message: MembershipMessage) -> io::Result<()> {
        match message {
            MembershipMessage::Join(member) => {
                let response_addr = member.addr;
                let known_members = self.state.handle_join(member);
                self.send_message(response_addr, &MembershipMessage::JoinAck(known_members))?;
            }
            MembershipMessage::JoinAck(members) => {
                for member in members {
                    self.state.apply_update(member);
                }
            }
            MembershipMessage::Ping { from, target } if target == self.state.local_id() => {
                let response_addr = self
                    .state
                    .member(&from)
                    .map(|member| member.addr)
                    .unwrap_or(source);
                self.send_message(
                    response_addr,
                    &MembershipMessage::Ack {
                        from: self.state.local_id().to_string(),
                        target: from,
                    },
                )?;
            }
            MembershipMessage::Ack { from, target } if target == self.state.local_id() => {
                self.state.handle_ack(&from);
            }
            MembershipMessage::Update(member) => {
                self.state.apply_update(member);
            }
            MembershipMessage::PingReq { .. }
            | MembershipMessage::Ping { .. }
            | MembershipMessage::Ack { .. } => {}
        }
        Ok(())
    }

    fn send_message(&self, target: SocketAddr, message: &MembershipMessage) -> io::Result<()> {
        let payload = message.encode_payload().map_err(invalid_data)?;
        let frame = Frame::new(message.opcode(), payload)
            .encode()
            .map_err(invalid_data)?;
        self.transport.send_to(target, &frame)?;
        Ok(())
    }
}

fn invalid_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::MembershipRuntime;
    use std::net::SocketAddr;
    use std::time::{Duration, Instant};

    #[test]
    fn join_request_converges_two_member_maps() {
        let mut node_a = MembershipRuntime::bind("node-a".to_string(), addr(0)).expect("a binds");
        let mut node_b = MembershipRuntime::bind("node-b".to_string(), addr(0)).expect("b binds");
        let node_b_addr = node_b.local_addr().expect("b addr is available");

        node_a
            .send_join_requests(&[node_b_addr])
            .expect("join sends");

        eventually(|| {
            node_b.receive_ready().expect("b receives");
            node_b.members().contains_key("node-a")
        });
        eventually(|| {
            node_a.receive_ready().expect("a receives");
            node_a.members().contains_key("node-b")
        });
    }

    fn eventually(mut condition: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if condition() {
                return;
            }
            assert!(Instant::now() < deadline, "condition timed out");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }
}
