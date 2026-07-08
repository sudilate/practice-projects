use crate::membership::{Member, MembershipMessage, SwimState, UpdateOutcome};
use crate::net::kqueue::Kqueue;
use crate::net::udp::UdpTransport;
use crate::protocol::Frame;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::SocketAddr;
use std::os::fd::RawFd;
use std::time::{Duration, Instant};

const DEFAULT_PROBE_INTERVAL: Duration = Duration::from_secs(1);
const DEFAULT_PROBE_TIMEOUT: Duration = Duration::from_millis(500);
const DEFAULT_SUSPECT_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_PIGGYBACK_LIMIT: usize = 4;
const DEFAULT_DISSEMINATION_TRANSMISSIONS: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MembershipConfig {
    pub probe_interval: Duration,
    pub probe_timeout: Duration,
    pub suspect_timeout: Duration,
    pub piggyback_limit: usize,
    pub dissemination_transmissions: usize,
}

impl Default for MembershipConfig {
    fn default() -> Self {
        Self {
            probe_interval: DEFAULT_PROBE_INTERVAL,
            probe_timeout: DEFAULT_PROBE_TIMEOUT,
            suspect_timeout: DEFAULT_SUSPECT_TIMEOUT,
            piggyback_limit: DEFAULT_PIGGYBACK_LIMIT,
            dissemination_transmissions: DEFAULT_DISSEMINATION_TRANSMISSIONS,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct DisseminationUpdate {
    member: Member,
    remaining_transmissions: usize,
}

#[derive(Debug)]
pub struct MembershipRuntime {
    transport: UdpTransport,
    state: SwimState,
    config: MembershipConfig,
    last_probe_at: Instant,
    probe_started_at: Option<Instant>,
    suspect_marked_at: HashMap<String, Instant>,
    dissemination_queue: VecDeque<DisseminationUpdate>,
}

impl MembershipRuntime {
    pub fn bind(local_id: String, local_addr: SocketAddr) -> io::Result<Self> {
        Self::bind_with_config(local_id, local_addr, MembershipConfig::default())
    }

    pub fn bind_with_config(
        local_id: String,
        local_addr: SocketAddr,
        config: MembershipConfig,
    ) -> io::Result<Self> {
        let transport = UdpTransport::bind(local_addr)?;
        let bound_addr = transport.local_addr()?;
        Ok(Self {
            transport,
            state: SwimState::new(local_id, bound_addr),
            config,
            last_probe_at: Instant::now(),
            probe_started_at: None,
            suspect_marked_at: HashMap::new(),
            dissemination_queue: VecDeque::new(),
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

    pub fn send_join_requests(&mut self, peers: &[SocketAddr]) -> io::Result<()> {
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

    pub fn tick(&mut self, now: Instant) -> io::Result<()> {
        let probe_timed_out = self.mark_timed_out_probe_suspect(now);
        self.mark_timed_out_suspects_failed(now);
        if probe_timed_out {
            Ok(())
        } else {
            self.start_probe_if_due(now)
        }
    }

    fn mark_timed_out_probe_suspect(&mut self, now: Instant) -> bool {
        let Some(started_at) = self.probe_started_at else {
            return false;
        };
        if now.duration_since(started_at) < self.config.probe_timeout {
            return false;
        }

        self.probe_started_at = None;
        if let Some(member) = self.state.mark_pending_ping_suspect() {
            self.suspect_marked_at.insert(member.id.clone(), now);
            self.queue_update(member);
        }
        true
    }

    fn mark_timed_out_suspects_failed(&mut self, now: Instant) {
        let timed_out: Vec<_> = self
            .suspect_marked_at
            .iter()
            .filter_map(|(id, marked_at)| {
                (now.duration_since(*marked_at) >= self.config.suspect_timeout).then(|| id.clone())
            })
            .collect();

        for id in timed_out {
            if let Some(member) = self.state.mark_failed(&id) {
                self.queue_update(member);
            }
            self.suspect_marked_at.remove(&id);
        }
    }

    fn start_probe_if_due(&mut self, now: Instant) -> io::Result<()> {
        if self.probe_started_at.is_some()
            || now.duration_since(self.last_probe_at) < self.config.probe_interval
        {
            return Ok(());
        }

        self.last_probe_at = now;
        let Some(target_id) = self.state.next_probe_target() else {
            return Ok(());
        };
        let Some(message) = self.state.start_direct_ping(target_id.clone()) else {
            return Ok(());
        };
        let Some(target_addr) = self.state.member(&target_id).map(|member| member.addr) else {
            return Ok(());
        };

        self.send_message(target_addr, &message)?;
        self.probe_started_at = Some(now);
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
                if self.state.apply_update(member.clone()) == UpdateOutcome::Applied {
                    self.queue_update(member);
                }
                let known_members = self.state.members().values().cloned().collect();
                self.send_message(response_addr, &MembershipMessage::JoinAck(known_members))?;
            }
            MembershipMessage::JoinAck(members) => {
                for member in members {
                    self.apply_remote_update(member);
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
                if self.state.handle_ack(&from) {
                    self.probe_started_at = None;
                    self.suspect_marked_at.remove(&from);
                }
            }
            MembershipMessage::Update(member) => {
                self.apply_remote_update(member);
            }
            MembershipMessage::PingReq { .. }
            | MembershipMessage::Ping { .. }
            | MembershipMessage::Ack { .. } => {}
        }
        Ok(())
    }

    fn apply_remote_update(&mut self, member: Member) {
        if self.state.apply_update(member.clone()) == UpdateOutcome::Applied {
            self.queue_update(member);
        }
    }

    fn queue_update(&mut self, member: Member) {
        if member.id == self.state.local_id() {
            return;
        }
        if self.config.dissemination_transmissions == 0 {
            return;
        }

        self.dissemination_queue
            .retain(|queued| queued.member.id != member.id);
        self.dissemination_queue.push_back(DisseminationUpdate {
            member,
            remaining_transmissions: self.config.dissemination_transmissions,
        });
    }

    fn send_message(&mut self, target: SocketAddr, message: &MembershipMessage) -> io::Result<()> {
        self.send_frame(target, message)?;
        if !matches!(message, MembershipMessage::Update(_)) {
            self.send_piggyback_updates(target)?;
        }
        Ok(())
    }

    fn send_frame(&self, target: SocketAddr, message: &MembershipMessage) -> io::Result<()> {
        let payload = message.encode_payload().map_err(invalid_data)?;
        let frame = Frame::new(message.opcode(), payload)
            .encode()
            .map_err(invalid_data)?;
        self.transport.send_to(target, &frame)?;
        Ok(())
    }

    fn send_piggyback_updates(&mut self, target: SocketAddr) -> io::Result<()> {
        let limit = self
            .config
            .piggyback_limit
            .min(self.dissemination_queue.len());
        for _ in 0..limit {
            let Some(mut update) = self.dissemination_queue.pop_front() else {
                break;
            };
            self.send_frame(target, &MembershipMessage::Update(update.member.clone()))?;
            update.remaining_transmissions = update.remaining_transmissions.saturating_sub(1);
            if update.remaining_transmissions > 0 {
                self.dissemination_queue.push_back(update);
            }
        }
        Ok(())
    }
}

fn invalid_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{MembershipConfig, MembershipRuntime};
    use crate::membership::MemberStatus;
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

    #[test]
    fn direct_ping_ack_keeps_member_alive() {
        let config = test_config();
        let mut node_a = MembershipRuntime::bind_with_config("node-a".to_string(), addr(0), config)
            .expect("a binds");
        let mut node_b = MembershipRuntime::bind_with_config("node-b".to_string(), addr(0), config)
            .expect("b binds");
        join_nodes(&mut node_a, &mut node_b);
        let now = Instant::now() + config.probe_interval;

        node_a.tick(now).expect("a probes b");
        eventually(|| {
            node_b.receive_ready().expect("b receives ping");
            node_a.receive_ready().expect("a receives ack");
            node_a
                .members()
                .get("node-b")
                .is_some_and(|member| member.status == MemberStatus::Alive)
                && node_a.state.pending_ping().is_none()
        });

        assert!(node_a.state.pending_ping().is_none());
    }

    #[test]
    fn missed_ack_marks_member_suspect_then_failed() {
        let config = test_config();
        let mut node_a = MembershipRuntime::bind_with_config("node-a".to_string(), addr(0), config)
            .expect("a binds");
        let mut node_b = MembershipRuntime::bind_with_config("node-b".to_string(), addr(0), config)
            .expect("b binds");
        join_nodes(&mut node_a, &mut node_b);
        let probe_started = Instant::now() + config.probe_interval;

        node_a.tick(probe_started).expect("a probes b");
        node_a
            .tick(probe_started + config.probe_timeout)
            .expect("probe times out");

        assert_eq!(
            node_a.members().get("node-b").unwrap().status,
            MemberStatus::Suspect
        );
        assert!(node_a.state.pending_ping().is_none());

        node_a
            .tick(probe_started + config.probe_timeout + config.suspect_timeout)
            .expect("suspect times out");

        assert_eq!(
            node_a.members().get("node-b").unwrap().status,
            MemberStatus::Failed
        );
    }

    #[test]
    fn failed_member_update_is_piggybacked_to_another_node() {
        let config = test_config();
        let mut node_a = MembershipRuntime::bind_with_config("node-a".to_string(), addr(0), config)
            .expect("a binds");
        let mut node_b = MembershipRuntime::bind_with_config("node-b".to_string(), addr(0), config)
            .expect("b binds");
        let mut node_c = MembershipRuntime::bind_with_config("node-c".to_string(), addr(0), config)
            .expect("c binds");
        join_nodes(&mut node_a, &mut node_b);
        join_nodes(&mut node_a, &mut node_c);
        eventually(|| node_c.members().contains_key("node-b"));
        let probe_started = Instant::now() + config.probe_interval;

        node_a.tick(probe_started).expect("a probes b");
        node_a
            .tick(probe_started + config.probe_timeout)
            .expect("b becomes suspect");
        node_a
            .tick(probe_started + config.probe_timeout + config.suspect_timeout)
            .expect("b becomes failed and a probes c");

        eventually(|| {
            node_c
                .receive_ready()
                .expect("c receives piggybacked update");
            node_c
                .members()
                .get("node-b")
                .is_some_and(|member| member.status == MemberStatus::Failed)
        });
    }

    fn join_nodes(node_a: &mut MembershipRuntime, node_b: &mut MembershipRuntime) {
        let node_b_addr = node_b.local_addr().expect("b addr is available");
        node_a
            .send_join_requests(&[node_b_addr])
            .expect("join sends");

        eventually(|| {
            node_b.receive_ready().expect("b receives join");
            node_b.members().contains_key("node-a")
        });
        eventually(|| {
            node_a.receive_ready().expect("a receives join ack");
            node_a.members().contains_key("node-b")
        });
    }

    fn test_config() -> MembershipConfig {
        MembershipConfig {
            probe_interval: Duration::from_millis(50),
            probe_timeout: Duration::from_millis(50),
            suspect_timeout: Duration::from_millis(100),
            piggyback_limit: 4,
            dissemination_transmissions: 4,
        }
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
