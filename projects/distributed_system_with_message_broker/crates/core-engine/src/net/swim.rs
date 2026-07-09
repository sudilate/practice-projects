use crate::membership::{Member, MemberStatus, MembershipMessage, SwimState, UpdateOutcome};
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
const DEFAULT_INDIRECT_PROBE_COUNT: usize = 3;
const DEFAULT_INSPECTION_LOG_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MembershipConfig {
    pub probe_interval: Duration,
    pub probe_timeout: Duration,
    pub suspect_timeout: Duration,
    pub piggyback_limit: usize,
    pub dissemination_transmissions: usize,
    pub indirect_probe_count: usize,
    pub inspection_log_interval: Option<Duration>,
}

impl Default for MembershipConfig {
    fn default() -> Self {
        Self {
            probe_interval: DEFAULT_PROBE_INTERVAL,
            probe_timeout: DEFAULT_PROBE_TIMEOUT,
            suspect_timeout: DEFAULT_SUSPECT_TIMEOUT,
            piggyback_limit: DEFAULT_PIGGYBACK_LIMIT,
            dissemination_transmissions: DEFAULT_DISSEMINATION_TRANSMISSIONS,
            indirect_probe_count: DEFAULT_INDIRECT_PROBE_COUNT,
            inspection_log_interval: Some(DEFAULT_INSPECTION_LOG_INTERVAL),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProbePhase {
    Direct,
    Indirect,
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
    probe_phase: Option<ProbePhase>,
    suspect_marked_at: HashMap<String, Instant>,
    dissemination_queue: VecDeque<DisseminationUpdate>,
    last_inspection_log_at: Instant,
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
            probe_phase: None,
            suspect_marked_at: HashMap::new(),
            dissemination_queue: VecDeque::new(),
            last_inspection_log_at: Instant::now(),
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
        let probe_timed_out = self.handle_timed_out_probe(now)?;
        self.mark_timed_out_suspects_failed(now);
        let result = if probe_timed_out {
            Ok(())
        } else {
            self.start_probe_if_due(now)
        };
        self.log_membership_if_due(now);
        result
    }

    pub fn membership_summary(&self) -> String {
        let mut members: Vec<_> = self.state.members().values().collect();
        members.sort_by(|left, right| left.id.cmp(&right.id));
        members
            .into_iter()
            .map(|member| {
                format!(
                    "{}={}@{}",
                    member.id,
                    status_label(member.status),
                    member.incarnation
                )
            })
            .collect::<Vec<_>>()
            .join(",")
    }

    fn log_membership_if_due(&mut self, now: Instant) {
        let Some(interval) = self.config.inspection_log_interval else {
            return;
        };
        if now.duration_since(self.last_inspection_log_at) < interval {
            return;
        }

        self.last_inspection_log_at = now;
        eprintln!(
            "membership {}: {}",
            self.state.local_id(),
            self.membership_summary()
        );
    }

    fn handle_timed_out_probe(&mut self, now: Instant) -> io::Result<bool> {
        let Some(started_at) = self.probe_started_at else {
            return Ok(false);
        };
        if now.duration_since(started_at) < self.config.probe_timeout {
            return Ok(false);
        }

        match self.probe_phase {
            Some(ProbePhase::Direct) => self.start_indirect_probe(now),
            Some(ProbePhase::Indirect) => {
                self.mark_pending_probe_suspect(now);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    fn start_indirect_probe(&mut self, now: Instant) -> io::Result<bool> {
        let Some(target) = self
            .state
            .pending_ping()
            .map(|pending| pending.target.clone())
        else {
            self.probe_started_at = None;
            self.probe_phase = None;
            return Ok(false);
        };
        let relays = self
            .state
            .indirect_ping_relays(&target, self.config.indirect_probe_count);
        if relays.is_empty() {
            self.mark_pending_probe_suspect(now);
            return Ok(true);
        }

        for relay in relays {
            let Some(relay_addr) = self.state.member(&relay).map(|member| member.addr) else {
                continue;
            };
            let Some(message) = self.state.start_indirect_ping(target.clone(), relay) else {
                continue;
            };
            self.send_message(relay_addr, &message)?;
        }
        self.probe_started_at = Some(now);
        self.probe_phase = Some(ProbePhase::Indirect);
        Ok(true)
    }

    fn mark_pending_probe_suspect(&mut self, now: Instant) {
        self.probe_started_at = None;
        self.probe_phase = None;
        if let Some(member) = self.state.mark_pending_ping_suspect() {
            self.suspect_marked_at.insert(member.id.clone(), now);
            self.queue_update(member);
        }
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
        self.probe_phase = Some(ProbePhase::Direct);
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
                    self.probe_phase = None;
                    self.suspect_marked_at.remove(&from);
                }
            }
            MembershipMessage::Update(member) => {
                self.apply_remote_update(member);
            }
            MembershipMessage::PingReq {
                from,
                target,
                relay,
            } if relay == self.state.local_id() => {
                if let Some(target_addr) = self.state.member(&target).map(|member| member.addr) {
                    self.send_message(target_addr, &MembershipMessage::Ping { from, target })?;
                }
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

fn status_label(status: MemberStatus) -> &'static str {
    match status {
        MemberStatus::Alive => "Alive",
        MemberStatus::Suspect => "Suspect",
        MemberStatus::Failed => "Failed",
        MemberStatus::Left => "Left",
    }
}

#[cfg(test)]
mod tests {
    use super::{MembershipConfig, MembershipRuntime};
    use crate::membership::{MemberStatus, MembershipMessage};
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
    fn indirect_ping_ack_prevents_suspicion() {
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

        node_a.tick(probe_started).expect("a directly probes b");
        node_a
            .tick(probe_started + config.probe_timeout)
            .expect("a asks c to ping b");

        eventually(|| {
            node_c.receive_ready().expect("c receives ping-req");
            node_b.receive_ready().expect("b receives relayed ping");
            node_a.receive_ready().expect("a receives indirect ack");
            node_a.state.pending_ping().is_none()
        });

        node_a
            .tick(probe_started + config.probe_timeout + config.probe_timeout)
            .expect("indirect timeout passes after ack");

        assert_eq!(
            node_a.members().get("node-b").unwrap().status,
            MemberStatus::Alive
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
        let failed = node_a.state.mark_failed("node-b").expect("b is failed");
        node_a.queue_update(failed);
        let node_c_addr = node_c.local_addr().expect("c addr is available");

        node_a
            .send_message(
                node_c_addr,
                &MembershipMessage::Ping {
                    from: "node-a".to_string(),
                    target: "node-c".to_string(),
                },
            )
            .expect("ping with piggyback sends");

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

    #[test]
    fn membership_summary_lists_members_in_stable_order() {
        let config = test_config();
        let mut node_a = MembershipRuntime::bind_with_config("node-a".to_string(), addr(0), config)
            .expect("a binds");
        let mut node_b = MembershipRuntime::bind_with_config("node-b".to_string(), addr(0), config)
            .expect("b binds");
        join_nodes(&mut node_a, &mut node_b);

        let summary = node_a.membership_summary();

        assert!(summary.starts_with("node-a=Alive@0"));
        assert!(summary.contains("node-b=Alive@0"));
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
            indirect_probe_count: 3,
            inspection_log_interval: None,
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
