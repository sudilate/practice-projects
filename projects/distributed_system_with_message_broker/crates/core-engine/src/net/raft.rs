use crate::protocol::{Frame, Opcode};
use crate::raft::{AppendEntries, RaftMessage, RaftState, RequestVote, Role};
use crate::storage::Wal;
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};

const DEFAULT_ELECTION_TIMEOUT_MIN: Duration = Duration::from_millis(150);
const DEFAULT_ELECTION_TIMEOUT_SPREAD: Duration = Duration::from_millis(150);
const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);
const TCP_TIMEOUT: Duration = Duration::from_millis(50);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftPeer {
    pub id: String,
    pub addr: SocketAddr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RaftRuntimeConfig {
    pub election_timeout_min: Duration,
    pub election_timeout_spread: Duration,
    pub heartbeat_interval: Duration,
}

impl Default for RaftRuntimeConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: DEFAULT_ELECTION_TIMEOUT_MIN,
            election_timeout_spread: DEFAULT_ELECTION_TIMEOUT_SPREAD,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
        }
    }
}

#[derive(Debug)]
pub struct RaftRuntime {
    node_id: String,
    peers: Vec<RaftPeer>,
    state: RaftState,
    config: RaftRuntimeConfig,
    election_deadline: Instant,
    last_heartbeat_at: Instant,
    rng_state: u64,
    leader_logged: bool,
    next_index: HashMap<String, u64>,
    match_index: HashMap<String, u64>,
}

impl RaftRuntime {
    pub fn new(node_id: String, peers: Vec<RaftPeer>) -> Self {
        Self::new_with_config(node_id, peers, RaftRuntimeConfig::default())
    }

    pub fn new_with_config(
        node_id: String,
        peers: Vec<RaftPeer>,
        config: RaftRuntimeConfig,
    ) -> Self {
        let now = Instant::now();
        let rng_state = seed_from(&node_id).max(1);
        let next_index = peers
            .iter()
            .map(|peer| (peer.id.clone(), 1))
            .collect::<HashMap<_, _>>();
        let match_index = peers
            .iter()
            .map(|peer| (peer.id.clone(), 0))
            .collect::<HashMap<_, _>>();
        let mut runtime = Self {
            state: RaftState::new(peers.len() + 1),
            node_id,
            peers,
            config,
            election_deadline: now,
            last_heartbeat_at: now,
            rng_state,
            leader_logged: false,
            next_index,
            match_index,
        };
        runtime.reset_election_deadline(now);
        runtime
    }

    pub fn role(&self) -> Role {
        self.state.role()
    }

    pub fn current_term(&self) -> u64 {
        self.state.current_term()
    }

    pub fn commit_index(&self) -> u64 {
        self.state.commit_index()
    }

    pub fn last_log_index(&self) -> u64 {
        self.state.last_log_index()
    }

    pub fn tick(&mut self, now: Instant) -> io::Result<()> {
        match self.state.role() {
            Role::Leader => self.send_heartbeats_if_due(now),
            Role::Follower | Role::Candidate if now >= self.election_deadline => {
                self.start_election(now)
            }
            Role::Follower | Role::Candidate => Ok(()),
        }
    }

    pub fn handle_frame(
        &mut self,
        opcode: Opcode,
        payload: &[u8],
        wal: &mut Wal,
    ) -> io::Result<Vec<u8>> {
        match opcode {
            Opcode::RequestVote => self.handle_request_vote_frame(payload),
            Opcode::AppendEntries => self.handle_append_entries_frame(payload, wal),
            _ => Err(invalid_data("unsupported raft opcode")),
        }
    }

    pub fn submit_task(&mut self, payload: &[u8], wal: &mut Wal) -> io::Result<bool> {
        if self.state.role() != Role::Leader {
            return Ok(false);
        }

        let task_id = format!("{}-{}", self.node_id, self.state.last_log_index() + 1);
        wal.append(payload)?;
        let entry =
            self.state
                .append_local_entry(self.state.current_term(), task_id, payload.to_vec());
        let mut replicated = 1;

        for peer in self.peers.clone() {
            let prev_index = entry.index.saturating_sub(1);
            let prev_term = self.state.term_at(prev_index).unwrap_or(0);
            let request = AppendEntries {
                term: self.state.current_term(),
                leader_id: self.node_id.clone(),
                prev_log_index: prev_index,
                prev_log_term: prev_term,
                entries: vec![entry.clone()],
                leader_commit: self.state.commit_index(),
            };
            let payload = RaftMessage::AppendEntries(request)
                .encode_append_entries_family()
                .map_err(invalid_data)?;
            let Ok(Some(RaftMessage::AppendEntriesReply(reply))) =
                self.send_raft_message(peer.addr, Opcode::AppendEntries, payload)
            else {
                continue;
            };

            if reply.term > self.state.current_term() {
                self.state.handle_higher_term(reply.term);
                self.reset_election_deadline(Instant::now());
                self.leader_logged = false;
                return Ok(false);
            }
            if reply.success {
                replicated += 1;
                self.match_index.insert(peer.id.clone(), reply.match_index);
                self.next_index.insert(peer.id, reply.match_index + 1);
            } else {
                let next = self
                    .next_index
                    .get(&peer.id)
                    .copied()
                    .unwrap_or(entry.index);
                self.next_index
                    .insert(peer.id, next.saturating_sub(1).max(1));
            }
        }

        if replicated >= self.majority() {
            self.state.commit_through(entry.index);
            eprintln!(
                "raft {}: committed entry {} with {replicated} replicas",
                self.node_id, entry.index
            );
            self.broadcast_heartbeat()?;
            return Ok(true);
        }

        Ok(false)
    }

    fn start_election(&mut self, now: Instant) -> io::Result<()> {
        self.state.become_candidate(self.node_id.clone());
        self.leader_logged = false;
        self.reset_election_deadline(now);
        let request = RequestVote {
            term: self.state.current_term(),
            candidate_id: self.node_id.clone(),
            last_log_index: self.state.last_log_index(),
            last_log_term: self.state.last_log_term(),
        };
        eprintln!(
            "raft {}: starting election term {}",
            self.node_id,
            self.state.current_term()
        );

        if self.peers.is_empty() {
            self.state.become_leader();
            self.log_leader();
            return Ok(());
        }

        for peer in self.peers.clone() {
            let message = RaftMessage::RequestVote(request.clone());
            let Ok(Some(RaftMessage::RequestVoteReply(reply))) = self.send_raft_message(
                peer.addr,
                Opcode::RequestVote,
                message.encode_request_vote_family().map_err(invalid_data)?,
            ) else {
                continue;
            };
            if self.state.handle_request_vote_reply(reply) == Some(Role::Leader) {
                self.log_leader();
                self.last_heartbeat_at = now;
                self.broadcast_heartbeat()?;
                break;
            }
        }

        Ok(())
    }

    fn send_heartbeats_if_due(&mut self, now: Instant) -> io::Result<()> {
        if now.duration_since(self.last_heartbeat_at) < self.config.heartbeat_interval {
            return Ok(());
        }
        self.last_heartbeat_at = now;
        self.broadcast_heartbeat()
    }

    fn broadcast_heartbeat(&mut self) -> io::Result<()> {
        let message = RaftMessage::AppendEntries(AppendEntries {
            term: self.state.current_term(),
            leader_id: self.node_id.clone(),
            prev_log_index: self.state.last_log_index(),
            prev_log_term: self.state.last_log_term(),
            entries: Vec::new(),
            leader_commit: self.state.commit_index(),
        });
        let payload = message
            .encode_append_entries_family()
            .map_err(invalid_data)?;

        for peer in self.peers.clone() {
            let Ok(Some(RaftMessage::AppendEntriesReply(reply))) =
                self.send_raft_message(peer.addr, Opcode::AppendEntries, payload.clone())
            else {
                continue;
            };
            if reply.term > self.state.current_term() {
                self.state.handle_higher_term(reply.term);
                self.reset_election_deadline(Instant::now());
                self.leader_logged = false;
                break;
            }
        }

        Ok(())
    }

    fn majority(&self) -> usize {
        ((self.peers.len() + 1) / 2) + 1
    }

    fn handle_request_vote_frame(&mut self, payload: &[u8]) -> io::Result<Vec<u8>> {
        let message = RaftMessage::decode_request_vote_family(payload).map_err(invalid_data)?;
        match message {
            RaftMessage::RequestVote(request) => {
                let reply = self.state.handle_request_vote(request);
                if reply.vote_granted {
                    self.reset_election_deadline(Instant::now());
                }
                self.leader_logged = self.state.role() == Role::Leader;
                let payload = RaftMessage::RequestVoteReply(reply)
                    .encode_request_vote_family()
                    .map_err(invalid_data)?;
                encode_frame(Opcode::RequestVote, payload)
            }
            RaftMessage::RequestVoteReply(_) => Err(invalid_data("unexpected RequestVoteReply")),
            _ => Err(invalid_data("invalid RequestVote family message")),
        }
    }

    fn handle_append_entries_frame(
        &mut self,
        payload: &[u8],
        wal: &mut Wal,
    ) -> io::Result<Vec<u8>> {
        let message = RaftMessage::decode_append_entries_family(payload).map_err(invalid_data)?;
        match message {
            RaftMessage::AppendEntries(request) => {
                let entries = request.entries.clone();
                let reply = self.state.handle_append_entries(request);
                if reply.success {
                    for entry in entries {
                        wal.append(&entry.payload)?;
                    }
                    self.reset_election_deadline(Instant::now());
                }
                self.leader_logged = self.state.role() == Role::Leader;
                let payload = RaftMessage::AppendEntriesReply(reply)
                    .encode_append_entries_family()
                    .map_err(invalid_data)?;
                encode_frame(Opcode::AppendEntries, payload)
            }
            RaftMessage::AppendEntriesReply(_) => {
                Err(invalid_data("unexpected AppendEntriesReply"))
            }
            _ => Err(invalid_data("invalid AppendEntries family message")),
        }
    }

    fn send_raft_message(
        &self,
        addr: SocketAddr,
        opcode: Opcode,
        payload: Vec<u8>,
    ) -> io::Result<Option<RaftMessage>> {
        let mut stream = TcpStream::connect_timeout(&addr, TCP_TIMEOUT)?;
        stream.set_read_timeout(Some(TCP_TIMEOUT))?;
        stream.set_write_timeout(Some(TCP_TIMEOUT))?;
        stream.write_all(&encode_frame(opcode, payload)?)?;

        let mut header = [0; crate::protocol::HEADER_LEN];
        stream.read_exact(&mut header)?;
        let payload_len = u32::from_be_bytes([header[0], header[1], header[2], header[3]]) as usize;
        let mut bytes = header.to_vec();
        let mut payload = vec![0; payload_len];
        stream.read_exact(&mut payload)?;
        bytes.extend_from_slice(&payload);
        let frame = Frame::decode(&bytes).map_err(invalid_data)?;
        match frame.opcode() {
            Opcode::RequestVote => Ok(Some(
                RaftMessage::decode_request_vote_family(frame.payload()).map_err(invalid_data)?,
            )),
            Opcode::AppendEntries => Ok(Some(
                RaftMessage::decode_append_entries_family(frame.payload()).map_err(invalid_data)?,
            )),
            _ => Ok(None),
        }
    }

    fn reset_election_deadline(&mut self, now: Instant) {
        let spread_ms = self.config.election_timeout_spread.as_millis() as u64;
        let jitter = if spread_ms == 0 {
            0
        } else {
            self.next_random_u64() % (spread_ms + 1)
        };
        self.election_deadline =
            now + self.config.election_timeout_min + Duration::from_millis(jitter);
    }

    fn log_leader(&mut self) {
        if self.leader_logged {
            return;
        }
        self.leader_logged = true;
        eprintln!(
            "raft {}: became leader term {}",
            self.node_id,
            self.state.current_term()
        );
    }

    fn next_random_u64(&mut self) -> u64 {
        let mut value = self.rng_state;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.rng_state = value.max(1);
        self.rng_state
    }
}

fn encode_frame(opcode: Opcode, payload: Vec<u8>) -> io::Result<Vec<u8>> {
    Frame::new(opcode, payload).encode().map_err(invalid_data)
}

fn invalid_data(error: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error.to_string())
}

fn seed_from(node_id: &str) -> u64 {
    let mut seed = 0xcbf29ce484222325_u64;
    for byte in node_id.bytes() {
        seed ^= u64::from(byte);
        seed = seed.wrapping_mul(0x100000001b3);
    }
    seed.max(1)
}

#[cfg(test)]
mod tests {
    use super::{RaftPeer, RaftRuntime, RaftRuntimeConfig};
    use crate::protocol::Opcode;
    use crate::raft::Role;
    use crate::raft::{AppendEntries, RaftLogEntry, RaftMessage};
    use crate::storage::Wal;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    #[test]
    fn single_node_runtime_elects_itself_on_timeout() {
        let config = RaftRuntimeConfig {
            election_timeout_min: Duration::from_millis(1),
            election_timeout_spread: Duration::ZERO,
            heartbeat_interval: Duration::from_millis(50),
        };
        let mut runtime = RaftRuntime::new_with_config("node-a".to_string(), Vec::new(), config);

        runtime
            .tick(Instant::now() + Duration::from_millis(2))
            .expect("tick succeeds");

        assert_eq!(runtime.role(), Role::Leader);
        assert_eq!(runtime.current_term(), 1);
    }

    #[test]
    fn runtime_starts_as_follower() {
        let runtime = RaftRuntime::new(
            "node-a".to_string(),
            vec![RaftPeer {
                id: "node-b".to_string(),
                addr: "127.0.0.1:7001".parse().unwrap(),
            }],
        );

        assert_eq!(runtime.role(), Role::Follower);
        assert_eq!(runtime.current_term(), 0);
    }

    #[test]
    fn single_node_leader_commits_submitted_task() {
        let config = RaftRuntimeConfig {
            election_timeout_min: Duration::from_millis(1),
            election_timeout_spread: Duration::ZERO,
            heartbeat_interval: Duration::from_millis(50),
        };
        let mut runtime = RaftRuntime::new_with_config("node-a".to_string(), Vec::new(), config);
        runtime
            .tick(Instant::now() + Duration::from_millis(2))
            .expect("leader elected");
        let wal_path = test_wal_path("single-node-commit");
        let mut wal = Wal::open(&wal_path).expect("wal opens");

        assert!(runtime
            .submit_task(b"task", &mut wal)
            .expect("task submits"));

        assert_eq!(runtime.last_log_index(), 1);
        assert_eq!(runtime.commit_index(), 1);
        assert_eq!(wal.get(0).expect("wal reads"), Some(b"task".to_vec()));
        let _ = fs::remove_file(wal_path);
    }

    #[test]
    fn follower_persists_append_entries_payload_to_wal() {
        let mut runtime = RaftRuntime::new("node-b".to_string(), Vec::new());
        let wal_path = test_wal_path("follower-append");
        let mut wal = Wal::open(&wal_path).expect("wal opens");
        let request = AppendEntries {
            term: 1,
            leader_id: "node-a".to_string(),
            prev_log_index: 0,
            prev_log_term: 0,
            entries: vec![RaftLogEntry {
                term: 1,
                index: 1,
                task_id: "task-1".to_string(),
                payload: b"replicated".to_vec(),
            }],
            leader_commit: 1,
        };
        let payload = RaftMessage::AppendEntries(request)
            .encode_append_entries_family()
            .expect("append entries encodes");

        let response = runtime
            .handle_frame(Opcode::AppendEntries, &payload, &mut wal)
            .expect("append entries handled");

        assert!(!response.is_empty());
        assert_eq!(runtime.last_log_index(), 1);
        assert_eq!(runtime.commit_index(), 1);
        assert_eq!(wal.get(0).expect("wal reads"), Some(b"replicated".to_vec()));
        let _ = fs::remove_file(wal_path);
    }

    fn test_wal_path(name: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos();
        std::env::temp_dir().join(format!("core-engine-raft-{name}-{nanos}.log"))
    }
}
