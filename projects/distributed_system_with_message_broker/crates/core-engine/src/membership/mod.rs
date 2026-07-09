use crate::protocol::Opcode;
use std::collections::HashMap;
use std::fmt;
use std::net::SocketAddr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberStatus {
    Alive,
    Suspect,
    Failed,
    Left,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    pub id: String,
    pub addr: SocketAddr,
    pub status: MemberStatus,
    pub incarnation: u64,
}

pub type MembershipMap = HashMap<String, Member>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipMessage {
    Join(Member),
    JoinAck(Vec<Member>),
    Ping {
        from: String,
        target: String,
    },
    Ack {
        from: String,
        target: String,
    },
    PingReq {
        from: String,
        target: String,
        relay: String,
    },
    Update(Member),
}

impl MembershipMessage {
    pub fn opcode(&self) -> Opcode {
        match self {
            Self::Join(_) => Opcode::Join,
            Self::JoinAck(_) => Opcode::JoinAck,
            Self::Ping { .. } => Opcode::Ping,
            Self::Ack { .. } => Opcode::AckPing,
            Self::PingReq { .. } => Opcode::PingReq,
            Self::Update(_) => Opcode::MembershipUpdate,
        }
    }

    pub fn encode_payload(&self) -> Result<Vec<u8>, MembershipCodecError> {
        let mut payload = Vec::new();
        match self {
            Self::Join(member) | Self::Update(member) => encode_member(&mut payload, member)?,
            Self::JoinAck(members) => {
                let count = u16::try_from(members.len())
                    .map_err(|_| MembershipCodecError::PayloadTooLarge)?;
                payload.extend_from_slice(&count.to_be_bytes());
                for member in members {
                    encode_member(&mut payload, member)?;
                }
            }
            Self::Ping { from, target } | Self::Ack { from, target } => {
                encode_string(&mut payload, from)?;
                encode_string(&mut payload, target)?;
            }
            Self::PingReq {
                from,
                target,
                relay,
            } => {
                encode_string(&mut payload, from)?;
                encode_string(&mut payload, target)?;
                encode_string(&mut payload, relay)?;
            }
        }
        Ok(payload)
    }

    pub fn decode(opcode: Opcode, payload: &[u8]) -> Result<Self, MembershipCodecError> {
        let mut reader = PayloadReader::new(payload);
        let message = match opcode {
            Opcode::Join => Self::Join(decode_member(&mut reader)?),
            Opcode::JoinAck => {
                let count = reader.read_u16()? as usize;
                let mut members = Vec::with_capacity(count);
                for _ in 0..count {
                    members.push(decode_member(&mut reader)?);
                }
                Self::JoinAck(members)
            }
            Opcode::Ping => Self::Ping {
                from: reader.read_string()?,
                target: reader.read_string()?,
            },
            Opcode::AckPing => Self::Ack {
                from: reader.read_string()?,
                target: reader.read_string()?,
            },
            Opcode::PingReq => Self::PingReq {
                from: reader.read_string()?,
                target: reader.read_string()?,
                relay: reader.read_string()?,
            },
            Opcode::MembershipUpdate => Self::Update(decode_member(&mut reader)?),
            opcode => return Err(MembershipCodecError::InvalidOpcode(opcode)),
        };
        reader.finish()?;
        Ok(message)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipCodecError {
    InvalidOpcode(Opcode),
    InvalidPayload,
    PayloadTooLarge,
}

impl fmt::Display for MembershipCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOpcode(opcode) => write!(f, "invalid membership opcode: {opcode:?}"),
            Self::InvalidPayload => write!(f, "invalid membership payload"),
            Self::PayloadTooLarge => write!(f, "membership payload too large"),
        }
    }
}

impl std::error::Error for MembershipCodecError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateOutcome {
    Applied,
    Ignored,
    SelfIncarnationBumped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingPing {
    pub target: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwimState {
    local_id: String,
    members: MembershipMap,
    pending_ping: Option<PendingPing>,
    rng_state: u64,
}

impl SwimState {
    pub fn new(local_id: String, local_addr: SocketAddr) -> Self {
        let local = Member {
            id: local_id.clone(),
            addr: local_addr,
            status: MemberStatus::Alive,
            incarnation: 0,
        };
        let mut members = HashMap::new();
        members.insert(local_id.clone(), local);

        Self {
            rng_state: seed_from(&local_id, local_addr),
            local_id,
            members,
            pending_ping: None,
        }
    }

    #[cfg(test)]
    fn new_with_seed(local_id: String, local_addr: SocketAddr, rng_state: u64) -> Self {
        let mut state = Self::new(local_id, local_addr);
        state.rng_state = rng_state.max(1);
        state
    }

    pub fn local_id(&self) -> &str {
        &self.local_id
    }

    pub fn members(&self) -> &MembershipMap {
        &self.members
    }

    pub fn member(&self, id: &str) -> Option<&Member> {
        self.members.get(id)
    }

    pub fn pending_ping(&self) -> Option<&PendingPing> {
        self.pending_ping.as_ref()
    }

    pub fn handle_join(&mut self, member: Member) -> Vec<Member> {
        self.apply_update(member);
        self.members.values().cloned().collect()
    }

    pub fn apply_update(&mut self, update: Member) -> UpdateOutcome {
        if update.id == self.local_id && update.status == MemberStatus::Suspect {
            return self.refute_self_suspect(update.incarnation);
        }

        match self.members.get(&update.id) {
            Some(existing) if is_stale_update(existing, &update) => UpdateOutcome::Ignored,
            _ => {
                self.members.insert(update.id.clone(), update);
                UpdateOutcome::Applied
            }
        }
    }

    pub fn next_probe_target(&mut self) -> Option<String> {
        let mut candidates: Vec<_> = self
            .members
            .values()
            .filter(|member| member.id != self.local_id && member.status != MemberStatus::Failed)
            .map(|member| member.id.clone())
            .collect();
        candidates.sort();

        if candidates.is_empty() {
            return None;
        }

        let index = self.next_random_index(candidates.len());
        Some(candidates[index].clone())
    }

    pub fn start_direct_ping(&mut self, target: String) -> Option<MembershipMessage> {
        if !self.is_probeable(&target) {
            return None;
        }

        self.pending_ping = Some(PendingPing {
            target: target.clone(),
        });
        Some(MembershipMessage::Ping {
            from: self.local_id.clone(),
            target,
        })
    }

    pub fn start_indirect_ping(&self, target: String, relay: String) -> Option<MembershipMessage> {
        if !self.is_probeable(&target) || !self.is_probeable(&relay) || target == relay {
            return None;
        }

        Some(MembershipMessage::PingReq {
            from: self.local_id.clone(),
            target,
            relay,
        })
    }

    pub fn indirect_ping_relays(&mut self, target: &str, limit: usize) -> Vec<String> {
        let mut relays: Vec<_> = self
            .members
            .values()
            .filter(|member| {
                member.id != self.local_id
                    && member.id != target
                    && member.status != MemberStatus::Failed
            })
            .map(|member| member.id.clone())
            .collect();
        relays.sort();
        self.shuffle(&mut relays);
        relays.truncate(limit);
        relays
    }

    pub fn handle_ack(&mut self, from: &str) -> bool {
        let Some(pending) = &self.pending_ping else {
            return false;
        };
        if pending.target != from {
            return false;
        }

        self.pending_ping = None;
        if let Some(member) = self.members.get_mut(from) {
            member.status = MemberStatus::Alive;
        }
        true
    }

    pub fn mark_pending_ping_suspect(&mut self) -> Option<Member> {
        let target = self.pending_ping.take()?.target;
        let member = self.members.get_mut(&target)?;
        if member.status == MemberStatus::Failed {
            return None;
        }

        member.status = MemberStatus::Suspect;
        Some(member.clone())
    }

    pub fn mark_failed(&mut self, id: &str) -> Option<Member> {
        let member = self.members.get_mut(id)?;
        member.status = MemberStatus::Failed;
        Some(member.clone())
    }

    fn refute_self_suspect(&mut self, suspect_incarnation: u64) -> UpdateOutcome {
        let local = self
            .members
            .get_mut(&self.local_id)
            .expect("local member is always present");
        if suspect_incarnation < local.incarnation {
            return UpdateOutcome::Ignored;
        }

        local.incarnation = suspect_incarnation + 1;
        local.status = MemberStatus::Alive;
        UpdateOutcome::SelfIncarnationBumped
    }

    fn is_probeable(&self, target: &str) -> bool {
        self.members.get(target).is_some_and(|member| {
            member.id != self.local_id && member.status != MemberStatus::Failed
        })
    }

    fn shuffle(&mut self, values: &mut [String]) {
        for index in (1..values.len()).rev() {
            let swap_index = self.next_random_index(index + 1);
            values.swap(index, swap_index);
        }
    }

    fn next_random_index(&mut self, upper_bound: usize) -> usize {
        debug_assert!(upper_bound > 0);
        (self.next_random_u64() as usize) % upper_bound
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

fn seed_from(local_id: &str, local_addr: SocketAddr) -> u64 {
    let mut seed = 0xcbf29ce484222325_u64;
    for byte in local_id
        .as_bytes()
        .iter()
        .copied()
        .chain(local_addr.to_string().bytes())
    {
        seed ^= u64::from(byte);
        seed = seed.wrapping_mul(0x100000001b3);
    }
    seed.max(1)
}

fn is_stale_update(existing: &Member, update: &Member) -> bool {
    update.incarnation < existing.incarnation
        || (update.incarnation == existing.incarnation
            && status_rank(update.status) <= status_rank(existing.status))
}

fn status_rank(status: MemberStatus) -> u8 {
    match status {
        MemberStatus::Alive => 0,
        MemberStatus::Suspect => 1,
        MemberStatus::Failed => 2,
        MemberStatus::Left => 3,
    }
}

fn encode_member(payload: &mut Vec<u8>, member: &Member) -> Result<(), MembershipCodecError> {
    encode_string(payload, &member.id)?;
    encode_string(payload, &member.addr.to_string())?;
    payload.push(status_to_u8(member.status));
    payload.extend_from_slice(&member.incarnation.to_be_bytes());
    Ok(())
}

fn decode_member(reader: &mut PayloadReader<'_>) -> Result<Member, MembershipCodecError> {
    let id = reader.read_string()?;
    let addr = reader
        .read_string()?
        .parse::<SocketAddr>()
        .map_err(|_| MembershipCodecError::InvalidPayload)?;
    let status = status_from_u8(reader.read_u8()?)?;
    let incarnation = reader.read_u64()?;

    Ok(Member {
        id,
        addr,
        status,
        incarnation,
    })
}

fn encode_string(payload: &mut Vec<u8>, value: &str) -> Result<(), MembershipCodecError> {
    let bytes = value.as_bytes();
    let length = u16::try_from(bytes.len()).map_err(|_| MembershipCodecError::PayloadTooLarge)?;
    payload.extend_from_slice(&length.to_be_bytes());
    payload.extend_from_slice(bytes);
    Ok(())
}

fn status_to_u8(status: MemberStatus) -> u8 {
    match status {
        MemberStatus::Alive => 0,
        MemberStatus::Suspect => 1,
        MemberStatus::Failed => 2,
        MemberStatus::Left => 3,
    }
}

fn status_from_u8(status: u8) -> Result<MemberStatus, MembershipCodecError> {
    match status {
        0 => Ok(MemberStatus::Alive),
        1 => Ok(MemberStatus::Suspect),
        2 => Ok(MemberStatus::Failed),
        3 => Ok(MemberStatus::Left),
        _ => Err(MembershipCodecError::InvalidPayload),
    }
}

struct PayloadReader<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> PayloadReader<'a> {
    fn new(payload: &'a [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    fn read_u8(&mut self) -> Result<u8, MembershipCodecError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u16(&mut self) -> Result<u16, MembershipCodecError> {
        let bytes = self.read_exact(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_u64(&mut self) -> Result<u64, MembershipCodecError> {
        let bytes = self.read_exact(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_string(&mut self) -> Result<String, MembershipCodecError> {
        let length = self.read_u16()? as usize;
        let bytes = self.read_exact(length)?;
        std::str::from_utf8(bytes)
            .map(|value| value.to_string())
            .map_err(|_| MembershipCodecError::InvalidPayload)
    }

    fn finish(&self) -> Result<(), MembershipCodecError> {
        if self.offset == self.payload.len() {
            Ok(())
        } else {
            Err(MembershipCodecError::InvalidPayload)
        }
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], MembershipCodecError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(MembershipCodecError::InvalidPayload)?;
        if end > self.payload.len() {
            return Err(MembershipCodecError::InvalidPayload);
        }

        let bytes = &self.payload[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Member, MemberStatus, MembershipCodecError, MembershipMessage, SwimState, UpdateOutcome,
    };
    use crate::protocol::Opcode;
    use std::net::SocketAddr;

    #[test]
    fn node_starts_with_itself_alive() {
        let state = state("node-a");

        let local = state.member("node-a").expect("local member exists");

        assert_eq!(state.local_id(), "node-a");
        assert_eq!(state.members().len(), 1);
        assert_eq!(local.status, MemberStatus::Alive);
        assert_eq!(local.incarnation, 0);
    }

    #[test]
    fn join_adds_new_member_and_returns_known_members() {
        let mut state = state("node-a");

        let known_members = state.handle_join(member("node-b", 7101, MemberStatus::Alive, 0));

        assert_eq!(state.member("node-b").unwrap().status, MemberStatus::Alive);
        assert_eq!(known_members.len(), 2);
    }

    #[test]
    fn duplicate_join_is_idempotent() {
        let mut state = state("node-a");
        let node_b = member("node-b", 7101, MemberStatus::Alive, 0);

        assert_eq!(state.apply_update(node_b.clone()), UpdateOutcome::Applied);
        assert_eq!(state.apply_update(node_b), UpdateOutcome::Ignored);

        assert_eq!(state.members().len(), 2);
    }

    #[test]
    fn higher_incarnation_update_wins() {
        let mut state = state("node-a");
        state.apply_update(member("node-b", 7101, MemberStatus::Alive, 1));

        let outcome = state.apply_update(member("node-b", 7101, MemberStatus::Suspect, 2));

        let node_b = state.member("node-b").unwrap();
        assert_eq!(outcome, UpdateOutcome::Applied);
        assert_eq!(node_b.status, MemberStatus::Suspect);
        assert_eq!(node_b.incarnation, 2);
    }

    #[test]
    fn stale_incarnation_update_is_ignored() {
        let mut state = state("node-a");
        state.apply_update(member("node-b", 7101, MemberStatus::Suspect, 2));

        let outcome = state.apply_update(member("node-b", 7101, MemberStatus::Alive, 1));

        assert_eq!(outcome, UpdateOutcome::Ignored);
        assert_eq!(
            state.member("node-b").unwrap().status,
            MemberStatus::Suspect
        );
    }

    #[test]
    fn equal_incarnation_uses_status_precedence() {
        let mut state = state("node-a");
        state.apply_update(member("node-b", 7101, MemberStatus::Alive, 1));

        assert_eq!(
            state.apply_update(member("node-b", 7101, MemberStatus::Suspect, 1)),
            UpdateOutcome::Applied
        );
        assert_eq!(
            state.apply_update(member("node-b", 7101, MemberStatus::Alive, 1)),
            UpdateOutcome::Ignored
        );
        assert_eq!(
            state.member("node-b").unwrap().status,
            MemberStatus::Suspect
        );
    }

    #[test]
    fn suspect_self_bumps_incarnation_and_stays_alive() {
        let mut state = state("node-a");

        let outcome = state.apply_update(member("node-a", 7100, MemberStatus::Suspect, 0));

        let local = state.member("node-a").unwrap();
        assert_eq!(outcome, UpdateOutcome::SelfIncarnationBumped);
        assert_eq!(local.status, MemberStatus::Alive);
        assert_eq!(local.incarnation, 1);
    }

    #[test]
    fn failed_members_are_not_selected_for_probing() {
        let mut state = state("node-a");
        state.apply_update(member("node-b", 7101, MemberStatus::Failed, 0));
        state.apply_update(member("node-c", 7102, MemberStatus::Alive, 0));

        assert_eq!(state.next_probe_target(), Some("node-c".to_string()));
    }

    #[test]
    fn direct_ping_tracks_pending_target_and_ack_clears_it() {
        let mut state = state("node-a");
        state.apply_update(member("node-b", 7101, MemberStatus::Alive, 0));

        let message = state.start_direct_ping("node-b".to_string());

        assert_eq!(
            message,
            Some(MembershipMessage::Ping {
                from: "node-a".to_string(),
                target: "node-b".to_string()
            })
        );
        assert_eq!(state.pending_ping().unwrap().target, "node-b");
        assert!(state.handle_ack("node-b"));
        assert!(state.pending_ping().is_none());
    }

    #[test]
    fn indirect_ping_req_uses_probeable_relay() {
        let mut state = state("node-a");
        state.apply_update(member("node-b", 7101, MemberStatus::Alive, 0));
        state.apply_update(member("node-c", 7102, MemberStatus::Alive, 0));

        let message = state.start_indirect_ping("node-b".to_string(), "node-c".to_string());

        assert_eq!(
            message,
            Some(MembershipMessage::PingReq {
                from: "node-a".to_string(),
                target: "node-b".to_string(),
                relay: "node-c".to_string()
            })
        );
    }

    #[test]
    fn indirect_ping_relays_exclude_target_self_and_failed_members() {
        let mut state = state("node-a");
        state.apply_update(member("node-b", 7101, MemberStatus::Alive, 0));
        state.apply_update(member("node-c", 7102, MemberStatus::Alive, 0));
        state.apply_update(member("node-d", 7103, MemberStatus::Failed, 0));
        state.apply_update(member("node-e", 7104, MemberStatus::Alive, 0));

        let relays = state.indirect_ping_relays("node-b", 3);

        assert_eq!(relays.len(), 2);
        assert!(relays.contains(&"node-c".to_string()));
        assert!(relays.contains(&"node-e".to_string()));
    }

    #[test]
    fn probe_target_selection_uses_seeded_randomness() {
        let mut state = SwimState::new_with_seed("node-a".to_string(), addr(7100), 1);
        state.apply_update(member("node-b", 7101, MemberStatus::Alive, 0));
        state.apply_update(member("node-c", 7102, MemberStatus::Alive, 0));
        state.apply_update(member("node-d", 7103, MemberStatus::Alive, 0));

        let selected: Vec<_> = (0..6)
            .map(|_| state.next_probe_target().expect("target selected"))
            .collect();

        assert_ne!(
            selected,
            vec![
                "node-b".to_string(),
                "node-c".to_string(),
                "node-d".to_string(),
                "node-b".to_string(),
                "node-c".to_string(),
                "node-d".to_string(),
            ]
        );
    }

    #[test]
    fn indirect_ping_req_rejects_failed_target_and_failed_relay() {
        let mut state = state("node-a");
        state.apply_update(member("node-b", 7101, MemberStatus::Failed, 0));
        state.apply_update(member("node-c", 7102, MemberStatus::Alive, 0));
        state.apply_update(member("node-d", 7103, MemberStatus::Failed, 0));

        assert_eq!(
            state.start_indirect_ping("node-b".to_string(), "node-c".to_string()),
            None
        );
        assert_eq!(
            state.start_indirect_ping("node-c".to_string(), "node-d".to_string()),
            None
        );
    }

    #[test]
    fn missing_direct_ping_ack_marks_member_suspect() {
        let mut state = state("node-a");
        state.apply_update(member("node-b", 7101, MemberStatus::Alive, 0));
        state.start_direct_ping("node-b".to_string());

        let update = state
            .mark_pending_ping_suspect()
            .expect("member is suspect");

        assert_eq!(update.id, "node-b");
        assert_eq!(update.status, MemberStatus::Suspect);
        assert_eq!(
            state.member("node-b").unwrap().status,
            MemberStatus::Suspect
        );
        assert!(state.pending_ping().is_none());
    }

    #[test]
    fn suspect_member_can_be_marked_failed() {
        let mut state = state("node-a");
        state.apply_update(member("node-b", 7101, MemberStatus::Suspect, 0));

        let update = state.mark_failed("node-b").expect("member marked failed");

        assert_eq!(update.status, MemberStatus::Failed);
        assert_eq!(state.member("node-b").unwrap().status, MemberStatus::Failed);
    }

    #[test]
    fn membership_join_payload_round_trips() {
        let message = MembershipMessage::Join(member("node-b", 7101, MemberStatus::Alive, 7));

        assert_round_trips(message);
    }

    #[test]
    fn membership_join_ack_payload_round_trips() {
        let message = MembershipMessage::JoinAck(vec![
            member("node-a", 7100, MemberStatus::Alive, 0),
            member("node-b", 7101, MemberStatus::Suspect, 2),
        ]);

        assert_round_trips(message);
    }

    #[test]
    fn membership_ping_req_payload_round_trips() {
        let message = MembershipMessage::PingReq {
            from: "node-a".to_string(),
            target: "node-b".to_string(),
            relay: "node-c".to_string(),
        };

        assert_round_trips(message);
    }

    #[test]
    fn membership_decoder_rejects_non_membership_opcode() {
        assert_eq!(
            MembershipMessage::decode(Opcode::AppendTask, &[]),
            Err(MembershipCodecError::InvalidOpcode(Opcode::AppendTask))
        );
    }

    #[test]
    fn membership_decoder_rejects_trailing_payload() {
        let message = MembershipMessage::Ack {
            from: "node-a".to_string(),
            target: "node-b".to_string(),
        };
        let mut payload = message.encode_payload().expect("payload encodes");
        payload.push(99);

        assert_eq!(
            MembershipMessage::decode(message.opcode(), &payload),
            Err(MembershipCodecError::InvalidPayload)
        );
    }

    fn state(id: &str) -> SwimState {
        SwimState::new(id.to_string(), addr(7100))
    }

    fn member(id: &str, port: u16, status: MemberStatus, incarnation: u64) -> Member {
        Member {
            id: id.to_string(),
            addr: addr(port),
            status,
            incarnation,
        }
    }

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    fn assert_round_trips(message: MembershipMessage) {
        let payload = message.encode_payload().expect("payload encodes");
        let decoded = MembershipMessage::decode(message.opcode(), &payload).expect("decodes");

        assert_eq!(decoded, message);
    }
}
