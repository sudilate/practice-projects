use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftLogEntry {
    pub term: u64,
    pub index: u64,
    pub task_id: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftMessage {
    RequestVote(RequestVote),
    RequestVoteReply(RequestVoteReply),
    AppendEntries(AppendEntries),
    AppendEntriesReply(AppendEntriesReply),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVote {
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteReply {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntries {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<RaftLogEntry>,
    pub leader_commit: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesReply {
    pub term: u64,
    pub success: bool,
    pub match_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftCodecError {
    InvalidPayload,
    PayloadTooLarge,
}

impl fmt::Display for RaftCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPayload => write!(f, "invalid raft payload"),
            Self::PayloadTooLarge => write!(f, "raft payload too large"),
        }
    }
}

impl std::error::Error for RaftCodecError {}

const REQUEST_VOTE_KIND: u8 = 1;
const REQUEST_VOTE_REPLY_KIND: u8 = 2;
const APPEND_ENTRIES_KIND: u8 = 1;
const APPEND_ENTRIES_REPLY_KIND: u8 = 2;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftState {
    role: Role,
    current_term: u64,
    voted_for: Option<String>,
    log: Vec<RaftLogEntry>,
    commit_index: u64,
    last_applied: u64,
    votes_received: usize,
    cluster_size: usize,
}

impl Default for RaftState {
    fn default() -> Self {
        Self::new(1)
    }
}

impl RaftState {
    pub fn new(cluster_size: usize) -> Self {
        Self {
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            votes_received: 0,
            cluster_size: cluster_size.max(1),
        }
    }

    pub fn role(&self) -> Role {
        self.role
    }

    pub fn current_term(&self) -> u64 {
        self.current_term
    }

    pub fn voted_for(&self) -> Option<&str> {
        self.voted_for.as_deref()
    }

    pub fn log(&self) -> &[RaftLogEntry] {
        &self.log
    }

    pub fn commit_index(&self) -> u64 {
        self.commit_index
    }

    pub fn last_applied(&self) -> u64 {
        self.last_applied
    }

    pub fn votes_received(&self) -> usize {
        self.votes_received
    }

    pub fn last_log_index(&self) -> u64 {
        self.log.last().map_or(0, |entry| entry.index)
    }

    pub fn last_log_term(&self) -> u64 {
        self.log.last().map_or(0, |entry| entry.term)
    }

    pub fn become_candidate(&mut self, node_id: String) {
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(node_id);
        self.votes_received = 1;
    }

    pub fn become_leader(&mut self) {
        self.role = Role::Leader;
    }

    pub fn append_local_entry(
        &mut self,
        term: u64,
        task_id: String,
        payload: Vec<u8>,
    ) -> RaftLogEntry {
        let index = self.last_log_index() + 1;
        let entry = RaftLogEntry {
            term,
            index,
            task_id,
            payload,
        };
        self.log.push(entry.clone());
        entry
    }

    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            return Some(0);
        }
        self.log
            .iter()
            .find(|entry| entry.index == index)
            .map(|entry| entry.term)
    }

    pub fn commit_through(&mut self, index: u64) {
        self.commit_index = index.min(self.last_log_index());
    }

    /// Return all committed entries that have not yet been applied and advance
    /// `last_applied` up to `commit_index`. The caller is responsible for
    /// feeding these entries into the application state machine.
    pub fn drain_applied_entries(&mut self) -> Vec<RaftLogEntry> {
        let mut applied = Vec::new();
        while self.last_applied < self.commit_index {
            self.last_applied += 1;
            if let Some(entry) = self
                .log
                .iter()
                .find(|e| e.index == self.last_applied)
                .cloned()
            {
                applied.push(entry);
            }
        }
        applied
    }

    pub fn handle_request_vote(&mut self, request: RequestVote) -> RequestVoteReply {
        if request.term < self.current_term {
            return RequestVoteReply {
                term: self.current_term,
                vote_granted: false,
            };
        }

        if request.term > self.current_term {
            self.handle_higher_term(request.term);
        }

        let can_vote = self.voted_for.is_none()
            || self.voted_for.as_deref() == Some(request.candidate_id.as_str());
        let log_is_up_to_date =
            self.is_candidate_log_up_to_date(request.last_log_index, request.last_log_term);
        let vote_granted = can_vote && log_is_up_to_date;

        if vote_granted {
            self.voted_for = Some(request.candidate_id);
        }

        RequestVoteReply {
            term: self.current_term,
            vote_granted,
        }
    }

    pub fn handle_request_vote_reply(&mut self, reply: RequestVoteReply) -> Option<Role> {
        if reply.term > self.current_term {
            self.handle_higher_term(reply.term);
            return Some(Role::Follower);
        }

        if self.role != Role::Candidate || reply.term != self.current_term || !reply.vote_granted {
            return None;
        }

        self.votes_received += 1;
        if self.votes_received >= self.majority() {
            self.become_leader();
            return Some(Role::Leader);
        }

        None
    }

    pub fn handle_append_entries(&mut self, request: AppendEntries) -> AppendEntriesReply {
        if request.term < self.current_term {
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                match_index: self.last_log_index(),
            };
        }

        if request.term > self.current_term || self.role != Role::Follower {
            self.handle_higher_term(request.term);
        }

        if !self.log_matches(request.prev_log_index, request.prev_log_term) {
            return AppendEntriesReply {
                term: self.current_term,
                success: false,
                match_index: self.last_log_index(),
            };
        }

        let match_index = request.prev_log_index + request.entries.len() as u64;
        self.append_entries_after(request.prev_log_index, request.entries);
        if request.leader_commit > self.commit_index {
            self.commit_index = request.leader_commit.min(self.last_log_index());
        }

        AppendEntriesReply {
            term: self.current_term,
            success: true,
            match_index,
        }
    }

    pub fn handle_higher_term(&mut self, term: u64) {
        if term > self.current_term {
            self.current_term = term;
        }
        self.role = Role::Follower;
        self.voted_for = None;
        self.votes_received = 0;
    }

    fn majority(&self) -> usize {
        (self.cluster_size / 2) + 1
    }

    fn is_candidate_log_up_to_date(&self, last_log_index: u64, last_log_term: u64) -> bool {
        last_log_term > self.last_log_term()
            || (last_log_term == self.last_log_term() && last_log_index >= self.last_log_index())
    }

    fn log_matches(&self, prev_log_index: u64, prev_log_term: u64) -> bool {
        if prev_log_index == 0 {
            return prev_log_term == 0;
        }

        self.log
            .iter()
            .find(|entry| entry.index == prev_log_index)
            .is_some_and(|entry| entry.term == prev_log_term)
    }

    fn append_entries_after(&mut self, prev_log_index: u64, entries: Vec<RaftLogEntry>) {
        if entries.is_empty() {
            return;
        }

        self.log.retain(|entry| entry.index <= prev_log_index);
        self.log.extend(entries);
    }
}

impl RaftMessage {
    pub fn encode_request_vote_family(&self) -> Result<Vec<u8>, RaftCodecError> {
        match self {
            Self::RequestVote(message) => encode_with_kind(REQUEST_VOTE_KIND, message.encode()?),
            Self::RequestVoteReply(message) => {
                encode_with_kind(REQUEST_VOTE_REPLY_KIND, message.encode())
            }
            _ => Err(RaftCodecError::InvalidPayload),
        }
    }

    pub fn decode_request_vote_family(payload: &[u8]) -> Result<Self, RaftCodecError> {
        let (kind, payload) = split_kind(payload)?;
        match kind {
            REQUEST_VOTE_KIND => Ok(Self::RequestVote(RequestVote::decode(payload)?)),
            REQUEST_VOTE_REPLY_KIND => {
                Ok(Self::RequestVoteReply(RequestVoteReply::decode(payload)?))
            }
            _ => Err(RaftCodecError::InvalidPayload),
        }
    }

    pub fn encode_append_entries_family(&self) -> Result<Vec<u8>, RaftCodecError> {
        match self {
            Self::AppendEntries(message) => {
                encode_with_kind(APPEND_ENTRIES_KIND, message.encode()?)
            }
            Self::AppendEntriesReply(message) => {
                encode_with_kind(APPEND_ENTRIES_REPLY_KIND, message.encode())
            }
            _ => Err(RaftCodecError::InvalidPayload),
        }
    }

    pub fn decode_append_entries_family(payload: &[u8]) -> Result<Self, RaftCodecError> {
        let (kind, payload) = split_kind(payload)?;
        match kind {
            APPEND_ENTRIES_KIND => Ok(Self::AppendEntries(AppendEntries::decode(payload)?)),
            APPEND_ENTRIES_REPLY_KIND => Ok(Self::AppendEntriesReply(AppendEntriesReply::decode(
                payload,
            )?)),
            _ => Err(RaftCodecError::InvalidPayload),
        }
    }
}

impl RequestVote {
    pub fn encode(&self) -> Result<Vec<u8>, RaftCodecError> {
        let mut payload = Vec::new();
        encode_u64(&mut payload, self.term);
        encode_string(&mut payload, &self.candidate_id)?;
        encode_u64(&mut payload, self.last_log_index);
        encode_u64(&mut payload, self.last_log_term);
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, RaftCodecError> {
        let mut reader = PayloadReader::new(payload);
        let message = Self {
            term: reader.read_u64()?,
            candidate_id: reader.read_string()?,
            last_log_index: reader.read_u64()?,
            last_log_term: reader.read_u64()?,
        };
        reader.finish()?;
        Ok(message)
    }
}

impl RequestVoteReply {
    pub fn encode(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(9);
        encode_u64(&mut payload, self.term);
        encode_bool(&mut payload, self.vote_granted);
        payload
    }

    pub fn decode(payload: &[u8]) -> Result<Self, RaftCodecError> {
        let mut reader = PayloadReader::new(payload);
        let message = Self {
            term: reader.read_u64()?,
            vote_granted: reader.read_bool()?,
        };
        reader.finish()?;
        Ok(message)
    }
}

impl AppendEntries {
    pub fn encode(&self) -> Result<Vec<u8>, RaftCodecError> {
        let mut payload = Vec::new();
        encode_u64(&mut payload, self.term);
        encode_string(&mut payload, &self.leader_id)?;
        encode_u64(&mut payload, self.prev_log_index);
        encode_u64(&mut payload, self.prev_log_term);
        encode_u64(&mut payload, self.leader_commit);
        let count =
            u16::try_from(self.entries.len()).map_err(|_| RaftCodecError::PayloadTooLarge)?;
        payload.extend_from_slice(&count.to_be_bytes());
        for entry in &self.entries {
            encode_log_entry(&mut payload, entry)?;
        }
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, RaftCodecError> {
        let mut reader = PayloadReader::new(payload);
        let term = reader.read_u64()?;
        let leader_id = reader.read_string()?;
        let prev_log_index = reader.read_u64()?;
        let prev_log_term = reader.read_u64()?;
        let leader_commit = reader.read_u64()?;
        let entry_count = reader.read_u16()? as usize;
        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            entries.push(decode_log_entry(&mut reader)?);
        }
        reader.finish()?;

        Ok(Self {
            term,
            leader_id,
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        })
    }
}

impl AppendEntriesReply {
    pub fn encode(&self) -> Vec<u8> {
        let mut payload = Vec::with_capacity(17);
        encode_u64(&mut payload, self.term);
        encode_bool(&mut payload, self.success);
        encode_u64(&mut payload, self.match_index);
        payload
    }

    pub fn decode(payload: &[u8]) -> Result<Self, RaftCodecError> {
        let mut reader = PayloadReader::new(payload);
        let message = Self {
            term: reader.read_u64()?,
            success: reader.read_bool()?,
            match_index: reader.read_u64()?,
        };
        reader.finish()?;
        Ok(message)
    }
}

fn encode_log_entry(payload: &mut Vec<u8>, entry: &RaftLogEntry) -> Result<(), RaftCodecError> {
    encode_u64(payload, entry.term);
    encode_u64(payload, entry.index);
    encode_string(payload, &entry.task_id)?;
    let payload_len =
        u32::try_from(entry.payload.len()).map_err(|_| RaftCodecError::PayloadTooLarge)?;
    payload.extend_from_slice(&payload_len.to_be_bytes());
    payload.extend_from_slice(&entry.payload);
    Ok(())
}

fn decode_log_entry(reader: &mut PayloadReader<'_>) -> Result<RaftLogEntry, RaftCodecError> {
    let term = reader.read_u64()?;
    let index = reader.read_u64()?;
    let task_id = reader.read_string()?;
    let payload_len = reader.read_u32()? as usize;
    let payload = reader.read_exact(payload_len)?.to_vec();
    Ok(RaftLogEntry {
        term,
        index,
        task_id,
        payload,
    })
}

fn encode_u64(payload: &mut Vec<u8>, value: u64) {
    payload.extend_from_slice(&value.to_be_bytes());
}

fn encode_with_kind(kind: u8, mut payload: Vec<u8>) -> Result<Vec<u8>, RaftCodecError> {
    let mut encoded = Vec::with_capacity(payload.len() + 1);
    encoded.push(kind);
    encoded.append(&mut payload);
    Ok(encoded)
}

fn split_kind(payload: &[u8]) -> Result<(u8, &[u8]), RaftCodecError> {
    let Some((kind, payload)) = payload.split_first() else {
        return Err(RaftCodecError::InvalidPayload);
    };
    Ok((*kind, payload))
}

fn encode_bool(payload: &mut Vec<u8>, value: bool) {
    payload.push(u8::from(value));
}

fn encode_string(payload: &mut Vec<u8>, value: &str) -> Result<(), RaftCodecError> {
    let bytes = value.as_bytes();
    let length = u16::try_from(bytes.len()).map_err(|_| RaftCodecError::PayloadTooLarge)?;
    payload.extend_from_slice(&length.to_be_bytes());
    payload.extend_from_slice(bytes);
    Ok(())
}

struct PayloadReader<'a> {
    payload: &'a [u8],
    offset: usize,
}

impl<'a> PayloadReader<'a> {
    fn new(payload: &'a [u8]) -> Self {
        Self { payload, offset: 0 }
    }

    fn read_u16(&mut self) -> Result<u16, RaftCodecError> {
        let bytes = self.read_exact(2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    fn read_u32(&mut self) -> Result<u32, RaftCodecError> {
        let bytes = self.read_exact(4)?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64(&mut self) -> Result<u64, RaftCodecError> {
        let bytes = self.read_exact(8)?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    fn read_bool(&mut self) -> Result<bool, RaftCodecError> {
        match self.read_exact(1)?[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(RaftCodecError::InvalidPayload),
        }
    }

    fn read_string(&mut self) -> Result<String, RaftCodecError> {
        let length = self.read_u16()? as usize;
        let bytes = self.read_exact(length)?;
        std::str::from_utf8(bytes)
            .map(|value| value.to_string())
            .map_err(|_| RaftCodecError::InvalidPayload)
    }

    fn finish(&self) -> Result<(), RaftCodecError> {
        if self.offset == self.payload.len() {
            Ok(())
        } else {
            Err(RaftCodecError::InvalidPayload)
        }
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], RaftCodecError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(RaftCodecError::InvalidPayload)?;
        if end > self.payload.len() {
            return Err(RaftCodecError::InvalidPayload);
        }

        let bytes = &self.payload[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AppendEntries, AppendEntriesReply, RaftCodecError, RaftLogEntry, RaftMessage, RaftState,
        RequestVote, RequestVoteReply, Role,
    };

    #[test]
    fn follower_starts_with_empty_log_and_term_zero() {
        let state = RaftState::default();

        assert_eq!(state.role(), Role::Follower);
        assert_eq!(state.current_term(), 0);
        assert_eq!(state.voted_for(), None);
        assert!(state.log().is_empty());
        assert_eq!(state.commit_index(), 0);
        assert_eq!(state.last_applied(), 0);
    }

    #[test]
    fn follower_can_start_election() {
        let mut state = RaftState::new(5);

        state.become_candidate("node-a".to_string());

        assert_eq!(state.role(), Role::Candidate);
        assert_eq!(state.current_term(), 1);
        assert_eq!(state.voted_for(), Some("node-a"));
        assert_eq!(state.votes_received(), 1);
    }

    #[test]
    fn request_vote_rejected_for_stale_term() {
        let mut state = RaftState::default();
        state.handle_higher_term(3);

        let reply = state.handle_request_vote(request_vote(2, "node-b", 0, 0));

        assert_eq!(reply.term, 3);
        assert!(!reply.vote_granted);
        assert_eq!(state.voted_for(), None);
    }

    #[test]
    fn request_vote_granted_for_first_vote_in_term() {
        let mut state = RaftState::default();

        let reply = state.handle_request_vote(request_vote(1, "node-b", 0, 0));

        assert_eq!(reply.term, 1);
        assert!(reply.vote_granted);
        assert_eq!(state.current_term(), 1);
        assert_eq!(state.voted_for(), Some("node-b"));
    }

    #[test]
    fn request_vote_rejected_if_already_voted_for_other() {
        let mut state = RaftState::default();
        assert!(
            state
                .handle_request_vote(request_vote(1, "node-b", 0, 0))
                .vote_granted
        );

        let reply = state.handle_request_vote(request_vote(1, "node-c", 0, 0));

        assert!(!reply.vote_granted);
        assert_eq!(state.voted_for(), Some("node-b"));
    }

    #[test]
    fn request_vote_rejected_if_candidate_log_term_is_behind() {
        let mut state = state_with_entries(&[(2, "task-a")]);

        let reply = state.handle_request_vote(request_vote(3, "node-b", 1, 1));

        assert!(!reply.vote_granted);
        assert_eq!(state.voted_for(), None);
    }

    #[test]
    fn request_vote_rejected_if_same_log_term_but_lower_index() {
        let mut state = state_with_entries(&[(2, "task-a"), (2, "task-b")]);

        let reply = state.handle_request_vote(request_vote(3, "node-b", 1, 2));

        assert!(!reply.vote_granted);
    }

    #[test]
    fn request_vote_granted_if_candidate_log_is_at_least_as_up_to_date() {
        let mut state = state_with_entries(&[(2, "task-a"), (2, "task-b")]);

        let reply = state.handle_request_vote(request_vote(3, "node-b", 2, 2));

        assert!(reply.vote_granted);
        assert_eq!(state.voted_for(), Some("node-b"));
    }

    #[test]
    fn higher_term_message_demotes_to_follower_and_clears_vote() {
        let mut state = RaftState::new(5);
        state.become_candidate("node-a".to_string());

        state.handle_higher_term(7);

        assert_eq!(state.role(), Role::Follower);
        assert_eq!(state.current_term(), 7);
        assert_eq!(state.voted_for(), None);
        assert_eq!(state.votes_received(), 0);
    }

    #[test]
    fn append_entries_heartbeat_resets_to_follower_and_accepts_leader() {
        let mut state = RaftState::new(5);
        state.become_candidate("node-a".to_string());

        let reply = state.handle_append_entries(append_entries(2, "node-b", 0, 0, Vec::new(), 0));

        assert!(reply.success);
        assert_eq!(reply.term, 2);
        assert_eq!(state.role(), Role::Follower);
        assert_eq!(state.current_term(), 2);
        assert_eq!(state.voted_for(), None);
    }

    #[test]
    fn append_entries_rejected_for_stale_term() {
        let mut state = RaftState::default();
        state.handle_higher_term(4);

        let reply = state.handle_append_entries(append_entries(3, "node-b", 0, 0, Vec::new(), 0));

        assert!(!reply.success);
        assert_eq!(reply.term, 4);
    }

    #[test]
    fn append_entries_rejected_when_prev_log_does_not_match() {
        let mut state = state_with_entries(&[(1, "task-a")]);

        let reply = state.handle_append_entries(append_entries(2, "node-b", 1, 2, Vec::new(), 0));

        assert!(!reply.success);
        assert_eq!(reply.match_index, 1);
    }

    #[test]
    fn append_entries_advances_commit_index_from_leader_commit() {
        let mut state = state_with_entries(&[(1, "task-a"), (1, "task-b")]);

        let reply = state.handle_append_entries(append_entries(2, "node-b", 2, 1, Vec::new(), 5));

        assert!(reply.success);
        assert_eq!(state.commit_index(), 2);
    }

    #[test]
    fn append_entries_appends_after_matching_previous_entry() {
        let mut state = state_with_entries(&[(1, "task-a")]);
        let entry = log_entry(2, 2, "task-b");

        let reply =
            state.handle_append_entries(append_entries(2, "node-b", 1, 1, vec![entry.clone()], 0));

        assert!(reply.success);
        assert_eq!(reply.match_index, 2);
        assert_eq!(state.log(), &[log_entry(1, 1, "task-a"), entry]);
    }

    #[test]
    fn request_vote_reply_triggers_candidate_to_leader_at_majority() {
        let mut state = RaftState::new(5);
        state.become_candidate("node-a".to_string());

        assert_eq!(
            state.handle_request_vote_reply(RequestVoteReply {
                term: 1,
                vote_granted: true,
            }),
            None
        );
        assert_eq!(
            state.handle_request_vote_reply(RequestVoteReply {
                term: 1,
                vote_granted: true,
            }),
            Some(Role::Leader)
        );
        assert_eq!(state.role(), Role::Leader);
    }

    #[test]
    fn request_vote_reply_from_higher_term_demotes_candidate() {
        let mut state = RaftState::new(5);
        state.become_candidate("node-a".to_string());

        let role = state.handle_request_vote_reply(RequestVoteReply {
            term: 2,
            vote_granted: false,
        });

        assert_eq!(role, Some(Role::Follower));
        assert_eq!(state.role(), Role::Follower);
        assert_eq!(state.current_term(), 2);
    }

    #[test]
    fn raft_payload_codecs_round_trip() {
        let vote = request_vote(3, "node-a", 9, 2);
        assert_eq!(RequestVote::decode(&vote.encode().unwrap()).unwrap(), vote);

        let vote_reply = RequestVoteReply {
            term: 3,
            vote_granted: true,
        };
        assert_eq!(
            RequestVoteReply::decode(&vote_reply.encode()).unwrap(),
            vote_reply
        );

        let append = append_entries(4, "node-a", 1, 2, vec![log_entry(4, 2, "task-b")], 1);
        assert_eq!(
            AppendEntries::decode(&append.encode().unwrap()).unwrap(),
            append
        );

        let append_reply = AppendEntriesReply {
            term: 4,
            success: true,
            match_index: 2,
        };
        assert_eq!(
            AppendEntriesReply::decode(&append_reply.encode()).unwrap(),
            append_reply
        );
    }

    #[test]
    fn raft_message_family_codecs_round_trip() {
        let vote = RaftMessage::RequestVote(request_vote(3, "node-a", 9, 2));
        assert_eq!(
            RaftMessage::decode_request_vote_family(&vote.encode_request_vote_family().unwrap())
                .unwrap(),
            vote
        );

        let vote_reply = RaftMessage::RequestVoteReply(RequestVoteReply {
            term: 3,
            vote_granted: true,
        });
        assert_eq!(
            RaftMessage::decode_request_vote_family(
                &vote_reply.encode_request_vote_family().unwrap()
            )
            .unwrap(),
            vote_reply
        );

        let append = RaftMessage::AppendEntries(append_entries(
            4,
            "node-a",
            1,
            2,
            vec![log_entry(4, 2, "task-b")],
            1,
        ));
        assert_eq!(
            RaftMessage::decode_append_entries_family(
                &append.encode_append_entries_family().unwrap()
            )
            .unwrap(),
            append
        );

        let append_reply = RaftMessage::AppendEntriesReply(AppendEntriesReply {
            term: 4,
            success: true,
            match_index: 2,
        });
        assert_eq!(
            RaftMessage::decode_append_entries_family(
                &append_reply.encode_append_entries_family().unwrap()
            )
            .unwrap(),
            append_reply
        );
    }

    #[test]
    fn raft_payload_codecs_reject_truncated_payload() {
        assert_eq!(
            RequestVote::decode(&[0, 1, 2]),
            Err(RaftCodecError::InvalidPayload)
        );
        assert_eq!(
            AppendEntries::decode(&[0, 1, 2]),
            Err(RaftCodecError::InvalidPayload)
        );
    }

    #[test]
    fn drain_applied_entries_returns_committed_not_yet_applied() {
        let mut state = state_with_entries(&[(1, "task-a"), (1, "task-b"), (1, "task-c")]);
        assert_eq!(state.commit_index(), 0);
        assert_eq!(state.last_applied(), 0);

        let applied = state.drain_applied_entries();
        assert!(applied.is_empty());

        state.commit_through(2);
        let applied = state.drain_applied_entries();
        assert_eq!(applied.len(), 2);
        assert_eq!(applied[0].task_id, "task-a");
        assert_eq!(applied[1].task_id, "task-b");
        assert_eq!(state.last_applied(), 2);

        state.commit_through(3);
        let applied = state.drain_applied_entries();
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].task_id, "task-c");
        assert_eq!(state.last_applied(), 3);
    }

    fn state_with_entries(entries: &[(u64, &str)]) -> RaftState {
        let mut state = RaftState::new(5);
        for (term, task_id) in entries {
            state.append_local_entry(*term, (*task_id).to_string(), task_id.as_bytes().to_vec());
        }
        state
    }

    fn request_vote(
        term: u64,
        candidate_id: &str,
        last_log_index: u64,
        last_log_term: u64,
    ) -> RequestVote {
        RequestVote {
            term,
            candidate_id: candidate_id.to_string(),
            last_log_index,
            last_log_term,
        }
    }

    fn append_entries(
        term: u64,
        leader_id: &str,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<RaftLogEntry>,
        leader_commit: u64,
    ) -> AppendEntries {
        AppendEntries {
            term,
            leader_id: leader_id.to_string(),
            prev_log_index,
            prev_log_term,
            entries,
            leader_commit,
        }
    }

    fn log_entry(term: u64, index: u64, task_id: &str) -> RaftLogEntry {
        RaftLogEntry {
            term,
            index,
            task_id: task_id.to_string(),
            payload: task_id.as_bytes().to_vec(),
        }
    }
}
