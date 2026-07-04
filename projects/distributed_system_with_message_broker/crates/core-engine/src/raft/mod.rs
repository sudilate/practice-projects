#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftState {
    role: Role,
    current_term: u64,
    voted_for: Option<String>,
}

impl Default for RaftState {
    fn default() -> Self {
        Self {
            role: Role::Follower,
            current_term: 0,
            voted_for: None,
        }
    }
}

impl RaftState {
    pub fn role(&self) -> Role {
        self.role
    }

    pub fn current_term(&self) -> u64 {
        self.current_term
    }

    pub fn become_candidate(&mut self, node_id: String) {
        self.role = Role::Candidate;
        self.current_term += 1;
        self.voted_for = Some(node_id);
    }
}

#[cfg(test)]
mod tests {
    use super::{RaftState, Role};

    #[test]
    fn node_starts_as_follower() {
        let state = RaftState::default();

        assert_eq!(state.role(), Role::Follower);
        assert_eq!(state.current_term(), 0);
    }

    #[test]
    fn follower_can_start_election() {
        let mut state = RaftState::default();

        state.become_candidate("node-a".to_string());

        assert_eq!(state.role(), Role::Candidate);
        assert_eq!(state.current_term(), 1);
    }
}
