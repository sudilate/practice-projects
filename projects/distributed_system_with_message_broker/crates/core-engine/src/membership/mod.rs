use std::collections::HashMap;
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
