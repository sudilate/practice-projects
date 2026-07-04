#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskEnvelope {
    pub id: String,
    pub payload: Vec<u8>,
}
