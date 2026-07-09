use std::convert::TryFrom;
use std::fmt;

pub const HEADER_LEN: usize = 5;
pub const MAX_PAYLOAD_LEN: usize = 1024 * 1024;
/// Wire protocol major version for this implementation.
pub const PROTOCOL_VERSION: u8 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    AppendTask = 1,
    Ack = 2,
    Error = 3,
    Ping = 4,
    AckPing = 5,
    RequestVote = 6,
    AppendEntries = 7,
    Join = 8,
    JoinAck = 9,
    PingReq = 10,
    MembershipUpdate = 11,
    GetTaskStatus = 12,
    TaskStatus = 13,
    /// Version negotiation: payload is a single `u8` client version.
    Hello = 14,
    /// Metrics scrape over the binary protocol (Prometheus text payload).
    GetMetrics = 15,
}

impl TryFrom<u8> for Opcode {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, ProtocolError> {
        match value {
            1 => Ok(Self::AppendTask),
            2 => Ok(Self::Ack),
            3 => Ok(Self::Error),
            4 => Ok(Self::Ping),
            5 => Ok(Self::AckPing),
            6 => Ok(Self::RequestVote),
            7 => Ok(Self::AppendEntries),
            8 => Ok(Self::Join),
            9 => Ok(Self::JoinAck),
            10 => Ok(Self::PingReq),
            11 => Ok(Self::MembershipUpdate),
            12 => Ok(Self::GetTaskStatus),
            13 => Ok(Self::TaskStatus),
            14 => Ok(Self::Hello),
            15 => Ok(Self::GetMetrics),
            _ => Err(ProtocolError::UnknownOpcode(value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    opcode: Opcode,
    payload: Vec<u8>,
}

impl Frame {
    pub fn new(opcode: Opcode, payload: Vec<u8>) -> Self {
        Self { opcode, payload }
    }

    pub fn opcode(&self) -> Opcode {
        self.opcode
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        if self.payload.len() > MAX_PAYLOAD_LEN {
            return Err(ProtocolError::PayloadTooLarge);
        }

        let length =
            u32::try_from(self.payload.len()).map_err(|_| ProtocolError::PayloadTooLarge)?;
        let mut bytes = Vec::with_capacity(HEADER_LEN + self.payload.len());
        bytes.extend_from_slice(&length.to_be_bytes());
        bytes.push(self.opcode as u8);
        bytes.extend_from_slice(&self.payload);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ProtocolError> {
        if bytes.len() < HEADER_LEN {
            return Err(ProtocolError::IncompleteFrame);
        }

        let length = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
        if length > MAX_PAYLOAD_LEN {
            return Err(ProtocolError::PayloadTooLarge);
        }

        let expected_len = HEADER_LEN + length;
        if bytes.len() < expected_len {
            return Err(ProtocolError::IncompleteFrame);
        }
        if bytes.len() > expected_len {
            return Err(ProtocolError::TrailingBytes);
        }

        Ok(Self {
            opcode: Opcode::try_from(bytes[4])?,
            payload: bytes[HEADER_LEN..].to_vec(),
        })
    }
}

#[derive(Debug, Default)]
pub struct StreamingDecoder {
    buffer: Vec<u8>,
}

impl StreamingDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Frame>, ProtocolError> {
        self.buffer.extend_from_slice(bytes);
        let mut frames = Vec::new();

        loop {
            if self.buffer.len() < HEADER_LEN {
                return Ok(frames);
            }

            let length = u32::from_be_bytes([
                self.buffer[0],
                self.buffer[1],
                self.buffer[2],
                self.buffer[3],
            ]) as usize;
            if length > MAX_PAYLOAD_LEN {
                return Err(ProtocolError::PayloadTooLarge);
            }

            let frame_len = HEADER_LEN + length;
            if self.buffer.len() < frame_len {
                return Ok(frames);
            }

            let frame_bytes: Vec<u8> = self.buffer.drain(..frame_len).collect();
            frames.push(Frame::decode(&frame_bytes)?);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ErrorResponse {
    pub code: u16,
    pub message: String,
}

impl ErrorResponse {
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let message = self.message.as_bytes();
        let message_len =
            u16::try_from(message.len()).map_err(|_| ProtocolError::PayloadTooLarge)?;
        let mut payload = Vec::with_capacity(4 + message.len());
        payload.extend_from_slice(&self.code.to_be_bytes());
        payload.extend_from_slice(&message_len.to_be_bytes());
        payload.extend_from_slice(message);
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ProtocolError> {
        if payload.len() < 4 {
            return Err(ProtocolError::InvalidErrorPayload);
        }

        let code = u16::from_be_bytes([payload[0], payload[1]]);
        let message_len = u16::from_be_bytes([payload[2], payload[3]]) as usize;
        if payload.len() != 4 + message_len {
            return Err(ProtocolError::InvalidErrorPayload);
        }

        let message = std::str::from_utf8(&payload[4..])
            .map_err(|_| ProtocolError::InvalidErrorPayload)?
            .to_string();

        Ok(Self { code, message })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    IncompleteFrame,
    PayloadTooLarge,
    InvalidErrorPayload,
    InvalidTaskStatusPayload,
    InvalidTaskStatus(u8),
    TrailingBytes,
    UnknownOpcode(u8),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncompleteFrame => write!(f, "incomplete frame"),
            Self::PayloadTooLarge => write!(f, "payload too large"),
            Self::InvalidErrorPayload => write!(f, "invalid error payload"),
            Self::InvalidTaskStatusPayload => write!(f, "invalid task status payload"),
            Self::InvalidTaskStatus(status) => write!(f, "invalid task status: {status}"),
            Self::TrailingBytes => write!(f, "frame contains trailing bytes"),
            Self::UnknownOpcode(opcode) => write!(f, "unknown opcode: {opcode}"),
        }
    }
}

impl std::error::Error for ProtocolError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TaskStatusCode {
    Pending = 0,
    Running = 1,
    Completed = 2,
    Failed = 3,
}

impl TryFrom<u8> for TaskStatusCode {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, ProtocolError> {
        match value {
            0 => Ok(Self::Pending),
            1 => Ok(Self::Running),
            2 => Ok(Self::Completed),
            3 => Ok(Self::Failed),
            _ => Err(ProtocolError::InvalidTaskStatus(value)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskStatusResponse {
    pub status: TaskStatusCode,
    pub output: String,
    pub error: String,
}

impl TaskStatusResponse {
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let output = self.output.as_bytes();
        let error = self.error.as_bytes();
        let output_len = u32::try_from(output.len()).map_err(|_| ProtocolError::PayloadTooLarge)?;
        let error_len = u32::try_from(error.len()).map_err(|_| ProtocolError::PayloadTooLarge)?;
        let mut payload = Vec::with_capacity(1 + 4 + output.len() + 4 + error.len());
        payload.push(self.status as u8);
        payload.extend_from_slice(&output_len.to_be_bytes());
        payload.extend_from_slice(output);
        payload.extend_from_slice(&error_len.to_be_bytes());
        payload.extend_from_slice(error);
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ProtocolError> {
        if payload.len() < 9 {
            return Err(ProtocolError::InvalidTaskStatusPayload);
        }
        let status = TaskStatusCode::try_from(payload[0])?;
        let output_len =
            u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]) as usize;
        let error_len_offset = 5 + output_len;
        if payload.len() < error_len_offset + 4 {
            return Err(ProtocolError::InvalidTaskStatusPayload);
        }
        let error_len = u32::from_be_bytes([
            payload[error_len_offset],
            payload[error_len_offset + 1],
            payload[error_len_offset + 2],
            payload[error_len_offset + 3],
        ]) as usize;
        if payload.len() != error_len_offset + 4 + error_len {
            return Err(ProtocolError::InvalidTaskStatusPayload);
        }
        let output = std::str::from_utf8(&payload[5..error_len_offset])
            .map_err(|_| ProtocolError::InvalidTaskStatusPayload)?
            .to_string();
        let error = std::str::from_utf8(&payload[error_len_offset + 4..])
            .map_err(|_| ProtocolError::InvalidTaskStatusPayload)?
            .to_string();
        Ok(Self {
            status,
            output,
            error,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskStatusRequest {
    pub task_id: String,
}

impl TaskStatusRequest {
    pub fn encode(&self) -> Result<Vec<u8>, ProtocolError> {
        let task_id = self.task_id.as_bytes();
        let task_id_len =
            u16::try_from(task_id.len()).map_err(|_| ProtocolError::PayloadTooLarge)?;
        let mut payload = Vec::with_capacity(2 + task_id.len());
        payload.extend_from_slice(&task_id_len.to_be_bytes());
        payload.extend_from_slice(task_id);
        Ok(payload)
    }

    pub fn decode(payload: &[u8]) -> Result<Self, ProtocolError> {
        if payload.len() < 2 {
            return Err(ProtocolError::InvalidTaskStatusPayload);
        }
        let task_id_len = u16::from_be_bytes([payload[0], payload[1]]) as usize;
        if payload.len() != 2 + task_id_len {
            return Err(ProtocolError::InvalidTaskStatusPayload);
        }
        let task_id = std::str::from_utf8(&payload[2..])
            .map_err(|_| ProtocolError::InvalidTaskStatusPayload)?
            .to_string();
        Ok(Self { task_id })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ErrorResponse, Frame, Opcode, ProtocolError, StreamingDecoder, TaskStatusCode,
        TaskStatusRequest, TaskStatusResponse, MAX_PAYLOAD_LEN, PROTOCOL_VERSION,
    };

    #[test]
    fn frame_round_trips() {
        let frame = Frame::new(Opcode::AppendTask, b"payload".to_vec());

        let encoded = frame.encode().expect("frame encodes");
        let decoded = Frame::decode(&encoded).expect("frame decodes");

        assert_eq!(decoded, frame);
    }

    #[test]
    fn hello_and_metrics_opcodes_round_trip() {
        let hello = Frame::new(Opcode::Hello, vec![PROTOCOL_VERSION]);
        let metrics = Frame::new(Opcode::GetMetrics, Vec::new());
        assert_eq!(
            Frame::decode(&hello.encode().expect("encodes")).expect("decodes"),
            hello
        );
        assert_eq!(
            Frame::decode(&metrics.encode().expect("encodes")).expect("decodes"),
            metrics
        );
        assert_eq!(PROTOCOL_VERSION, 1);
    }

    #[test]
    fn decode_rejects_unknown_opcode() {
        let bytes = [0, 0, 0, 0, 99];

        assert_eq!(Frame::decode(&bytes), Err(ProtocolError::UnknownOpcode(99)));
    }

    #[test]
    fn decode_rejects_oversized_payload() {
        let length = (MAX_PAYLOAD_LEN as u32 + 1).to_be_bytes();
        let bytes = [
            length[0],
            length[1],
            length[2],
            length[3],
            Opcode::AppendTask as u8,
        ];

        assert_eq!(Frame::decode(&bytes), Err(ProtocolError::PayloadTooLarge));
    }

    #[test]
    fn streaming_decoder_waits_for_partial_frame() {
        let frame = Frame::new(Opcode::AppendTask, b"hello".to_vec())
            .encode()
            .expect("frame encodes");
        let mut decoder = StreamingDecoder::new();

        assert!(decoder
            .push(&frame[..3])
            .expect("partial decode succeeds")
            .is_empty());
        let frames = decoder.push(&frame[3..]).expect("full decode succeeds");

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].payload(), b"hello");
    }

    #[test]
    fn streaming_decoder_handles_multiple_frames() {
        let first = Frame::new(Opcode::AppendTask, b"first".to_vec())
            .encode()
            .expect("first encodes");
        let second = Frame::new(Opcode::Ack, b"second".to_vec())
            .encode()
            .expect("second encodes");
        let mut bytes = first;
        bytes.extend_from_slice(&second);
        let mut decoder = StreamingDecoder::new();

        let frames = decoder.push(&bytes).expect("frames decode");

        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].payload(), b"first");
        assert_eq!(frames[1].payload(), b"second");
    }

    #[test]
    fn error_response_payload_round_trips() {
        let response = ErrorResponse {
            code: 400,
            message: "bad frame".to_string(),
        };

        let decoded = ErrorResponse::decode(&response.encode().expect("payload encodes"))
            .expect("payload decodes");

        assert_eq!(decoded, response);
    }

    #[test]
    fn task_status_request_payload_round_trips() {
        let request = TaskStatusRequest {
            task_id: "task-123".to_string(),
        };

        let decoded = TaskStatusRequest::decode(&request.encode().expect("payload encodes"))
            .expect("payload decodes");

        assert_eq!(decoded, request);
    }

    #[test]
    fn task_status_response_payload_round_trips() {
        let response = TaskStatusResponse {
            status: TaskStatusCode::Completed,
            output: "HELLO".to_string(),
            error: "".to_string(),
        };

        let decoded = TaskStatusResponse::decode(&response.encode().expect("payload encodes"))
            .expect("payload decodes");

        assert_eq!(decoded, response);
    }
}
