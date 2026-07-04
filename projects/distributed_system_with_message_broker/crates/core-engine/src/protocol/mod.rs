use std::convert::TryFrom;
use std::fmt;

pub const HEADER_LEN: usize = 5;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    IncompleteFrame,
    PayloadTooLarge,
    TrailingBytes,
    UnknownOpcode(u8),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IncompleteFrame => write!(f, "incomplete frame"),
            Self::PayloadTooLarge => write!(f, "payload too large"),
            Self::TrailingBytes => write!(f, "frame contains trailing bytes"),
            Self::UnknownOpcode(opcode) => write!(f, "unknown opcode: {opcode}"),
        }
    }
}

impl std::error::Error for ProtocolError {}

#[cfg(test)]
mod tests {
    use super::{Frame, Opcode, ProtocolError};

    #[test]
    fn frame_round_trips() {
        let frame = Frame::new(Opcode::AppendTask, b"payload".to_vec());

        let encoded = frame.encode().expect("frame encodes");
        let decoded = Frame::decode(&encoded).expect("frame decodes");

        assert_eq!(decoded, frame);
    }

    #[test]
    fn decode_rejects_unknown_opcode() {
        let bytes = [0, 0, 0, 0, 99];

        assert_eq!(Frame::decode(&bytes), Err(ProtocolError::UnknownOpcode(99)));
    }
}
