export const HEADER_LENGTH = 5;

export enum Opcode {
  AppendTask = 1,
  Ack = 2,
  Error = 3,
  Ping = 4,
  AckPing = 5,
  RequestVote = 6,
  AppendEntries = 7,
}

export interface Frame {
  opcode: Opcode;
  payload: Uint8Array;
}

export function encodeFrame(frame: Frame): Uint8Array {
  const bytes = new Uint8Array(HEADER_LENGTH + frame.payload.byteLength);
  const view = new DataView(bytes.buffer);
  view.setUint32(0, frame.payload.byteLength, false);
  view.setUint8(4, frame.opcode);
  bytes.set(frame.payload, HEADER_LENGTH);
  return bytes;
}

export function decodeFrame(bytes: Uint8Array): Frame {
  if (bytes.byteLength < HEADER_LENGTH) {
    throw new Error("incomplete frame");
  }

  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const payloadLength = view.getUint32(0, false);
  const expectedLength = HEADER_LENGTH + payloadLength;
  if (bytes.byteLength !== expectedLength) {
    throw new Error(`invalid frame length: expected ${expectedLength}, got ${bytes.byteLength}`);
  }

  const opcode = view.getUint8(4);
  if (!Object.values(Opcode).includes(opcode)) {
    throw new Error(`unknown opcode: ${opcode}`);
  }

  return {
    opcode: opcode as Opcode,
    payload: bytes.slice(HEADER_LENGTH),
  };
}
