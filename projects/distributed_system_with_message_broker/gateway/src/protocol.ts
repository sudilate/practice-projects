export const HEADER_LENGTH = 5;
export const MAX_PAYLOAD_LENGTH = 1024 * 1024;
export const PROTOCOL_VERSION = 1;

type Bytes = Uint8Array<ArrayBufferLike>;

export enum Opcode {
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
  Hello = 14,
  GetMetrics = 15,
}

export interface Frame {
  opcode: Opcode;
  payload: Bytes;
}

export interface ErrorResponse {
  code: number;
  message: string;
}

export enum TaskStatusCode {
  Pending = 0,
  Running = 1,
  Completed = 2,
  Failed = 3,
}

export interface TaskStatusResponse {
  status: TaskStatusCode;
  output: string;
  error: string;
}

export interface TaskStatusRequest {
  taskId: string;
}

export function encodeFrame(frame: Frame): Bytes {
  if (frame.payload.byteLength > MAX_PAYLOAD_LENGTH) {
    throw new Error("payload too large");
  }

  const bytes = new Uint8Array(HEADER_LENGTH + frame.payload.byteLength);
  const view = new DataView(bytes.buffer);
  view.setUint32(0, frame.payload.byteLength, false);
  view.setUint8(4, frame.opcode);
  bytes.set(frame.payload, HEADER_LENGTH);
  return bytes;
}

export function decodeFrame(bytes: Bytes): Frame {
  if (bytes.byteLength < HEADER_LENGTH) {
    throw new Error("incomplete frame");
  }

  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const payloadLength = view.getUint32(0, false);
  if (payloadLength > MAX_PAYLOAD_LENGTH) {
    throw new Error("payload too large");
  }

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

export class StreamingDecoder {
  private buffer: Bytes = new Uint8Array(0);

  push(bytes: Bytes): Frame[] {
    this.buffer = concatBytes(this.buffer, bytes);
    const frames: Frame[] = [];

    while (this.buffer.byteLength >= HEADER_LENGTH) {
      const view = new DataView(this.buffer.buffer, this.buffer.byteOffset, this.buffer.byteLength);
      const payloadLength = view.getUint32(0, false);
      if (payloadLength > MAX_PAYLOAD_LENGTH) {
        throw new Error("payload too large");
      }

      const frameLength = HEADER_LENGTH + payloadLength;
      if (this.buffer.byteLength < frameLength) {
        break;
      }

      frames.push(decodeFrame(this.buffer.slice(0, frameLength)));
      this.buffer = this.buffer.slice(frameLength);
    }

    return frames;
  }
}

export function encodeErrorResponse(response: ErrorResponse): Bytes {
  const message = new TextEncoder().encode(response.message);
  if (response.code < 0 || response.code > 0xffff || message.byteLength > 0xffff) {
    throw new Error("invalid error response");
  }

  const bytes = new Uint8Array(4 + message.byteLength);
  const view = new DataView(bytes.buffer);
  view.setUint16(0, response.code, false);
  view.setUint16(2, message.byteLength, false);
  bytes.set(message, 4);
  return bytes;
}

export function decodeErrorResponse(payload: Bytes): ErrorResponse {
  if (payload.byteLength < 4) {
    throw new Error("invalid error payload");
  }

  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  const code = view.getUint16(0, false);
  const messageLength = view.getUint16(2, false);
  if (payload.byteLength !== 4 + messageLength) {
    throw new Error("invalid error payload");
  }

  return {
    code,
    message: new TextDecoder().decode(payload.slice(4)),
  };
}

export function encodeTaskStatusRequest(request: TaskStatusRequest): Bytes {
  const taskId = new TextEncoder().encode(request.taskId);
  if (taskId.byteLength > 0xffff) {
    throw new Error("task id too long");
  }
  const bytes = new Uint8Array(2 + taskId.byteLength);
  const view = new DataView(bytes.buffer);
  view.setUint16(0, taskId.byteLength, false);
  bytes.set(taskId, 2);
  return bytes;
}

export function decodeTaskStatusRequest(payload: Bytes): TaskStatusRequest {
  if (payload.byteLength < 2) {
    throw new Error("invalid task status request");
  }
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  const taskIdLength = view.getUint16(0, false);
  if (payload.byteLength !== 2 + taskIdLength) {
    throw new Error("invalid task status request");
  }
  return {
    taskId: new TextDecoder().decode(payload.slice(2)),
  };
}

export function encodeTaskStatusResponse(response: TaskStatusResponse): Bytes {
  const output = new TextEncoder().encode(response.output);
  const error = new TextEncoder().encode(response.error);
  if (output.byteLength > 0xffffffff || error.byteLength > 0xffffffff) {
    throw new Error("task status response too large");
  }
  const bytes = new Uint8Array(1 + 4 + output.byteLength + 4 + error.byteLength);
  const view = new DataView(bytes.buffer);
  view.setUint8(0, response.status);
  view.setUint32(1, output.byteLength, false);
  bytes.set(output, 5);
  view.setUint32(5 + output.byteLength, error.byteLength, false);
  bytes.set(error, 9 + output.byteLength);
  return bytes;
}

export function decodeTaskStatusResponse(payload: Bytes): TaskStatusResponse {
  if (payload.byteLength < 9) {
    throw new Error("invalid task status response");
  }
  const view = new DataView(payload.buffer, payload.byteOffset, payload.byteLength);
  const status = view.getUint8(0) as TaskStatusCode;
  const outputLength = view.getUint32(1, false);
  const errorLengthOffset = 5 + outputLength;
  if (payload.byteLength < errorLengthOffset + 4) {
    throw new Error("invalid task status response");
  }
  const errorLength = view.getUint32(errorLengthOffset, false);
  if (payload.byteLength !== errorLengthOffset + 4 + errorLength) {
    throw new Error("invalid task status response");
  }
  return {
    status,
    output: new TextDecoder().decode(payload.slice(5, errorLengthOffset)),
    error: new TextDecoder().decode(payload.slice(errorLengthOffset + 4)),
  };
}

function concatBytes(left: Bytes, right: Bytes): Bytes {
  const bytes = new Uint8Array(left.byteLength + right.byteLength);
  bytes.set(left, 0);
  bytes.set(right, left.byteLength);
  return bytes;
}
