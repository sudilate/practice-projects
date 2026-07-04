import { describe, expect, test } from "bun:test";
import {
  decodeErrorResponse,
  decodeFrame,
  encodeErrorResponse,
  encodeFrame,
  MAX_PAYLOAD_LENGTH,
  Opcode,
  StreamingDecoder,
} from "../src/protocol";

describe("binary protocol", () => {
  test("round trips a frame", () => {
    const payload = new TextEncoder().encode("hello");

    const decoded = decodeFrame(encodeFrame({ opcode: Opcode.AppendTask, payload }));

    expect(decoded.opcode).toBe(Opcode.AppendTask);
    expect(new TextDecoder().decode(decoded.payload)).toBe("hello");
  });

  test("rejects oversized payloads", () => {
    const length = MAX_PAYLOAD_LENGTH + 1;
    const bytes = new Uint8Array([length >>> 24, length >>> 16, length >>> 8, length, Opcode.AppendTask]);

    expect(() => decodeFrame(bytes)).toThrow("payload too large");
  });

  test("streaming decoder waits for partial frames", () => {
    const payload = new TextEncoder().encode("hello");
    const bytes = encodeFrame({ opcode: Opcode.AppendTask, payload });
    const decoder = new StreamingDecoder();

    expect(decoder.push(bytes.slice(0, 3))).toEqual([]);
    const frames = decoder.push(bytes.slice(3));

    expect(frames).toHaveLength(1);
    expect(new TextDecoder().decode(frames[0].payload)).toBe("hello");
  });

  test("streaming decoder handles multiple frames", () => {
    const encoder = new TextEncoder();
    const first = encodeFrame({ opcode: Opcode.AppendTask, payload: encoder.encode("first") });
    const second = encodeFrame({ opcode: Opcode.Ack, payload: encoder.encode("second") });
    const bytes = new Uint8Array(first.byteLength + second.byteLength);
    bytes.set(first, 0);
    bytes.set(second, first.byteLength);
    const decoder = new StreamingDecoder();

    const frames = decoder.push(bytes);

    expect(frames).toHaveLength(2);
    expect(new TextDecoder().decode(frames[0].payload)).toBe("first");
    expect(new TextDecoder().decode(frames[1].payload)).toBe("second");
  });

  test("error response payload round trips", () => {
    const response = { code: 400, message: "bad frame" };

    expect(decodeErrorResponse(encodeErrorResponse(response))).toEqual(response);
  });
});
