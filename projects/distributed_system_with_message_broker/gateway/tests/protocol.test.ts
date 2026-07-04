import { describe, expect, test } from "bun:test";
import { decodeFrame, encodeFrame, Opcode } from "../src/protocol";

describe("binary protocol", () => {
  test("round trips a frame", () => {
    const payload = new TextEncoder().encode("hello");

    const decoded = decodeFrame(encodeFrame({ opcode: Opcode.AppendTask, payload }));

    expect(decoded.opcode).toBe(Opcode.AppendTask);
    expect(new TextDecoder().decode(decoded.payload)).toBe("hello");
  });
});
