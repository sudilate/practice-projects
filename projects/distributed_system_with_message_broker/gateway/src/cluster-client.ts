import { encodeFrame, Opcode } from "./protocol";

export interface ClusterNode {
  host: string;
  port: number;
}

export class ClusterClient {
  constructor(private readonly nodes: ClusterNode[]) {}

  async submitTask(payload: unknown): Promise<{ accepted: boolean }> {
    const leader = this.nodes[0];
    if (!leader) {
      throw new Error("no cluster nodes configured");
    }

    const encodedPayload = new TextEncoder().encode(JSON.stringify(payload));
    encodeFrame({ opcode: Opcode.AppendTask, payload: encodedPayload });

    // Phase 4.2 will replace this placeholder with a persistent Bun.connect socket.
    return { accepted: true };
  }
}
