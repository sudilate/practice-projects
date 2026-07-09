import { describe, expect, test } from "bun:test";
import { ClusterClient, ClusterError } from "../src/cluster-client";
import {
  encodeErrorResponse,
  encodeFrame,
  Opcode,
  StreamingDecoder,
} from "../src/protocol";

interface MockServer {
  port: number;
  stop: () => void;
  closeActive: () => void;
  requestCount: () => number;
}

function startMockCluster(options: {
  acceptLeader?: boolean;
  failFirstConnections?: number;
} = {}): MockServer {
  const decoders = new Map<Bun.Socket<unknown>, StreamingDecoder>();
  const active = new Set<Bun.Socket<unknown>>();
  let requests = 0;
  let remainingFailures = options.failFirstConnections ?? 0;

  const server = Bun.listen({
    hostname: "127.0.0.1",
    port: 0,
    socket: {
      open(socket) {
        if (remainingFailures > 0) {
          remainingFailures -= 1;
          socket.end();
          return;
        }
        active.add(socket);
        decoders.set(socket, new StreamingDecoder());
      },
      data(socket, data: Uint8Array) {
        const decoder = decoders.get(socket);
        if (!decoder) {
          return;
        }
        const frames = decoder.push(data);
        for (const frame of frames) {
          requests += 1;
          if (frame.opcode === Opcode.AppendTask) {
            if (options.acceptLeader === false) {
              socket.write(
                encodeFrame({
                  opcode: Opcode.Error,
                  payload: encodeErrorResponse({
                    code: 409,
                    message: "not raft leader",
                  }),
                })
              );
            } else {
              socket.write(encodeFrame({ opcode: Opcode.Ack, payload: new Uint8Array(0) }));
            }
          }
        }
      },
      close(socket) {
        active.delete(socket);
        decoders.delete(socket);
      },
    },
  });

  return {
    port: server.port,
    stop: () => server.stop(true),
    closeActive: () => {
      for (const socket of active) {
        socket.end();
      }
      active.clear();
    },
    requestCount: () => requests,
  };
}

describe("cluster client reconnect and failover", () => {
  test("fails over to a healthy node when the first node is down", async () => {
    const healthy = startMockCluster();
    const client = new ClusterClient([
      { host: "127.0.0.1", port: 1 },
      { host: "127.0.0.1", port: healthy.port },
    ]);

    try {
      const result = await client.submitTask({ type: "echo", payload: "hi" });
      expect(result.accepted).toBe(true);
      expect(typeof result.taskId).toBe("string");
      expect(healthy.requestCount()).toBe(1);
    } finally {
      client.close();
      healthy.stop();
    }
  });

  test("reconnects after an idle connection is closed", async () => {
    const mock = startMockCluster();
    const client = new ClusterClient([{ host: "127.0.0.1", port: mock.port }]);

    try {
      const first = await client.submitTask({ type: "echo", payload: "one" });
      expect(first.accepted).toBe(true);

      mock.closeActive();
      await Bun.sleep(50);

      const second = await client.submitTask({ type: "echo", payload: "two" });
      expect(second.accepted).toBe(true);
      expect(mock.requestCount()).toBe(2);
    } finally {
      client.close();
      mock.stop();
    }
  });

  test("reports all nodes unavailable when every endpoint is down", async () => {
    const client = new ClusterClient([
      { host: "127.0.0.1", port: 1 },
      { host: "127.0.0.1", port: 2 },
    ]);

    try {
      await expect(client.submitTask({ type: "echo", payload: "x" })).rejects.toBeInstanceOf(
        ClusterError
      );
    } finally {
      client.close();
    }
  });
});
