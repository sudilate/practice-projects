import { describe, expect, test, beforeAll, afterAll } from "bun:test";
import { buildServer } from "../src/server";
import {
  decodeFrame,
  encodeErrorResponse,
  encodeFrame,
  encodeTaskStatusRequest,
  encodeTaskStatusResponse,
  Opcode,
  StreamingDecoder,
  TaskStatusCode,
} from "../src/protocol";

interface MockServer {
  port: number;
  stop: () => void;
}

function startMockCluster(options: {
  acceptLeader?: boolean;
  status?: { status: TaskStatusCode; output: string; error: string };
} = {}): MockServer {
  const decoders = new Map<Bun.Socket<unknown>, StreamingDecoder>();
  const server = Bun.listen({
    hostname: "127.0.0.1",
    port: 0,
    socket: {
      open(socket) {
        decoders.set(socket, new StreamingDecoder());
      },
      data(socket, data: Uint8Array) {
        const decoder = decoders.get(socket);
        if (!decoder) {
          return;
        }
        const frames = decoder.push(data);
        for (const frame of frames) {
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
          } else if (frame.opcode === Opcode.GetTaskStatus) {
            const status = options.status ?? {
              status: TaskStatusCode.Completed,
              output: "HELLO",
              error: "",
            };
            socket.write(
              encodeFrame({
                opcode: Opcode.TaskStatus,
                payload: encodeTaskStatusResponse(status),
              })
            );
          }
        }
      },
      close(socket) {
        decoders.delete(socket);
      },
    },
  });

  return {
    port: server.port,
    stop: () => server.stop(),
  };
}

describe("gateway HTTP routes", () => {
  let mock: MockServer;

  beforeAll(() => {
    mock = startMockCluster();
  });

  afterAll(() => {
    mock.stop();
  });

  test("POST /tasks accepts a valid task and returns 202", async () => {
    const server = buildServer({ clusterNodes: [{ host: "127.0.0.1", port: mock.port }] });

    const response = await server.inject({
      method: "POST",
      url: "/tasks",
      payload: { type: "uppercase", payload: "hello" },
    });

    expect(response.statusCode).toBe(202);
    const body = JSON.parse(response.body);
    expect(body.accepted).toBe(true);
    expect(typeof body.taskId).toBe("string");
  });

  test("POST /tasks rejects an invalid task with 400", async () => {
    const server = buildServer({ clusterNodes: [{ host: "127.0.0.1", port: mock.port }] });

    const response = await server.inject({
      method: "POST",
      url: "/tasks",
      payload: { type: "unknown", payload: "hello" },
    });

    expect(response.statusCode).toBe(400);
  });

  test("GET /tasks/:id returns task status", async () => {
    const server = buildServer({ clusterNodes: [{ host: "127.0.0.1", port: mock.port }] });

    const response = await server.inject({
      method: "GET",
      url: "/tasks/task-123",
    });

    expect(response.statusCode).toBe(200);
    const body = JSON.parse(response.body);
    expect(body.status).toBe("completed");
    expect(body.output).toBe("HELLO");
  });

  test("GET /metrics returns prometheus text", async () => {
    const server = buildServer({ clusterNodes: [{ host: "127.0.0.1", port: mock.port }] });

    await server.inject({
      method: "POST",
      url: "/tasks",
      payload: { type: "uppercase", payload: "hello" },
    });

    const response = await server.inject({
      method: "GET",
      url: "/metrics",
    });

    expect(response.statusCode).toBe(200);
    expect(response.body).toContain("gateway_http_requests_total");
    expect(response.body).toContain("gateway_tasks_submitted_total");
  });

  test("POST /tasks follows leader redirection on 409", async () => {
    const leader = await startMockCluster({ acceptLeader: true });
    const follower = await startMockCluster({ acceptLeader: false });

    try {
      const server = buildServer({
        clusterNodes: [
          { host: "127.0.0.1", port: follower.port },
          { host: "127.0.0.1", port: leader.port },
        ],
      });

      const response = await server.inject({
        method: "POST",
        url: "/tasks",
        payload: { type: "uppercase", payload: "hello" },
      });

      expect(response.statusCode).toBe(202);
    } finally {
      leader.stop();
      follower.stop();
    }
  });
});
