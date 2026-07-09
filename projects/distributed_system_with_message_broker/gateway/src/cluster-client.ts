import {
  decodeErrorResponse,
  decodeTaskStatusResponse,
  encodeErrorResponse,
  encodeFrame,
  encodeTaskStatusRequest,
  Frame,
  Opcode,
  StreamingDecoder,
  TaskStatusRequest,
  TaskStatusResponse,
} from "./protocol";

export interface ClusterNode {
  host: string;
  port: number;
}

export interface SubmitTaskResult {
  accepted: boolean;
  taskId: string;
}

interface NodeConnection {
  socket: Bun.Socket<unknown>;
  decoder: StreamingDecoder;
  pending: ((frame: Frame) => void) | null;
  correlationId: string | null;
}

interface NodeFailure {
  count: number;
  lastFailureAt: number;
}

function nodeKey(node: ClusterNode): string {
  return `${node.host}:${node.port}`;
}

function generateId(): string {
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
}

const BACKOFF_BASE_MS = 50;
const BACKOFF_MAX_MS = 1000;

export class ClusterClient {
  private connections = new Map<string, NodeConnection>();
  private failures = new Map<string, NodeFailure>();
  private leaderIndex = 0;

  constructor(private readonly nodes: ClusterNode[]) {}

  async submitTask(payload: unknown): Promise<SubmitTaskResult> {
    const correlationId = generateId();
    const taskId = generateId();
    const body = { id: taskId, ...this.normalizePayload(payload) };
    const encodedPayload = new TextEncoder().encode(JSON.stringify(body));
    const frame = encodeFrame({ opcode: Opcode.AppendTask, payload: encodedPayload });

    const response = await this.sendWithLeaderFailover(frame, correlationId);
    if (response.opcode === Opcode.Ack) {
      return { accepted: true, taskId };
    }
    if (response.opcode === Opcode.Error) {
      const error = decodeErrorResponse(response.payload);
      throw new ClusterError(error.code, error.message);
    }
    throw new ClusterError(500, "unexpected response opcode");
  }

  async getTaskStatus(taskId: string): Promise<TaskStatusResponse> {
    const correlationId = generateId();
    const request: TaskStatusRequest = { taskId };
    const frame = encodeFrame({
      opcode: Opcode.GetTaskStatus,
      payload: encodeTaskStatusRequest(request),
    });

    const response = await this.sendWithLeaderFailover(frame, correlationId);
    if (response.opcode === Opcode.TaskStatus) {
      return decodeTaskStatusResponse(response.payload);
    }
    if (response.opcode === Opcode.Error) {
      const error = decodeErrorResponse(response.payload);
      throw new ClusterError(error.code, error.message);
    }
    throw new ClusterError(500, "unexpected response opcode");
  }

  close(): void {
    for (const connection of this.connections.values()) {
      connection.socket.end();
    }
    this.connections.clear();
  }

  private normalizePayload(payload: unknown): Record<string, unknown> {
    if (payload === null || payload === undefined) {
      return {};
    }
    if (typeof payload !== "object" || Array.isArray(payload)) {
      return { value: payload };
    }
    return payload as Record<string, unknown>;
  }

  private async sendWithLeaderFailover(
    frame: Uint8Array,
    correlationId: string
  ): Promise<Frame> {
    const tried = new Set<number>();
    let current = this.leaderIndex;

    while (tried.size < this.nodes.length) {
      tried.add(current);
      const node = this.nodes[current];

      if (this.isNodeInBackoff(node)) {
        current = this.nextNodeIndex(current);
        continue;
      }

      try {
        const response = await this.sendToNode(node, frame, correlationId);
        this.recordSuccess(node);
        if (response.opcode === Opcode.Error) {
          const error = decodeErrorResponse(response.payload);
          if (error.code === 409) {
            this.leaderIndex = this.nextNodeIndex(current);
            current = this.leaderIndex;
            continue;
          }
        }
        this.leaderIndex = current;
        return response;
      } catch (error) {
        this.recordFailure(node);
        this.leaderIndex = this.nextNodeIndex(current);
        current = this.leaderIndex;
        if (tried.size >= this.nodes.length) {
          throw new ClusterError(503, "all cluster nodes unavailable");
        }
      }
    }

    throw new ClusterError(503, "all cluster nodes unavailable");
  }

  private isNodeInBackoff(node: ClusterNode): boolean {
    const failure = this.failures.get(nodeKey(node));
    if (!failure) {
      return false;
    }
    const backoff = Math.min(BACKOFF_BASE_MS * 2 ** failure.count, BACKOFF_MAX_MS);
    return Date.now() - failure.lastFailureAt < backoff;
  }

  private recordFailure(node: ClusterNode): void {
    const key = nodeKey(node);
    const existing = this.failures.get(key);
    this.failures.set(key, {
      count: existing ? existing.count + 1 : 1,
      lastFailureAt: Date.now(),
    });
  }

  private recordSuccess(node: ClusterNode): void {
    this.failures.delete(nodeKey(node));
  }

  private nextNodeIndex(index: number): number {
    return (index + 1) % this.nodes.length;
  }

  private async sendToNode(
    node: ClusterNode,
    frame: Uint8Array,
    correlationId: string
  ): Promise<Frame> {
    const connection = await this.ensureConnection(node);
    if (connection.pending) {
      throw new ClusterError(503, "connection busy");
    }

    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        connection.pending = null;
        connection.correlationId = null;
        this.connections.delete(nodeKey(node));
        reject(new ClusterError(504, `cluster request timeout [${correlationId}]`));
      }, 5000);

      connection.pending = (responseFrame: Frame) => {
        clearTimeout(timeout);
        connection.pending = null;
        connection.correlationId = null;
        resolve(responseFrame);
      };
      connection.correlationId = correlationId;

      connection.socket.write(frame);
    });
  }

  private async ensureConnection(node: ClusterNode): Promise<NodeConnection> {
    const key = nodeKey(node);
    const existing = this.connections.get(key);
    if (existing) {
      return existing;
    }

    const decoder = new StreamingDecoder();
    const connections = this.connections;
    const socket = await Bun.connect({
      hostname: node.host,
      port: node.port,
      socket: {
        data(socket, data: Uint8Array) {
          const connection = connections.get(key);
          if (!connection) {
            socket.end();
            return;
          }
          try {
            const frames = connection.decoder.push(data);
            for (const frame of frames) {
              if (connection.pending) {
                connection.pending(frame);
              }
            }
          } catch (error) {
            if (connection.pending) {
              connection.pending({
                opcode: Opcode.Error,
                payload: encodeErrorResponse({
                  code: 400,
                  message: error instanceof Error ? error.message : "decode error",
                }),
              });
            }
          }
        },
        close() {
          connections.delete(key);
        },
        error() {
          connections.delete(key);
        },
      },
    });

    const connection: NodeConnection = {
      socket,
      decoder,
      pending: null,
      correlationId: null,
    };
    connections.set(key, connection);
    return connection;
  }
}

export class ClusterError extends Error {
  constructor(
    public readonly code: number,
    message: string
  ) {
    super(message);
    this.name = "ClusterError";
  }
}
