import Fastify from "fastify";
import { z } from "zod";
import { ClusterClient, ClusterError } from "./cluster-client";

const taskRequestSchema = z.object({
  type: z.enum(["uppercase", "echo", "reverse"]),
  payload: z.string().max(1024),
});

export interface ServerOptions {
  clusterNodes?: { host: string; port: number }[];
}

interface GatewayMetrics {
  httpRequestsTotal: number;
  tasksSubmittedTotal: number;
  tasksQueriedTotal: number;
  errorsTotal: number;
  startedAtMs: number;
}

export function buildServer(options: ServerOptions = {}) {
  const server = Fastify({
    logger: {
      level: process.env.LOG_LEVEL ?? "info",
    },
  });
  const nodes = options.clusterNodes ?? [{ host: "127.0.0.1", port: 7000 }];
  const cluster = new ClusterClient(nodes);
  const metrics: GatewayMetrics = {
    httpRequestsTotal: 0,
    tasksSubmittedTotal: 0,
    tasksQueriedTotal: 0,
    errorsTotal: 0,
    startedAtMs: Date.now(),
  };

  server.addHook("onRequest", async () => {
    metrics.httpRequestsTotal += 1;
  });

  server.addHook("onClose", async () => {
    cluster.close();
    server.log.info("gateway closed cluster connections");
  });

  server.get("/health", async () => ({ ok: true }));

  server.get("/metrics", async (_request, reply) => {
    const uptimeSeconds = Math.floor((Date.now() - metrics.startedAtMs) / 1000);
    const body = [
      "# HELP gateway_http_requests_total Total HTTP requests",
      "# TYPE gateway_http_requests_total counter",
      `gateway_http_requests_total ${metrics.httpRequestsTotal}`,
      "# HELP gateway_tasks_submitted_total Tasks accepted via POST /tasks",
      "# TYPE gateway_tasks_submitted_total counter",
      `gateway_tasks_submitted_total ${metrics.tasksSubmittedTotal}`,
      "# HELP gateway_tasks_queried_total Task status lookups",
      "# TYPE gateway_tasks_queried_total counter",
      `gateway_tasks_queried_total ${metrics.tasksQueriedTotal}`,
      "# HELP gateway_errors_total Handler errors",
      "# TYPE gateway_errors_total counter",
      `gateway_errors_total ${metrics.errorsTotal}`,
      "# HELP gateway_uptime_seconds Process uptime",
      "# TYPE gateway_uptime_seconds gauge",
      `gateway_uptime_seconds ${uptimeSeconds}`,
      "",
    ].join("\n");
    return reply.type("text/plain; version=0.0.4").send(body);
  });

  server.post<{ Body: unknown }>("/tasks", async (request, reply) => {
    const parse = taskRequestSchema.safeParse(request.body);
    if (!parse.success) {
      metrics.errorsTotal += 1;
      return reply.code(400).send({ error: parse.error.issues.map((e) => e.message).join("; ") });
    }
    try {
      const result = await cluster.submitTask(parse.data);
      metrics.tasksSubmittedTotal += 1;
      return reply.code(202).send(result);
    } catch (error) {
      metrics.errorsTotal += 1;
      return reply.code(clusterErrorCode(error)).send(clusterErrorBody(error));
    }
  });

  server.get<{
    Params: { taskId: string };
  }>("/tasks/:taskId", async (request, reply) => {
    try {
      const status = await cluster.getTaskStatus(request.params.taskId);
      metrics.tasksQueriedTotal += 1;
      return reply.code(200).send({
        taskId: request.params.taskId,
        status: statusCodeToString(status.status),
        output: status.output || null,
        error: status.error || null,
      });
    } catch (error) {
      metrics.errorsTotal += 1;
      return reply.code(clusterErrorCode(error)).send(clusterErrorBody(error));
    }
  });

  return server;
}

function clusterErrorCode(error: unknown): number {
  if (error instanceof ClusterError) {
    return error.code;
  }
  return 500;
}

function clusterErrorBody(error: unknown): { error: string } {
  if (error instanceof ClusterError) {
    return { error: error.message };
  }
  if (error instanceof Error) {
    return { error: error.message };
  }
  return { error: "internal error" };
}

function statusCodeToString(status: number): string {
  switch (status) {
    case 0:
      return "pending";
    case 1:
      return "running";
    case 2:
      return "completed";
    case 3:
      return "failed";
    default:
      return "unknown";
  }
}

function parseClusterNodes(): { host: string; port: number }[] {
  const multi = Bun.env.CLUSTER_NODES;
  if (multi && multi.trim().length > 0) {
    return multi.split(",").map((entry) => {
      const trimmed = entry.trim();
      const [host, portText] = trimmed.split(":");
      const port = Number(portText);
      if (!host || !Number.isFinite(port)) {
        throw new Error(`invalid CLUSTER_NODES entry: ${trimmed}`);
      }
      return { host, port };
    });
  }
  return [
    {
      host: Bun.env.CLUSTER_HOST ?? "127.0.0.1",
      port: Number(Bun.env.CLUSTER_PORT ?? 7000),
    },
  ];
}

if (import.meta.main) {
  const port = Number(Bun.env.PORT ?? 3000);
  const host = Bun.env.HOST ?? "0.0.0.0";
  const server = buildServer({
    clusterNodes: parseClusterNodes(),
  });

  const shutdown = async (signal: string) => {
    server.log.info({ signal }, "gateway shutting down");
    try {
      await server.close();
      process.exit(0);
    } catch (error) {
      server.log.error(error);
      process.exit(1);
    }
  };

  process.on("SIGINT", () => {
    void shutdown("SIGINT");
  });
  process.on("SIGTERM", () => {
    void shutdown("SIGTERM");
  });

  server.listen({ port, host }, (error, address) => {
    if (error) {
      server.log.error(error);
      process.exit(1);
    }

    server.log.info(`gateway listening at ${address}`);
  });
}
