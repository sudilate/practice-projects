import Fastify from "fastify";
import { z } from "zod";
import { ClusterClient, ClusterError } from "./cluster-client";

const taskRequestSchema = z.object({
  type: z.enum(["uppercase", "echo", "reverse"]),
  payload: z.string().max(1024),
});

type TaskRequestBody = z.infer<typeof taskRequestSchema>;

export interface ServerOptions {
  clusterNodes?: { host: string; port: number }[];
}

export function buildServer(options: ServerOptions = {}) {
  const server = Fastify({ logger: true });
  const nodes = options.clusterNodes ?? [{ host: "127.0.0.1", port: 7000 }];
  const cluster = new ClusterClient(nodes);

  server.addHook("onClose", async () => {
    cluster.close();
  });

  server.get("/health", async () => ({ ok: true }));

  server.post<{ Body: unknown }>("/tasks", async (request, reply) => {
    const parse = taskRequestSchema.safeParse(request.body);
    if (!parse.success) {
      return reply.code(400).send({ error: parse.error.issues.map((e) => e.message).join("; ") });
    }
    try {
      const result = await cluster.submitTask(parse.data);
      return reply.code(202).send(result);
    } catch (error) {
      return reply.code(clusterErrorCode(error)).send(clusterErrorBody(error));
    }
  });

  server.get<{
    Params: { taskId: string };
  }>("/tasks/:taskId", async (request, reply) => {
    try {
      const status = await cluster.getTaskStatus(request.params.taskId);
      return reply.code(200).send({
        taskId: request.params.taskId,
        status: statusCodeToString(status.status),
        output: status.output || null,
        error: status.error || null,
      });
    } catch (error) {
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

if (import.meta.main) {
  const port = Number(Bun.env.PORT ?? 3000);
  const clusterHost = Bun.env.CLUSTER_HOST ?? "127.0.0.1";
  const clusterPort = Number(Bun.env.CLUSTER_PORT ?? 7000);
  const server = buildServer({
    clusterNodes: [{ host: clusterHost, port: clusterPort }],
  });

  server.listen({ port, host: "0.0.0.0" }, (error, address) => {
    if (error) {
      server.log.error(error);
      process.exit(1);
    }

    server.log.info(`gateway listening at ${address}`);
  });
}
