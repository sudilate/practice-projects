import Fastify from "fastify";
import { ClusterClient } from "./cluster-client";

interface TaskRequestBody {
  type: string;
  payload: unknown;
}

export function buildServer() {
  const server = Fastify({ logger: true });
  const cluster = new ClusterClient([{ host: "127.0.0.1", port: 7000 }]);

  server.get("/health", async () => ({ ok: true }));

  server.post<{ Body: TaskRequestBody }>("/tasks", async (request, reply) => {
    const result = await cluster.submitTask(request.body);
    return reply.code(200).send(result);
  });

  return server;
}

if (import.meta.main) {
  const port = Number(Bun.env.PORT ?? 3000);
  const server = buildServer();

  server.listen({ port, host: "0.0.0.0" }, (error, address) => {
    if (error) {
      server.log.error(error);
      process.exit(1);
    }

    server.log.info(`gateway listening at ${address}`);
  });
}
