import crypto from "node:crypto";
import express from "express";

import { connectWithRetry } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
const ORDERS_EXCHANGE = "orders.exchange";

const orders = new Map<string, CanonicalOrder>();

async function main(): Promise<void> {
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);
  const app = express();

  app.use(express.json());

  app.post("/orders", (req, res) => {
    const order = req.body as CanonicalOrder;
    const correlationId = crypto.randomUUID();

    channel.publish(
      ORDERS_EXCHANGE,
      "",
      Buffer.from(JSON.stringify(order)),
      {
        headers: { correlationId },
        contentType: "application/json",
      },
    );

    orders.set(correlationId, order);

    res.status(201).json({
      correlationId,
      status: "accepted",
    });
  });

  app.get("/orders/:correlationId", (req, res) => {
    const { correlationId } = req.params;
    const order = orders.get(correlationId);

    if (!order) {
      res.status(404).json({
        correlationId,
        error: "Order not found",
      });
      return;
    }

    res.status(200).json({
      correlationId,
      order,
    });
  });

  app.get("/health", (_req, res) => {
    res.status(200).json({ status: "ok" });
  });

  app.listen(PORT, () => {
    console.log(`[order-service] listening on ${PORT}`);
  });
}

main().catch((error) => {
  console.error("[order-service] failed to start", error);
  process.exit(1);
});
