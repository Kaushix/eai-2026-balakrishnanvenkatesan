import { appendFile, readFile, writeFile } from "node:fs/promises";
import type { ConsumeMessage } from "amqplib";

import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "notifications.queue";
const RESULTS_EXCHANGE = "results.notification";
const MAX_RETRIES = Number(process.env.MAX_RETRIES) || 3;
const PROCESSED_IDS_FILE = "/data/processed-ids.json";
const NOTIFICATION_LOG = "/data/notification.log";

async function loadProcessedIds(): Promise<string[]> {
  try {
    const content = await readFile(PROCESSED_IDS_FILE, "utf8");
    return JSON.parse(content) as string[];
  } catch {
    await writeFile(PROCESSED_IDS_FILE, "[]\n", "utf8");
    return [];
  }
}

async function saveProcessedIds(processedIds: string[]): Promise<void> {
  await writeFile(
    PROCESSED_IDS_FILE,
    `${JSON.stringify(processedIds, null, 2)}\n`,
    "utf8",
  );
}

async function main(): Promise<void> {
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);

  await channel.prefetch(1);

  const processedIds = await loadProcessedIds();

  console.log(`[Notification] Consuming from ${QUEUE}`);

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const correlationId =
      msg.properties.headers?.["correlationId"] as string | undefined;
    const retryCount = getRetryCount(msg);

    console.log(
      `[Notification] Processing ${correlationId} (attempt ${retryCount + 1})`,
    );

    try {
      if (!correlationId) {
        throw new Error("Missing correlationId");
      }

      if (processedIds.includes(correlationId)) {
        channel.ack(msg);
        console.log(`[Notification] duplicate skipped: ${correlationId}`);
        return;
      }

      const order = JSON.parse(msg.content.toString()) as CanonicalOrder;

      const notification = {
        correlationId,
        orderId: order.orderId,
        customerEmail: order.customer.email,
        timestamp: new Date().toISOString(),
        message: "Order received",
      };

      await appendFile(
        NOTIFICATION_LOG,
        `${JSON.stringify(notification)}\n`,
        "utf8",
      );

      processedIds.push(correlationId);
      await saveProcessedIds(processedIds);

      channel.ack(msg);

      channel.publish(
        RESULTS_EXCHANGE,
        "",
        Buffer.from(
          JSON.stringify({
            correlationId,
            source: "notification",
            status: "success",
            timestamp: new Date().toISOString(),
            details: {
              message: "Notification processed successfully",
            },
          }),
        ),
        {
          headers: { correlationId },
          contentType: "application/json",
        },
      );
    } catch (err) {
      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(process.env.DLQ_EXCHANGE!, "", msg.content, {
          headers: msg.properties.headers,
        });

        channel.ack(msg);

        console.log(
          `[Notification] -> DLQ after ${retryCount + 1} attempts: ${(err as Error).message}`,
        );
      } else {
        channel.nack(msg, false, false);

        console.log(
          `[Notification] -> Retry (attempt ${retryCount + 1}): ${(err as Error).message}`,
        );
      }
    }
  });
}

main().catch(console.error);
