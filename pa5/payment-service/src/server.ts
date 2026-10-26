/**
 * PA5 payment-service — the reference consumer. Connection, consume setup,
 * and retry/DLQ logic are given below and work as-is; only the TODO in the
 * `try` block is yours. Study this file, then replicate the pattern in
 * inventory-service and notification-service.
 */

import type { ConsumeMessage } from "amqplib";

import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "payments.queue";
const RESULTS_EXCHANGE = "results.payment";
const MAX_RETRIES = Number(process.env.MAX_RETRIES) || 3;
const FAIL_RATE = Number(process.env.PAYMENT_FAIL_RATE) || 20;

async function main(): Promise<void> {
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);

  await channel.prefetch(1);

  console.log(`[Payment] Consuming from ${QUEUE}, fail rate: ${FAIL_RATE}%`);

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const order = JSON.parse(msg.content.toString()) as CanonicalOrder;
    const correlationId =
      msg.properties.headers?.["correlationId"] as string | undefined;
    const retryCount = getRetryCount(msg);

    console.log(
      `[Payment] Processing ${correlationId} (attempt ${retryCount + 1})`,
    );

    try {
      const random = Math.random() * 100;

      if (random < FAIL_RATE) {
        throw new Error("Payment validation failed");
      }

      channel.ack(msg);

      channel.publish(
        RESULTS_EXCHANGE,
        "",
        Buffer.from(
          JSON.stringify({
            correlationId,
            source: "payment",
            status: "success",
            timestamp: new Date().toISOString(),
            details: {
              message: "Payment validated successfully",
            },
          }),
        ),
        {
          headers: { correlationId },
          contentType: "application/json",
        },
      );
    } catch (err) {
      // Retry / DLQ logic (provided as reference — study this for the other
      // two services). Do not modify.
      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(process.env.DLQ_EXCHANGE!, "", msg.content, {
          headers: msg.properties.headers,
        });
        channel.ack(msg);
        console.log(
          `[Payment] -> DLQ after ${retryCount + 1} attempts: ${(err as Error).message}`,
        );
      } else {
        channel.nack(msg, false, false);
        console.log(
          `[Payment] -> Retry (attempt ${retryCount + 1}): ${(err as Error).message}`,
        );
      }
    }
  });
}

main().catch(console.error);
