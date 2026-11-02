/**
 * PA6 orchestrator scaffold — ported from the practice-03-orchestration JS lab.
 *
 * This scaffold is intentionally incomplete. `GET /health`, request validation
 * against the canonical order schema, the Idempotency-Key contract shell, and
 * restart-safe file persistence are already wired up. What is NOT implemented
 * is the actual saga:
 *
 *   1) payment authorize
 *   2) inventory reserve
 *   3) shipping create
 *   4) notification send
 *
 * with strict sequencing, trace recording, timeout handling, compensation
 * (in reverse order of completed steps), and the full idempotent-replay
 * policy. See the TODO block inside the POST /checkout handler.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import express, { type Request, type Response } from "express";
import Ajv from "ajv";
import addFormats from "ajv-formats";

const app = express();
app.use(express.json());
app.use(express.static(path.join(process.cwd(), "public")));

interface Config {
  port: number;
  paymentUrl: string;
  inventoryUrl: string;
  shippingUrl: string;
  notificationUrl: string;
  requestTimeoutMs: number;
}

function readRequiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function loadConfig(): Config {
  return {
    port: Number(process.env.ORCHESTRATOR_PORT || 3000),
    paymentUrl: readRequiredEnv("PAYMENT_URL"),
    inventoryUrl: readRequiredEnv("INVENTORY_URL"),
    shippingUrl: readRequiredEnv("SHIPPING_URL"),
    notificationUrl: readRequiredEnv("NOTIFICATION_URL"),
    requestTimeoutMs: Number(process.env.REQUEST_TIMEOUT_MS || 2500),
  };
}

const config = loadConfig();

const DATA_DIR = "/data";
const IDEMPOTENCY_STORE_PATH = path.join(DATA_DIR, "idempotency-store.json");
const SAGA_STORE_PATH = path.join(DATA_DIR, "saga-store.json");

// ---------------------------------------------------------------- contract --
// These shapes mirror grading/schema/*.json. Keep them in sync if you change
// either side — the tests validate persisted files against those schemas.

export interface TraceItem {
  step: string;
  status: string;
  startedAt: string;
  finishedAt: string;
  durationMs: number;
}

export type SagaState = "completed" | "failed" | "compensated";
export type IdempotencyState = "in_progress" | "completed" | "failed" | "compensated";

interface IdempotencyRecord {
  requestHash: string;
  state: IdempotencyState;
  httpStatus: number;
  response: unknown;
  updatedAt: string;
}

interface IdempotencyStore {
  records: Record<string, IdempotencyRecord>;
}

interface SagaRecord {
  idempotencyKey: string;
  state: SagaState;
  steps: TraceItem[];
  updatedAt: string;
}

interface SagaStore {
  sagas: Record<string, SagaRecord>;
}

function ensureJsonFile(filePath: string, initialData: unknown): void {
  const dirPath = path.dirname(filePath);
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(filePath, JSON.stringify(initialData, null, 2), "utf8");
  }
}

function readJsonFile<T>(filePath: string): T {
  ensureJsonFile(filePath, {});
  const raw = fs.readFileSync(filePath, "utf8");
  return JSON.parse(raw || "{}") as T;
}

function writeJsonFile(filePath: string, value: unknown): void {
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2), "utf8");
}

function nowIso(): string {
  return new Date().toISOString();
}

function payloadHash(payload: unknown): string {
  const normalized = JSON.stringify(payload);
  const hash = crypto.createHash("sha256").update(normalized).digest("hex");
  return `sha256:${hash}`;
}

// The request body follows the CheckoutRequest contract in openapi.yaml.
function validateCheckoutPayload(payload: unknown): string | null {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return "Request body must be an object";
  }

  const body = payload as Record<string, unknown>;

  if (typeof body.orderId !== "string" || body.orderId.length < 1) {
    return "orderId is required";
  }

  if (typeof body.recipient !== "string" || body.recipient.length < 1) {
    return "recipient is required";
  }

  if (!Array.isArray(body.items) || body.items.length < 1 || body.items.length > 50) {
    return "items must contain between 1 and 50 entries";
  }

  for (const item of body.items) {
    if (!item || typeof item !== "object" || Array.isArray(item)) {
      return "each item must be an object";
    }

    const entry = item as Record<string, unknown>;

    if (typeof entry.sku !== "string" || entry.sku.length < 1) {
      return "each item requires sku";
    }

    if (
      typeof entry.quantity !== "number" ||
      !Number.isInteger(entry.quantity) ||
      entry.quantity < 1
    ) {
      return "each item requires a positive integer quantity";
    }

    if (
      typeof entry.price !== "number" ||
      !Number.isFinite(entry.price) ||
      entry.price < 0
    ) {
      return "each item requires a non-negative numeric price";
    }
  }

  if (
    typeof body.amount !== "number" ||
    !Number.isFinite(body.amount) ||
    body.amount < 0
  ) {
    return "amount must be a non-negative number";
  }

  return null;
}

function bootstrapStores(): void {
  ensureJsonFile(IDEMPOTENCY_STORE_PATH, { records: {} });
  ensureJsonFile(SAGA_STORE_PATH, { sagas: {} });
}

app.get("/health", (_req: Request, res: Response) => {
  res.status(200).json({ status: "ok" });
});

app.get("/debug/trace/:orderId", (req: Request, res: Response) => {
  const orderIdParam = req.params.orderId ?? "";
  const sagaStore = readJsonFile<SagaStore>(SAGA_STORE_PATH);
  const saga = sagaStore?.sagas?.[orderIdParam];
  if (!saga) {
    res.status(404).json({ code: "not_found", message: "No saga found for this orderId" });
    return;
  }
  res.status(200).json(saga);
});

app.post("/checkout", async (req: Request, res: Response) => {
  const idempotencyKey = req.header("Idempotency-Key");
  if (!idempotencyKey) {
    res.status(400).json({
      code: "validation_error",
      message: "Idempotency-Key header is required",
    });
    return;
  }

  const validationError = validateCheckoutPayload(req.body);
  if (validationError) {
    res.status(400).json({
      code: "validation_error",
      message: validationError,
    });
    return;
  }

  const requestHash = payloadHash(req.body);
  const idempotencyStore = readJsonFile<IdempotencyStore>(IDEMPOTENCY_STORE_PATH);
  if (!idempotencyStore.records) {
    idempotencyStore.records = {};
  }

  const existing = idempotencyStore.records[idempotencyKey];
  if (existing) {
    if (existing.requestHash !== requestHash) {
      res.status(409).json({
        code: "idempotency_payload_mismatch",
        message: "This Idempotency-Key is already used for a different payload",
      });
      return;
    }

    if (existing.state === "in_progress") {
      res.status(409).json({
        code: "idempotency_conflict",
        message: "A request with this Idempotency-Key is already in progress",
      });
      return;
    }

    res.status(existing.httpStatus).json(existing.response);
    return;
  }

  const order = req.body as {
    orderId: string;
    recipient: string;
    items: Array<{ sku: string; quantity: number; price: number }>;
    amount: number;
  };
  const orderId = order.orderId;
  idempotencyStore.records[idempotencyKey] = {
    requestHash,
    state: "in_progress",
    httpStatus: 202,
    response: {
      orderId,
      status: "in_progress",
    },
    updatedAt: nowIso(),
  };
  writeJsonFile(IDEMPOTENCY_STORE_PATH, idempotencyStore);

  // --------------------------------------------------------------------------
  // TODO (student): Implement full orchestration flow. README.md §4 has the
  // exact request bodies, trace step names and the outcome table.
  //   1) payment authorize   { orderId, amount }
  //   2) inventory reserve   { orderId, items }
  //   3) shipping create     { orderId }
  //   4) notification send   { orderId, recipient }
  // with strict sequencing, trace recording, timeout handling, compensation
  // (in reverse order of the steps that actually completed), idempotent
  // replay policy, and restart-safe persistence updates.
  //
  // The canonical order carries neither an amount nor a recipient:
  //   - amount    = the sum of unitPrice × quantity over order.items, sent as
  //                 a decimal string with two fraction digits ("49.50").
  //                 unitPrice is already a string: add up whole cents as
  //                 integers, then format once. Never go through a float.
  //   - recipient = order.customer.email
  //
  // config.paymentUrl / config.inventoryUrl / config.shippingUrl /
  // config.notificationUrl / config.requestTimeoutMs are already loaded from
  // the environment above — use them, do not hardcode downstream URLs.
  // --------------------------------------------------------------------------

  const trace: TraceItem[] = [];

  type DownstreamResult = {
    ok: boolean;
    timedOut: boolean;
    status: number;
    data: Record<string, unknown>;
  };

  const callDownstream = async (
    url: string,
    body: Record<string, unknown>,
  ): Promise<DownstreamResult> => {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), config.requestTimeoutMs);

    try {
      const response = await fetch(url, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
        signal: controller.signal,
      });

      let data: Record<string, unknown> = {};
      try {
        const parsed: unknown = await response.json();
        if (parsed && typeof parsed === "object") {
          data = parsed as Record<string, unknown>;
        }
      } catch {
        // Empty/non-JSON responses are represented by {}.
      }

      return {
        ok: response.status >= 200 && response.status < 300,
        timedOut: false,
        status: response.status,
        data,
      };
    } catch (error: unknown) {
      if (error instanceof Error && error.name === "AbortError") {
        return {
          ok: false,
          timedOut: true,
          status: 0,
          data: {},
        };
      }
      throw error;
    } finally {
      clearTimeout(timer);
    }
  };

  const addTrace = (
    step: string,
    status: string,
    startedAt: string,
  ): void => {
    const finishedAt = nowIso();
    trace.push({
      step,
      status,
      startedAt,
      finishedAt,
      durationMs: Math.max(
        0,
        new Date(finishedAt).getTime() - new Date(startedAt).getTime(),
      ),
    });
  };

  const amountInCents = order.items.reduce((total, item) => {
    const cents = Math.round(item.price * 100);
    return total + cents * item.quantity;
  }, 0);

  const amount = amountInCents / 100;
  const recipient = order.recipient;

  let paymentCompleted = false;
  let inventoryCompleted = false;

  const compensate = async (): Promise<boolean> => {
    let successful = true;

    if (inventoryCompleted) {
      const startedAt = nowIso();
      try {
        const result = await callDownstream(
          `${config.inventoryUrl}/inventory/release`,
          { orderId },
        );
        addTrace(
          "inventory_release",
          result.ok ? "success" : "failed",
          startedAt,
        );
        if (!result.ok) {
          successful = false;
        }
      } catch {
        addTrace("inventory_release", "failed", startedAt);
        successful = false;
      }
    }

    if (paymentCompleted) {
      const startedAt = nowIso();
      try {
        const result = await callDownstream(
          `${config.paymentUrl}/payment/refund`,
          { orderId },
        );
        addTrace(
          "payment_refund",
          result.ok ? "success" : "failed",
          startedAt,
        );
        if (!result.ok) {
          successful = false;
        }
      } catch {
        addTrace("payment_refund", "failed", startedAt);
        successful = false;
      }
    }

    return successful;
  };

  const persistSaga = (state: SagaState): void => {
    const sagaStore = readJsonFile<SagaStore>(SAGA_STORE_PATH);
    if (!sagaStore.sagas) {
      sagaStore.sagas = {};
    }

    sagaStore.sagas[orderId] = {
      idempotencyKey,
      state,
      steps: trace,
      updatedAt: nowIso(),
    };

    writeJsonFile(SAGA_STORE_PATH, sagaStore);
  };

  const finish = (
    state: IdempotencyState,
    httpStatus: number,
    response: Record<string, unknown>,
  ): void => {
    persistSaga(
      state === "completed"
        ? "completed"
        : state === "compensated"
          ? "compensated"
          : "failed",
    );

    idempotencyStore.records[idempotencyKey] = {
      requestHash,
      state,
      httpStatus,
      response,
      updatedAt: nowIso(),
    };

    writeJsonFile(IDEMPOTENCY_STORE_PATH, idempotencyStore);
  };

  // 1) Payment
  {
    const startedAt = nowIso();
    try {
      const result = await callDownstream(
        `${config.paymentUrl}/payment/authorize`,
        { orderId, amount },
      );

      addTrace(
        "payment_authorize",
        result.ok ? "success" : result.timedOut ? "timeout" : "failed",
        startedAt,
      );

      if (result.timedOut) {
        const response = {
          orderId,
          status: "failed",
          code: "timeout",
          trace,
        };
        finish("failed", 504, response);
        res.status(504).json(response);
        return;
      }

      if (!result.ok) {
        const response = {
          orderId,
          status: "failed",
          code: result.data.code || "payment_failed",
          trace,
        };
        finish("failed", 422, response);
        res.status(422).json(response);
        return;
      }

      paymentCompleted = true;
    } catch {
      addTrace("payment_authorize", "failed", startedAt);
      const response = {
        orderId,
        status: "failed",
        code: "payment_failed",
        trace,
      };
      finish("failed", 422, response);
      res.status(422).json(response);
      return;
    }
  }

  // 2) Inventory
  {
    const startedAt = nowIso();
    try {
      const result = await callDownstream(
        `${config.inventoryUrl}/inventory/reserve`,
        { orderId, items: order.items },
      );

      addTrace(
        "inventory_reserve",
        result.ok ? "success" : result.timedOut ? "timeout" : "failed",
        startedAt,
      );

      if (!result.ok) {
        const compensationSucceeded = await compensate();
        const httpStatus = result.timedOut ? 504 : 422;
        const response = {
          orderId,
          status: compensationSucceeded ? "compensated" : "failed",
          code: compensationSucceeded
            ? result.timedOut
              ? "timeout"
              : result.data.code || "inventory_failed"
            : "compensation_failed",
          trace,
        };

        finish(
          compensationSucceeded ? "compensated" : "failed",
          httpStatus,
          response,
        );
        res.status(httpStatus).json(response);
        return;
      }

      inventoryCompleted = true;
    } catch {
      addTrace("inventory_reserve", "failed", startedAt);
      const compensationSucceeded = await compensate();
      const response = {
        orderId,
        status: compensationSucceeded ? "compensated" : "failed",
        code: compensationSucceeded ? "inventory_failed" : "compensation_failed",
        trace,
      };
      finish(
        compensationSucceeded ? "compensated" : "failed",
        422,
        response,
      );
      res.status(422).json(response);
      return;
    }
  }

  // 3) Shipping
  {
    const startedAt = nowIso();
    try {
      const result = await callDownstream(
        `${config.shippingUrl}/shipping/create`,
        { orderId },
      );

      addTrace(
        "shipping_create",
        result.ok ? "success" : result.timedOut ? "timeout" : "failed",
        startedAt,
      );

      if (!result.ok) {
        const compensationSucceeded = await compensate();
        const httpStatus = result.timedOut ? 504 : 422;
        const response = {
          orderId,
          status: compensationSucceeded ? "compensated" : "failed",
          code: compensationSucceeded
            ? result.timedOut
              ? "timeout"
              : result.data.code || "shipping_failed"
            : "compensation_failed",
          trace,
        };

        finish(
          compensationSucceeded ? "compensated" : "failed",
          httpStatus,
          response,
        );
        res.status(httpStatus).json(response);
        return;
      }
    } catch {
      addTrace("shipping_create", "failed", startedAt);
      const compensationSucceeded = await compensate();
      const response = {
        orderId,
        status: compensationSucceeded ? "compensated" : "failed",
        code: compensationSucceeded ? "shipping_failed" : "compensation_failed",
        trace,
      };
      finish(
        compensationSucceeded ? "compensated" : "failed",
        422,
        response,
      );
      res.status(422).json(response);
      return;
    }
  }

  // 4) Notification
  {
    const startedAt = nowIso();
    try {
      const result = await callDownstream(
        `${config.notificationUrl}/notification/send`,
        { orderId, recipient },
      );

      addTrace(
        "notification_send",
        result.ok ? "success" : result.timedOut ? "timeout" : "failed",
        startedAt,
      );

      if (!result.ok) {
        const compensationSucceeded = await compensate();
        const httpStatus = result.timedOut ? 504 : 422;
        const response = {
          orderId,
          status: compensationSucceeded ? "compensated" : "failed",
          code: compensationSucceeded
            ? result.timedOut
              ? "timeout"
              : result.data.code || "notification_failed"
            : "compensation_failed",
          trace,
        };

        finish(
          compensationSucceeded ? "compensated" : "failed",
          httpStatus,
          response,
        );
        res.status(httpStatus).json(response);
        return;
      }
    } catch {
      addTrace("notification_send", "failed", startedAt);
      const compensationSucceeded = await compensate();
      const response = {
        orderId,
        status: compensationSucceeded ? "compensated" : "failed",
        code: compensationSucceeded
          ? "notification_failed"
          : "compensation_failed",
        trace,
      };
      finish(
        compensationSucceeded ? "compensated" : "failed",
        422,
        response,
      );
      res.status(422).json(response);
      return;
    }
  }

  const response = {
    orderId,
    status: "completed",
    trace,
  };

  finish("completed", 200, response);
  res.status(200).json(response);
});

bootstrapStores();

app.listen(config.port, () => {
  console.log(`[orchestrator] listening on port ${config.port}`);
  console.log("[orchestrator] downstream targets loaded from env", {
    paymentUrl: config.paymentUrl,
    inventoryUrl: config.inventoryUrl,
    shippingUrl: config.shippingUrl,
    notificationUrl: config.notificationUrl,
    requestTimeoutMs: config.requestTimeoutMs,
  });
});
