import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import express, { type Request, type Response } from "express";
import axios, { AxiosError } from "axios";

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

export interface TraceItem {
  step: string;
  status: string;
  startedAt: string;
  finishedAt: string;
  durationMs: number;
}

export type SagaState = "completed" | "failed" | "compensated";
export type IdempotencyState =
  | "in_progress"
  | "completed"
  | "failed"
  | "compensated";

interface IdempotencyRecord {
  requestHash: string;
  state: IdempotencyState;
  httpStatus: number;
  response: Record<string, unknown>;
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

interface CheckoutItem {
  sku: string;
  quantity: number;
  price: number;
}

interface CheckoutPayload {
  orderId: string;
  customerId?: string;
  recipient: string;
  items: CheckoutItem[];
  amount: number;
}

interface DownstreamResult {
  ok: boolean;
  timedOut: boolean;
  status: number;
  data: Record<string, unknown>;
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

function readJsonFile<T>(filePath: string, initialData: T): T {
  ensureJsonFile(filePath, initialData);

  const raw = fs.readFileSync(filePath, "utf8");

  if (!raw.trim()) {
    return initialData;
  }

  return JSON.parse(raw) as T;
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

function validateCheckoutPayload(payload: unknown): string | null {
  if (!payload || typeof payload !== "object") {
    return "Request body must be a JSON object";
  }

  const body = payload as Record<string, unknown>;

  if (typeof body.orderId !== "string" || body.orderId.trim() === "") {
    return 'Field "orderId" is required and must be a non-empty string';
  }


  if (!Array.isArray(body.items) || body.items.length === 0) {
    return 'Field "items" is required and must be a non-empty array';
  }

  if (body.items.length > 50) {
    return 'Field "items" must contain at most 50 items';
  }

  for (const item of body.items) {
    if (!item || typeof item !== "object") {
      return "Each item must be an object";
    }

    const line = item as Record<string, unknown>;

    if (typeof line.sku !== "string" || line.sku.trim() === "") {
      return 'Each item requires a non-empty "sku"';
    }

    if (
      typeof line.quantity !== "number" ||
      !Number.isInteger(line.quantity) ||
      line.quantity <= 0
    ) {
      return 'Each item "quantity" must be a positive integer';
    }

    if (
      typeof line.price !== "number" ||
      !Number.isFinite(line.price) ||
      line.price < 0
    ) {
      return 'Each item "price" must be a non-negative number';
    }
  }

  if (typeof body.amount !== "number" || !Number.isFinite(body.amount)) {
    return 'Field "amount" is required and must be numeric';
  }

  if (body.amount < 0) {
    return 'Field "amount" must be non-negative';
  }

  if (typeof body.recipient !== "string" || body.recipient.trim() === "") {
    return 'Field "recipient" is required and must be a non-empty string';
  }

  return null;
}

function bootstrapStores(): void {
  ensureJsonFile(IDEMPOTENCY_STORE_PATH, { records: {} });
  ensureJsonFile(SAGA_STORE_PATH, { sagas: {} });
}

function updateSaga(
  orderId: string,
  idempotencyKey: string,
  state: SagaState,
  steps: TraceItem[],
): void {
  const sagaStore = readJsonFile<SagaStore>(SAGA_STORE_PATH, { sagas: {} });

  sagaStore.sagas[orderId] = {
    idempotencyKey,
    state,
    steps,
    updatedAt: nowIso(),
  };

  writeJsonFile(SAGA_STORE_PATH, sagaStore);
}

function updateIdempotency(
  key: string,
  requestHash: string,
  state: IdempotencyState,
  httpStatus: number,
  response: Record<string, unknown>,
): void {
  const store = readJsonFile<IdempotencyStore>(
    IDEMPOTENCY_STORE_PATH,
    { records: {} },
  );

  store.records[key] = {
    requestHash,
    state,
    httpStatus,
    response,
    updatedAt: nowIso(),
  };

  writeJsonFile(IDEMPOTENCY_STORE_PATH, store);
}

function addTrace(
  trace: TraceItem[],
  step: string,
  status: string,
  startedAt: string,
): TraceItem {
  const finishedAt = nowIso();

  const item: TraceItem = {
    step,
    status,
    startedAt,
    finishedAt,
    durationMs: Math.max(
      0,
      new Date(finishedAt).getTime() - new Date(startedAt).getTime(),
    ),
  };

  trace.push(item);

  return item;
}

async function callDownstream(
  url: string,
  body: Record<string, unknown>,
): Promise<DownstreamResult> {
  try {
    const response = await axios.post(url, body, {
      timeout: config.requestTimeoutMs,
      validateStatus: () => true,
    });

    return {
      ok: response.status >= 200 && response.status < 300,
      timedOut: false,
      status: response.status,
      data:
        response.data && typeof response.data === "object"
          ? (response.data as Record<string, unknown>)
          : {},
    };
  } catch (error: unknown) {
    if (
      error instanceof AxiosError &&
      (error.code === "ECONNABORTED" || error.code === "ETIMEDOUT")
    ) {
      return {
        ok: false,
        timedOut: true,
        status: 0,
        data: {},
      };
    }

    throw error;
  }
}

async function compensate(
  trace: TraceItem[],
  orderId: string,
  inventoryCompleted: boolean,
  paymentCompleted: boolean,
): Promise<boolean> {
  let compensationFailed = false;

  if (inventoryCompleted) {
    const startedAt = nowIso();

    try {
      const result = await callDownstream(
        `${config.inventoryUrl}/inventory/release`,
        { orderId },
      );

      addTrace(
        trace,
        "inventory_release",
        result.ok ? "success" : "failed",
        startedAt,
      );

      if (!result.ok) {
        compensationFailed = true;
      }
    } catch {
      addTrace(trace, "inventory_release", "failed", startedAt);
      compensationFailed = true;
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
        trace,
        "payment_refund",
        result.ok ? "success" : "failed",
        startedAt,
      );

      if (!result.ok) {
        compensationFailed = true;
      }
    } catch {
      addTrace(trace, "payment_refund", "failed", startedAt);
      compensationFailed = true;
    }
  }

  return !compensationFailed;
}

app.get("/health", (_req: Request, res: Response) => {
  res.status(200).json({ status: "ok" });
});

app.get("/debug/trace/:orderId", (req: Request, res: Response) => {
  const orderId = req.params.orderId ?? "";

  const sagaStore = readJsonFile<SagaStore>(
    SAGA_STORE_PATH,
    { sagas: {} },
  );

  const saga = sagaStore.sagas[orderId];

  if (!saga) {
    res.status(404).json({
      code: "not_found",
      message: "No saga found for this orderId",
    });
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

  const payload = req.body as CheckoutPayload;
  const requestHash = payloadHash(payload);

  const idempotencyStore = readJsonFile<IdempotencyStore>(
    IDEMPOTENCY_STORE_PATH,
    { records: {} },
  );

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

  const orderId = payload.orderId;

  updateIdempotency(
    idempotencyKey,
    requestHash,
    "in_progress",
    202,
    {
      orderId,
      status: "in_progress",
      trace: [],
    },
  );

  const trace: TraceItem[] = [];

  let paymentCompleted = false;
  let inventoryCompleted = false;

  try {
    // 1. Payment
    {
      const startedAt = nowIso();

      const result = await callDownstream(
        `${config.paymentUrl}/payment/authorize`,
        {
          orderId,
          amount: Number(payload.amount.toFixed(2)),
        },
      );

      addTrace(
        trace,
        "payment",
        result.timedOut ? "timeout" : result.ok ? "success" : "failed",
        startedAt,
      );

      if (!result.ok) {
        const response = {
          orderId,
          status: "failed",
          ...(result.timedOut ? { code: "timeout" } : {}),
          trace,
        };

        updateSaga(orderId, idempotencyKey, "failed", trace);

        updateIdempotency(
          idempotencyKey,
          requestHash,
          "failed",
          result.timedOut ? 504 : 422,
          response,
        );

        res.status(result.timedOut ? 504 : 422).json(response);
        return;
      }

      paymentCompleted = true;
    }

    // 2. Inventory
    {
      const startedAt = nowIso();

      const result = await callDownstream(
        `${config.inventoryUrl}/inventory/reserve`,
        {
          orderId,
          items: payload.items,
        },
      );

      addTrace(
        trace,
        "inventory",
        result.timedOut ? "timeout" : result.ok ? "success" : "failed",
        startedAt,
      );

      if (!result.ok) {
        const compensationOk = await compensate(
          trace,
          orderId,
          false,
          paymentCompleted,
        );

        const response = {
          orderId,
          status: compensationOk ? "compensated" : "failed",
          ...(compensationOk
            ? {}
            : { code: "compensation_failed" }),
          trace,
        };

        const httpStatus = compensationOk ? 422 : 422;

        updateSaga(
          orderId,
          idempotencyKey,
          compensationOk ? "compensated" : "failed",
          trace,
        );

        updateIdempotency(
          idempotencyKey,
          requestHash,
          compensationOk ? "compensated" : "failed",
          httpStatus,
          response,
        );

        res.status(httpStatus).json(response);
        return;
      }

      inventoryCompleted = true;
    }

    // 3. Shipping
    {
      const startedAt = nowIso();

      const result = await callDownstream(
        `${config.shippingUrl}/shipping/create`,
        {
          orderId,
        },
      );

      addTrace(
        trace,
        "shipping",
        result.timedOut ? "timeout" : result.ok ? "success" : "failed",
        startedAt,
      );

      if (!result.ok) {
        const compensationOk = await compensate(
          trace,
          orderId,
          inventoryCompleted,
          paymentCompleted,
        );

        const response = {
          orderId,
          status: compensationOk ? "compensated" : "failed",
          ...(result.timedOut && compensationOk
            ? { code: "timeout" }
            : !compensationOk
              ? { code: "compensation_failed" }
              : {}),
          trace,
        };

        const httpStatus = result.timedOut ? 504 : 422;

        updateSaga(
          orderId,
          idempotencyKey,
          compensationOk ? "compensated" : "failed",
          trace,
        );

        updateIdempotency(
          idempotencyKey,
          requestHash,
          compensationOk ? "compensated" : "failed",
          !compensationOk ? 422 : httpStatus,
          response,
        );

        res.status(!compensationOk ? 422 : httpStatus).json(response);
        return;
      }
    }

    // 4. Notification
    {
      const startedAt = nowIso();

      const result = await callDownstream(
        `${config.notificationUrl}/notification/send`,
        {
          orderId,
          recipient: payload.recipient,
        },
      );

      addTrace(
        trace,
        "notification",
        result.timedOut ? "timeout" : result.ok ? "success" : "failed",
        startedAt,
      );

      if (!result.ok) {
        const compensationOk = await compensate(
          trace,
          orderId,
          inventoryCompleted,
          paymentCompleted,
        );

        const response = {
          orderId,
          status: compensationOk ? "compensated" : "failed",
          ...(result.timedOut && compensationOk
            ? { code: "timeout" }
            : !compensationOk
              ? { code: "compensation_failed" }
              : {}),
          trace,
        };

        const httpStatus = result.timedOut ? 504 : 422;

        updateSaga(
          orderId,
          idempotencyKey,
          compensationOk ? "compensated" : "failed",
          trace,
        );

        updateIdempotency(
          idempotencyKey,
          requestHash,
          compensationOk ? "compensated" : "failed",
          !compensationOk ? 422 : httpStatus,
          response,
        );

        res.status(!compensationOk ? 422 : httpStatus).json(response);
        return;
      }
    }

    const response = {
      orderId,
      status: "completed",
      trace,
    };

    updateSaga(orderId, idempotencyKey, "completed", trace);

    updateIdempotency(
      idempotencyKey,
      requestHash,
      "completed",
      200,
      response,
    );

    res.status(200).json(response);
  } catch (error: unknown) {
    console.error("[orchestrator] unexpected error", error);

    const response = {
      orderId,
      status: "failed",
      code: "internal_error",
      trace,
    };

    updateSaga(orderId, idempotencyKey, "failed", trace);

    updateIdempotency(
      idempotencyKey,
      requestHash,
      "failed",
      422,
      response,
    );

    res.status(422).json(response);
  }
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
