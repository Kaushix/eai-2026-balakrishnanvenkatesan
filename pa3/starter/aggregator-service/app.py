"""
Aggregator Service
==================

Collects item results from orders.results and publishes exactly one
completion message per order.
"""

import json
import os
import threading
import time

import pika


lock = threading.Lock()

# orderId -> {
#     "correlationId": "...",
#     "totalItems": 3,
#     "results": {0: result_message, 1: result_message, ...},
#     "lastActivity": monotonic_timestamp,
# }
in_flight = {}

# Once an order has produced its one final completion message, remember it
# so a late duplicate result cannot create a second completion message.
completed_orders = set()

IDLE_TIMEOUT_SECONDS = float(
    os.environ.get("AGGREGATOR_IDLE_TIMEOUT_SECONDS", "5")
)

SWEEP_INTERVAL_SECONDS = 1.0


def get_rabbitmq_connection():
    """Create a connection to RabbitMQ using environment variable for host."""
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=os.environ.get("RABBITMQ_HOST", "localhost")
        )
    )


def publish_completion(message):
    """Publish one completion message to orders.complete."""
    connection = get_rabbitmq_connection()
    channel = connection.channel()

    channel.queue_declare(queue="orders.complete", durable=True)

    channel.basic_publish(
        exchange="",
        routing_key="orders.complete",
        body=json.dumps(message),
        properties=pika.BasicProperties(delivery_mode=2),
    )

    connection.close()


def aggregate_result(ch, method, properties, body):
    """Collect one item result and complete the order when all items arrive."""
    try:
        result = json.loads(body)

        order_id = result["orderId"]
        correlation_id = result["correlationId"]
        item_index = result["itemIndex"]
        total_items = result["totalItems"]

        now = time.monotonic()
        completion_message = None

        with lock:
            # Ignore late results for an order that already has its one
            # final completion message.
            if order_id in completed_orders:
                ch.basic_ack(delivery_tag=method.delivery_tag)
                return

            state = in_flight.get(order_id)

            if state is None:
                state = {
                    "correlationId": correlation_id,
                    "totalItems": total_items,
                    "results": {},
                    "lastActivity": now,
                }
                in_flight[order_id] = state

            # A result arriving for an existing order resets the idle timer,
            # including a duplicate result.
            state["lastActivity"] = now

            # Keep the first result received for an itemIndex.
            # This prevents duplicate deliveries from being counted twice.
            if item_index not in state["results"]:
                state["results"][item_index] = result

            received_items = len(state["results"])

            if received_items == state["totalItems"]:
                completion_message = {
                    "orderId": order_id,
                    "correlationId": state["correlationId"],
                    "status": "complete",
                    "totalItems": state["totalItems"],
                    "receivedItems": received_items,
                    "itemResults": list(state["results"].values()),
                    "missingItemIndexes": [],
                }

                del in_flight[order_id]
                completed_orders.add(order_id)

        # Publish outside the lock so RabbitMQ I/O does not block other orders.
        if completion_message is not None:
            publish_completion(completion_message)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        print(f"[Aggregator] Invalid result message: {exc}")
        ch.basic_ack(delivery_tag=method.delivery_tag)


def sweep_timeouts():
    """Publish partial results for orders that have been idle too long."""
    while True:
        time.sleep(SWEEP_INTERVAL_SECONDS)

        now = time.monotonic()
        timed_out = []

        with lock:
            for order_id, state in list(in_flight.items()):
                idle_time = now - state["lastActivity"]

                if idle_time >= IDLE_TIMEOUT_SECONDS:
                    received_indexes = set(state["results"].keys())

                    missing_indexes = [
                        index
                        for index in range(state["totalItems"])
                        if index not in received_indexes
                    ]

                    completion_message = {
                        "orderId": order_id,
                        "correlationId": state["correlationId"],
                        "status": "partial",
                        "totalItems": state["totalItems"],
                        "receivedItems": len(state["results"]),
                        "itemResults": list(state["results"].values()),
                        "missingItemIndexes": missing_indexes,
                    }

                    timed_out.append(completion_message)

                    del in_flight[order_id]
                    completed_orders.add(order_id)

        # Publish after releasing the lock.
        for completion_message in timed_out:
            try:
                publish_completion(completion_message)
            except Exception as exc:
                print(f"[Aggregator] Failed to publish timeout result: {exc}")


def main():
    """Main entry point: consume result messages and run timeout sweeper."""
    connection = get_rabbitmq_connection()
    channel = connection.channel()

    channel.queue_declare(queue="orders.results", durable=True)
    channel.queue_declare(queue="orders.complete", durable=True)

    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(
        queue="orders.results",
        on_message_callback=aggregate_result,
    )

    sweeper = threading.Thread(
        target=sweep_timeouts,
        daemon=True,
    )
    sweeper.start()

    print("[Aggregator] Waiting for results...")
    channel.start_consuming()


if __name__ == "__main__":
    main()
