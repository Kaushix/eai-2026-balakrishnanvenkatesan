"""
Router Service
==============

Splits incoming orders into individual item messages and routes each
item to the queue matching its item type.
"""

import json
import os

import pika


def get_rabbitmq_connection():
    """Create a connection to RabbitMQ using environment variable for host."""
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=os.environ.get("RABBITMQ_HOST", "localhost")
        )
    )


ROUTES = {
    "physical": "orders.physical",
    "digital": "orders.digital",
    "subscription": "orders.subscription",
}


def route_order(ch, method, properties, body):
    """
    Split one order into item messages and route each item by type.
    The original incoming message is acknowledged only after all items
    have been published.
    """
    order = json.loads(body)

    order_id = order["orderId"]
    correlation_id = order["correlationId"]

    print(f"[Router] Processing order {order_id}")

    connection = get_rabbitmq_connection()
    channel = connection.channel()

    for queue_name in ROUTES.values():
        channel.queue_declare(queue=queue_name, durable=True)

    items = order.get("items", [])
    item_count = len(items)

    try:
        for item_index, item in enumerate(items):
            item_message = {
                "orderId": order_id,
                "correlationId": correlation_id,
                "itemIndex": item_index,
                "totalItems": item_count,
                "item": item,
            }

            routing_key = ROUTES.get(item.get("type"))

            if routing_key is None:
                print(
                    f"[Router] Unknown item type {item.get('type')!r} "
                    f"for order {order_id}; skipping item {item_index}"
                )
                continue

            channel.basic_publish(
                exchange="",
                routing_key=routing_key,
                body=json.dumps(item_message),
                properties=pika.BasicProperties(delivery_mode=2),
            )

    finally:
        connection.close()

    ch.basic_ack(delivery_tag=method.delivery_tag)

    print(
        f"[Router] Order {order_id} split into "
        f"{item_count} items and routed"
    )


def main():
    """Main entry point: connect to RabbitMQ and start consuming orders."""
    connection = get_rabbitmq_connection()
    channel = connection.channel()

    channel.queue_declare(queue="orders.incoming", durable=True)

    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(
        queue="orders.incoming",
        on_message_callback=route_order,
    )

    print("[Router] Waiting for orders...")
    channel.start_consuming()


if __name__ == "__main__":
    main()
