import argparse
import json
import os
import random
import time
import uuid

from kafka import KafkaProducer


EVENT_TYPES = ["click", "add_to_cart", "purchase", "abandon"]
CATEGORIES = ["electronics", "fashion", "home", "beauty", "sports", "toys"]
COUNTRIES = ["FR", "DE", "ES", "US", "UK", "MA"]
DEVICES = ["mobile", "desktop", "tablet"]


def generate_event():
    event_type = random.choices(
        EVENT_TYPES,
        weights=[0.6, 0.2, 0.12, 0.08],
        k=1,
    )[0]

    user_id = f"user-{random.randint(1, 500)}"
    product_id = f"SKU-{random.randint(1000, 9999)}"
    session_id = f"sess-{random.randint(1, 10000)}"

    price = round(random.uniform(5, 200), 2)
    quantity = random.randint(1, 4)

    # Inject occasional anomalies to make them visible in streaming.
    if random.random() < 0.05:
        user_id = "user-burst"
        event_type = "click"

    if event_type == "purchase" and random.random() < 0.05:
        price = round(random.uniform(600, 2000), 2)

    if random.random() < 0.05:
        event_type = "abandon"

    if event_type in ("click", "abandon"):
        quantity = 1

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "user_id": user_id,
        "product_id": product_id,
        "category": random.choice(CATEGORIES),
        "price": price,
        "quantity": quantity,
        "session_id": session_id,
        "device": random.choice(DEVICES),
        "country": random.choice(COUNTRIES),
        "ts_ms": int(time.time() * 1000),
    }
    return event


def build_producer(bootstrap_servers):
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        linger_ms=50,
        acks=1,
    )


def parse_args():
    parser = argparse.ArgumentParser(
        description="Kafka producer for ecommerce events."
    )
    parser.add_argument(
        "--bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers.",
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC", "ecommerce_events"),
        help="Kafka topic name.",
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=10.0,
        help="Events per second to generate.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    producer = build_producer(args.bootstrap)
    delay = 1.0 / args.rate if args.rate > 0 else 0

    print(
        f"Producing to {args.topic} at {args.bootstrap} "
        f"({args.rate} events/sec). Ctrl+C to stop."
    )

    try:
        while True:
            event = generate_event()
            producer.send(args.topic, event)
            if delay > 0:
                time.sleep(delay)
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
