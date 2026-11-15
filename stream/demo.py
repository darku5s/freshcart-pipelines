%%writefile stream/demo.py

import json
import random
import threading
import time
import uuid
from datetime import datetime

import pika

EXCHANGE = "rapido-dispatch"


# ---------------- Dispatcher ---------------- #

def dispatcher():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters("localhost")
    )
    channel = connection.channel()

    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type="topic"
    )

    cities = ["bangalore", "chennai"]

    zones = {
        "bangalore": ["koramangala", "indiranagar", "whitefield"],
        "chennai": ["tnagar", "whitefield"]
    }

    types = ["food", "ride"]

    print("\nDispatcher started...\n")

    counter = 1

    while True:

        city = random.choice(cities)
        zone = random.choice(zones[city])
        req_type = random.choice(types)

        routing_key = f"{city}.{zone}.{req_type}"

        message = {
            "id": str(uuid.uuid4())[:8],
            "city": city,
            "type": req_type,
            "ts": datetime.now().strftime("%H:%M:%S")
        }

        # Every 10th message is bad (missing zone)
        if counter % 10 != 0:
            message["zone"] = zone

        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=routing_key,
            body=json.dumps(message)
        )

        if counter % 10 == 0:
            print(f"\n[Dispatcher] BAD MESSAGE -> {routing_key}")
        else:
            print(f"\n[Dispatcher] Sent -> {routing_key}")

        counter += 1
        time.sleep(2)


# ---------------- Driver ---------------- #

def run_driver(name, pattern):

    connection = pika.BlockingConnection(
        pika.ConnectionParameters("localhost")
    )

    channel = connection.channel()

    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type="topic"
    )

    result = channel.queue_declare(
        queue="",
        exclusive=True
    )

    queue = result.method.queue

    channel.queue_bind(
        exchange=EXCHANGE,
        queue=queue,
        routing_key=pattern
    )

    print(f"{name} listening on {pattern}")

    def callback(ch, method, properties, body):

        msg = json.loads(body)

        if "zone" not in msg:
            print("\n---------------------------")
            print(f"{name} rejected bad message")
            print(msg)

            ch.basic_nack(
                delivery_tag=method.delivery_tag,
                requeue=False
            )
            return

        print("\n---------------------------")
        print(f"Driver : {name}")
        print(f"Routing key : {method.routing_key}")
        print(msg)

        ch.basic_ack(
            delivery_tag=method.delivery_tag
        )

    channel.basic_consume(
        queue=queue,
        on_message_callback=callback
    )

    channel.start_consuming()


# ---------------- Main ---------------- #

drivers = [
    ("Ravi", "bangalore.koramangala.*"),
    ("Priya", "bangalore.indiranagar.*"),
    ("Arjun", "bangalore.*.ride"),
    ("Meena", "bangalore.*.food"),
    ("Karthik", "chennai.tnagar.food"),
    ("Control", "bangalore.#")
]

threads = []

for name, pattern in drivers:
    t = threading.Thread(
        target=run_driver,
        args=(name, pattern),
        daemon=True
    )
    t.start()
    threads.append(t)

time.sleep(2)

dispatch = threading.Thread(
    target=dispatcher,
    daemon=True
)

dispatch.start()
dispatch.join()