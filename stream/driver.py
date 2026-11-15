%%writefile stream/driver.py
import json
import threading
import pika


def run_driver(name, pattern):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters("localhost")
    )

    channel = connection.channel()

    channel.exchange_declare(
        exchange="rapido-dispatch",
        exchange_type="topic"
    )

    result = channel.queue_declare(
        queue="",
        exclusive=True
    )

    queue_name = result.method.queue

    channel.queue_bind(
        exchange="rapido-dispatch",
        queue=queue_name,
        routing_key=pattern
    )

    print(f"{name} listening on {pattern}")

    def callback(ch, method, properties, body):
        msg = json.loads(body)

        print("\n----------------------------")
        print(f"Driver: {name}")
        print(f"Pattern: {pattern}")
        print(f"Routing key: {method.routing_key}")
        print(msg)

        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(
        queue=queue_name,
        on_message_callback=callback
    )

    channel.start_consuming()


drivers = [
    ("Ravi", "bangalore.koramangala.*"),
    ("Arjun", "bangalore.*.ride"),
    ("Control", "bangalore.#"),
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

for t in threads:
    t.join()