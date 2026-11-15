%%writefile stream/dispatcher.py
import json
import random
import time
import uuid
from datetime import datetime

import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters("localhost")
)
channel = connection.channel()

channel.exchange_declare(
    exchange="rapido-dispatch",
    exchange_type="topic"
)

cities = ["bangalore", "chennai"]

zones = {
    "bangalore": [
        "koramangala",
        "indiranagar",
        "whitefield"
    ],
    "chennai": [
        "tnagar",
        "whitefield"
    ]
}

types = ["food", "ride"]

print("Dispatcher started...\n")

while True:
    city = random.choice(cities)
    zone = random.choice(zones[city])
    req_type = random.choice(types)

    routing_key = f"{city}.{zone}.{req_type}"

    message = {
        "id": str(uuid.uuid4())[:8],
        "city": city,
        "zone": zone,
        "type": req_type,
        "ts": datetime.now().isoformat()
    }

    channel.basic_publish(
        exchange="rapido-dispatch",
        routing_key=routing_key,
        body=json.dumps(message)
    )

    print("Sent:", routing_key)

    time.sleep(2)