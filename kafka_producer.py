import random
import uuid
import json
import time
from datetime import datetime, timedelta
from confluent_kafka import Producer

# Kafka settings
TOPIC_NAME = "my-topic"
BOOTSTRAP_SERVERS = "34.134.223.206:9094"

# Producer configuration
producer_config = {
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "client.id": "python-producer"
}

def generate_random_json():
    name = f"user-{random.randint(1, 1000)}"
    age = random.randint(18, 88)
    email = f"{name}@example.com"
    events_time = int((datetime.now() - timedelta(days=1)).timestamp() * 1000)
    return json.dumps({
        "name": name,
        "age": age,
        "email": email,
        "events_timestamp": events_time
    })

def main():
    # Create Kafka producer
    producer = Producer(producer_config)

    while True:
        # Generate random JSON data
        json_data = generate_random_json()
        key = str(uuid.uuid4())

        # Create ProducerRecord
        producer.produce(
            topic=TOPIC_NAME,
            key=key,
            value=json_data
        )

        # Print produced record
        print(f"Produced record with key: {key} and value: {json_data}")

        # Sleep for 0.5 seconds before producing the next record
        time.sleep(0.5)

if __name__ == "__main__":
    main()