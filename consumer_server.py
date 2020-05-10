import time
from confluent_kafka import Consumer

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "sf.police.calls"


def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    c = Consumer(
        {"bootstrap.servers": BROKER_URL, "group.id": "0"}
    )
    c.subscribe([topic_name])

    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")
        time.sleep(1)


if __name__ == "__main__":
    try:
        consume(TOPIC_NAME)
    except KeyboardInterrupt as e:
        print("shutting down")
