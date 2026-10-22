import os
import json
from confluent_kafka import Producer, Consumer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

def get_producer() -> Producer:
    return Producer({"bootstrap.servers": BOOTSTRAP})

def produce_request(producer: Producer, request_id: str, prompt: str, max_tokens: int):
    payload = json.dumps({"request_id": request_id, "prompt": prompt, "max_tokens": max_tokens})
    producer.produce("inference-requests", key=request_id, value=payload) # sends the message to the inference-requests topic
    producer.flush()
    # confluent_kafka uses an asynchronous producer
    # produce() typically puts the message into a local queue
    # and returns without waiting for Kafka to confirm delivery
    # tells the producer to wait for its queued messages to finish delivery
    # otherwise function could finish while messages are still waiting in the producer's queue

def get_results_consumer(group_id: str = "router-results") -> Consumer:
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": group_id,
        "auto.offset.reset": "latest",
        # when there is no valid committed offset
        # consumer starts at end of partition, reads new messages arriving afterward
        # it does not replay older messages already in the topic.

    })
    c.subscribe(["inference-results"])
    return c