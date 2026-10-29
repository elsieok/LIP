import os
import json
import time
from confluent_kafka import Producer, Consumer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

def get_producer() -> Producer:
    return Producer({"bootstrap.servers": BOOTSTRAP})

REQUEST_TTL_S = float(os.environ.get("REQUEST_TTL_S", "55"))

def produce_request(producer: Producer, request_id: str, prompt: str, max_tokens: int):
    payload = json.dumps({
        "request_id": request_id,
        "prompt": prompt,
        "max_tokens": max_tokens,
        "deadline": time.time() + REQUEST_TTL_S,
    })
    producer.produce("inference-requests", key=request_id, value=payload)
    producer.flush()

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