import os
import socket
from confluent_kafka import Consumer, Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
WORKER_ID = os.environ.get("HOSTNAME", socket.gethostname())

def get_consumer() -> Consumer:
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "inference-workers",
        "auto.offset.reset": "earliest", # a brand-new group reads from the beginning, so no prompts are missed
        "enable.auto.commit": False, # commit manually, only after full completion
    })
    c.subscribe(["inference-requests"])
    return c

def get_result_producer() -> Producer:
    return Producer({"bootstrap.servers": BOOTSTRAP})
