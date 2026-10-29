import os
import socket
from confluent_kafka import Consumer, Producer

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
WORKER_ID = os.environ.get("HOSTNAME", socket.gethostname())

def get_consumer() -> Consumer:
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "inference-workers",
        "max.poll.interval.ms": 900000,   # 15 min: a batch of slow requests must not look like a dead consumer
        "auto.offset.reset": "earliest", # a brand-new group reads from the beginning, so no prompts are missed
        "enable.auto.commit": False, # commit manually, only after full completion
    })
    c.subscribe(["inference-requests"], on_assign=_on_assign, on_revoke=_on_revoke)
    return c

def get_result_producer() -> Producer:
    return Producer({"bootstrap.servers": BOOTSTRAP})

def _on_assign(consumer, partitions):
    print(f"[{WORKER_ID}] ASSIGNED partitions {[p.partition for p in partitions]}", flush=True)

def _on_revoke(consumer, partitions):
    print(f"[{WORKER_ID}] REVOKED partitions {[p.partition for p in partitions]}", flush=True)
