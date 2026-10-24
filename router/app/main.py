import json
import uuid
import threading
import queue
import os
from typing import Any
from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer, TopicPartition
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse
from .kafka_client import get_producer, get_results_consumer, produce_request
from contextlib import asynccontextmanager
from .metrics import metrics_app, REQUEST_COUNT, REQUEST_LATENCY

_waiters: dict[str, queue.Queue[dict[str, Any]]] = {}
# allows main thread and bg consume_loop thread to share data
# HTTP thread:     _waiters[request_id] = queue  ← registers a mailbox with "address" request_id
# results thread:  q = _waiters.get(request_id)  ← finds the mailbox
#                  q.put(token)  ← drops a token in
# HTTP thread:     q = _waiters.get(request_id)  ← picks the token up and streams it
_lock = threading.Lock()
# stops race conitions. only one thread can mess with a queue at a time 

LAG_THRESHOLD = int(os.environ.get("LAG_THRESHOLD", "200"))

def results_loop():
    consumer = get_results_consumer()
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        value = msg.value()
    
        if value is None:
            print("Received Kafka message with no value")
            consumer.commit(message=msg, asynchronous=False)
            continue

        payload = json.loads(value)
        request_id = payload["request_id"]
        with _lock:
            q = _waiters.get(request_id)
        if q is not None:
            q.put(payload)

@asynccontextmanager
async def lifespan(app: FastAPI):
    threading.Thread(target=results_loop, daemon=True).start()

    yield

app = FastAPI(lifespan=lifespan)
app.mount("/metrics", metrics_app)
producer = get_producer()

class GenerateRequest(BaseModel):
    prompt: str
    max_tokens: int = 256

def current_consumer_lag() -> int:
    c = Consumer({
        "bootstrap.servers": os.environ.get(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092"),
            "group.id": "lag_checker",
            "enable.auto.commit": False
    })
    # sum of (high watermark - commited offset) across partitions for worker groups
    total_lag = 0
    md = c.list_topics(topic="inference-requests", timeout=5)
    partitions = [TopicPartition("inference-requests", p) for p in md.topics["inference-requests"].partitions]
    committed = c.committed([TopicPartition("inference-requests", p.partition, ) for p in partitions], timeout=5)
    for tp in partitions:
        low, high = c.get_watermark_offsets(tp, timeout=5)
        offset = next((cp.offset for cp in committed if cp.partition == tp.partition), low)
        offset = max(offset, 0)
        total_lag += max(high - offset, 0)
    c.close()
    return total_lag

@app.post("/generate")
async def generate_endpoint(req: GenerateRequest):
    REQUEST_COUNT.inc()

    lag = current_consumer_lag()
    if lag > LAG_THRESHOLD:
        raise HTTPException(status_code=503, detail=f"system overloaded (lag={lag}), try again shortly")
    
    request_id = str(uuid.uuid4())
    q = queue.Queue()
    with _lock:
        _waiters[request_id] = q
    produce_request(producer, request_id, req.prompt, req.max_tokens)

    def event_generator():
        try:
            with REQUEST_LATENCY.time():
                while True:
                    payload = q.get(timeout=60)
                    if payload["done"]:
                        yield {"event": "done", "data": payload["worker_id"]}
                        break
                    yield {"event": "token", "data": payload["token"]}
        finally:
            with _lock:
                _waiters.pop(request_id, None)

    return EventSourceResponse(event_generator())

@app.get("/health")
def health():
    return {"status": "ok"}
