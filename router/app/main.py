import json
import uuid
import threading
import queue
import os
import time
from typing import Any
from confluent_kafka.admin import AdminClient
from confluent_kafka import Consumer, TopicPartition
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse
from .kafka_client import get_producer, get_results_consumer, produce_request
from contextlib import asynccontextmanager
from .metrics import metrics_app, REQUEST_COUNT, REQUEST_LATENCY, CONSUMER_LAG

_waiters: dict[str, queue.Queue[dict[str, Any]]] = {}
# allows main thread and bg consume_loop thread to share data
# HTTP thread:     _waiters[request_id] = queue  ← registers a mailbox with "address" request_id
# results thread:  q = _waiters.get(request_id)  ← finds the mailbox
#                  q.put(token)  ← drops a token in
# HTTP thread:     q = _waiters.get(request_id)  ← picks the token up and streams it
_lock = threading.Lock()
# stops race conitions. only one thread can mess with a queue at a time 

LAG_THRESHOLD = int(os.environ.get("LAG_THRESHOLD", "200"))
_current_lag = 0

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
    threading.Thread(target=lag_loop, daemon=True).start()
    yield

app = FastAPI(lifespan=lifespan)
app.mount("/metrics", metrics_app)
producer = get_producer()

class GenerateRequest(BaseModel):
    prompt: str
    max_tokens: int = 256

def compute_lag() -> int:
    c = Consumer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": "inference-workers",     # must match the workers' group
        "enable.auto.commit": False,
    })
    try:
        md = c.list_topics(topic="inference-requests", timeout=5)
        parts = [TopicPartition("inference-requests", p)
                 for p in md.topics["inference-requests"].partitions]
        total = 0
        for tp in c.committed(parts, timeout=5):
            low, high = c.get_watermark_offsets(tp, timeout=5)
            offset = tp.offset if tp.offset >= 0 else low   # nothing committed yet
            total += max(high - offset, 0)
        return total
    finally:
        c.close()

def lag_loop():
    global _current_lag
    while True:
        try:
            _current_lag = compute_lag()
            CONSUMER_LAG.set(_current_lag)
        except Exception as e:
            print(f"lag check failed: {e}", flush=True)
        time.sleep(5)

@app.post("/generate")
async def generate_endpoint(req: GenerateRequest):
    REQUEST_COUNT.inc()

    lag = _current_lag
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
