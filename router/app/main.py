import json
import uuid
import threading
import queue
from typing import Any
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse
from .kafka_client import get_producer, get_results_consumer

from contextlib import asynccontextmanager

_waiters: dict[str, queue.Queue[dict[str, Any]]] = {}
# allows main thread and bg consume_loop thread to share data
# HTTP thread:     _waiters[request_id] = queue  ← registers a mailbox with "address" request_id
# results thread:  q = _waiters.get(request_id)  ← finds the mailbox
#                  q.put(token)  ← drops a token in
# HTTP thread:     q = _waiters.get(request_id)  ← picks the token up and streams it
_lock = threading.Lock()
# stops race conitions. only one thread can mess with a queue at a time 

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
producer = get_producer()

class GenerateRequest(BaseModel):
    prompt: str
    max_tokens: int = 256

@app.post("/generate")
async def generate_endpoint(req: GenerateRequest):
    request_id = str(uuid.uuid4())
    q = queue.Queue()
    with _lock:
        _waiters[request_id] = q
    from .kafka_client import produce_request
    produce_request(producer, request_id, req.prompt, req.max_tokens)

    def event_generator():
        try:
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
