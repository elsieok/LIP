from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import json
import threading
import os
import time
from confluent_kafka import Producer, Consumer
from .kafka_client import get_consumer, get_result_producer, WORKER_ID
from .inference import get_model, generate, generate_stream
from contextlib import asynccontextmanager
from sse_starlette.sse import EventSourceResponse
from .metrics import metrics_app, REQUEST_COUNT, REQUEST_LATENCY, TOKENS, EXPIRED

_model_ready = False
BATCH_WINDOW_MS = int(os.environ.get("BATCH_WINDOW_MS", 30))

def consume_loop():
    consumer: Consumer = get_consumer()
    producer: Producer = get_result_producer()
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        batch = [msg]
        window_end = time.monotonic() + BATCH_WINDOW_MS / 1000
        while len(batch) < 8 and time.monotonic() < window_end:
            m = consumer.poll(max(window_end - time.monotonic(), 0))
            if m is not None and not m.error():
                batch.append(m)

        for msg in batch:
            value = msg.value()
            if not value:
                continue
            payload = json.loads(value)
            request_id = payload["request_id"]
            expires = payload.get("deadline")
            if expires is not None and time.time() > expires:
                EXPIRED.inc()
                print(f"[{WORKER_ID}] EXPIRED {request_id} late_by={time.time() - expires:.1f}s", flush=True)
                consumer.commit(message=msg, asynchronous=False)
                continue
            REQUEST_COUNT.inc()
            print(f"[{WORKER_ID}] RECEIVED {request_id} partition={msg.partition()} offset={msg.offset()}", flush=True)
            try:
                with REQUEST_LATENCY.time():
                    for token in generate_stream(payload["prompt"], payload.get("max_tokens", 256)):
                        TOKENS.inc()
                        producer.produce(
                            "inference-results",
                            key=request_id,
                            value=json.dumps({"request_id": request_id, "token": token, "worker_id": WORKER_ID, "done": False}),
                        )
                    producer.produce(
                        "inference-results",
                        key=request_id,
                        value=json.dumps({"request_id": request_id, "token": "", "worker_id": WORKER_ID, "done": True}),
                    )
                    producer.flush()
                consumer.commit(message=msg, asynchronous=False)
            except Exception as e:
                print(f"generation failed for {request_id}: {e}", flush=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # run once when the application starts
    global _model_ready
    # warm model at startup, not on first request
    get_model()
    _model_ready = True
    threading.Thread(target=consume_loop, daemon=True).start()
    # creates new thread that only runs consume_loop
    # oterwise program would get stuck here and couldn't run things like health or FastAPI (HTTP requests)
    # daemon means background so when main thread exits, bg thread will alsi exit

    yield # marks the point where FastAPI begins serving requests

    # cleanup when the application shuts down, if needed

app = FastAPI(lifespan=lifespan)
app.mount("/metrics", metrics_app)

class GenerateRequest(BaseModel):
    prompt: str
    max_tokens: int = 256
    
@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/healthz/live") # is api up
def liveness():
    return {"status": "alive"}

@app.get("/healthz/ready") # is model ready
def readiness():
    if not _model_ready:
        raise HTTPException(status_code=503, detail="Model not loaded")
    return {"status": "ready"}

@app.post("/generate")
def generate_endpoint(req: GenerateRequest):
    text = generate(req.prompt, req.max_tokens)
    return {"completion": text}

@app.post("/generate/stream")
async def generate_stream_endpoint(req: GenerateRequest):
    def event_generator():
        for token in generate_stream(req.prompt, req.max_tokens):
            yield {"event": "token", "data": token}
        yield {"event": "done", "data": ""}
    return EventSourceResponse(event_generator())
