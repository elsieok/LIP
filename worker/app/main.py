from fastapi import FastAPI
from pydantic import BaseModel
import json
import threading
from confluent_kafka import Producer, Consumer
from .kafka_client import get_consumer, get_result_producer, WORKER_ID

from .inference import get_model, generate, generate_stream

from contextlib import asynccontextmanager

from sse_starlette.sse import EventSourceResponse

def consume_loop():
    consumer: Consumer = get_consumer()
    producer: Producer = get_result_producer()
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
        try:
            for token in generate_stream(payload["prompt"], payload.get("max_tokens", 256)):
                producer.produce(
                    "inference-results",
                    key=request_id,
                    value=json.dumps({"request_id": request_id, "token": token, "worker_id": WORKER_ID, "done": False})
                )
            producer.produce(
                "inference-results",
                key=request_id,
                value=json.dumps({"request_id": request_id, "token": "token", "worker_id": WORKER_ID, "done": True})
            )
            producer.flush()
            consumer.commit(message=msg, asynchronous=False) # only commit after full generation succeeded
        except Exception as e:
            # deliberately do NOT commit — message will be redelivered to another worker on rebalance
            # documented limitation: partial output before the
            # crash is not de-duplicated on the client side.
            print(f"generation failed for {request_id}: {e}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # run once when the application starts
    # warm model at startup, not on first request
    get_model()
    threading.Thread(target=consume_loop, daemon=True).start()
    # creates new thread that only runs consume_loop
    # oterwise program would get stuck here and couldn't run things like health or FastAPI (HTTP requests)
    # daemon means background so when main thread exits, bg thread will alsi exit

    yield # marks the point where FastAPI begins serving requests

    # cleanup when the application shuts down, if needed

app = FastAPI(lifespan=lifespan)

class GenerateRequest(BaseModel):
    prompt: str
    max_tokens: int = 256
    
@app.get("/health")
def health():
    return {"status": "ok"}

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
