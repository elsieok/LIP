from fastapi import FastAPI
from pydantic import BaseModel
from .inference import get_model, generate, generate_stream

from contextlib import asynccontextmanager

from sse_starlette.sse import EventSourceResponse

@asynccontextmanager
async def lifespan(app: FastAPI):
    # run once when the application starts
    # warm model at startup, not on first request
    get_model()

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
