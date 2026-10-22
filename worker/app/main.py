from fastapi import FastAPI
from pydantic import BaseModel
from .inference import generate, get_model

from contextlib import asynccontextmanager

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
