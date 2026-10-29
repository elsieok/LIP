import os
from collections.abc import Iterator
from llama_cpp import Llama
from typing import cast
from llama_cpp.llama_types import CreateChatCompletionResponse, CreateChatCompletionStreamResponse

MODEL_PATH = os.environ.get("MODEL_PATH", "models/qwen2.5-1.5b-instruct-q4_k_m.gguf")

# n_ctx=int(os.environ.get("LLAMA_N_CTX", "2048")),
# n_threads=int(os.environ.get("LLAMA_N_THREADS", "1")),
# n_threads_batch=int(os.environ.get("LLAMA_N_THREADS", "1")),

_llm = None

def get_model() -> Llama:
    global _llm
    if _llm is None:
       _llm = Llama(
            model_path=MODEL_PATH,
            n_ctx=int(os.environ.get("LLAMA_N_CTX", "2048")),
            n_threads=int(os.environ.get("LLAMA_N_THREADS", "1")),
            n_threads_batch=int(os.environ.get("LLAMA_N_THREADS", "1")),
            verbose=False,
        )
    return _llm

def generate(prompt: str, max_tokens: int = 256) -> str:
    llm = get_model()
    result = cast(CreateChatCompletionResponse,
        llm.create_chat_completion(
        messages=[{"role": "user", "content": prompt}],
        max_tokens=max_tokens,
        )
    )

    content = result["choices"][0]["message"]["content"]
    if not isinstance(content, str):
        raise TypeError("Model response content is not a string")

    return content

def generate_stream(prompt: str, max_tokens: int = 256):
    llm = get_model()
    stream = cast(Iterator[CreateChatCompletionStreamResponse],
                  llm.create_chat_completion(
        messages=[{"role": "user", "content": prompt}],
        max_tokens=max_tokens,
        stream=True # model sends response chunks as they become available
    ))

    for chunk in stream:
        delta = chunk["choices"][0]["delta"]
        if "content" in delta:
            yield delta["content"]
