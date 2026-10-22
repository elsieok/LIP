import os
from llama_cpp import Llama
from typing import cast
from llama_cpp.llama_types import CreateChatCompletionResponse

MODEL_PATH = os.environ.get("MODEL_PATH", "models/qwen2.5-1.5b-instruct-q4_k_m.gguf")

_llm = None

def get_model() -> Llama:
    global _llm
    if _llm is None:
        _llm = Llama(
            model_path=MODEL_PATH,
            n_ctx=2048,
            n_threads=int(os.environ.get("N_THREADS", 4)),
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

