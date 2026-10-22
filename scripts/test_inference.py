from typing import cast
from llama_cpp import Llama
from llama_cpp.llama_types import CreateChatCompletionResponse

llm = Llama(
    model_path="models/qwen2.5-1.5b-instruct-q4_k_m.gguf",
    n_ctx=2048,
    n_threads=4,
    verbose=False,
)

output = cast(CreateChatCompletionResponse,
    llm.create_chat_completion(
    messages=[{"role": "user", "content": "Say hello in one sentence."}],
    max_tokens=64,
    stream=False
    )
)

print(output["choices"][0]["message"]["content"])
