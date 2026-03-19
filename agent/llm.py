from openai import AsyncOpenAI
from typing import List, Dict, Optional, AsyncGenerator
import json


class LLMClient:
    def __init__(self, api_key: str, base_url: str, model: str):
        self.client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        self.model = model

    async def chat(self, messages: List[Dict], tools: Optional[List] = None) -> object:
        """Non-streaming chat, returns the message object."""
        kwargs = dict(model=self.model, messages=messages, temperature=0.1)
        if tools:
            kwargs["tools"] = tools
        resp = await self.client.chat.completions.create(**kwargs)
        return resp.choices[0].message

    async def stream_chat(self, messages: List[Dict], tools: Optional[List] = None) -> AsyncGenerator:
        """Streaming chat, yields raw chunks. Final synthetic chunk may be {"type": "usage", "tokens": N}."""
        kwargs = dict(
            model=self.model,
            messages=messages,
            temperature=0.1,
            stream=True,
            stream_options={"include_usage": True},
        )
        if tools:
            kwargs["tools"] = tools
        stream = await self.client.chat.completions.create(**kwargs)
        async for chunk in stream:
            if chunk.usage is not None:
                total = (chunk.usage.prompt_tokens or 0) + (chunk.usage.completion_tokens or 0)
                yield {"type": "usage", "tokens": total}
            else:
                yield chunk
