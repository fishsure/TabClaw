from openai import AsyncOpenAI
from typing import List, Dict, Optional, AsyncGenerator


class LLMClient:
    def __init__(self, api_key: str, base_url: str, model: str, model_extra_params: dict = None):
        self.client = AsyncOpenAI(api_key=api_key, base_url=base_url)
        self.model = model
        self.model_extra_params = model_extra_params

    async def chat(self, messages: List[Dict], tools: Optional[List] = None) -> object:
        """Non-streaming chat, returns the message object."""
        kwargs = dict(model=self.model, extra_body=self.model_extra_params, messages=messages, temperature=0.1)
        if tools:
            kwargs["tools"] = tools
        resp = await self.client.chat.completions.create(**kwargs)
        return resp.choices[0].message

    async def stream_chat(self, messages: List[Dict], tools: Optional[List] = None) -> AsyncGenerator:
        """Streaming chat, yields raw chunks."""
        kwargs = dict(model=self.model, extra_body=self.model_extra_params, messages=messages, temperature=0.1, stream=True)
        if tools:
            kwargs["tools"] = tools
        stream = await self.client.chat.completions.create(**kwargs)
        async for chunk in stream:
            yield chunk
