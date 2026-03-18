# ⚙️ Configuration

## setting.txt

Copy the example file and fill in your credentials:

```bash
cp setting.txt.example setting.txt
```

`setting.txt` is listed in `.gitignore` and will **never** be committed to git.

```ini
# Your LLM API key
API_KEY=your_api_key_here

# API base URL (OpenAI-compatible endpoint)
BASE_URL=https://api.openai.com/v1
```

---

## Supported Providers

TabClaw uses the **OpenAI-compatible API format** (`/chat/completions` with streaming and tool/function calling). It works with any provider that supports this interface:

| Provider | BASE_URL | Notes |
|---|---|---|
| OpenAI | `https://api.openai.com/v1` | gpt-4o, gpt-4-turbo |
| DeepSeek | `https://api.deepseek.com/v1` | Default: DeepSeek-V3 |
| SiliconFlow | `https://api.siliconflow.cn/v1` | Many open-source models |
| Ollama (local) | `http://localhost:11434/v1` | Requires tool-call support |
| Any OpenAI-compatible API | your endpoint | — |

---

## Model Selection

The default model is set in `config.py`:

```python
DEFAULT_MODEL = "deepseek-ai/DeepSeek-V3"
```

Change this to any model your provider supports. TabClaw requires a model that supports **parallel tool/function calling** and **streaming**. Recommended models:

| Model | Strengths |
|---|---|
| `deepseek-ai/DeepSeek-V3` | Strong reasoning, fast, cost-effective — default |
| `gpt-4o` | Best instruction following, reliable tool call JSON |
| `deepseek-chat` | DeepSeek native endpoint alias |
| `qwen2.5-72b-instruct` | Good open-source alternative via SiliconFlow |

### DeepSeek V3 Notes

TabClaw includes a specific workaround for DeepSeek V3's tool call behaviour: the model occasionally leaks raw tool-call markup (`<｜tool▁call▁begin｜>…`) into `delta.content` during streaming. The executor detects this marker and suppresses the affected chunks, ensuring the chat UI only displays clean reasoning text. If you switch to a different model, this suppression is inert and does not affect output.

---

## Runtime Data

The following files are created automatically at runtime and are gitignored:

| File | Contents |
|---|---|
| `data/memory.json` | User memory — preferences, domain knowledge, context, history insights |
| `data/custom_skills.json` | Custom skills saved from the UI or learned by the skill distiller |
| `uploads/` | Temporarily uploaded CSV/Excel files |

### Memory File Format

```json
{
  "preferences": {
    "output_language": {
      "value": "Chinese",
      "updated": "2026-01-15T10:23:41+00:00"
    }
  },
  "domain_knowledge": {},
  "user_context": {},
  "history_insights": {}
}
```

### Custom Skills File Format

```json
[
  {
    "id": "a1b2c3d4",
    "name": "profit_margin_ranking",
    "description": "Rank products or regions by profit margin (profit / revenue).",
    "prompt": "",
    "code": "# generalised Python code …",
    "parameters": {}
  }
]
```

---

## Requirements

```
fastapi
uvicorn[standard]
python-multipart
aiofiles
openai>=1.0
pandas
openpyxl
```

Install with:

```bash
pip install -r requirements.txt
```

The `openai` SDK is used as the async HTTP client for any OpenAI-compatible endpoint — no OpenAI account is required.
