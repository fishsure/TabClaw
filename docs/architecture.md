# 🏗️ Architecture

## Project Structure

```
TabClaw/
├── app.py                  # FastAPI app — all HTTP/SSE endpoints
├── config.py               # Loads API_KEY / BASE_URL / DEFAULT_MODEL
├── setting.txt.example     # Config template
├── requirements.txt
├── run.sh
│
├── agent/
│   ├── executor.py         # ReAct agent loop (execute / execute_plan)
│   ├── planner.py          # Plan generation + intent clarification
│   ├── multi_agent.py      # Parallel per-table agents + aggregator
│   ├── skill_distiller.py  # Post-task skill extraction
│   ├── memory.py           # Persistent user memory (JSON)
│   └── llm.py              # Async OpenAI-compatible LLM client
│
├── skills/
│   ├── registry.py         # Built-in + custom skill registry & tool defs
│   ├── builtin.py          # 16 built-in pandas skills
│   └── code_skill.py       # AST-checked Python sandbox
│
├── static/
│   ├── index.html          # Single-page UI
│   ├── app.js              # Frontend state, streaming, rendering
│   └── style.css           # Dark/light theme, all component styles
│
├── examples/               # Demo CSV datasets
├── docs/                   # This documentation
├── asset/                  # Logo and images
└── data/                   # Runtime state (gitignored)
```

---

## System Design

```
Browser (SSE stream)
      │
      ▼
FastAPI (app.py)
      │
      ├─ POST /api/clarify ────► Planner.check_clarification()
      ├─ POST /api/generate-plan ► Planner.generate()
      │
      ├─ POST /api/chat ───────► AgentExecutor.execute()
      │                       └► MultiAgentExecutor.execute_multi()
      │                              ├─ Agent (table 1) ─┐ parallel
      │                              ├─ Agent (table 2) ─┤
      │                              └─ Aggregator ───────┘
      │
      └─ POST /api/execute-plan ► AgentExecutor.execute_plan()
                                        │
                              ┌─────────┴──────────┐
                              ▼                    ▼
                       ReAct loop           SkillDistiller
                    (tool calls via         (post-task skill
                     SkillRegistry)          extraction)
```

---

## Core: The ReAct Streaming Loop

The heart of TabClaw is a **streaming ReAct (Reason → Act → Observe)** loop implemented in `agent/executor.py::_agent_stream()`. It runs up to **12 iterations**, with each iteration:

1. **Stream** the LLM response chunk by chunk via SSE
2. **Accumulate** tool call deltas across streaming chunks into a per-index dict
3. If tool calls are present, **execute** each skill synchronously in a thread pool (`asyncio.to_thread`)
4. **Append** the tool result back into the conversation as a `role: tool` message
5. Loop — the LLM sees all prior tool results and decides whether to call more tools or produce a final answer

### Streaming Tool Call Accumulation

OpenAI-compatible APIs stream tool call arguments as partial JSON across multiple chunks. TabClaw merges them:

```python
tool_calls_acc: Dict[int, Dict] = {}   # index → {id, name, arguments}
for chunk in stream:
    for tc in delta.tool_calls:
        idx = tc.index
        tool_calls_acc.setdefault(idx, {"id": "", "name": "", "arguments": ""})
        tool_calls_acc[idx]["arguments"] += tc.function.arguments or ""
```

After streaming ends, the accumulated JSON is parsed and each skill is dispatched.

### DeepSeek V3 Markup Suppression

DeepSeek V3 occasionally leaks raw tool-call markup (`<｜tool▁call▁begin｜>…`) into `delta.content`. TabClaw detects this marker and suppresses the affected text chunks from display, then strips any residual markup with a regex before building the assistant message:

```python
_TOOL_MARKER = "<｜tool▁call▁begin｜>"
_TOOL_RE = re.compile(r"<｜tool[\s\S]*?(?:<｜tool▁call▁end｜>|$)", re.DOTALL)
```

This ensures the chat UI only shows clean reasoning text, not model internals.

---

## Plan Mode: Context-Chained Step Execution

When Plan Mode is active, `execute_plan()` runs each step as a separate `_agent_stream` call but maintains a **running conversation** that chains results across steps:

```python
conversation = list(base_messages)
for i, step in enumerate(steps):
    step_messages = conversation + [{"role": "user", "content": step_msg}]
    async for event in self._agent_stream(step_messages, ...):
        ...
    # Feed this step's output into the next step's context
    conversation.append({"role": "assistant", "content": final_text})
```

This means step 3 has full visibility into what steps 1 and 2 found — it can reference intermediate tables by name, build on prior conclusions, and avoid redundant work.

### Self-Check Pass

After all steps complete, a lightweight reflection prompt is injected:

> *"Was the original request fully addressed? Are there obvious errors or gaps? If complete: confirm in 1–2 sentences. If something is missing: fix it now."*

The agent can call additional tools in this pass. This catches cases where a step silently failed or the plan missed part of the user's intent.

---

## Multi-Agent: Parallel Analysis via AsyncIO Queue

`MultiAgentExecutor.execute_multi()` activates when ≥ 2 tables are uploaded and the user's message contains comparison keywords. Implementation:

```python
queue: asyncio.Queue = asyncio.Queue()

async def run_agent(tid, table):
    # Each agent uses a SCOPED system prompt — sees only its assigned table
    scoped_system = self._system_prompt({tid: table})
    ...
    async for event in self._agent_stream(scoped_msgs, ...):
        await queue.put({**event, "agent_id": tid})  # tag with source
    await queue.put(_DONE)

tasks = [asyncio.create_task(run_agent(tid, t)) for tid, t in tables.items()]
```

A single consumer loop reads from the queue, forwarding events to the SSE stream as they arrive — **interleaving output from multiple agents in real time**. After all agents finish, an **Aggregator** LLM synthesises findings and applies epistemic markers:

- **[CONSENSUS]** — conclusions most or all agents agree on
- **[UNCERTAIN]** — conflicting or caveat-heavy findings

### Why Scoped Prompts?

Each specialist agent's system prompt only lists its assigned table. This prevents cross-contamination: agent A cannot accidentally reference columns from table B, making each agent's conclusions cleanly attributable to its data source.

---

## Skill Distillation Pipeline

After every task with ≥ 3 tool calls, `SkillDistiller.try_distill()` sends the tool call log to the LLM with this question: *"Is there a reusable, generalizable skill worth saving?"*

The log is summarised (capped at 25 entries, string params truncated to 80 chars), and the LLM must distinguish:

| Good candidate | Bad candidate |
|---|---|
| Recurring pattern (profit margin ranking, top-N per category, KPI report) | One-off task specific to this dataset |
| Parameterisable — not hard-coded column names | Duplicate of an existing built-in |
| Adds value beyond a single built-in skill | Trivially simple (single filter or sort) |

The LLM also receives the full list of existing built-in and custom skills to prevent duplicates. Output is either a `code`-mode or `prompt`-mode skill saved immediately to `data/custom_skills.json`.

---

## Memory: Automatic Extraction and Relevance Filtering

After each interaction, `_try_update_memory()` runs a lightweight LLM call:

> *"From this user interaction, extract any preferences or important facts worth remembering."*

Output is a JSON array of up to 3 items tagged with one of four categories:

| Category | Examples |
|---|---|
| `preferences` | Output language, chart style, decimal places |
| `domain_knowledge` | "profit margin = profit / revenue", column semantics |
| `user_context` | Industry, team role, project goals |
| `history_insights` | Recurring analysis patterns |

At query time, `memory.get_relevant(query)` applies a keyword filter — it always includes `preferences` and keyword-matches other categories — so only relevant facts are injected into the system prompt. This keeps the prompt lean for unrelated queries.

Natural-language forgetting (`"forget my output format preference"`) uses an LLM to identify which memory keys to delete, rather than requiring the user to navigate a tree structure.

---

## SSE Event Reference

All agent responses stream via **Server-Sent Events**. The frontend (`app.js`) dispatches on `event.type`:

| Event | Description |
|---|---|
| `text_chunk` | Streaming LLM text delta |
| `tool_call` | Skill invoked — name + params |
| `tool_result` | Skill output text |
| `table` | New result table created |
| `step_start` / `step_done` | Plan step progress |
| `reflect_start` / `reflect_done` | Self-check pass |
| `agent_pool_start` | Multi-agent mode started |
| `agent_start` / `agent_done` | Per-table agent lifecycle |
| `aggregate_start` | Aggregator phase started |
| `skill_learned` | New custom skill auto-saved |
| `final_text` | Complete response content |
| `error` | Error from agent or skill |
