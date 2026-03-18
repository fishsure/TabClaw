# ✨ Features

## ReAct Agent Loop

TabClaw runs a **ReAct (Reason → Act → Observe)** loop powered by an OpenAI-compatible LLM. On every turn the agent:

1. Reasons about the user's request in plain text
2. Decides which tool (skill) to call and with what parameters
3. Executes the tool and reads the result
4. Loops — up to **12 iterations** — until it produces a final answer

The loop streams all output in real time via **Server-Sent Events (SSE)**: reasoning text, tool calls, tool results, and result tables all appear incrementally as the agent works. There is no waiting for a complete response.

The system prompt injects the current table list (names, shapes, column names) and the user's memory into every request. The agent is instructed to call `table_info` first on any unfamiliar table before operating on it.

---

## Plan Mode

Toggle **Plan Mode** in the input toolbar. Instead of executing immediately, the LLM first drafts a structured step-by-step plan:

```json
{
  "title": "Sales analysis by region and quarter",
  "steps": [
    {"id": 1, "description": "Call table_info to understand the dataset structure"},
    {"id": 2, "description": "Aggregate revenue and profit by region, sort descending"},
    ...
  ]
}
```

You can **add, delete, or rewrite any step** in the plan editor before clicking Execute. This gives you full control over the analysis strategy before a single tool is called.

### Context-Chained Execution

Each step runs as its own `_agent_stream` call, but the conversation **accumulates across steps** — step N receives the system prompt, all prior history, and the output of steps 1 through N-1. This means later steps can reference intermediate tables by name, build on earlier findings, and avoid redundant tool calls.

### Self-Check Pass

After all steps complete, TabClaw runs a lightweight reflection prompt:

> *"Was the original request fully addressed? Are there obvious errors or gaps? Fix anything missing now."*

The agent can call additional tools in this pass. It catches silent step failures and ensures the original intent was met end to end.

---

## Intent Clarification

Before executing any request, TabClaw silently calls a clarification check (`POST /api/clarify`). The LLM evaluates whether the request is **genuinely ambiguous** — i.e., whether different reasonable interpretations would lead to meaningfully different analyses.

If ambiguous, an interactive card appears with **2–4 concrete option chips** plus a free-text field. The user's selection is appended to the original message before the agent runs. Unambiguous requests pass through instantly with no delay.

The LLM is instructed to match the user's language (Chinese or English) in the options, and to default to `needs_clarification: false` for specific requests — preventing false positives that would slow down the workflow.

---

## Multi-Agent Parallel Analysis

When **two or more tables** are uploaded and the query contains comparison keywords (`compare`, `对比`, `差异`, `each table`, etc.), TabClaw spawns one **specialist agent per table** using `asyncio.create_task`. Agents run **truly in parallel** — not sequentially.

Each specialist agent's system prompt is **scoped to its assigned table only**: it cannot see or reference other tables, making its findings cleanly attributable. Agents share a common result table store so they can read computed outputs from other agents if needed.

An **asyncio queue** collects interleaved SSE events from all agents and forwards them to the browser in arrival order, so you see live output from multiple agents simultaneously.

### Aggregation with Epistemic Markers

Once all agents finish, an **Aggregator** LLM receives all conclusions and synthesises them:

- **[CONSENSUS]** — findings that most or all agents agree on (high confidence)
- **[UNCERTAIN]** — conflicting results or caveat-heavy conclusions (low confidence)

The aggregator is explicitly instructed to provide **cross-table insights** — comparisons, correlations, and patterns that span multiple datasets — rather than simply repeating each agent's output.

---

## Skill Learning

After any task involving ≥ 3 tool calls, TabClaw runs the **SkillDistiller**: it sends the full tool call log to the LLM and asks whether a reusable, generalisable skill can be extracted.

The LLM must judge:

| Good candidate | Bad candidate |
|---|---|
| Recurring analytical pattern (profit ranking, top-N per group, KPI report) | One-off task hard-coded to specific column names |
| Can be parameterised to work on any table | Near-duplicate of an existing built-in skill |
| Combines multiple operations in a non-obvious way | Trivially simple (single filter or sort) |

The distiller provides the full list of existing built-in and custom skills so the LLM cannot create duplicates. A successful extraction produces either:

- **Code mode** — generalised Python code running in the sandbox
- **Prompt mode** — a reusable system prompt template

The new skill is saved to `data/custom_skills.json`, announced in the chat with a 🧠 badge, and available for all future sessions immediately.

---

## Persistent Memory

TabClaw extracts preferences and domain facts from every conversation and persists them across sessions in `data/memory.json`, organised into four categories:

| Category | Stored examples |
|---|---|
| `preferences` | Output language, always show results as tables, 2 decimal places |
| `domain_knowledge` | "profit margin = profit / revenue × 100", column semantics |
| `user_context` | Industry vertical, team role, project goals |
| `history_insights` | Recurring analysis patterns the user returns to |

At query time, a **relevance filter** is applied: `preferences` are always included; other categories are keyword-matched against the current query. This keeps the system prompt lean.

You can view, edit, add, or delete individual memory items from the **Memory** sidebar panel. Natural-language forgetting is also supported — say *"forget my output format preference"* and the LLM identifies and removes the matching keys.

---

## Custom Skills

Beyond the 16 built-in skills, you can define your own from the **Skills** sidebar panel.

- **Prompt mode** — write a system prompt template; the agent runs it against a 30-row table preview as context
- **Code mode** — write sandboxed Python (pandas/numpy); runs in the same AST-checked sandbox as Code Tool

Custom skills are exposed to the agent as OpenAI-compatible tool definitions alongside built-in skills. The agent selects them automatically based on the task description.

---

## Code Tool

Enable **Code Tool** in the toolbar to let the agent write and execute arbitrary pandas code in a sandboxed Python environment. Three security layers are applied before any code runs:

1. **AST inspection** — blocks forbidden imports, dangerous built-in calls, and unsafe dunder attribute access
2. **Restricted namespace** — `__builtins__` is replaced with a curated safe dictionary; `open`, `exec`, `eval`, `__import__` are absent
3. **30-second daemon thread timeout** — prevents runaway loops

Allowed libraries: `pandas`, `numpy`, `math`, `re`, `json`, `collections`, `itertools`, `datetime`, `statistics`, `functools`. All uploaded tables are pre-loaded by ID and by sanitised name.

---

## Light / Dark Mode

Click ☀️ / 🌙 in the header. The preference is saved to `localStorage` and applied on next visit. The theme system uses 20 CSS custom properties (variables) so every component switches atomically with no hard-coded colour overrides scattered through the stylesheet.

---

## Demo Mode

Click **一键体验** to load a pre-built scenario. Four scenarios are available:

| Scenario | Dataset(s) | Focus |
|---|---|---|
| 销售业绩全景分析 | `sales_2023.csv` | Region/quarter breakdown, pivot table |
| HR 人才数据洞察 | `employees.csv` | Salary distribution, high-performance analysis |
| 订单与产品关联分析 | `products.csv` + `orders.csv` | Cross-table merge, returns analysis |
| 用户 NPS 满意度分析 | `survey_nps.csv` | Satisfaction by country and usage frequency |

The app auto-loads the datasets and executes a guided sequence of 4 queries with a 600 ms pause between steps.
