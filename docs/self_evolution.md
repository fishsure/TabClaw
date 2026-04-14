# Self-evolution mechanism

This document describes **how TabClaw’s self-evolution is implemented in code** (as of the v2 pipeline): what gets recorded, how skills are created or upgraded, how feedback flows in, and how the Growth Dashboard is computed.

---

## 1. Big picture

Self-evolution rests on three ideas:

1. **Every completed analysis is a workflow** — stored as JSON so we can mine patterns and attach feedback.
2. **Skills are SKILL.md-style packages** — stored under `data/skills/<slug>/`, versioned, and injected into the agent like ClawHub/OpenClaw packages.
3. **The LLM is the “evolution operator”** — distillation, discovery, and improvement are all **prompted LLM calls** with structured JSON outputs; there is no separate training loop.

```
User task → Agent + tools → WorkflowRecord saved to data/workflows/
                │
                ├─ Skill Learning + ≥3 tool calls → try_distill → new package skill (SKILL.md)
                ├─ Discover from history → recurring tool_sequence → LLM suggestions (user accepts)
                ├─ 👍/👎 or implicit feedback → record_feedback → failure_count ≥ 2 → try_improve → upgraded SKILL.md
                └─ get_growth_profile() → Growth Dashboard (domains, efficiency, timeline)
```

---

## 2. Workflow recording

**Module:** `agent/workflow_recorder.py`  
**Storage:** `data/workflows/<session_id>.json`

When a plan run finishes, `AgentExecutor` builds a `WorkflowRecord`: user message, tables involved, optional plan, each tool step (name, params, short result summary, duration), final conclusion, duration, and which **package skills** were used (`skills_used`).

Important derived fields:

| Field | Role |
|--------|------|
| `tool_sequence` | Fingerprint `tool_a → tool_b → …` used for **recurring pattern** detection |
| `user_feedback` / `feedback_detail` | `good` / `bad`, or implicit path (see §5) |
| `skill_distilled` | Name of a skill created in this session by post-task distillation (if any) |

`update_workflow_feedback(session_id, feedback, detail)` rewrites the JSON file when the user rates a reply or when implicit feedback is applied.

---

## 3. Three ways skills evolve

All three are implemented in **`agent/skill_distiller.py`** (`SkillDistiller`), using **`skills/registry.py`** to create or upgrade packages.

### 3.1 Post-task distillation (`try_distill`)

**Trigger:** End of a plan execution when **Skill Learning** is enabled and the task used **at least 3 tool calls** (`MIN_TOOL_CALLS = 3`).

**Input:** User message + ordered tool-call log (tool name + params).

**Logic:** The LLM decides whether there is a **reusable, generalisable** pattern worth a new package skill. It receives lists of built-in tool names and **existing package skill names** to avoid duplicates. On success it returns JSON with `name`, `description`, and Markdown `body` for `SKILL.md`.

**Persist:** `SkillRegistry.create_package(..., source="distilled", derived_from=workflow_id)`. The workflow record is updated with `skill_distilled` pointing at the new skill name.

### 3.2 Pattern discovery (`discover`)

**Trigger:** User clicks **Discover skills from history** in the UI → `POST /api/skills/discover` (see `app.py`).

**Mechanical step:** `_find_uncaptured_patterns()` scans all `data/workflows/*.json` and groups sessions by **`tool_sequence`**. Sequences that appear **≥ 2 times**, have enough steps, and are **not** already “covered” by an existing skill’s tool set (heuristic overlap with `SKILL.md` bodies) are candidates.

**LLM step:** Top patterns (with example user messages) are sent to the LLM, which returns a JSON **array of suggestions** (name, description, body) — same package format as distillation, but **not** created until the user accepts in the UI.

### 3.3 Feedback-driven improvement (`try_improve`)

**Trigger:** User sends **👎** on a reply (`POST /api/workflow/{session_id}/feedback`). For each **package skill** used in that workflow, `SkillRegistry.record_feedback(slug, "bad")` increments failure stats.

**Automatic upgrade:** If a skill’s **`failure_count` ≥ 2**, the server calls **`try_improve(slug)`**. The distiller loads **bad-feedback workflows** whose tool sequence overlaps the tools referenced in that skill’s `SKILL.md` (built-in names in backticks). It sends the current body plus excerpts of bad sessions to the LLM, which returns either `improved: false` or a **new full `body`** + reason.

**Persist:** `SkillRegistry.upgrade_package(slug, new_body, reason)` bumps the version and keeps history as implemented in the registry.

**Manual:** The UI can also call **`POST /api/skills/{slug}/improve`** to force the same path.

---

## 4. Feedback and skills

**Module:** `skills/registry.py` — `record_feedback(slug, "good" | "bad")` updates per-package success/failure counters used for growth stats and upgrade thresholds.

Explicit feedback hits **`/api/workflow/{session_id}/feedback`** and updates the workflow JSON **and** package stats.

---

## 5. Implicit feedback

**Optional:** When the client sends **implicit feedback** mode with the **last workflow id**, the next user message is classified by an LLM (`_classify_implicit_feedback` in `app.py`) against the previous reply’s conclusion.

If the verdict is `good` or `bad` and the workflow was not already rated, **the same** `update_workflow_feedback` and `record_feedback` paths run as for explicit 👍/👎.

---

## 6. Growth Dashboard (`get_growth_profile`)

**Module:** `agent/workflow_recorder.py` — `get_growth_profile()`

It walks **all** workflow JSON files and aggregates:

- Session counts, 👍/👎 counts, satisfaction rate  
- **Domain buckets** — `_classify_domain()` scores user text + table names against **built-in keyword lists** (`_DOMAIN_KEYWORDS`) plus **`data/custom_domains.json`** (user-defined domains from the UI)  
- **Tool frequency** across steps  
- **Efficiency** — compares average duration and step count between **early** and **recent** half of sessions (when enough data)  
- **Timeline** events — e.g. when `skill_distilled` appears  
- **Milestones** — derived in the same function from counts and events  

The frontend calls **`GET /api/growth/profile`** to render the Growth Dashboard.

---

## 7. API summary (evolution-related)

| Endpoint | Purpose |
|----------|---------|
| `POST /api/workflow/{session_id}/feedback` | 👍/👎; may trigger `try_improve` and return `skill_upgraded` |
| `POST /api/skills/discover` | Run pattern discovery; returns suggestions |
| `POST /api/skills/{slug}/improve` | Manually trigger skill upgrade from bad workflows |
| `GET /api/growth/profile` | Growth Dashboard payload |
| `POST /api/growth/domains` | Add custom domain keywords (`custom_domains.json`) |
| `GET /api/workflows` | List recent workflow metadata |

Chat streaming may emit SSE events such as `workflow_id`, `skill_learned`, and `implicit_feedback_applied` — see `app.py` and `static/app.js`.

---

## 8. Files to read in the repo

| Path | Content |
|------|---------|
| `agent/workflow_recorder.py` | Workflow schema, feedback updates, `find_recurring_patterns`, `get_growth_profile`, domain keywords |
| `agent/skill_distiller.py` | `try_distill`, `discover`, `try_improve`, pattern mining |
| `agent/executor.py` | Saves workflow after run; calls `try_distill` when `auto_learn` is on |
| `skills/registry.py` | Package CRUD, versioning, `record_feedback`, `upgrade_package` |
| `app.py` | HTTP routes, implicit feedback, workflow feedback → upgrade |

---

## 9. Limitations (honest scope)

- **No gradient training** — evolution is **prompt + JSON skill files**, not model fine-tuning.  
- **Pattern discovery** is driven by **identical tool sequences** and heuristics; similar-but-not-identical workflows may need manual skill authoring.  
- **Domain labels** are **keyword-based**; they are for reporting and UX, not ground-truth classification.

For product-level behaviour (Plan Mode, multi-agent, memory), see [Features](features.md) and [Architecture](architecture.md).
