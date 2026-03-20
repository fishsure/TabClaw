---
hide:
  - navigation
  - toc
---

<div class="tc-hero">
  <h1 class="tc-title">TabClaw: Interactive AI Agent for Table Analysis</h1>
  <img src="assets/logo_rmbg.png" alt="TabClaw" />
  <p class="tc-tagline">Personalized. Self-evolving. Fully interactive.</p>
  <p class="tc-sub">
    Drop in a CSV or Excel file and describe what you want.
    TabClaw shows you its plan before acting, dispatches parallel agents across your tables,
    remembers your preferences across sessions, and distils reusable skills from every interaction —
    growing smarter the more you use it.
  </p>
</div>

---

<img src="assets/TabClaw_demo.png" alt="TabClaw UI" class="tc-demo-img" />

---

## What makes TabClaw different

<div class="grid cards" markdown>

-   :material-map-marker-path:{ .lg .middle } **Plans before acting**

    ---

    Before touching your data, TabClaw drafts a step-by-step execution plan and shows it to you. Reorder steps, rewrite them, or add new ones — then approve and execute. A self-check pass verifies completeness after execution.

    [:octicons-arrow-right-24: Plan Mode](features.md#plan-mode)

-   :material-robot-outline:{ .lg .middle } **Multi-agent parallel analysis**

    ---

    When multiple tables are uploaded, TabClaw spawns a specialist agent per table running in parallel. An aggregator synthesises their findings and marks where conclusions **[CONSENSUS]** agree or **[UNCERTAIN]** conflict.

    [:octicons-arrow-right-24: Multi-Agent](features.md#multi-agent-parallel-analysis)

-   :material-brain:{ .lg .middle } **Learns from every session**

    ---

    After non-trivial tasks (≥ 3 tool calls), TabClaw distils the interaction into a reusable custom skill. Next time you ask something similar, it calls that skill directly. The more you use it, the smarter it gets.

    [:octicons-arrow-right-24: Skill Learning](features.md#skill-learning)

-   :material-database-outline:{ .lg .middle } **Remembers your preferences**

    ---

    TabClaw automatically extracts preferences and domain facts from every conversation and injects them into future sessions. View, edit, or clear memory at any time from the sidebar.

    [:octicons-arrow-right-24: Memory](features.md#persistent-memory)

-   :material-help-circle-outline:{ .lg .middle } **Asks when it's not sure**

    ---

    Ambiguous requests get a concise set of clarification options before execution. Unambiguous requests pass through instantly with no delay. No silent wrong assumptions.

    [:octicons-arrow-right-24: Intent Clarification](features.md#intent-clarification)

-   :material-puzzle-outline:{ .lg .middle } **Fully extensible**

    ---

    Define custom skills in prompt-template or Python code mode. Combined with automatic skill learning, TabClaw gradually builds a personal library tailored to your specific workflows.

    [:octicons-arrow-right-24: Custom Skills](skills.md#custom-skills)

</div>

---

## Architecture

<img src="assets/TabClaw_framework.png" alt="TabClaw Architecture" style="border-radius:8px;border:1px solid var(--tc-border);max-width:100%;" />

The full technical design — ReAct streaming loop, context-chained plan execution, asyncio multi-agent coordination, skill distillation pipeline, and three-layer code sandbox — is documented in the Architecture section.

[:octicons-arrow-right-24: Architecture deep dive](architecture.md)

---

## Quick Start

```bash
git clone https://github.com/fishsure/TabClaw.git
cd TabClaw

cp setting.txt.example setting.txt
# Fill in API_KEY and BASE_URL in setting.txt

pip install -r requirements.txt
bash run.sh
```

Open **[http://localhost:8000](http://localhost:8000)** in your browser.

!!! tip "Supported LLM providers"
    TabClaw works with any OpenAI-compatible endpoint: OpenAI, DeepSeek, SiliconFlow, Ollama (local), and more.
    See [Configuration](configuration.md) for details.

---

## Team

Built at the **State Key Laboratory of Cognitive Intelligence, University of Science and Technology of China**.

| Role | |
|---|---|
| Team Members | Shuo Yu · Daoyu Wang · Qingchuan Li |
| Supervisors | Mingyue Cheng · Qi Liu |

---

## Related Projects

### :fontawesome-solid-shrimp: [Claw-R1](https://agentr1.github.io/Claw-R1/) — Agentic RL for General Agents

From the same team: **Claw-R1** is a training framework that bridges Agentic RL and next-generation general agents. It introduces a **Middleware Layer** as the sole bridge between the agent side and the training side, enabling white-box and black-box agents to participate in RL training via standard HTTP.

[Project Page](https://agentr1.github.io/){ .md-button } [Documentation](https://agentr1.github.io/Claw-R1/){ .md-button }

---

## Citation

```bibtex
@misc{tabclaw2026,
  title        = {TabClaw: A Local AI Agent for Conversational Table Analysis},
  author       = {Yu, Shuo and Wang, Daoyu and Li, Qingchuan and Cheng, Mingyue and Liu, Qi},
  year         = {2026},
  howpublished = {\url{https://github.com/fishsure/TabClaw}}
}
```
