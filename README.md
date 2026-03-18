<img src="asset/logo_rmbg.png" alt="TabClaw" width="100%" />

# TabClaw: Plan-First AI for Table Analysis

> **Your data, your machine, your agent.**

Drop in a CSV or Excel file and ask your question in plain language. TabClaw drafts an execution plan, dispatches parallel agents across your tables, distils reusable skills from every session, and delivers structured answers — entirely on your machine, no cloud required.

---

## Architecture

<img src="asset/TabClaw_framework.png" alt="TabClaw Architecture" width="100%" />

---

## What makes TabClaw different

### 🗺️ It plans before it acts
Before touching your data, TabClaw drafts a step-by-step execution plan and shows it to you. You can reorder steps, rewrite them, or add new ones — then approve and execute. After finishing, it does a self-check to make sure nothing was missed.

### 🤖 Multiple agents work in parallel
When you upload more than one table and ask a comparative question, TabClaw automatically assigns a dedicated analyst agent to each table. They run in parallel, then an aggregator synthesises their findings — highlighting where results agree (**[CONSENSUS]**) and where they conflict (**[UNCERTAIN]**).

### 🧠 It learns from every session
After completing a non-trivial task, TabClaw reflects on what it did and distils the pattern into a reusable **custom skill**. Next time you ask something similar, it calls that skill directly. The more you use it, the smarter it gets.

### 💾 It remembers your preferences
TabClaw picks up on how you like to work — preferred metrics, output format, domain terminology — and carries that context into every future conversation. You can view, edit, or clear memory anytime from the sidebar.

### 🙋 It asks when it's not sure
If a request could reasonably mean several different things, TabClaw pauses and presents you with a concise set of clarification options before proceeding. No silent wrong assumptions.

### 🛠️ Fully extensible
You can define your own skills — write a prompt template or drop in Python code — and the agent will call them just like built-in skills. Combined with skill learning, TabClaw gradually builds a library tailored to your specific workflows.

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

Open **http://localhost:8000** in your browser.

---

## Demo

<img src="asset/TabClaw_demo.png" alt="TabClaw Demo" width="100%" />

---

## Docs

| | |
|---|---|
| [✨ Features](docs/features.md) | Full feature details |
| [⚙️ Configuration](docs/configuration.md) | API providers, model selection |
| [🏗️ Architecture](docs/architecture.md) | System design, project structure |
| [🛠️ Skills Reference](docs/skills.md) | Built-in skills, custom skills, sandbox |

---

## Related Projects

### 🦞 [Claw-R1](https://agentr1.github.io/Claw-R1/) — Agentic RL for General Agents

From the same team: **Claw-R1** is a training framework that bridges Agentic RL and next-generation general agents (like TabClaw, OpenClaw, Claude Code). It introduces a **Middleware Layer** as the sole bridge between the agent side and training side, enabling white-box and black-box agents to participate in RL training via standard HTTP — a paradigm no existing framework supports.

→ [Project Page](https://agentr1.github.io/) · [Documentation](https://agentr1.github.io/Claw-R1/)

---
## Contributors

**Team Members**: Shuo Yu, Daoyu Wang, Qingchuan Li

**Supervisors**: Qi Liu, Mingyue Cheng

**Affiliation**: State Key Laboratory of Cognitive Intelligence, University of Science and Technology of China

---

## Acknowledgements

TabClaw builds upon the vision of personal AI assistants pioneered by [OpenClaw](https://github.com/openclaw/openclaw), whose work on agentic interaction design deeply inspired our approach to conversational table analysis. We are grateful to the broader open-source agent community for the tools and ideas that made this project possible.

---

## Citation

If you use TabClaw in your research or project, please cite:

```bibtex
@misc{tabclaw2026,
  title        = {TabClaw: A Local AI Agent for Conversational Table Analysis},
  author       = {Yu, Shuo and Wang, Daoyu and Li, Qingchuan and Cheng, Mingyue and Liu, Qi},
  year         = {2026},
  howpublished = {\url{https://github.com/fishsure/TabClaw}}
}
```