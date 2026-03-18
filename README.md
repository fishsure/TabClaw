<img src="asset/logo_rmbg.png" alt="TabClaw" width="100%" />

> **Upload a table. Ask anything. TabClaw figures out the rest.**

TabClaw is a local AI agent for table analysis. Drop in a CSV or Excel file, describe what you want in plain language — filter, aggregate, compare, visualize patterns, merge datasets — and watch it work step by step. No SQL. No code. No cloud.

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

Open **http://localhost:8022** in your browser.

---

## Docs

| | |
|---|---|
| [✨ Features](docs/features.md) | Full feature details |
| [⚙️ Configuration](docs/configuration.md) | API providers, model selection |
| [🏗️ Architecture](docs/architecture.md) | System design, project structure |
| [🛠️ Skills Reference](docs/skills.md) | Built-in skills, custom skills, sandbox |

---

## License

MIT
