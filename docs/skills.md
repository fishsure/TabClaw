# 🛠️ Skills Reference

## Built-in Skills

These 16 skills are always available. The agent selects them automatically based on the task. Each is implemented as a pure pandas function in `skills/builtin.py` and exposed to the LLM as an OpenAI-compatible tool definition.

| Skill | Key parameters | Description |
|---|---|---|
| `table_info` | `table_id` | Shape, columns, dtypes, missing values, 5 sample rows |
| `filter_rows` | `table_id`, `condition` | Filter via pandas query string (`engine="python"`) |
| `select_columns` | `table_id`, `columns` | Project a subset of columns |
| `aggregate` | `table_id`, `group_by`, `agg_config` | Group by + aggregate (`{"col": "sum/mean/count/…"}`) |
| `sort_table` | `table_id`, `by`, `ascending` | Sort rows by one or more columns |
| `merge_tables` | `left_table_id`, `right_table_id`, `on`, `how` | Join two tables (inner / left / right / outer) |
| `pivot_table` | `table_id`, `index`, `columns`, `values`, `aggfunc` | Create a cross-tabulation |
| `add_column` | `table_id`, `column_name`, `expression` | Computed column via `df.eval(expression)` |
| `describe_stats` | `table_id`, `columns?` | Mean, std, quartiles, min, max for numeric columns |
| `find_values` | `table_id`, `column`, `value`/`pattern` | Exact match or regex search in a column |
| `drop_duplicates` | `table_id`, `subset?` | Remove duplicate rows |
| `rename_columns` | `table_id`, `rename_map` | Rename columns via a `{"old": "new"}` map |
| `sample_rows` | `table_id`, `n` | Random sample of N rows |
| `value_counts` | `table_id`, `column` | Frequency table for a categorical column |
| `correlation_matrix` | `table_id`, `columns?` | Pearson correlation matrix for numeric columns |
| `head_rows` | `table_id`, `n` | First N rows |

### How Skill Results Flow

When a skill returns a DataFrame, the executor:

1. Assigns it a unique ID (`r_` + 6 hex chars)
2. Stores it in the session's `result_tables_store`
3. Emits a `table` SSE event so the UI renders it immediately as an interactive table
4. Appends a creation message to the LLM context so subsequent tool calls can reference it by ID

---

## Code Tool (`execute_python`)

Enable **Code Tool** in the toolbar to add a 17th skill. The agent can write arbitrary pandas code executed in a **three-layer sandbox**.

### Security Architecture

**Layer 1 — AST inspection** (runs before any code executes):

```python
class _SafetyChecker(ast.NodeVisitor):
    def visit_Import(self, node):        # blocks forbidden modules
    def visit_ImportFrom(self, node):    # blocks `from os import ...`
    def visit_Attribute(self, node):     # blocks unsafe dunder access
    def visit_Call(self, node):          # blocks open(), exec(), eval(), …
```

Blocked imports include: `os`, `sys`, `subprocess`, `socket`, `shutil`, `pathlib`, `http`, `urllib`, `pickle`, `ctypes`, and all network/filesystem modules.

**Layer 2 — Restricted namespace**:

`__builtins__` is replaced with a curated safe dictionary. `open`, `exec`, `eval`, `__import__`, `getattr`, `setattr`, `globals`, `locals` are absent. A controlled `__import__` wrapper is provided that only resolves whitelisted modules.

**Layer 3 — Timeout**:

Code runs in a **daemon thread** with a 30-second join timeout. If the thread is still alive after 30 seconds, execution is aborted and an error is returned.

### Allowed Libraries

`pandas` (`pd`), `numpy` (`np`), `math`, `re`, `json`, `collections`, `itertools`, `datetime`, `statistics`, `functools`, `operator`

### Table Access

All uploaded tables are pre-loaded into the execution namespace by **two aliases**:

```python
# By table ID
r_abc123   # the DataFrame directly

# By sanitised file name
sales_2023  # underscores replace non-word characters

# Generic mapping for iteration
tables = {tid: {"name": "...", "df": DataFrame}, ...}
```

### Output Convention

```python
# Assign a DataFrame to `result` to produce a new table in the UI
result = df.groupby('region')['profit'].sum().reset_index()

# Use print() for intermediate diagnostic output
print(df.shape)
```

---

## Custom Skills

Add your own skills from the **Skills** sidebar panel. Custom skills are exposed to the agent as tool definitions alongside built-in skills and selected automatically based on the task description.

### Prompt Mode

Write a system prompt template. Placeholders:

| Placeholder | Replaced with |
|---|---|
| `{table_name}` | Name of the selected table |
| `{user_request}` | The invocation instruction from the agent |

A 30-row CSV preview of the selected table is automatically appended as context. The LLM executes the prompt and returns text (no DataFrame output in prompt mode).

### Code Mode

Write Python code that runs in the same sandbox as Code Tool. The agent passes `table_id` and `user_request` parameters when invoking the skill.

```python
# Access tables via the `tables` dict
tid = list(tables.keys())[0]   # or use the specific table_id parameter
df = tables[tid]['df']
result = df.groupby('region')['profit'].sum().reset_index()
```

---

## Skill Learning

After every task involving ≥ 3 tool calls, the **SkillDistiller** reviews the interaction and may create a new custom skill automatically.

### How Distillation Works

1. The tool call log is summarised (capped at 25 entries; string params truncated to 80 chars)
2. The log is sent to the LLM alongside the full list of existing built-in and custom skills (to prevent duplicates)
3. The LLM decides whether a **reusable, generalisable** pattern exists
4. If yes, it returns either a code-mode or prompt-mode skill definition
5. The skill is saved, and a 🧠 badge appears in the chat

### What Makes a Good Candidate

| Good | Bad |
|---|---|
| Recurring pattern: profit ranking, top-N per group, cohort KPI report | One-off task specific to this exact dataset |
| Parameterisable — not hard-coded column names or filter values | Near-duplicate of a built-in skill |
| Multi-step combination that pays off in future sessions | Trivially simple: single filter, sort, or lookup |

### Managing Learned Skills

Learned skills appear with a 🧠 badge in the Skills panel. You can **edit**, **test**, or **delete** them at any time. The **Clear** button removes all custom skills at once.

---

## Skill Registry

`skills/registry.py` maintains two lists: built-in functions loaded from `builtin.py`, and custom skills loaded from `data/custom_skills.json`. At query time, `get_tool_definitions(code_tool=False)` returns the merged OpenAI-compatible tool schema:

```json
{
  "type": "function",
  "function": {
    "name": "aggregate",
    "description": "Group by columns and aggregate.",
    "parameters": { "type": "object", "properties": { ... } }
  }
}
```

Custom skills are added to this list with a generated schema based on their name and description, so the LLM can invoke them using the same tool-calling mechanism as built-ins.
