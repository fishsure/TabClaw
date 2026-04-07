"""
SkillDistiller: after a task completes, review the tool call log and
try to extract a reusable skill as a SKILL.md package.

Skills are stored as instruction-based SKILL.md files in data/skills/<slug>/,
compatible with the OpenClaw/ClawHub format. The skill body describes
a behavioral pattern and guides the agent to use existing built-in tools.

Triggered only when >= MIN_TOOL_CALLS tool calls were made — trivial
one-shot queries are skipped entirely.
"""
import json
import re
from typing import Dict, List, Optional

MIN_TOOL_CALLS = 3  # minimum tool calls before attempting distillation

_BUILTIN_NAMES = {
    "table_info", "filter_rows", "select_columns", "aggregate", "sort_table",
    "merge_tables", "pivot_table", "add_column", "describe_stats", "find_values",
    "drop_duplicates", "rename_columns", "sample_rows", "value_counts",
    "correlation_matrix", "head_rows", "execute_python",
}


class SkillDistiller:
    def __init__(self, llm, skill_registry):
        self.llm = llm
        self.skills = skill_registry

    async def try_distill(
        self, message: str, tool_calls_log: List[Dict]
    ) -> Optional[Dict]:
        """
        Analyse the tool call log for a completed task and create a new
        package skill (SKILL.md) if a reusable pattern is found.
        Returns the created package dict, or None.
        """
        if len(tool_calls_log) < MIN_TOOL_CALLS:
            return None

        existing_names = {p["name"] for p in self.skills.list_packages()}
        summary = self._format_tool_log(tool_calls_log)

        builtin_str = ", ".join(sorted(_BUILTIN_NAMES))
        existing_str = ", ".join(sorted(existing_names)) if existing_names else "none"

        prompt = f"""You are a skill-extraction assistant for a data analysis tool.

A user just completed this task:
"{message}"

Tool calls made (in order):
{summary}

Already available built-in skills (do NOT recreate): {builtin_str}
Already saved package skills (do NOT duplicate): {existing_str}

Decide: is there a REUSABLE, GENERALIZABLE analytical pattern worth capturing as a skill?

GOOD candidate:
- Addresses a recurring data-analysis pattern (e.g. profit margin ranking,
  top-N per category, cohort retention, KPI report with multiple metrics)
- Can be described as general guidance — not hard-coded to one specific dataset or column name
- Adds meaningful value beyond a single built-in skill

BAD candidate:
- One-off task specific to this exact dataset / columns
- Duplicate or near-duplicate of an existing skill
- Trivially simple (single filter, sort, or lookup)

If a good candidate exists, return JSON with this format:
{{
  "create": true,
  "name": "descriptive_snake_case_name",
  "description": "One sentence: what it does and when to use it.",
  "body": "Markdown instruction body for SKILL.md — describe the pattern and step-by-step guidance using built-in tools (table_info, aggregate, sort_table, filter_rows, add_column, pivot_table, merge_tables, etc.). Be concrete about which tools to call and in what order. Use ## headers and numbered lists."
}}

The body should guide the agent on HOW to approach this type of task, e.g.:
"## Pattern: Revenue Ranking by Group\\n\\nWhen asked to rank groups by a metric:\\n1. Call `table_info` to identify grouping and value columns\\n2. Use `aggregate` with the relevant group_by and agg_config\\n3. Use `sort_table` to rank results\\n..."

If no good candidate: {{"create": false}}

Output ONLY valid JSON, no other text."""

        try:
            resp = await self.llm.chat([{"role": "user", "content": prompt}])
            raw = re.sub(
                r"^```(?:json)?\s*|\s*```$", "",
                (resp.content or "").strip(),
            )
            result = json.loads(raw)

            if not result.get("create"):
                return None

            name = (result.get("name") or "").strip()
            description = (result.get("description") or "").strip()
            body = (result.get("body") or "").strip()
            if not name or not description or not body:
                return None
            if name in existing_names:
                return None

            return self.skills.create_package(name, description, body, source="distilled")
        except Exception:
            return None

    def _format_tool_log(self, tool_calls_log: List[Dict]) -> str:
        lines = []
        for entry in tool_calls_log[:25]:
            name = entry.get("name", "?")
            params = entry.get("params", {})
            result_preview = (entry.get("result") or "")[:150]
            compact = {
                k: (v[:80] + "…" if isinstance(v, str) and len(v) > 80 else v)
                for k, v in params.items()
            }
            lines.append(f"  [{name}] {json.dumps(compact, ensure_ascii=False)}")
            if result_preview:
                lines.append(f"    → {result_preview}")
        return "\n".join(lines)
