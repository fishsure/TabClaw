"""
SkillDistiller — extracts, discovers, and improves reusable skills.

Three modes of operation:
  1. try_distill()   — after a task, extract a new skill from tool-call log
  2. discover()      — scan workflow history for recurring patterns not yet captured
  3. try_improve()   — given bad-feedback workflows, upgrade an existing skill

Skills are stored as instruction-based SKILL.md files in data/skills/<slug>/,
compatible with the OpenClaw/ClawHub format.
"""
import json
import re
from typing import Dict, List, Optional

from agent.workflow_recorder import WORKFLOWS_DIR

MIN_TOOL_CALLS = 3

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

    # ------------------------------------------------------------------
    # 1. Post-task distillation (existing, unchanged logic)
    # ------------------------------------------------------------------

    async def try_distill(
        self, message: str, tool_calls_log: List[Dict],
        workflow_id: str = "",
    ) -> Optional[Dict]:
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

            return self.skills.create_package(
                name, description, body,
                source="distilled", derived_from=workflow_id,
            )
        except Exception:
            return None

    # ------------------------------------------------------------------
    # 2. Pattern discovery — scan workflows, suggest new skills
    # ------------------------------------------------------------------

    async def discover(self) -> List[Dict]:
        """Scan workflow history for recurring patterns not yet captured as skills.
        Returns a list of skill suggestions (not yet created)."""
        patterns = self._find_uncaptured_patterns()
        if not patterns:
            return []

        existing_names = {p["name"] for p in self.skills.list_packages()}
        builtin_str = ", ".join(sorted(_BUILTIN_NAMES))
        existing_str = ", ".join(sorted(existing_names)) if existing_names else "none"

        patterns_text = "\n\n".join(
            f"Pattern #{i+1} (seen {p['occurrences']} times):\n"
            f"  Tool sequence: {p['pattern']}\n"
            f"  Example queries: {'; '.join(s['user_message'] for s in p['sessions'][:3])}"
            for i, p in enumerate(patterns[:5])
        )

        prompt = f"""You are a skill-discovery assistant for a data analysis tool.

The following tool-call patterns have been observed repeatedly in user sessions
but are NOT yet captured as reusable skills:

{patterns_text}

Already available built-in skills: {builtin_str}
Already saved package skills: {existing_str}

For each pattern that would make a good reusable skill (not a duplicate, not trivial),
return a JSON array of suggestions:
[{{
  "pattern_index": 1,
  "name": "descriptive_snake_case_name",
  "description": "One sentence: what it does and when to use it.",
  "body": "Markdown instruction body for SKILL.md (same format as before)"
}}]

Only include genuinely useful, generalizable patterns. Return [] if none qualify.
Output ONLY valid JSON, no other text."""

        try:
            resp = await self.llm.chat([{"role": "user", "content": prompt}])
            raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", (resp.content or "").strip())
            suggestions = json.loads(raw)
            if not isinstance(suggestions, list):
                return []
            valid = []
            for s in suggestions:
                name = (s.get("name") or "").strip()
                if name and name not in existing_names and s.get("body"):
                    valid.append({
                        "name": name,
                        "description": (s.get("description") or "").strip(),
                        "body": (s.get("body") or "").strip(),
                        "pattern_index": s.get("pattern_index"),
                    })
            return valid
        except Exception:
            return []

    def _find_uncaptured_patterns(self) -> List[Dict]:
        """Find recurring patterns not already captured as a skill."""
        if not WORKFLOWS_DIR.exists():
            return []

        existing_bodies = set()
        for p in self.skills.list_packages():
            pkg = next((pk for pk in self.skills._packages if pk["slug"] == p["slug"]), None)
            if pkg and pkg.get("body"):
                tools_in_body = set(re.findall(r'`(\w+)`', pkg["body"])) & _BUILTIN_NAMES
                if tools_in_body:
                    existing_bodies.add(frozenset(tools_in_body))

        pattern_map: Dict[str, List[Dict]] = {}
        for f in WORKFLOWS_DIR.glob("*.json"):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
            except Exception:
                continue
            seq = data.get("tool_sequence", "")
            steps = data.get("steps", [])
            if not seq or len(steps) < 2:
                continue
            if data.get("skill_distilled"):
                continue
            pattern_map.setdefault(seq, []).append({
                "session_id": data["session_id"],
                "user_message": data["user_message"][:100],
                "feedback": data.get("user_feedback"),
            })

        uncaptured = []
        for pattern, sessions in pattern_map.items():
            if len(sessions) < 2:
                continue
            tools_in_pattern = set(pattern.replace(" → ", " ").split()) & _BUILTIN_NAMES
            if tools_in_pattern and frozenset(tools_in_pattern) in existing_bodies:
                continue
            feedbacks = [s["feedback"] for s in sessions if s["feedback"]]
            uncaptured.append({
                "pattern": pattern,
                "occurrences": len(sessions),
                "sessions": sessions[:5],
                "positive_rate": (
                    sum(1 for fb in feedbacks if fb == "good") / len(feedbacks)
                    if feedbacks else None
                ),
            })
        uncaptured.sort(key=lambda x: x["occurrences"], reverse=True)
        return uncaptured

    # ------------------------------------------------------------------
    # 3. Feedback-driven skill improvement
    # ------------------------------------------------------------------

    async def try_improve(self, slug: str) -> Optional[Dict]:
        """Analyse bad-feedback workflows that used a skill and generate
        an improved version of that skill's SKILL.md body.
        Returns the upgrade result dict or None."""
        pkg = next((p for p in self.skills._packages if p["slug"] == slug), None)
        if not pkg:
            return None

        bad_workflows = self._find_bad_workflows_for_skill(slug, pkg["body"])
        if len(bad_workflows) < 1:
            return None

        workflows_text = "\n\n".join(
            f"Session {w['session_id']}:\n"
            f"  User asked: {w['user_message'][:150]}\n"
            f"  Tool sequence: {w.get('tool_sequence', '')}\n"
            f"  Feedback detail: {w.get('feedback_detail', '(none)')}"
            for w in bad_workflows[:5]
        )

        prompt = f"""You are a skill-improvement assistant for a data analysis tool.

The following skill has received negative user feedback:

Skill name: {pkg['name']}
Current SKILL.md body:
{pkg['body']}

Bad-feedback sessions that used this skill pattern:
{workflows_text}

Analyse what went wrong and produce an IMPROVED version of the skill body.
The new body should:
- Fix the patterns that led to bad results
- Be more specific about edge cases and error handling
- Keep the same overall structure (## headers, numbered steps)
- Reference the same built-in tools

Return JSON:
{{
  "improved": true,
  "reason": "Brief explanation of what was improved",
  "body": "The complete new SKILL.md body (markdown)"
}}

If the skill is fine and failures were due to external factors: {{"improved": false}}

Output ONLY valid JSON, no other text."""

        try:
            resp = await self.llm.chat([{"role": "user", "content": prompt}])
            raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", (resp.content or "").strip())
            result = json.loads(raw)

            if not result.get("improved"):
                return None

            new_body = (result.get("body") or "").strip()
            reason = (result.get("reason") or "").strip()
            if not new_body:
                return None

            return self.skills.upgrade_package(slug, new_body, reason)
        except Exception:
            return None

    def _find_bad_workflows_for_skill(self, slug: str, skill_body: str) -> List[Dict]:
        """Find workflows with bad feedback whose tool sequence overlaps with a skill."""
        if not WORKFLOWS_DIR.exists():
            return []
        tools_in_skill = set(re.findall(r'`(\w+)`', skill_body)) & _BUILTIN_NAMES
        if not tools_in_skill:
            return []

        bad = []
        for f in WORKFLOWS_DIR.glob("*.json"):
            try:
                data = json.loads(f.read_text(encoding="utf-8"))
            except Exception:
                continue
            if data.get("user_feedback") != "bad":
                continue
            seq = data.get("tool_sequence", "")
            tools_in_wf = set(seq.replace(" → ", " ").split()) & _BUILTIN_NAMES
            overlap = tools_in_skill & tools_in_wf
            if len(overlap) >= 2:
                bad.append(data)
        return bad

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

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
