"""Skill registry — manages built-in and package (SKILL.md) skills.

All user-created and auto-distilled skills are stored as SKILL.md packages
in data/skills/<slug>/, compatible with the OpenClaw/ClawHub format.
"""
import json
import re
import shutil
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Any, Optional

from skills.builtin import BUILTIN_SKILLS
from skills.code_skill import execute_python
from skills.hook_runner import get_skill_hooks, run_event_hooks as _run_event_hooks
from skills.workspace_tools import WORKSPACE_TOOL_DEFS, WORKSPACE_SKILLS, WORKSPACE_DIR

SKILLS_DIR = Path(__file__).parent.parent / "data" / "skills"


def _parse_skill_md(text: str) -> Dict:
    """Parse YAML frontmatter from a SKILL.md file and return {name, description, metadata, body}."""
    fm: Dict[str, Any] = {}
    body = text
    match = re.match(r"^---\s*\n(.*?)\n---\s*\n", text, re.DOTALL)
    if match:
        body = text[match.end():]
        for line in match.group(1).splitlines():
            if ":" in line:
                key, _, val = line.partition(":")
                fm[key.strip()] = val.strip().strip('"').strip("'")
    return {
        "name": fm.get("name", ""),
        "description": fm.get("description", ""),
        "metadata": fm,
        "body": body.strip(),
    }


# OpenAI-format tool definitions for every built-in skill
BUILTIN_TOOL_DEFS = [
    {
        "type": "function",
        "function": {
            "name": "table_info",
            "description": "Get metadata (shape, columns, dtypes, missing values, sample rows) for a table.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_id": {"type": "string", "description": "ID of the table to inspect"},
                },
                "required": ["table_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "filter_rows",
            "description": "Filter rows using a pandas query string (e.g. 'age > 30 and city == \"NYC\"').",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_id": {"type": "string"},
                    "condition": {"type": "string", "description": "Pandas query expression"},
                    "result_name": {"type": "string", "description": "Name for the resulting table"},
                },
                "required": ["table_id", "condition"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "select_columns",
            "description": "Select a subset of columns from a table.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_id": {"type": "string"},
                    "columns": {"type": "array", "items": {"type": "string"}, "description": "List of column names"},
                    "result_name": {"type": "string"},
                },
                "required": ["table_id", "columns"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "aggregate",
            "description": "Group by columns and aggregate with functions like sum, mean, count, max, min.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_id": {"type": "string"},
                    "group_by": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Columns to group by",
                    },
                    "agg_config": {
                        "type": "object",
                        "description": "Dict of {column_name: agg_function} e.g. {\"sales\": \"sum\", \"qty\": \"mean\"}",
                    },
                    "result_name": {"type": "string"},
                },
                "required": ["table_id", "group_by", "agg_config"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "sort_table",
            "description": "Sort a table by one or more columns.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_id": {"type": "string"},
                    "by": {
                        "oneOf": [
                            {"type": "string"},
                            {"type": "array", "items": {"type": "string"}},
                        ],
                        "description": "Column name or list of column names to sort by",
                    },
                    "ascending": {"type": "boolean", "default": True},
                    "result_name": {"type": "string"},
                },
                "required": ["table_id", "by"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "merge_tables",
            "description": "Merge/join two tables (inner, left, right, outer join).",
            "parameters": {
                "type": "object",
                "properties": {
                    "left_table_id": {"type": "string"},
                    "right_table_id": {"type": "string"},
                    "on": {"type": "string", "description": "Column to join on (if same name in both)"},
                    "left_on": {"type": "string"},
                    "right_on": {"type": "string"},
                    "how": {"type": "string", "enum": ["inner", "left", "right", "outer"], "default": "inner"},
                    "result_name": {"type": "string"},
                },
                "required": ["left_table_id", "right_table_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "pivot_table",
            "description": "Create a pivot table.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_id": {"type": "string"},
                    "index": {
                        "oneOf": [{"type": "string"}, {"type": "array", "items": {"type": "string"}}]
                    },
                    "columns": {"type": "string"},
                    "values": {
                        "oneOf": [{"type": "string"}, {"type": "array", "items": {"type": "string"}}]
                    },
                    "aggfunc": {"type": "string", "default": "sum"},
                    "result_name": {"type": "string"},
                },
                "required": ["table_id", "index", "values"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "add_column",
            "description": "Add a new computed column using a pandas eval expression.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_id": {"type": "string"},
                    "column_name": {"type": "string", "description": "Name for the new column"},
                    "expression": {
                        "type": "string",
                        "description": "Pandas eval expression, e.g. 'price * quantity'",
                    },
                    "result_name": {"type": "string"},
                },
                "required": ["table_id", "column_name", "expression"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "describe_stats",
            "description": "Return descriptive statistics (count, mean, std, min, max, quartiles) for a table.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_id": {"type": "string"},
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional: only include these columns",
                    },
                },
                "required": ["table_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "find_values",
            "description": "Find rows where a column equals a value or matches a regex pattern.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_id": {"type": "string"},
                    "column": {"type": "string"},
                    "value": {"description": "Exact value to search for"},
                    "pattern": {"type": "string", "description": "Regex pattern (case-insensitive)"},
                    "result_name": {"type": "string"},
                },
                "required": ["table_id", "column"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "drop_duplicates",
            "description": "Remove duplicate rows from a table.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_id": {"type": "string"},
                    "subset": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional: only consider these columns for duplicates",
                    },
                    "result_name": {"type": "string"},
                },
                "required": ["table_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "rename_columns",
            "description": "Rename one or more columns in a table.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_id": {"type": "string"},
                    "rename_map": {
                        "type": "object",
                        "description": "Dict of {old_name: new_name}",
                    },
                    "result_name": {"type": "string"},
                },
                "required": ["table_id", "rename_map"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "sample_rows",
            "description": "Get a random sample of N rows from a table.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_id": {"type": "string"},
                    "n": {"type": "integer", "default": 10},
                    "result_name": {"type": "string"},
                },
                "required": ["table_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "value_counts",
            "description": "Count occurrences of each unique value in a column.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_id": {"type": "string"},
                    "column": {"type": "string"},
                    "result_name": {"type": "string"},
                },
                "required": ["table_id", "column"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "correlation_matrix",
            "description": "Compute a Pearson correlation matrix for numeric columns.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_id": {"type": "string"},
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional: only include these columns",
                    },
                    "result_name": {"type": "string"},
                },
                "required": ["table_id"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "head_rows",
            "description": "Get the first N rows of a table.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_id": {"type": "string"},
                    "n": {"type": "integer", "default": 10},
                    "result_name": {"type": "string"},
                },
                "required": ["table_id"],
            },
        },
    },
]

BUILTIN_META = {
    "table_info": {"description": "Get table metadata (shape, columns, dtypes, sample)", "category": "inspection"},
    "filter_rows": {"description": "Filter rows using a query condition", "category": "transformation"},
    "select_columns": {"description": "Select a subset of columns", "category": "transformation"},
    "aggregate": {"description": "Group by and aggregate (sum, mean, count…)", "category": "analysis"},
    "sort_table": {"description": "Sort rows by one or more columns", "category": "transformation"},
    "merge_tables": {"description": "Join/merge two tables", "category": "transformation"},
    "pivot_table": {"description": "Create a pivot table", "category": "analysis"},
    "add_column": {"description": "Add a computed column via expression", "category": "transformation"},
    "describe_stats": {"description": "Descriptive statistics (mean, std, quartiles…)", "category": "analysis"},
    "find_values": {"description": "Find rows matching a value or regex pattern", "category": "inspection"},
    "drop_duplicates": {"description": "Remove duplicate rows", "category": "cleaning"},
    "rename_columns": {"description": "Rename columns", "category": "cleaning"},
    "sample_rows": {"description": "Get a random sample of rows", "category": "inspection"},
    "value_counts": {"description": "Count unique values in a column", "category": "analysis"},
    "correlation_matrix": {"description": "Pearson correlation matrix for numeric columns", "category": "analysis"},
    "head_rows": {"description": "Get the first N rows", "category": "inspection"},
}


# Tool definition for the optional sandboxed code execution skill
CODE_TOOL_DEF = {
    "type": "function",
    "function": {
        "name": "execute_python",
        "description": (
            "Execute sandboxed Python code for advanced table analysis or operations. "
            "Available libraries: pd (pandas), np (numpy), math, re, json, "
            "collections, itertools, datetime, statistics. "
            "Each uploaded table is pre-loaded as a variable: accessible by its ID "
            "(e.g. 'r_abc123') AND by its sanitised name (e.g. 'sales_2023'). "
            "To produce a new result table, assign a DataFrame to the variable 'result'. "
            "Use print() for intermediate output. Do NOT import os, sys, subprocess, "
            "or any network/file-system libraries."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": "Python code to execute",
                },
                "result_name": {
                    "type": "string",
                    "description": "Descriptive name for the result table (if 'result' is assigned)",
                },
            },
            "required": ["code"],
        },
    },
}


class SkillRegistry:
    def __init__(self):
        self._packages: List[Dict] = self._load_packages()

    # ------------------------------------------------------------------
    # Package (instruction) skills — ClawHub / OpenClaw compatible
    # ------------------------------------------------------------------

    def _load_packages(self) -> List[Dict]:
        """Scan data/skills/*/SKILL.md and load metadata for each."""
        packages: List[Dict] = []
        if not SKILLS_DIR.exists():
            return packages
        for skill_dir in sorted(SKILLS_DIR.iterdir()):
            if not skill_dir.is_dir():
                continue
            skill_md = skill_dir / "SKILL.md"
            if not skill_md.exists():
                continue
            parsed = _parse_skill_md(skill_md.read_text(encoding="utf-8"))
            slug = skill_dir.name
            meta: Dict[str, Any] = {}
            meta_path = skill_dir / "_meta.json"
            if meta_path.exists():
                try:
                    meta = json.loads(meta_path.read_text(encoding="utf-8"))
                except Exception:
                    pass
            state_path = skill_dir / "_state.json"
            state: Dict[str, Any] = {"enabled": True}
            if state_path.exists():
                try:
                    state = json.loads(state_path.read_text(encoding="utf-8"))
                except Exception:
                    pass
            packages.append({
                "slug": slug,
                "name": parsed["name"] or slug,
                "description": parsed["description"],
                "version": meta.get("version", ""),
                "source": meta.get("source", "manual"),
                "enabled": state.get("enabled", True),
                "body": parsed["body"],
                "type": "package",
                "skill_dir": str(skill_dir),
                "hooks": get_skill_hooks(skill_dir),
            })
        return packages

    def create_package(self, name: str, description: str, body: str, source: str = "manual") -> Dict:
        """Create a new SKILL.md package directly from name/description/body."""
        SKILLS_DIR.mkdir(parents=True, exist_ok=True)
        slug = re.sub(r"[^a-z0-9_-]", "-", name.lower().replace(" ", "-")).strip("-")
        if not slug:
            slug = "skill"

        # Make slug unique if it already exists
        base_slug = slug
        counter = 1
        while (SKILLS_DIR / slug).exists():
            slug = f"{base_slug}-{counter}"
            counter += 1

        dest = SKILLS_DIR / slug
        dest.mkdir(parents=True)

        skill_md_content = f"""---
name: {name}
description: "{description}"
---

{body}
"""
        (dest / "SKILL.md").write_text(skill_md_content, encoding="utf-8")
        (dest / "_meta.json").write_text(
            json.dumps({"slug": slug, "source": source}), encoding="utf-8"
        )

        self._packages = self._load_packages()
        pkg = next((p for p in self._packages if p["slug"] == slug), None)
        return pkg or {"slug": slug, "name": name, "description": description, "status": "created"}

    def install_from_zip(self, zip_bytes: bytes) -> Dict:
        """Extract a ClawHub skill zip into data/skills/<slug>/."""
        SKILLS_DIR.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
            skill_md_path: Optional[str] = None
            prefix = ""
            for n in names:
                if n == "SKILL.md" or n.endswith("/SKILL.md"):
                    parts = n.split("/")
                    if len(parts) <= 2:
                        skill_md_path = n
                        prefix = "/".join(parts[:-1])
                        if prefix:
                            prefix += "/"
                        break
            if not skill_md_path:
                raise ValueError("No SKILL.md found in zip archive")

            parsed = _parse_skill_md(zf.read(skill_md_path).decode("utf-8"))

            slug = ""
            meta_json_path = prefix + "_meta.json" if prefix else "_meta.json"
            if meta_json_path in names:
                try:
                    meta = json.loads(zf.read(meta_json_path).decode("utf-8"))
                    slug = meta.get("slug", "")
                except Exception:
                    pass
            if not slug:
                slug = re.sub(r"[^a-z0-9_-]", "-", (parsed["name"] or "skill").lower().replace(" ", "-"))

            dest = SKILLS_DIR / slug
            if dest.exists():
                shutil.rmtree(dest)
            dest.mkdir(parents=True)

            for member in names:
                if member.endswith("/"):
                    continue
                rel = member[len(prefix):] if prefix and member.startswith(prefix) else member
                out = dest / rel
                out.parent.mkdir(parents=True, exist_ok=True)
                out.write_bytes(zf.read(member))

        # Copy .learnings/ templates to workspace if they don't exist there yet
        learnings_src = dest / ".learnings"
        if learnings_src.is_dir():
            workspace_learnings = WORKSPACE_DIR / ".learnings"
            workspace_learnings.mkdir(parents=True, exist_ok=True)
            for tmpl in learnings_src.iterdir():
                if tmpl.is_file():
                    dest_file = workspace_learnings / tmpl.name
                    if not dest_file.exists():
                        shutil.copy2(str(tmpl), str(dest_file))

        self._packages = self._load_packages()
        pkg = next((p for p in self._packages if p["slug"] == slug), None)
        return pkg or {"slug": slug, "name": parsed["name"], "status": "installed"}

    def delete_package(self, slug: str) -> Dict:
        dest = SKILLS_DIR / slug
        if not dest.exists():
            raise ValueError(f"Package skill '{slug}' not found")
        shutil.rmtree(dest)
        self._packages = [p for p in self._packages if p["slug"] != slug]
        return {"status": "deleted"}

    def clear_packages(self) -> Dict:
        """Delete all package skills."""
        count = len(self._packages)
        if SKILLS_DIR.exists():
            for skill_dir in list(SKILLS_DIR.iterdir()):
                if skill_dir.is_dir():
                    shutil.rmtree(skill_dir)
        self._packages = []
        return {"cleared": count}

    def toggle_package(self, slug: str, enabled: bool) -> Dict:
        dest = SKILLS_DIR / slug
        if not dest.exists():
            raise ValueError(f"Package skill '{slug}' not found")
        state_path = dest / "_state.json"
        state_path.write_text(json.dumps({"enabled": enabled}), encoding="utf-8")
        for p in self._packages:
            if p["slug"] == slug:
                p["enabled"] = enabled
                break
        return {"slug": slug, "enabled": enabled}

    def list_packages(self) -> List[Dict]:
        _strip = {"body", "skill_dir", "hooks"}
        return [{k: v for k, v in p.items() if k not in _strip} for p in self._packages]

    def get_instruction_context(self) -> str:
        """Return combined SKILL.md bodies from all enabled package skills,
        suitable for injection into the system prompt."""
        parts = []
        for p in self._packages:
            if p.get("enabled") and p.get("body"):
                parts.append(f"### Skill: {p['name']}\n{p['body']}")
        return "\n\n".join(parts)

    def run_event_hooks(self, event: str, tool_output: str = "") -> str:
        """Run all enabled hook scripts for the given lifecycle event.

        event: "user_prompt" | "post_tool" | "new_session" | "reset"
        Returns combined stdout from all matching scripts.
        """
        return _run_event_hooks(self._packages, event, tool_output)

    def has_package_skills(self) -> bool:
        return any(p.get("enabled") for p in self._packages)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def list_all(self) -> Dict:
        params_lookup = {
            td["function"]["name"]: td["function"].get("parameters", {})
            for td in BUILTIN_TOOL_DEFS
        }
        builtin = [
            {
                "id": name,
                "name": name,
                "type": "builtin",
                **BUILTIN_META[name],
                "parameters": params_lookup.get(name, {}),
            }
            for name in BUILTIN_META
        ]
        packages = self.list_packages()
        return {"builtin": builtin, "packages": packages}

    def get_tool_definitions(self, code_tool: bool = False) -> List[Dict]:
        """Return OpenAI-format tool definitions for all available skills.

        Workspace file tools (read_file, write_file, list_files) are included
        whenever at least one package skill is enabled, so the LLM can persist
        learnings and notes as instructed by those skills.
        """
        if code_tool:
            table_info_def = next(d for d in BUILTIN_TOOL_DEFS if d["function"]["name"] == "table_info")
            tools = [table_info_def, CODE_TOOL_DEF]
        else:
            tools = list(BUILTIN_TOOL_DEFS)
        if self.has_package_skills():
            tools = tools + WORKSPACE_TOOL_DEFS
        return tools

    def execute_sync(self, skill_name: str, params: Dict, tables: Dict) -> Any:
        """Execute a built-in, workspace, or code skill synchronously."""
        if skill_name in WORKSPACE_SKILLS:
            return WORKSPACE_SKILLS[skill_name](params, tables)
        if skill_name == "execute_python":
            return execute_python(params, tables)
        if skill_name not in BUILTIN_SKILLS:
            raise ValueError(f"Unknown skill '{skill_name}'")
        return BUILTIN_SKILLS[skill_name](params, tables)
