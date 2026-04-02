"""Skill registry — manages built-in, custom, and package (instruction) skills."""
import json
import re
import shutil
import zipfile
from io import BytesIO
from pathlib import Path
from typing import Dict, List, Any, Optional

from skills.builtin import BUILTIN_SKILLS
from skills.code_skill import execute_python

DATA_PATH = Path(__file__).parent.parent / "data" / "custom_skills.json"
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
        self._custom: List[Dict] = self._load_custom()
        self._packages: List[Dict] = self._load_packages()

    def _load_custom(self) -> List[Dict]:
        if DATA_PATH.exists():
            with open(DATA_PATH) as f:
                return json.load(f)
        return []

    def _save_custom(self):
        DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(DATA_PATH, "w") as f:
            json.dump(self._custom, f, indent=2, ensure_ascii=False)

    # ------------------------------------------------------------------
    # Package (instruction) skills — ClawdHub-compatible directory format
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
                "enabled": state.get("enabled", True),
                "body": parsed["body"],
                "type": "package",
            })
        return packages

    def install_from_zip(self, zip_bytes: bytes) -> Dict:
        """Extract a ClawdHub skill zip into data/skills/<slug>/."""
        SKILLS_DIR.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(BytesIO(zip_bytes)) as zf:
            names = zf.namelist()
            # Find SKILL.md — may be at root or inside a single top-level directory
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

            # Determine slug from _meta.json or frontmatter name
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
        return [{k: v for k, v in p.items() if k != "body"} for p in self._packages]

    def get_instruction_context(self) -> str:
        """Return combined SKILL.md bodies from all enabled package skills,
        suitable for injection into the system prompt."""
        parts = []
        for p in self._packages:
            if p.get("enabled") and p.get("body"):
                parts.append(f"### Skill: {p['name']}\n{p['body']}")
        return "\n\n".join(parts)

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
        return {"builtin": builtin, "custom": self._custom, "packages": packages}

    def list_custom(self) -> List[Dict]:
        return self._custom

    def get_tool_definitions(self, code_tool: bool = False) -> List[Dict]:
        """Return OpenAI-format tool definitions for all enabled skills."""
        if code_tool:
            # Only keep table_info for structure inspection; execute_python handles everything else
            table_info_def = next(d for d in BUILTIN_TOOL_DEFS if d["function"]["name"] == "table_info")
            defs = [table_info_def, CODE_TOOL_DEF]
        else:
            defs = list(BUILTIN_TOOL_DEFS)

        # Always register custom skills as callable tools
        for s in self._custom:
            mode_hint = "Executes Python code." if s.get("code") else "Uses an LLM sub-call guided by a custom prompt."
            defs.append({
                "type": "function",
                "function": {
                    "name": s["name"],
                    "description": f"{s['description']} ({mode_hint})",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "table_id": {"type": "string", "description": "ID of the table to work with"},
                            "user_request": {"type": "string", "description": "Specific instructions or context for this invocation"},
                        },
                        "required": [],
                    },
                },
            })
        return defs

    def add_custom(self, skill_id: str, skill: Dict) -> Dict:
        entry = {"id": skill_id, "type": "custom", **skill}
        self._custom.append(entry)
        self._save_custom()
        return entry

    def update_custom(self, skill_id: str, skill: Dict) -> Dict:
        for i, s in enumerate(self._custom):
            if s["id"] == skill_id:
                self._custom[i] = {"id": skill_id, "type": "custom", **skill}
                self._save_custom()
                return self._custom[i]
        raise ValueError(f"Custom skill '{skill_id}' not found")

    def delete_custom(self, skill_id: str) -> Dict:
        before = len(self._custom)
        self._custom = [s for s in self._custom if s["id"] != skill_id]
        if len(self._custom) == before:
            raise ValueError(f"Custom skill '{skill_id}' not found")
        self._save_custom()
        return {"status": "deleted"}

    def clear_custom(self) -> Dict:
        count = len(self._custom)
        self._custom = []
        self._save_custom()
        return {"cleared": count}

    def execute_sync(self, skill_name: str, params: Dict, tables: Dict) -> Any:
        """Execute a built-in or code skill synchronously."""
        if skill_name == "execute_python":
            return execute_python(params, tables)
        if skill_name not in BUILTIN_SKILLS:
            raise ValueError(f"Unknown skill '{skill_name}'")
        return BUILTIN_SKILLS[skill_name](params, tables)
