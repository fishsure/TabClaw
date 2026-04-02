"""Sandboxed workspace file tools — read/write within data/workspace/.

These tools let the LLM persist notes, learnings, and logs across sessions.
All paths are relative to WORKSPACE_DIR; path traversal is rejected.

Typical usage by package skills:
  write_file  ".learnings/ERRORS.md"        append an error entry
  write_file  ".learnings/LEARNINGS.md"     append a learning entry
  write_file  ".learnings/FEATURE_REQUESTS.md"  append a feature request
  read_file   ".learnings/LEARNINGS.md"     review past learnings
  list_files  "."                           see all workspace files
"""
from pathlib import Path
from typing import Any, Dict, List

WORKSPACE_DIR = Path(__file__).parent.parent / "data" / "workspace"


def _resolve_safe(rel_path: str) -> Path:
    """Resolve a workspace-relative path, blocking traversal outside WORKSPACE_DIR."""
    WORKSPACE_DIR.mkdir(parents=True, exist_ok=True)
    resolved = (WORKSPACE_DIR / rel_path).resolve()
    if not str(resolved).startswith(str(WORKSPACE_DIR.resolve())):
        raise ValueError(f"Path traversal not allowed: {rel_path}")
    return resolved


def read_file(params: Dict, _tables: Dict) -> Dict:
    path_str = (params.get("path") or "").strip()
    if not path_str:
        return {"text": "Error: path is required"}
    try:
        path = _resolve_safe(path_str)
        if not path.exists():
            return {"text": f"File not found: {path_str}\n(Use write_file to create it first)"}
        content = path.read_text(encoding="utf-8")
        return {"text": content or "(empty file)"}
    except Exception as e:
        return {"text": f"Error reading file: {e}"}


def write_file(params: Dict, _tables: Dict) -> Dict:
    path_str = (params.get("path") or "").strip()
    content: str = params.get("content", "")
    mode: str = params.get("mode", "append")
    if not path_str:
        return {"text": "Error: path is required"}
    try:
        path = _resolve_safe(path_str)
        path.parent.mkdir(parents=True, exist_ok=True)
        if mode == "write":
            path.write_text(content, encoding="utf-8")
            action = "Written"
        else:
            with open(path, "a", encoding="utf-8") as f:
                f.write(content)
            action = "Appended to"
        return {"text": f"{action}: {path_str} ({len(content)} chars)"}
    except Exception as e:
        return {"text": f"Error writing file: {e}"}


def list_files(params: Dict, _tables: Dict) -> Dict:
    path_str = (params.get("path") or ".").strip()
    try:
        path = _resolve_safe(path_str)
        if not path.exists():
            return {"text": f"Directory not found: {path_str} (workspace may be empty)"}
        if not path.is_dir():
            return {"text": f"Not a directory: {path_str}"}
        items: List[str] = []
        for f in sorted(path.rglob("*")):
            if f.is_file():
                rel = f.relative_to(WORKSPACE_DIR)
                items.append(f"  {rel}  ({f.stat().st_size} bytes)")
        return {"text": "\n".join(items) if items else "(workspace is empty)"}
    except Exception as e:
        return {"text": f"Error listing files: {e}"}


WORKSPACE_TOOL_DEFS: List[Dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "read_file",
            "description": (
                "Read a file from the workspace. "
                "Use for .learnings/, session logs, notes, or any persisted workspace file. "
                "Path is relative to workspace root (e.g. '.learnings/ERRORS.md')."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Relative path within workspace (e.g. '.learnings/ERRORS.md')",
                    },
                },
                "required": ["path"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "write_file",
            "description": (
                "Write or append to a file in the workspace. "
                "Use to log entries to .learnings/ERRORS.md, .learnings/LEARNINGS.md, "
                ".learnings/FEATURE_REQUESTS.md, or any notes file. "
                "Path is relative to workspace root; directories are created automatically."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Relative path in workspace (e.g. '.learnings/ERRORS.md')",
                    },
                    "content": {
                        "type": "string",
                        "description": "Content to write or append",
                    },
                    "mode": {
                        "type": "string",
                        "enum": ["append", "write"],
                        "description": "'append' adds to end of file (default); 'write' overwrites",
                        "default": "append",
                    },
                },
                "required": ["path", "content"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "list_files",
            "description": "List all files in the workspace directory (or a subdirectory).",
            "parameters": {
                "type": "object",
                "properties": {
                    "path": {
                        "type": "string",
                        "description": "Subdirectory to list (default '.' for workspace root)",
                        "default": ".",
                    },
                },
                "required": [],
            },
        },
    },
]

WORKSPACE_SKILLS: Dict[str, Any] = {
    "read_file": read_file,
    "write_file": write_file,
    "list_files": list_files,
}
