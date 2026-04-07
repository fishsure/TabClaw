"""Hook runner — discover and execute OpenClaw/ClawHub-compatible hook scripts.

Supports two hook formats:
  1. scripts/activator.sh      → fires on every user prompt (UserPromptSubmit)
     scripts/error-detector.sh → fires after every tool call (PostToolUse)
  2. hooks/<platform>/HOOK.md  → parses `metadata.events`; runs handler.sh if present
     (TypeScript/JS handlers are silently skipped; only shell scripts are executed)
"""
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Dict, List


# Map raw event names to internal event keys
_EVENT_MAP: Dict[str, str] = {
    "agent:bootstrap": "user_prompt",   # treat bootstrap as per-request injection
    "UserPromptSubmit": "user_prompt",
    "PostToolUse": "post_tool",
    "command:new": "new_session",
    "command:reset": "reset",
}

# Well-known script names in scripts/ → event
_NAMED_SCRIPTS: List[tuple] = [
    ("activator.sh", "user_prompt"),
    ("error-detector.sh", "post_tool"),
]


def _parse_hook_md(text: str) -> Dict:
    """Extract event list from HOOK.md frontmatter."""
    fm: Dict = {}
    match = re.match(r"^---\s*\n(.*?)\n---\s*\n", text, re.DOTALL)
    if match:
        for line in match.group(1).splitlines():
            if ":" in line:
                key, _, val = line.partition(":")
                fm[key.strip()] = val.strip().strip('"').strip("'")

    events: List[str] = []
    meta_str = fm.get("metadata", "")
    if meta_str:
        try:
            meta = json.loads(meta_str)
            # {"events": [...]} or {"openclaw": {"events": [...]}}
            if "events" in meta:
                events = meta["events"]
            elif isinstance(meta.get("openclaw"), dict):
                events = meta["openclaw"].get("events", [])
        except Exception:
            pass

    return {
        "events": events,
        "name": fm.get("name", ""),
        "description": fm.get("description", ""),
    }


def get_skill_hooks(skill_dir: Path) -> List[Dict]:
    """Return a list of runnable hook descriptors for the given skill directory.

    Each descriptor:  {event, script, name, source}
    event: "user_prompt" | "post_tool" | "new_session" | "reset"
    """
    hooks: List[Dict] = []

    # 1. Named scripts in scripts/ — highest priority, most universally supported
    scripts_dir = skill_dir / "scripts"
    if scripts_dir.is_dir():
        for script_name, event in _NAMED_SCRIPTS:
            script_path = scripts_dir / script_name
            if script_path.exists():
                hooks.append({
                    "event": event,
                    "script": str(script_path),
                    "name": script_name,
                    "source": "scripts",
                })

    # 2. hooks/<platform>/HOOK.md with an executable handler.sh
    hooks_dir = skill_dir / "hooks"
    if hooks_dir.is_dir():
        for platform_dir in sorted(hooks_dir.iterdir()):
            if not platform_dir.is_dir():
                continue
            hook_md_path = platform_dir / "HOOK.md"
            if not hook_md_path.exists():
                continue
            handler_sh = platform_dir / "handler.sh"
            if not handler_sh.exists():
                continue  # skip TS/JS handlers we cannot execute
            parsed = _parse_hook_md(hook_md_path.read_text(encoding="utf-8"))
            for raw_event in parsed["events"]:
                event = _EVENT_MAP.get(raw_event)
                if not event:
                    continue
                # Avoid duplicating events already covered by scripts/
                already = any(
                    h["event"] == event and h["source"] == "scripts" for h in hooks
                )
                if not already:
                    hooks.append({
                        "event": event,
                        "script": str(handler_sh),
                        "name": parsed["name"] or platform_dir.name,
                        "source": "hooks",
                    })

    return hooks


def _run_script(script_path: str, cwd: str, env_extra: Dict = None) -> str:
    """Execute a shell script, return its stdout (empty string on failure/timeout)."""
    script = Path(script_path)
    if not script.exists():
        return ""
    try:
        script.chmod(script.stat().st_mode | 0o111)
        env = os.environ.copy()
        env["SKILL_DIR"] = cwd
        if env_extra:
            for k, v in env_extra.items():
                env[k] = str(v)
        result = subprocess.run(
            ["bash", str(script)],
            capture_output=True,
            text=True,
            timeout=10,
            cwd=cwd,
            env=env,
        )
        return result.stdout.strip()
    except Exception:
        return ""


def run_event_hooks(packages: List[Dict], event: str, tool_output: str = "") -> str:
    """Run all enabled hooks matching `event` across all packages.

    Returns the combined stdout from every matching script (separated by blank lines).
    """
    outputs: List[str] = []
    for pkg in packages:
        if not pkg.get("enabled"):
            continue
        skill_dir = pkg.get("skill_dir", "")
        if not skill_dir:
            continue
        for hook in pkg.get("hooks", []):
            if hook.get("event") != event:
                continue
            script = hook.get("script")
            if not script:
                continue
            env_extra: Dict = {}
            if tool_output:
                env_extra["CLAUDE_TOOL_OUTPUT"] = tool_output
                env_extra["TOOL_OUTPUT"] = tool_output
            out = _run_script(script, skill_dir, env_extra)
            if out:
                outputs.append(out)
    return "\n\n".join(outputs)
