"""
WorkflowRecorder — records complete analysis sessions and persists them
to data/workflows/<session_id>.json for pattern mining and skill evolution.

Each workflow captures:
  user intent → clarification → plan → tool calls → conclusion → feedback
"""
import json
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

WORKFLOWS_DIR = Path(__file__).parent.parent / "data" / "workflows"


class StepRecord:
    __slots__ = ("tool_name", "params", "result_summary", "produced_table", "duration_ms")

    def __init__(self, tool_name: str, params: dict, result_summary: str = "",
                 produced_table: Optional[str] = None, duration_ms: int = 0):
        self.tool_name = tool_name
        self.params = params
        self.result_summary = result_summary
        self.produced_table = produced_table
        self.duration_ms = duration_ms

    def to_dict(self) -> dict:
        return {
            "tool_name": self.tool_name,
            "params": self.params,
            "result_summary": self.result_summary[:300],
            "produced_table": self.produced_table,
            "duration_ms": self.duration_ms,
        }


class WorkflowRecord:
    def __init__(self, user_message: str, tables_involved: List[str]):
        self.session_id = uuid.uuid4().hex[:12]
        self.timestamp = datetime.now(timezone.utc).isoformat()
        self.user_message = user_message
        self.tables_involved = tables_involved
        self.plan: Optional[List[dict]] = None
        self.steps: List[StepRecord] = []
        self.final_conclusion = ""
        self.skills_used: List[str] = []
        self.skill_distilled: Optional[str] = None
        self.user_feedback: Optional[str] = None
        self.feedback_detail: Optional[str] = None
        self._start_time = time.monotonic()
        self.duration_ms = 0

    def add_step(self, step: StepRecord):
        self.steps.append(step)

    def finish(self, conclusion: str = ""):
        self.final_conclusion = conclusion
        self.duration_ms = int((time.monotonic() - self._start_time) * 1000)

    def tool_sequence_fingerprint(self) -> str:
        """Abstract the tool call sequence into a comparable pattern string."""
        return " → ".join(s.tool_name for s in self.steps)

    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "timestamp": self.timestamp,
            "user_message": self.user_message,
            "tables_involved": self.tables_involved,
            "plan": self.plan,
            "steps": [s.to_dict() for s in self.steps],
            "final_conclusion": self.final_conclusion[:500],
            "tool_sequence": self.tool_sequence_fingerprint(),
            "skills_used": self.skills_used,
            "skill_distilled": self.skill_distilled,
            "user_feedback": self.user_feedback,
            "feedback_detail": self.feedback_detail,
            "duration_ms": self.duration_ms,
        }

    def save(self):
        WORKFLOWS_DIR.mkdir(parents=True, exist_ok=True)
        path = WORKFLOWS_DIR / f"{self.session_id}.json"
        path.write_text(json.dumps(self.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")


def load_workflow(session_id: str) -> Optional[Dict]:
    path = WORKFLOWS_DIR / f"{session_id}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def update_workflow_feedback(session_id: str, feedback: str, detail: str = "") -> bool:
    path = WORKFLOWS_DIR / f"{session_id}.json"
    if not path.exists():
        return False
    data = json.loads(path.read_text(encoding="utf-8"))
    data["user_feedback"] = feedback
    if detail:
        data["feedback_detail"] = detail
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return True


def list_workflows(limit: int = 50) -> List[Dict]:
    if not WORKFLOWS_DIR.exists():
        return []
    files = sorted(WORKFLOWS_DIR.glob("*.json"), key=lambda f: f.stat().st_mtime, reverse=True)
    results = []
    for f in files[:limit]:
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            results.append({
                "session_id": data["session_id"],
                "timestamp": data["timestamp"],
                "user_message": data["user_message"][:100],
                "tool_count": len(data.get("steps", [])),
                "tool_sequence": data.get("tool_sequence", ""),
                "skill_distilled": data.get("skill_distilled"),
                "user_feedback": data.get("user_feedback"),
                "duration_ms": data.get("duration_ms", 0),
            })
        except Exception:
            continue
    return results


def find_recurring_patterns(min_occurrences: int = 2) -> List[Dict]:
    """Scan all workflows and find recurring tool-call sequences."""
    if not WORKFLOWS_DIR.exists():
        return []

    pattern_map: Dict[str, List[Dict]] = {}
    for f in WORKFLOWS_DIR.glob("*.json"):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        seq = data.get("tool_sequence", "")
        if not seq or len(data.get("steps", [])) < 2:
            continue
        pattern_map.setdefault(seq, []).append({
            "session_id": data["session_id"],
            "user_message": data["user_message"][:100],
            "feedback": data.get("user_feedback"),
        })

    recurring = []
    for pattern, sessions in pattern_map.items():
        if len(sessions) >= min_occurrences:
            feedbacks = [s["feedback"] for s in sessions if s["feedback"]]
            recurring.append({
                "pattern": pattern,
                "occurrences": len(sessions),
                "sessions": sessions[:5],
                "positive_rate": (
                    sum(1 for f in feedbacks if f == "good") / len(feedbacks)
                    if feedbacks else None
                ),
            })

    recurring.sort(key=lambda x: x["occurrences"], reverse=True)
    return recurring


def get_growth_profile() -> Dict[str, Any]:
    """Aggregate workflow data into a growth profile for the UI."""
    if not WORKFLOWS_DIR.exists():
        return {"total_sessions": 0, "skills_learned": 0, "satisfaction_rate": None,
                "recent_events": [], "tool_frequency": {}}

    total = 0
    skills_learned = 0
    good = 0
    bad = 0
    tool_freq: Dict[str, int] = {}
    recent_events: List[Dict] = []

    for f in sorted(WORKFLOWS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        total += 1
        fb = data.get("user_feedback")
        if fb == "good":
            good += 1
        elif fb == "bad":
            bad += 1

        if data.get("skill_distilled"):
            skills_learned += 1
            recent_events.append({
                "date": data["timestamp"][:10],
                "event": f"学会了「{data['skill_distilled']}」",
                "type": "skill_learned",
            })

        for step in data.get("steps", []):
            name = step.get("tool_name", "")
            if name:
                tool_freq[name] = tool_freq.get(name, 0) + 1

    total_feedback = good + bad
    return {
        "total_sessions": total,
        "skills_learned": skills_learned,
        "satisfaction_rate": round(good / total_feedback, 2) if total_feedback else None,
        "feedback_counts": {"good": good, "bad": bad},
        "recent_events": recent_events[:10],
        "tool_frequency": dict(sorted(tool_freq.items(), key=lambda x: -x[1])[:15]),
    }
