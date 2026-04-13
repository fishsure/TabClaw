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


_DOMAIN_KEYWORDS: Dict[str, List[str]] = {
    "销售分析": ["sales", "revenue", "profit", "销售", "收入", "利润", "营收", "业绩", "订单", "gmv"],
    "HR/人才": ["employee", "salary", "hr", "人才", "薪资", "绩效", "部门", "员工", "hiring", "离职", "招聘"],
    "财务报表": ["finance", "cost", "budget", "财务", "成本", "预算", "报表", "支出", "资产", "费用"],
    "用户分析": ["user", "customer", "nps", "satisfaction", "用户", "客户", "满意度", "留存", "churn", "活跃"],
    "产品分析": ["product", "category", "rating", "产品", "品类", "评分", "库存", "sku", "商品"],
    "医疗健康": ["patient", "medical", "hospital", "患者", "医疗", "诊断", "病例", "健康", "药品"],
    "教育培训": ["student", "score", "exam", "学生", "成绩", "考试", "课程", "教育", "培训"],
    "物流运输": ["logistics", "shipping", "delivery", "物流", "运输", "配送", "仓储", "快递"],
    "营销推广": ["marketing", "campaign", "ad", "广告", "营销", "推广", "投放", "转化", "roi"],
}

_CUSTOM_DOMAINS_PATH = Path(__file__).parent.parent / "data" / "custom_domains.json"


def _load_custom_domains() -> Dict[str, List[str]]:
    if _CUSTOM_DOMAINS_PATH.exists():
        try:
            return json.loads(_CUSTOM_DOMAINS_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_custom_domains(domains: Dict[str, List[str]]):
    _CUSTOM_DOMAINS_PATH.parent.mkdir(parents=True, exist_ok=True)
    _CUSTOM_DOMAINS_PATH.write_text(
        json.dumps(domains, ensure_ascii=False, indent=2), encoding="utf-8",
    )


def _classify_domain(user_message: str, tables: List[str]) -> str:
    text = (user_message + " " + " ".join(tables)).lower()
    all_domains = {**_DOMAIN_KEYWORDS, **_load_custom_domains()}
    scores: Dict[str, int] = {}
    for domain, keywords in all_domains.items():
        if not keywords:
            continue
        scores[domain] = sum(1 for kw in keywords if kw in text)
    if not scores or max(scores.values()) == 0:
        return "通用数据"
    return max(scores, key=lambda d: scores[d])


def add_custom_domain(name: str, keywords: List[str]) -> Dict[str, List[str]]:
    """Add or update a user-defined domain with its keywords."""
    custom = _load_custom_domains()
    merged = list(set(custom.get(name, []) + keywords))
    custom[name] = merged
    _save_custom_domains(custom)
    return custom


def list_domains() -> Dict[str, List[str]]:
    """Return all domains (built-in + custom)."""
    result = {k: list(v) for k, v in _DOMAIN_KEYWORDS.items()}
    result.update(_load_custom_domains())
    return result


def get_growth_profile() -> Dict[str, Any]:
    """Aggregate workflow data into a rich growth profile for the UI."""
    if not WORKFLOWS_DIR.exists():
        return {
            "total_sessions": 0, "skills_learned": 0, "satisfaction_rate": None,
            "recent_events": [], "tool_frequency": {}, "domains": [],
            "milestones": [], "efficiency": {},
        }

    total = 0
    skills_learned = 0
    good = 0
    bad = 0
    tool_freq: Dict[str, int] = {}
    recent_events: List[Dict] = []
    domain_stats: Dict[str, Dict[str, Any]] = {}
    all_durations: List[int] = []
    all_step_counts: List[int] = []
    skill_reuse_count = 0
    upgrade_events: List[Dict] = []

    files = sorted(WORKFLOWS_DIR.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    for f in files:
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

        duration = data.get("duration_ms", 0)
        step_count = len(data.get("steps", []))
        all_durations.append(duration)
        all_step_counts.append(step_count)

        if data.get("skills_used"):
            skill_reuse_count += 1

        domain = _classify_domain(
            data.get("user_message", ""),
            data.get("tables_involved", []),
        )
        if domain not in domain_stats:
            domain_stats[domain] = {"sessions": 0, "good": 0, "bad": 0, "total_steps": 0}
        ds = domain_stats[domain]
        ds["sessions"] += 1
        ds["total_steps"] += step_count
        if fb == "good":
            ds["good"] += 1
        elif fb == "bad":
            ds["bad"] += 1

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

    # Domain proficiency
    domains = []
    for dname, ds in sorted(domain_stats.items(), key=lambda x: -x[1]["sessions"]):
        total_fb = ds["good"] + ds["bad"]
        proficiency = 0.0
        if total_fb > 0:
            proficiency = round(ds["good"] / total_fb, 2)
        elif ds["sessions"] > 0:
            proficiency = 0.5
        domains.append({
            "name": dname,
            "sessions": ds["sessions"],
            "proficiency": proficiency,
            "avg_steps": round(ds["total_steps"] / ds["sessions"], 1) if ds["sessions"] else 0,
        })

    # Efficiency metrics: compare first-half vs second-half of sessions
    efficiency: Dict[str, Any] = {}
    if len(all_durations) >= 4:
        mid = len(all_durations) // 2
        old_durations = all_durations[mid:]
        new_durations = all_durations[:mid]
        old_steps = all_step_counts[mid:]
        new_steps = all_step_counts[:mid]
        old_avg_d = sum(old_durations) / len(old_durations)
        new_avg_d = sum(new_durations) / len(new_durations)
        old_avg_s = sum(old_steps) / len(old_steps)
        new_avg_s = sum(new_steps) / len(new_steps)
        efficiency = {
            "early_avg_duration_ms": int(old_avg_d),
            "recent_avg_duration_ms": int(new_avg_d),
            "duration_change_pct": round((new_avg_d - old_avg_d) / old_avg_d * 100, 1) if old_avg_d else 0,
            "early_avg_steps": round(old_avg_s, 1),
            "recent_avg_steps": round(new_avg_s, 1),
            "steps_change_pct": round((new_avg_s - old_avg_s) / old_avg_s * 100, 1) if old_avg_s else 0,
        }

    # Milestones
    milestones = []
    milestone_thresholds = [
        (1, "🎯 完成第一次分析"),
        (10, "🔟 累计 10 次分析"),
        (50, "🏅 累计 50 次分析"),
        (100, "💯 累计 100 次分析"),
    ]
    for threshold, label in milestone_thresholds:
        if total >= threshold:
            milestones.append({"threshold": threshold, "label": label, "reached": True})
        else:
            milestones.append({"threshold": threshold, "label": label, "reached": False})
            break

    skill_milestones = [
        (1, "🧠 学会第一个技能"),
        (5, "🎓 累计学会 5 个技能"),
        (10, "🏆 累计学会 10 个技能"),
    ]
    for threshold, label in skill_milestones:
        if skills_learned >= threshold:
            milestones.append({"threshold": threshold, "label": label, "reached": True})
        else:
            milestones.append({"threshold": threshold, "label": label, "reached": False})
            break

    if skill_reuse_count >= 1:
        milestones.append({"threshold": 1, "label": "♻️ 首次复用已学技能", "reached": True})

    total_feedback = good + bad
    return {
        "total_sessions": total,
        "skills_learned": skills_learned,
        "skill_reuse_count": skill_reuse_count,
        "satisfaction_rate": round(good / total_feedback, 2) if total_feedback else None,
        "feedback_counts": {"good": good, "bad": bad},
        "recent_events": recent_events[:10],
        "tool_frequency": dict(sorted(tool_freq.items(), key=lambda x: -x[1])[:15]),
        "domains": domains,
        "efficiency": efficiency,
        "milestones": milestones,
    }
