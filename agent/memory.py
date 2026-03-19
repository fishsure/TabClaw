import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Optional


_DEFAULT_DATA_PATH = Path(__file__).parent.parent / "data" / "memory.json"

CATEGORIES = ["preferences", "domain_knowledge", "user_context", "history_insights"]


class MemoryManager:
    def __init__(self, data_path: Optional[Path] = None):
        self._data_path = data_path or _DEFAULT_DATA_PATH
        self._load()

    def _load(self):
        if self._data_path.exists():
            with open(self._data_path) as f:
                self._data = json.load(f)
        else:
            self._data = {c: {} for c in CATEGORIES}
        # Ensure all categories exist
        for c in CATEGORIES:
            self._data.setdefault(c, {})

    def _save(self):
        self._data_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self._data_path, "w") as f:
            json.dump(self._data, f, indent=2, ensure_ascii=False)

    def get_all(self) -> Dict:
        return self._data

    def get_relevant(self, query: str) -> str:
        """Return a compact text representation of memory relevant to query."""
        lines = []
        query_lower = query.lower()
        for category, items in self._data.items():
            for key, entry in items.items():
                val = entry["value"] if isinstance(entry, dict) else entry
                # Include if keyword matches or category is preferences
                if category == "preferences" or any(
                    w in query_lower or w in key.lower() or w in str(val).lower()
                    for w in query_lower.split()
                ):
                    lines.append(f"[{category}] {key}: {val}")
        return "\n".join(lines) if lines else "No relevant memory."

    def set(self, category: str, key: str, value: str):
        if category not in self._data:
            self._data[category] = {}
        self._data[category][key] = {
            "value": value,
            "updated": datetime.now(timezone.utc).isoformat(),
        }
        self._save()

    def delete(self, category: str, key: str) -> bool:
        if category in self._data and key in self._data[category]:
            del self._data[category][key]
            self._save()
            return True
        return False

    def clear_category(self, category: str):
        self._data[category] = {}
        self._save()

    def clear_all(self):
        self._data = {c: {} for c in CATEGORIES}
        self._save()

    async def forget_by_query(self, query: str, all_memory: Dict, llm) -> List[Dict]:
        """Use LLM to identify which memory items to delete based on a natural-language query."""
        memory_text = json.dumps(all_memory, indent=2, ensure_ascii=False)
        prompt = f"""The user wants to forget: "{query}"

Current memory:
{memory_text}

Which memory items should be removed? Return ONLY a JSON array of items to delete.
Format: [{{"category": "...", "key": "..."}}]
If nothing matches, return [].
Output ONLY the JSON array:"""
        try:
            resp = await llm.chat([{"role": "user", "content": prompt}])
            content = (resp.content or "").strip()
            match = re.search(r"\[.*?\]", content, re.DOTALL)
            if not match:
                return []
            items = json.loads(match.group())
            deleted = []
            for item in items:
                cat = item.get("category")
                key = item.get("key")
                if cat and key and self.delete(cat, key):
                    deleted.append(item)
            return deleted
        except Exception as e:
            return []
