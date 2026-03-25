import io
import json
import uuid
import asyncio
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from config import API_KEY, BASE_URL, DEFAULT_MODEL, DEFAULT_MODEL_EXTRA_PARAMS
from agent.llm import LLMClient
from agent.executor import AgentExecutor
from agent.planner import Planner
from agent.memory import MemoryManager
from agent.multi_agent import MultiAgentExecutor
from skills.registry import SkillRegistry

# ---------------------------------------------------------------------------
# App & component setup
# ---------------------------------------------------------------------------

app = FastAPI(title="TabClaw", version="0.1.0")

llm = LLMClient(api_key=API_KEY, base_url=BASE_URL, model=DEFAULT_MODEL, model_extra_params=DEFAULT_MODEL_EXTRA_PARAMS)
skill_registry = SkillRegistry()
memory_manager = MemoryManager()
executor = AgentExecutor(llm, skill_registry, memory_manager)
multi_executor = MultiAgentExecutor(llm, skill_registry, memory_manager)
planner = Planner(llm, memory_manager)

# Global state (single-user local app)
tables: Dict[str, Dict] = {}          # table_id -> {name, df, source, filename}
chat_history: List[Dict] = []

AUTO_COMPACT_THRESHOLD = 20   # messages before auto-compaction kicks in

# Static files
STATIC_DIR = Path(__file__).parent / "static"
ASSET_DIR = Path(__file__).parent / "asset"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
app.mount("/asset", StaticFiles(directory=str(ASSET_DIR)), name="asset")


@app.get("/")
async def root():
    return FileResponse(str(STATIC_DIR / "index.html"))


# ---------------------------------------------------------------------------
# Table endpoints
# ---------------------------------------------------------------------------

@app.post("/api/upload")
async def upload_table(file: UploadFile = File(...)):
    content = await file.read()
    fname = file.filename or "table"
    try:
        if fname.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(content))
        elif fname.endswith((".xlsx", ".xls")):
            df = pd.read_excel(io.BytesIO(content))
        else:
            raise HTTPException(400, "Only CSV and Excel (.xlsx/.xls) files are supported")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, f"Failed to parse file: {e}")

    table_id = uuid.uuid4().hex[:8]
    name = fname.rsplit(".", 1)[0]
    tables[table_id] = {"name": name, "df": df, "source": "uploaded", "filename": fname}

    return {
        "table_id": table_id,
        "name": name,
        "rows": len(df),
        "cols": len(df.columns),
        "columns": df.columns.tolist(),
        "dtypes": {c: str(t) for c, t in df.dtypes.items()},
        "preview": df.head(5).fillna("").to_dict("records"),
    }


@app.get("/api/tables")
async def list_tables():
    result = []
    for tid, t in tables.items():
        df = t["df"]
        result.append({
            "table_id": tid,
            "name": t["name"],
            "rows": len(df),
            "cols": len(df.columns),
            "columns": df.columns.tolist(),
            "source": t.get("source", "unknown"),
        })
    return result


@app.get("/api/tables/{table_id}")
async def get_table(table_id: str, page: int = 1, page_size: int = 50):
    if table_id not in tables:
        raise HTTPException(404, "Table not found")
    df = tables[table_id]["df"]
    total = len(df)
    start = (page - 1) * page_size
    end = start + page_size
    return {
        "table_id": table_id,
        "name": tables[table_id]["name"],
        "total_rows": total,
        "page": page,
        "page_size": page_size,
        "total_pages": max(1, -(-total // page_size)),
        "columns": df.columns.tolist(),
        "dtypes": {c: str(t) for c, t in df.dtypes.items()},
        "rows": df.iloc[start:end].fillna("").to_dict("records"),
    }


@app.delete("/api/tables/{table_id}")
async def delete_table(table_id: str):
    if table_id not in tables:
        raise HTTPException(404, "Table not found")
    del tables[table_id]
    return {"status": "deleted"}


@app.get("/api/tables/{table_id}/download")
async def download_table(table_id: str):
    if table_id not in tables:
        raise HTTPException(404, "Table not found")
    df = tables[table_id]["df"]
    name = tables[table_id]["name"]
    csv = df.to_csv(index=False)
    return StreamingResponse(
        iter([csv]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{name}.csv"'},
    )


# ---------------------------------------------------------------------------
# Chat / agent endpoints
# ---------------------------------------------------------------------------

class ChatRequest(BaseModel):
    message: str
    code_tool: bool = False
    skill_learn: bool = False


class PlanRequest(BaseModel):
    message: str


class ClarifyRequest(BaseModel):
    message: str


class ExecutePlanRequest(BaseModel):
    message: str
    steps: List[Dict]
    code_tool: bool = False
    skill_learn: bool = False


def _sse(obj: Any) -> str:
    return f"data: {json.dumps(obj, default=str, ensure_ascii=False)}\n\n"


@app.post("/api/generate-plan")
async def generate_plan(request: PlanRequest):
    plan = await planner.generate(request.message, tables)
    return plan


@app.post("/api/clarify")
async def clarify(request: ClarifyRequest):
    return await planner.check_clarification(request.message, tables)


@app.post("/api/chat")
async def chat(request: ChatRequest):
    use_multi = multi_executor.should_activate(request.message, tables)

    async def generate():
        # Auto-compact: if history is long, summarise before the new request
        if len(chat_history) >= AUTO_COMPACT_THRESHOLD:
            old_count = len(chat_history)
            summary = await _do_compact(chat_history)
            if summary:
                chat_history[:] = [{"role": "assistant", "content": summary}]
                yield _sse({"type": "compacted", "old_count": old_count,
                            "summary": summary[:120] + ("…" if len(summary) > 120 else "")})
        try:
            if use_multi:
                gen = multi_executor.execute_multi(
                    message=request.message,
                    tables=tables,
                    history=chat_history,
                    result_tables_store=tables,
                    code_tool=request.code_tool,
                )
            else:
                gen = executor.execute(
                    message=request.message,
                    tables=tables,
                    history=chat_history,
                    result_tables_store=tables,
                    code_tool=request.code_tool,
                    auto_learn=request.skill_learn,
                )
            async for event in gen:
                yield _sse(event)
                await asyncio.sleep(0)
        except Exception as e:
            yield _sse({"type": "error", "content": str(e)})
        finally:
            chat_history.append({"role": "user", "content": request.message})
            if len(chat_history) > 40:
                chat_history[:] = chat_history[-40:]
            yield "data: [DONE]\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/execute-plan")
async def execute_plan(request: ExecutePlanRequest):
    async def generate():
        # Auto-compact before plan execution
        if len(chat_history) >= AUTO_COMPACT_THRESHOLD:
            old_count = len(chat_history)
            summary = await _do_compact(chat_history)
            if summary:
                chat_history[:] = [{"role": "assistant", "content": summary}]
                yield _sse({"type": "compacted", "old_count": old_count,
                            "summary": summary[:120] + ("…" if len(summary) > 120 else "")})
        try:
            async for event in executor.execute_plan(
                message=request.message,
                steps=request.steps,
                tables=tables,
                history=chat_history,
                result_tables_store=tables,
                code_tool=request.code_tool,
                auto_learn=request.skill_learn,
            ):
                yield _sse(event)
                await asyncio.sleep(0)
        except Exception as e:
            yield _sse({"type": "error", "content": str(e)})
        finally:
            chat_history.append({"role": "user", "content": f"[Plan] {request.message}"})
            if len(chat_history) > 40:
                chat_history[:] = chat_history[-40:]
            yield "data: [DONE]\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.delete("/api/chat/history")
async def clear_history():
    chat_history.clear()
    return {"status": "cleared"}


async def _do_compact(history: List[Dict]) -> Optional[str]:
    """Ask the LLM to summarise chat_history. Returns summary text or None."""
    if not history:
        return None
    lines = []
    for m in history:
        role = m.get("role", "")
        content = (m.get("content") or "")[:400]
        if role in ("user", "assistant") and content:
            lines.append(f"[{role.upper()}]: {content}")
    if not lines:
        return None
    prompt = (
        "Summarise the following conversation history into a compact context block "
        "(max 350 words). Preserve: key user goals, table names and structures "
        "discussed, important findings, analysis performed, and any user preferences "
        "mentioned. Write in third person, starting with "
        "\"Summary of previous conversation:\".\n\n"
        + "\n\n".join(lines)
    )
    try:
        resp = await llm.chat([{"role": "user", "content": prompt}])
        return (resp.content or "").strip() or None
    except Exception:
        return None


@app.post("/api/chat/compact")
async def compact_history():
    """Manual compaction: summarise history and replace it with the summary."""
    old_count = len(chat_history)
    if old_count < 4:
        return {"status": "skipped", "reason": "history too short", "old_count": old_count}
    summary = await _do_compact(chat_history)
    if not summary:
        return {"status": "error", "reason": "LLM failed to generate summary"}
    chat_history[:] = [{"role": "assistant", "content": summary}]
    return {"status": "compacted", "old_count": old_count, "summary": summary}


# ---------------------------------------------------------------------------
# Skills endpoints
# ---------------------------------------------------------------------------

@app.get("/api/skills")
async def list_skills():
    return skill_registry.list_all()


class CustomSkillBody(BaseModel):
    name: str
    description: str
    prompt: str
    code: Optional[str] = None
    parameters: Optional[Dict] = {}


@app.post("/api/skills")
async def add_skill(body: CustomSkillBody):
    skill_id = uuid.uuid4().hex[:8]
    return skill_registry.add_custom(skill_id, body.model_dump())


@app.put("/api/skills/{skill_id}")
async def update_skill(skill_id: str, body: CustomSkillBody):
    try:
        return skill_registry.update_custom(skill_id, body.model_dump())
    except ValueError as e:
        raise HTTPException(404, str(e))


@app.delete("/api/skills/{skill_id}")
async def delete_skill(skill_id: str):
    try:
        return skill_registry.delete_custom(skill_id)
    except ValueError as e:
        raise HTTPException(404, str(e))


@app.delete("/api/skills")
async def clear_skills():
    return skill_registry.clear_custom()


# ---------------------------------------------------------------------------
# Memory endpoints
# ---------------------------------------------------------------------------

@app.get("/api/memory")
async def get_memory():
    return memory_manager.get_all()


class MemoryItemBody(BaseModel):
    category: str
    key: str
    value: str


@app.post("/api/memory")
async def set_memory(body: MemoryItemBody):
    memory_manager.set(body.category, body.key, body.value)
    return {"status": "ok"}


@app.delete("/api/memory/{category}/{key}")
async def delete_memory(category: str, key: str):
    ok = memory_manager.delete(category, key)
    if not ok:
        raise HTTPException(404, "Memory item not found")
    return {"status": "deleted"}


@app.delete("/api/memory")
async def clear_memory():
    memory_manager.clear_all()
    return {"status": "cleared"}


class ForgetBody(BaseModel):
    query: str


@app.post("/api/memory/forget")
async def forget_memory(body: ForgetBody):
    forgotten = await memory_manager.forget_by_query(body.query, memory_manager.get_all(), llm)
    return {"forgotten": forgotten, "count": len(forgotten)}


@app.post("/api/memory/summarize")
async def summarize_memory():
    """Use the LLM to generate a structured user preference document from current memory."""
    mem = memory_manager.get_all()
    # Flatten memory into readable lines
    lines = []
    for cat, items in mem.items():
        for k, entry in items.items():
            v = entry["value"] if isinstance(entry, dict) else entry
            lines.append(f"[{cat}] {k}: {v}")
    if not lines:
        return {"summary": "暂无记忆数据。请先与 TabClaw 交互，或手动添加偏好信息。"}

    mem_text = "\n".join(lines)
    prompt = f"""以下是用户在使用 TabClaw 数据分析助手过程中积累的记忆条目：

{mem_text}

请根据上述信息，撰写一份结构清晰、可读性强的「用户偏好概览」文档（使用中文）。

要求：
- 使用 Markdown 格式，包含若干小节（如：分析偏好、数据处理习惯、领域背景等）
- 每节用 2–4 句话或要点概括，不要逐条罗列原始条目
- 语气专业，像是一份给新协作者的简短简报
- 总长度控制在 300 字以内

直接输出 Markdown 文档，不要加任何前言或解释："""

    resp = await llm.chat([{"role": "user", "content": prompt}])
    return {"summary": (resp.content or "").strip()}


# ---------------------------------------------------------------------------
# Demo / one-click experience endpoints
# ---------------------------------------------------------------------------

EXAMPLES_DIR = Path(__file__).parent / "examples"

class DemoLoadBody(BaseModel):
    files: List[str]
    clear: bool = True


@app.post("/api/demo/load")
async def demo_load(body: DemoLoadBody):
    """Load example CSV files from the examples/ directory into the table store."""
    if body.clear:
        tables.clear()
        chat_history.clear()
    loaded = []
    for filename in body.files:
        # Security: only plain CSV filenames, no path traversal
        if not filename.endswith(".csv") or "/" in filename or ".." in filename:
            continue
        path = EXAMPLES_DIR / filename
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path)
            table_id = uuid.uuid4().hex[:8]
            name = filename.rsplit(".", 1)[0]
            tables[table_id] = {
                "name": name, "df": df,
                "source": "demo", "filename": filename,
            }
            loaded.append({
                "table_id": table_id, "name": name,
                "rows": len(df), "cols": len(df.columns),
                "columns": df.columns.tolist(),
            })
        except Exception:
            pass
    return {"loaded": loaded}


@app.get("/api/demo/scenarios")
async def demo_scenarios():
    """Return metadata about available demo files."""
    result = []
    for f in sorted(EXAMPLES_DIR.glob("*.csv")):
        try:
            df = pd.read_csv(f, nrows=0)          # read only header
            result.append({
                "filename": f.name,
                "name": f.stem,
                "columns": df.columns.tolist(),
            })
        except Exception:
            pass
    return result
