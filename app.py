import io
import json
import uuid
import asyncio
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from config import API_KEY, BASE_URL, DEFAULT_MODEL
from agent.llm import LLMClient
from agent.executor import AgentExecutor
from agent.planner import Planner
from agent.memory import MemoryManager
from agent.multi_agent import MultiAgentExecutor
from skills.registry import SkillRegistry
from auth import db as auth_db
from auth import jwt_utils
from auth.dependencies import get_current_user, get_current_user_optional
from auth.crypto import decrypt_api_key, encrypt_api_key

# ---------------------------------------------------------------------------
# App & component setup
# ---------------------------------------------------------------------------

app = FastAPI(title="TabClaw", version="0.2.0")

# Initialize DB on startup
auth_db.init_db()

# ---------------------------------------------------------------------------
# Per-user session state
# ---------------------------------------------------------------------------

_user_sessions: Dict[str, dict] = {}  # keyed by str(user_id)


def _make_llm_for_user(user: dict) -> LLMClient:
    """Return LLMClient using user's own key if set, else admin key (if quota remains)."""
    if user.get("own_api_key_enc"):
        try:
            own_key = decrypt_api_key(user["own_api_key_enc"])
            own_url = user.get("own_base_url") or BASE_URL
            return LLMClient(api_key=own_key, base_url=own_url, model=DEFAULT_MODEL)
        except Exception:
            pass
    # Check admin quota
    budget = user.get("token_budget", 1_000_000)
    used = user.get("token_used", 0)
    if budget <= 0 or used >= budget:
        raise HTTPException(
            status_code=402,
            detail="Free token quota exhausted. Please configure your own API key in Settings.",
        )
    return LLMClient(api_key=API_KEY, base_url=BASE_URL, model=DEFAULT_MODEL)


def get_user_state(user: dict) -> dict:
    uid = str(user["id"])
    if uid not in _user_sessions:
        user_dir = Path(__file__).parent / "data" / "users" / uid
        user_dir.mkdir(parents=True, exist_ok=True)
        llm = _make_llm_for_user(user)
        skill_registry = SkillRegistry(data_path=user_dir / "custom_skills.json")
        memory_manager = MemoryManager(data_path=user_dir / "memory.json")
        executor = AgentExecutor(llm, skill_registry, memory_manager)
        multi_executor = MultiAgentExecutor(llm, skill_registry, memory_manager)
        planner = Planner(llm, memory_manager)
        _user_sessions[uid] = {
            "tables": {},
            "chat_history": [],
            "executor": executor,
            "multi_executor": multi_executor,
            "planner": planner,
            "memory_manager": memory_manager,
            "skill_registry": skill_registry,
            "llm": llm,
        }
    return _user_sessions[uid]


def _refresh_user_llm(user: dict):
    """Re-create LLM client after user updates their API key."""
    uid = str(user["id"])
    if uid in _user_sessions:
        try:
            new_llm = _make_llm_for_user(user)
        except HTTPException:
            new_llm = LLMClient(api_key=API_KEY, base_url=BASE_URL, model=DEFAULT_MODEL)
        s = _user_sessions[uid]
        s["llm"] = new_llm
        s["executor"].llm = new_llm
        s["multi_executor"].llm = new_llm
        s["planner"].llm = new_llm


AUTO_COMPACT_THRESHOLD = 20   # messages before auto-compaction kicks in

# Static files
STATIC_DIR = Path(__file__).parent / "static"
ASSET_DIR = Path(__file__).parent / "asset"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
app.mount("/asset", StaticFiles(directory=str(ASSET_DIR)), name="asset")


@app.get("/")
async def root(user: Optional[dict] = Depends(get_current_user_optional)):
    if user is None:
        return FileResponse(str(STATIC_DIR / "login.html"))
    return FileResponse(str(STATIC_DIR / "index.html"))


# ---------------------------------------------------------------------------
# Auth endpoints
# ---------------------------------------------------------------------------

class RegisterBody(BaseModel):
    username: str
    password: str


class LoginBody(BaseModel):
    username: str
    password: str


@app.post("/api/auth/register")
async def register(body: RegisterBody):
    if not body.username or len(body.username) < 3:
        raise HTTPException(400, "Username must be at least 3 characters")
    if not body.password or len(body.password) < 6:
        raise HTTPException(400, "Password must be at least 6 characters")
    user = auth_db.create_user(body.username, body.password)
    if user is None:
        raise HTTPException(409, "Username already taken")
    token = jwt_utils.create_token(user["id"], user["username"])
    import json as _json
    payload = jwt_utils.verify_token(token)
    jti = payload["jti"]
    exp_iso = __import__("datetime").datetime.fromtimestamp(
        payload["exp"], tz=__import__("datetime").timezone.utc
    ).isoformat()
    auth_db.add_session(jti, user["id"], exp_iso)
    response = JSONResponse({"status": "ok", "username": user["username"]})
    response.set_cookie(
        "session", token,
        httponly=True, samesite="lax", max_age=7 * 24 * 3600, path="/"
    )
    return response


@app.post("/api/auth/login")
async def login(body: LoginBody):
    user = auth_db.get_user_by_username(body.username)
    if not user or not auth_db.verify_password(user, body.password):
        raise HTTPException(401, "Invalid username or password")
    token = jwt_utils.create_token(user["id"], user["username"])
    payload = jwt_utils.verify_token(token)
    jti = payload["jti"]
    import datetime as _dt
    exp_iso = _dt.datetime.fromtimestamp(payload["exp"], tz=_dt.timezone.utc).isoformat()
    auth_db.add_session(jti, user["id"], exp_iso)
    response = JSONResponse({"status": "ok", "username": user["username"]})
    response.set_cookie(
        "session", token,
        httponly=True, samesite="lax", max_age=7 * 24 * 3600, path="/"
    )
    return response


@app.post("/api/auth/logout")
async def logout(user: dict = Depends(get_current_user)):
    # We don't have the jti easily here without re-parsing the cookie, so just clear all sessions
    # for this user_id via a direct DB approach
    response = JSONResponse({"status": "ok"})
    response.delete_cookie("session", path="/")
    return response


@app.get("/api/auth/me")
async def me(user: dict = Depends(get_current_user)):
    # Re-fetch latest user data from DB
    fresh = auth_db.get_user_by_id(user["id"])
    return {
        "username": fresh["username"],
        "token_budget": fresh["token_budget"],
        "token_used": fresh["token_used"],
        "has_own_key": bool(fresh.get("own_api_key_enc")),
    }


# ---------------------------------------------------------------------------
# Settings endpoints
# ---------------------------------------------------------------------------

class ApiKeyBody(BaseModel):
    api_key: str
    base_url: str = ""


@app.post("/api/settings/api-key")
async def save_api_key(body: ApiKeyBody, user: dict = Depends(get_current_user)):
    if not body.api_key:
        raise HTTPException(400, "api_key is required")
    encrypted = encrypt_api_key(body.api_key)
    base_url = body.base_url.strip() or BASE_URL
    auth_db.save_user_api_key(user["id"], encrypted, base_url)
    fresh = auth_db.get_user_by_id(user["id"])
    _refresh_user_llm(fresh)
    return {"status": "ok"}


@app.delete("/api/settings/api-key")
async def clear_api_key(user: dict = Depends(get_current_user)):
    auth_db.clear_user_api_key(user["id"])
    fresh = auth_db.get_user_by_id(user["id"])
    _refresh_user_llm(fresh)
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Table endpoints
# ---------------------------------------------------------------------------

@app.post("/api/upload")
async def upload_table(
    file: UploadFile = File(...),
    user: dict = Depends(get_current_user),
):
    s = get_user_state(user)
    tables = s["tables"]
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
async def list_tables(user: dict = Depends(get_current_user)):
    tables = get_user_state(user)["tables"]
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
async def get_table(table_id: str, page: int = 1, page_size: int = 50, user: dict = Depends(get_current_user)):
    tables = get_user_state(user)["tables"]
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
async def delete_table(table_id: str, user: dict = Depends(get_current_user)):
    tables = get_user_state(user)["tables"]
    if table_id not in tables:
        raise HTTPException(404, "Table not found")
    del tables[table_id]
    return {"status": "deleted"}


@app.get("/api/tables/{table_id}/download")
async def download_table(table_id: str, user: dict = Depends(get_current_user)):
    tables = get_user_state(user)["tables"]
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
async def generate_plan(request: PlanRequest, user: dict = Depends(get_current_user)):
    s = get_user_state(user)
    plan = await s["planner"].generate(request.message, s["tables"])
    return plan


@app.post("/api/clarify")
async def clarify(request: ClarifyRequest, user: dict = Depends(get_current_user)):
    s = get_user_state(user)
    return await s["planner"].check_clarification(request.message, s["tables"])


@app.post("/api/chat")
async def chat(request: ChatRequest, user: dict = Depends(get_current_user)):
    s = get_user_state(user)
    tables = s["tables"]
    chat_history = s["chat_history"]
    executor = s["executor"]
    multi_executor = s["multi_executor"]
    uid = user["id"]
    use_multi = multi_executor.should_activate(request.message, tables)

    async def generate():
        # Auto-compact: if history is long, summarise before the new request
        if len(chat_history) >= AUTO_COMPACT_THRESHOLD:
            old_count = len(chat_history)
            summary = await _do_compact(chat_history, s["llm"])
            if summary:
                chat_history[:] = [{"role": "assistant", "content": summary}]
                yield _sse({"type": "compacted", "old_count": old_count,
                            "summary": summary[:120] + ("…" if len(summary) > 120 else "")})
        token_total = 0
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
                if isinstance(event, dict) and event.get("type") == "usage":
                    token_total += event.get("tokens", 0)
                    continue
                yield _sse(event)
                await asyncio.sleep(0)
        except Exception as e:
            yield _sse({"type": "error", "content": str(e)})
        finally:
            chat_history.append({"role": "user", "content": request.message})
            if len(chat_history) > 40:
                chat_history[:] = chat_history[-40:]
            # Update token usage and emit update event
            if token_total > 0:
                auth_db.update_token_usage(uid, token_total)
                fresh = auth_db.get_user_by_id(uid)
                yield _sse({
                    "type": "token_update",
                    "used": fresh["token_used"],
                    "budget": fresh["token_budget"],
                    "has_own_key": bool(fresh.get("own_api_key_enc")),
                })
            yield "data: [DONE]\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/api/execute-plan")
async def execute_plan(request: ExecutePlanRequest, user: dict = Depends(get_current_user)):
    s = get_user_state(user)
    tables = s["tables"]
    chat_history = s["chat_history"]
    executor = s["executor"]
    uid = user["id"]

    async def generate():
        if len(chat_history) >= AUTO_COMPACT_THRESHOLD:
            old_count = len(chat_history)
            summary = await _do_compact(chat_history, s["llm"])
            if summary:
                chat_history[:] = [{"role": "assistant", "content": summary}]
                yield _sse({"type": "compacted", "old_count": old_count,
                            "summary": summary[:120] + ("…" if len(summary) > 120 else "")})
        token_total = 0
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
                if isinstance(event, dict) and event.get("type") == "usage":
                    token_total += event.get("tokens", 0)
                    continue
                yield _sse(event)
                await asyncio.sleep(0)
        except Exception as e:
            yield _sse({"type": "error", "content": str(e)})
        finally:
            chat_history.append({"role": "user", "content": f"[Plan] {request.message}"})
            if len(chat_history) > 40:
                chat_history[:] = chat_history[-40:]
            if token_total > 0:
                auth_db.update_token_usage(uid, token_total)
                fresh = auth_db.get_user_by_id(uid)
                yield _sse({
                    "type": "token_update",
                    "used": fresh["token_used"],
                    "budget": fresh["token_budget"],
                    "has_own_key": bool(fresh.get("own_api_key_enc")),
                })
            yield "data: [DONE]\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.delete("/api/chat/history")
async def clear_history(user: dict = Depends(get_current_user)):
    get_user_state(user)["chat_history"].clear()
    return {"status": "cleared"}


async def _do_compact(history: List[Dict], llm) -> Optional[str]:
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
async def compact_history(user: dict = Depends(get_current_user)):
    s = get_user_state(user)
    chat_history = s["chat_history"]
    old_count = len(chat_history)
    if old_count < 4:
        return {"status": "skipped", "reason": "history too short", "old_count": old_count}
    summary = await _do_compact(chat_history, s["llm"])
    if not summary:
        return {"status": "error", "reason": "LLM failed to generate summary"}
    chat_history[:] = [{"role": "assistant", "content": summary}]
    return {"status": "compacted", "old_count": old_count, "summary": summary}


# ---------------------------------------------------------------------------
# Skills endpoints
# ---------------------------------------------------------------------------

@app.get("/api/skills")
async def list_skills(user: dict = Depends(get_current_user)):
    return get_user_state(user)["skill_registry"].list_all()


class CustomSkillBody(BaseModel):
    name: str
    description: str
    prompt: str
    code: Optional[str] = None
    parameters: Optional[Dict] = {}


@app.post("/api/skills")
async def add_skill(body: CustomSkillBody, user: dict = Depends(get_current_user)):
    skill_id = uuid.uuid4().hex[:8]
    return get_user_state(user)["skill_registry"].add_custom(skill_id, body.model_dump())


@app.put("/api/skills/{skill_id}")
async def update_skill(skill_id: str, body: CustomSkillBody, user: dict = Depends(get_current_user)):
    try:
        return get_user_state(user)["skill_registry"].update_custom(skill_id, body.model_dump())
    except ValueError as e:
        raise HTTPException(404, str(e))


@app.delete("/api/skills/{skill_id}")
async def delete_skill(skill_id: str, user: dict = Depends(get_current_user)):
    try:
        return get_user_state(user)["skill_registry"].delete_custom(skill_id)
    except ValueError as e:
        raise HTTPException(404, str(e))


@app.delete("/api/skills")
async def clear_skills(user: dict = Depends(get_current_user)):
    return get_user_state(user)["skill_registry"].clear_custom()


# ---------------------------------------------------------------------------
# Memory endpoints
# ---------------------------------------------------------------------------

@app.get("/api/memory")
async def get_memory(user: dict = Depends(get_current_user)):
    return get_user_state(user)["memory_manager"].get_all()


class MemoryItemBody(BaseModel):
    category: str
    key: str
    value: str


@app.post("/api/memory")
async def set_memory(body: MemoryItemBody, user: dict = Depends(get_current_user)):
    get_user_state(user)["memory_manager"].set(body.category, body.key, body.value)
    return {"status": "ok"}


@app.delete("/api/memory/{category}/{key}")
async def delete_memory(category: str, key: str, user: dict = Depends(get_current_user)):
    ok = get_user_state(user)["memory_manager"].delete(category, key)
    if not ok:
        raise HTTPException(404, "Memory item not found")
    return {"status": "deleted"}


@app.delete("/api/memory")
async def clear_memory(user: dict = Depends(get_current_user)):
    get_user_state(user)["memory_manager"].clear_all()
    return {"status": "cleared"}


class ForgetBody(BaseModel):
    query: str


@app.post("/api/memory/forget")
async def forget_memory(body: ForgetBody, user: dict = Depends(get_current_user)):
    s = get_user_state(user)
    mm = s["memory_manager"]
    forgotten = await mm.forget_by_query(body.query, mm.get_all(), s["llm"])
    return {"forgotten": forgotten, "count": len(forgotten)}


@app.post("/api/memory/summarize")
async def summarize_memory(user: dict = Depends(get_current_user)):
    s = get_user_state(user)
    mem = s["memory_manager"].get_all()
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

    resp = await s["llm"].chat([{"role": "user", "content": prompt}])
    return {"summary": (resp.content or "").strip()}


# ---------------------------------------------------------------------------
# Demo / one-click experience endpoints
# ---------------------------------------------------------------------------

EXAMPLES_DIR = Path(__file__).parent / "examples"


class DemoLoadBody(BaseModel):
    files: List[str]
    clear: bool = True


@app.post("/api/demo/load")
async def demo_load(body: DemoLoadBody, user: dict = Depends(get_current_user)):
    s = get_user_state(user)
    tables = s["tables"]
    chat_history = s["chat_history"]
    if body.clear:
        tables.clear()
        chat_history.clear()
    loaded = []
    for filename in body.files:
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
async def demo_scenarios(user: dict = Depends(get_current_user)):
    result = []
    for f in sorted(EXAMPLES_DIR.glob("*.csv")):
        try:
            df = pd.read_csv(f, nrows=0)
            result.append({
                "filename": f.name,
                "name": f.stem,
                "columns": df.columns.tolist(),
            })
        except Exception:
            pass
    return result
