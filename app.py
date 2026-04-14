import io
import json
import uuid
import asyncio
from pathlib import Path
from typing import Dict, List, Optional, Any

import pandas as pd
from fastapi import Depends, FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from config import API_KEY, BASE_URL, DEFAULT_MODEL, DEFAULT_MODEL_EXTRA_PARAMS
from agent.llm import LLMClient
from agent.executor import AgentExecutor
from agent.planner import Planner
from agent.memory import MemoryManager
from agent.multi_agent import MultiAgentExecutor
from agent.workflow_recorder import (
    update_workflow_feedback, list_workflows, load_workflow,
    find_recurring_patterns, get_growth_profile,
    add_custom_domain, list_domains,
)
from skills.registry import SkillRegistry
from auth import db as auth_db
from auth import jwt_utils
from auth.dependencies import get_current_user, get_current_user_optional
from auth.crypto import decrypt_api_key, encrypt_api_key

# ---------------------------------------------------------------------------
# App & component setup
# ---------------------------------------------------------------------------

app = FastAPI(title="TabClaw", version="0.1.0")

auth_db.init_db()

# Per-user runtime state
_user_sessions: Dict[str, Dict[str, Any]] = {}

AUTO_COMPACT_THRESHOLD = 20   # messages before auto-compaction kicks in

# Manual / blank tables (in-browser editing)
MANUAL_MAX_ROWS = 500
MANUAL_MAX_COLS = 50
MANUAL_FETCH_CAP = 2000


def _sanitize_column_names(names: List[str]) -> List[str]:
    out: List[str] = []
    for i, raw in enumerate(names):
        base = str(raw).strip() if raw is not None else ""
        if not base:
            base = f"Col{i + 1}"
        cand = base
        n = 2
        while cand in out:
            cand = f"{base}_{n}"
            n += 1
        out.append(cand)
    return out


def _make_llm_for_user(user: dict) -> LLMClient:
    """Use user's own API key if configured, otherwise fall back to admin key quota."""
    if user.get("own_api_key_enc"):
        try:
            own_key = decrypt_api_key(user["own_api_key_enc"])
            own_url = user.get("own_base_url") or BASE_URL
            own_model = user.get("own_model") or DEFAULT_MODEL
            return LLMClient(
                api_key=own_key,
                base_url=own_url,
                model=own_model,
                model_extra_params=DEFAULT_MODEL_EXTRA_PARAMS,
            )
        except Exception:
            pass
    budget = user.get("token_budget", 1_000_000)
    used = user.get("token_used", 0)
    if budget <= 0 or used >= budget:
        raise HTTPException(
            status_code=402,
            detail="Free token quota exhausted. Please configure your own API key in Settings.",
        )
    return LLMClient(
        api_key=API_KEY,
        base_url=BASE_URL,
        model=DEFAULT_MODEL,
        model_extra_params=DEFAULT_MODEL_EXTRA_PARAMS,
    )


def get_user_state(user: dict) -> Dict[str, Any]:
    uid = str(user["id"])
    if uid not in _user_sessions:
        llm = _make_llm_for_user(user)
        skill_registry = SkillRegistry()
        memory_manager = MemoryManager()
        executor = AgentExecutor(llm, skill_registry, memory_manager)
        multi_executor = MultiAgentExecutor(llm, skill_registry, memory_manager)
        planner = Planner(llm, memory_manager)
        _user_sessions[uid] = {
            "tables": {},
            "chat_history": [],
            "llm": llm,
            "skill_registry": skill_registry,
            "memory_manager": memory_manager,
            "executor": executor,
            "multi_executor": multi_executor,
            "planner": planner,
        }
    return _user_sessions[uid]


def _refresh_user_llm(user: dict):
    uid = str(user["id"])
    if uid not in _user_sessions:
        return
    try:
        new_llm = _make_llm_for_user(user)
    except HTTPException:
        new_llm = LLMClient(
            api_key=API_KEY,
            base_url=BASE_URL,
            model=DEFAULT_MODEL,
            model_extra_params=DEFAULT_MODEL_EXTRA_PARAMS,
        )
    s = _user_sessions[uid]
    s["llm"] = new_llm
    s["executor"].llm = new_llm
    s["multi_executor"].llm = new_llm
    s["planner"].llm = new_llm


# Static files
STATIC_DIR = Path(__file__).parent / "static"
ASSET_DIR = Path(__file__).parent / "asset"
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
app.mount("/asset", StaticFiles(directory=str(ASSET_DIR)), name="asset")


@app.get("/")
async def root(user: Optional[dict] = Depends(get_current_user_optional)):
    if user is None:
        r = FileResponse(str(STATIC_DIR / "login.html"))
    else:
        r = FileResponse(str(STATIC_DIR / "index.html"))
    r.headers["Cache-Control"] = "no-store, no-cache, must-revalidate"
    r.headers["Pragma"] = "no-cache"
    return r


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
    payload = jwt_utils.verify_token(token)
    jti = payload["jti"]
    import datetime as _dt
    exp_iso = _dt.datetime.fromtimestamp(payload["exp"], tz=_dt.timezone.utc).isoformat()
    auth_db.add_session(jti, user["id"], exp_iso)
    response = JSONResponse({"status": "ok", "username": user["username"]})
    response.set_cookie(
        "session",
        token,
        httponly=True,
        samesite="lax",
        max_age=7 * 24 * 3600,
        path="/",
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
        "session",
        token,
        httponly=True,
        samesite="lax",
        max_age=7 * 24 * 3600,
        path="/",
    )
    return response


@app.post("/api/auth/logout")
async def logout(user: dict = Depends(get_current_user)):
    response = JSONResponse({"status": "ok"})
    response.delete_cookie("session", path="/")
    return response


@app.get("/api/auth/me")
async def me(user: dict = Depends(get_current_user)):
    fresh = auth_db.get_user_by_id(user["id"])
    return {
        "username": fresh["username"],
        "token_budget": fresh["token_budget"],
        "token_used": fresh["token_used"],
        "has_own_key": bool(fresh.get("own_api_key_enc")),
        "own_model": fresh.get("own_model") or "",
        "own_base_url": fresh.get("own_base_url") or "",
    }


# ---------------------------------------------------------------------------
# Settings endpoints
# ---------------------------------------------------------------------------

class ApiKeyBody(BaseModel):
    api_key: str
    base_url: str = ""
    model: str = ""


@app.post("/api/settings/api-key")
async def save_api_key(body: ApiKeyBody, user: dict = Depends(get_current_user)):
    if not body.api_key:
        raise HTTPException(400, "api_key is required")
    encrypted = encrypt_api_key(body.api_key)
    base_url = body.base_url.strip() or BASE_URL
    model = body.model.strip() or DEFAULT_MODEL
    auth_db.save_user_api_key(user["id"], encrypted, base_url, model)
    fresh = auth_db.get_user_by_id(user["id"])
    _refresh_user_llm(fresh)
    return {"status": "ok"}


@app.get("/api/settings/providers")
async def get_providers():
    return {
        "default_model": DEFAULT_MODEL,
        "providers": [
            {
                "id": "deepseek",
                "name": "DeepSeek",
                "base_url": "https://api.deepseek.com/v1",
                "models": [
                    {"id": "deepseek-chat", "name": "DeepSeek-V3 (deepseek-chat)"},
                    {"id": "deepseek-reasoner", "name": "DeepSeek-R1 (deepseek-reasoner)"},
                ],
            },
            {
                "id": "openai",
                "name": "OpenAI",
                "base_url": "https://api.openai.com/v1",
                "models": [
                    {"id": "gpt-4o", "name": "GPT-4o"},
                    {"id": "gpt-4o-mini", "name": "GPT-4o mini"},
                    {"id": "gpt-4-turbo", "name": "GPT-4 Turbo"},
                    {"id": "o1", "name": "o1"},
                    {"id": "o1-mini", "name": "o1-mini"},
                    {"id": "o3-mini", "name": "o3-mini"},
                ],
            },
            {
                "id": "zhipuai",
                "name": "智谱 AI (ZhipuAI)",
                "base_url": "https://open.bigmodel.cn/api/paas/v4",
                "models": [
                    {"id": "glm-4-plus", "name": "GLM-4-Plus"},
                    {"id": "glm-4-air", "name": "GLM-4-Air"},
                    {"id": "glm-4-flash", "name": "GLM-4-Flash (免费)"},
                    {"id": "glm-z1-flash", "name": "GLM-Z1-Flash (推理·免费)"},
                ],
            },
            {
                "id": "openrouter",
                "name": "OpenRouter（多模型代理·支持 Claude）",
                "base_url": "https://openrouter.ai/api/v1",
                "models": [
                    {"id": "anthropic/claude-opus-4-5", "name": "Claude Opus 4.5"},
                    {"id": "anthropic/claude-sonnet-4-5", "name": "Claude Sonnet 4.5"},
                    {"id": "anthropic/claude-haiku-3-5", "name": "Claude Haiku 3.5"},
                    {"id": "openai/gpt-4o", "name": "GPT-4o"},
                    {"id": "google/gemini-2.5-pro", "name": "Gemini 2.5 Pro"},
                    {"id": "deepseek/deepseek-chat", "name": "DeepSeek-V3"},
                ],
            },
            {
                "id": "moonshot",
                "name": "月之暗面 (Moonshot)",
                "base_url": "https://api.moonshot.cn/v1",
                "models": [
                    {"id": "moonshot-v1-8k", "name": "Moonshot v1 8k"},
                    {"id": "moonshot-v1-32k", "name": "Moonshot v1 32k"},
                    {"id": "moonshot-v1-128k", "name": "Moonshot v1 128k"},
                ],
            },
            {
                "id": "qwen",
                "name": "阿里云百炼 (Qwen/DashScope)",
                "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
                "models": [
                    {"id": "qwen-max", "name": "Qwen Max"},
                    {"id": "qwen-plus", "name": "Qwen Plus"},
                    {"id": "qwen-turbo", "name": "Qwen Turbo"},
                    {"id": "qwen-long", "name": "Qwen Long"},
                ],
            },
            {
                "id": "siliconflow",
                "name": "硅基流动 (SiliconFlow)",
                "base_url": "https://api.siliconflow.cn/v1",
                "models": [
                    {"id": "deepseek-ai/DeepSeek-V3", "name": "DeepSeek-V3"},
                    {"id": "deepseek-ai/DeepSeek-R1", "name": "DeepSeek-R1"},
                    {"id": "Qwen/Qwen2.5-72B-Instruct", "name": "Qwen2.5-72B"},
                ],
            },
            {
                "id": "other",
                "name": "其他 / 自定义",
                "base_url": "",
                "models": [],
                "custom": True,
            },
        ],
    }


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
async def upload_table(file: UploadFile = File(...), user: dict = Depends(get_current_user)):
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


class CreateTableBody(BaseModel):
    name: str = "Untitled"
    rows: int = 8
    cols: int = 6


class UpdateTableBody(BaseModel):
    name: Optional[str] = None
    columns: List[str]
    data: List[List[Any]]


@app.post("/api/tables/create")
async def create_blank_table(body: CreateTableBody, user: dict = Depends(get_current_user)):
    """Create an empty table for manual entry or paste (stored like uploaded tables)."""
    tables = get_user_state(user)["tables"]
    nrows = max(0, min(int(body.rows), MANUAL_MAX_ROWS))
    ncols = max(1, min(int(body.cols), MANUAL_MAX_COLS))
    col_names = [f"Col{i + 1}" for i in range(ncols)]
    if nrows == 0:
        df = pd.DataFrame(columns=col_names)
    else:
        df = pd.DataFrame([[""] * ncols for _ in range(nrows)], columns=col_names)
    table_id = uuid.uuid4().hex[:8]
    name = (body.name or "").strip() or "Untitled"
    tables[table_id] = {"name": name, "df": df, "source": "manual"}
    return {
        "table_id": table_id,
        "name": name,
        "rows": len(df),
        "cols": len(df.columns),
        "columns": df.columns.tolist(),
        "source": "manual",
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
async def get_table(
    table_id: str,
    page: int = 1,
    page_size: int = 50,
    user: dict = Depends(get_current_user),
):
    tables = get_user_state(user)["tables"]
    if table_id not in tables:
        raise HTTPException(404, "Table not found")
    t = tables[table_id]
    df = t["df"]
    source = t.get("source", "unknown")
    total = len(df)
    if source == "manual":
        end = min(total, MANUAL_FETCH_CAP)
        start = 0
        page = 1
        page_size = max(end, 1)
        total_pages = 1
    else:
        start = (page - 1) * page_size
        end = start + page_size
        total_pages = max(1, -(-total // page_size))
    return {
        "table_id": table_id,
        "name": t["name"],
        "source": source,
        "total_rows": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
        "columns": df.columns.tolist(),
        "dtypes": {c: str(t) for c, t in df.dtypes.items()},
        "rows": df.iloc[start:end].fillna("").to_dict("records"),
    }


@app.put("/api/tables/{table_id}")
async def update_manual_table(table_id: str, body: UpdateTableBody, user: dict = Depends(get_current_user)):
    """Replace data for a manually created table (for in-app editing / paste)."""
    tables = get_user_state(user)["tables"]
    if table_id not in tables:
        raise HTTPException(404, "Table not found")
    t = tables[table_id]
    if t.get("source") != "manual":
        raise HTTPException(400, "Only manually created tables can be edited here")

    cols = _sanitize_column_names([str(c) for c in body.columns])
    if not cols:
        raise HTTPException(400, "At least one column is required")
    if len(cols) > MANUAL_MAX_COLS:
        raise HTTPException(400, f"Too many columns (max {MANUAL_MAX_COLS})")

    for i, row in enumerate(body.data):
        if len(row) != len(cols):
            raise HTTPException(400, f"Row {i}: expected {len(cols)} cells, got {len(row)}")
    if len(body.data) > MANUAL_MAX_ROWS:
        raise HTTPException(400, f"Too many rows (max {MANUAL_MAX_ROWS})")

    str_rows: List[List[str]] = []
    for row in body.data:
        str_rows.append(["" if v is None else str(v) for v in row])

    df = pd.DataFrame(str_rows, columns=cols) if str_rows else pd.DataFrame(columns=cols)
    t["df"] = df
    if body.name is not None:
        t["name"] = body.name.strip() or t["name"]
    return {
        "table_id": table_id,
        "name": t["name"],
        "rows": len(df),
        "cols": len(df.columns),
        "columns": df.columns.tolist(),
        "source": "manual",
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
    implicit_feedback: bool = False
    last_workflow_id: Optional[str] = None


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
    llm = s["llm"]
    uid = user["id"]
    use_multi = multi_executor.should_activate(request.message, tables)

    async def generate():
        # Implicit feedback: classify the new message against the previous workflow
        if request.implicit_feedback and request.last_workflow_id:
            wf = load_workflow(request.last_workflow_id)
            if wf:
                prev_context = wf.get("conclusion") or wf.get("user_message", "")
                if prev_context:
                    verdict = await _classify_implicit_feedback(request.message, prev_context, llm)
                    if verdict in ("good", "bad"):
                        already_rated = bool(wf.get("user_feedback"))
                        if not already_rated:
                            update_workflow_feedback(
                                request.last_workflow_id, verdict, "implicit"
                            )
                            wf2 = load_workflow(request.last_workflow_id)
                            skills_used = wf2.get("skills_used", []) if wf2 else []
                            for slug in skills_used:
                                s["skill_registry"].record_feedback(slug, verdict)
                        yield _sse({
                            "type": "implicit_feedback_applied",
                            "session_id": request.last_workflow_id,
                            "feedback": verdict,
                            "already_rated": already_rated,
                        })

        # Auto-compact: if history is long, summarise before the new request
        if len(chat_history) >= AUTO_COMPACT_THRESHOLD:
            old_count = len(chat_history)
            summary = await _do_compact(chat_history, llm)
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
    llm = s["llm"]
    uid = user["id"]

    async def generate():
        # Auto-compact before plan execution
        if len(chat_history) >= AUTO_COMPACT_THRESHOLD:
            old_count = len(chat_history)
            summary = await _do_compact(chat_history, llm)
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


async def _classify_implicit_feedback(user_msg: str, prev_context: str, llm) -> Optional[str]:
    """Classify whether a user's new message implies positive/negative feedback on the previous response.

    Returns 'good', 'bad', or None (neutral/unrelated).
    Uses a minimal prompt to keep latency low.
    """
    prompt = (
        "You are a feedback signal classifier. Given the previous AI response summary and the user's "
        "next message, determine whether the user's message implicitly expresses satisfaction or "
        "dissatisfaction with the previous response.\n\n"
        f"Previous AI response summary:\n{prev_context[:600]}\n\n"
        f"User's next message:\n{user_msg[:300]}\n\n"
        "Classify the implicit signal:\n"
        "- good: user expresses thanks, satisfaction, agreement, confirms it's correct, "
        "or continues positively based on the result\n"
        "- bad: user says it's wrong, incorrect, asks to redo, expresses frustration, "
        "or corrects the AI's mistake\n"
        "- neutral: user asks a completely new unrelated question, or the message carries no feedback signal\n\n"
        "Reply with ONLY one word: good, bad, or neutral"
    )
    try:
        resp = await llm.chat([{"role": "user", "content": prompt}])
        result = (resp.content or "").strip().lower().split()[0] if resp.content else ""
        if result in ("good", "bad"):
            return result
        return None
    except Exception:
        return None


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
    """Manual compaction: summarise history and replace it with the summary."""
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


class CreateSkillBody(BaseModel):
    name: str
    description: str
    body: str


@app.post("/api/skills/create")
async def create_skill(req: CreateSkillBody, user: dict = Depends(get_current_user)):
    """Create a new package skill from name, description, and SKILL.md body."""
    if not req.name or not req.description or not req.body:
        raise HTTPException(400, "name, description, and body are required")
    return get_user_state(user)["skill_registry"].create_package(req.name, req.description, req.body, source="manual")


@app.delete("/api/skills")
async def clear_skills(user: dict = Depends(get_current_user)):
    """Delete all package skills."""
    return get_user_state(user)["skill_registry"].clear_packages()


# Package (instruction) skills — ClawHub / OpenClaw-compatible
@app.post("/api/skills/import")
async def import_skill_package(file: UploadFile = File(...), user: dict = Depends(get_current_user)):
    if not (file.filename or "").endswith(".zip"):
        raise HTTPException(400, "Only .zip files are supported")
    content = await file.read()
    try:
        result = get_user_state(user)["skill_registry"].install_from_zip(content)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return result


@app.delete("/api/skills/package/{slug}")
async def delete_skill_package(slug: str, user: dict = Depends(get_current_user)):
    try:
        return get_user_state(user)["skill_registry"].delete_package(slug)
    except ValueError as e:
        raise HTTPException(404, str(e))


class PackageToggleBody(BaseModel):
    enabled: bool


@app.put("/api/skills/package/{slug}/toggle")
async def toggle_skill_package(slug: str, body: PackageToggleBody, user: dict = Depends(get_current_user)):
    try:
        return get_user_state(user)["skill_registry"].toggle_package(slug, body.enabled)
    except ValueError as e:
        raise HTTPException(404, str(e))


@app.get("/api/skills/package/{slug}/detail")
async def skill_package_detail(slug: str, user: dict = Depends(get_current_user)):
    """Return full detail for a package skill including version history and stats."""
    skill_registry = get_user_state(user)["skill_registry"]
    pkg = next((p for p in skill_registry._packages if p["slug"] == slug), None)
    if not pkg:
        raise HTTPException(404, "Skill not found")
    meta_path = Path(pkg["skill_dir"]) / "_meta.json"
    meta: Dict[str, Any] = {}
    if meta_path.exists():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {
        "slug": pkg["slug"],
        "name": pkg["name"],
        "description": pkg["description"],
        "body": pkg["body"],
        "enabled": pkg["enabled"],
        "source": meta.get("source", "manual"),
        "version": meta.get("version", 1),
        "created_at": meta.get("created_at"),
        "usage_count": meta.get("usage_count", 0),
        "success_count": meta.get("success_count", 0),
        "failure_count": meta.get("failure_count", 0),
        "last_used_at": meta.get("last_used_at"),
        "upgrade_history": meta.get("upgrade_history", []),
        "derived_from_workflow": meta.get("derived_from_workflow", ""),
    }


@app.post("/api/skills/discover")
async def discover_skills(user: dict = Depends(get_current_user)):
    """Scan workflow history for recurring patterns and suggest new skills."""
    executor = get_user_state(user)["executor"]
    suggestions = await executor.distiller.discover()
    return {"suggestions": suggestions, "count": len(suggestions)}


class AcceptSkillBody(BaseModel):
    name: str
    description: str
    body: str


@app.post("/api/skills/accept")
async def accept_discovered_skill(req: AcceptSkillBody, user: dict = Depends(get_current_user)):
    """Create a skill from a discovery suggestion."""
    if not req.name or not req.body:
        raise HTTPException(400, "name and body are required")
    return get_user_state(user)["skill_registry"].create_package(
        req.name, req.description, req.body, source="discovered",
    )


@app.post("/api/skills/package/{slug}/improve")
async def improve_skill(slug: str, user: dict = Depends(get_current_user)):
    """Trigger LLM-driven improvement of a skill based on bad-feedback workflows."""
    executor = get_user_state(user)["executor"]
    result = await executor.distiller.try_improve(slug)
    if not result:
        return {"status": "no_improvement", "reason": "No actionable feedback found"}
    return result


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
    forgotten = await s["memory_manager"].forget_by_query(body.query, s["memory_manager"].get_all(), s["llm"])
    return {"forgotten": forgotten, "count": len(forgotten)}


@app.post("/api/memory/summarize")
async def summarize_memory(user: dict = Depends(get_current_user)):
    """Use the LLM to generate a structured user preference document from current memory."""
    s = get_user_state(user)
    mem = s["memory_manager"].get_all()
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

    resp = await s["llm"].chat([{"role": "user", "content": prompt}])
    return {"summary": (resp.content or "").strip()}


# ---------------------------------------------------------------------------
# Workflow & feedback endpoints
# ---------------------------------------------------------------------------

class FeedbackBody(BaseModel):
    feedback: str   # "good" or "bad"
    detail: str = ""


@app.post("/api/workflow/{session_id}/feedback")
async def workflow_feedback(session_id: str, body: FeedbackBody, user: dict = Depends(get_current_user)):
    if body.feedback not in ("good", "bad"):
        raise HTTPException(400, "feedback must be 'good' or 'bad'")
    ok = update_workflow_feedback(session_id, body.feedback, body.detail)
    if not ok:
        raise HTTPException(404, "Workflow not found")

    result: Dict[str, Any] = {"status": "ok", "session_id": session_id, "feedback": body.feedback}
    s = get_user_state(user)
    skill_registry = s["skill_registry"]
    executor = s["executor"]

    wf = load_workflow(session_id)
    skills_used = wf.get("skills_used", []) if wf else []
    for slug in skills_used:
        skill_registry.record_feedback(slug, body.feedback)
        if body.feedback == "bad":
            stats = next(
                (s for s in skill_registry.get_skill_stats() if s["slug"] == slug), None
            )
            if stats and stats.get("failure_count", 0) >= 2:
                upgrade = await executor.distiller.try_improve(slug)
                if upgrade and upgrade.get("status") == "upgraded":
                    result["skill_upgraded"] = {
                        "slug": slug,
                        "name": upgrade.get("name", slug),
                        "version": upgrade.get("version"),
                        "reason": upgrade.get("reason", ""),
                    }

    return result


@app.get("/api/workflows")
async def get_workflows(limit: int = 50, user: dict = Depends(get_current_user)):
    return list_workflows(limit)


@app.get("/api/workflow/{session_id}")
async def get_workflow(session_id: str, user: dict = Depends(get_current_user)):
    data = load_workflow(session_id)
    if not data:
        raise HTTPException(404, "Workflow not found")
    return data


@app.get("/api/growth/profile")
async def growth_profile(user: dict = Depends(get_current_user)):
    skill_registry = get_user_state(user)["skill_registry"]
    profile = get_growth_profile()
    profile["skills_stats"] = skill_registry.get_skill_stats()
    return profile


@app.get("/api/growth/patterns")
async def growth_patterns(user: dict = Depends(get_current_user)):
    return find_recurring_patterns(min_occurrences=2)


@app.get("/api/growth/domains")
async def get_domains(user: dict = Depends(get_current_user)):
    """List all domains (built-in + custom)."""
    return list_domains()


class CustomDomainBody(BaseModel):
    name: str
    keywords: List[str]


@app.post("/api/growth/domains")
async def create_custom_domain(body: CustomDomainBody, user: dict = Depends(get_current_user)):
    """Add or update a custom domain with keywords."""
    if not body.name or not body.keywords:
        raise HTTPException(400, "name and keywords are required")
    result = add_custom_domain(body.name, body.keywords)
    return {"status": "ok", "domains": result}


@app.get("/api/skills/stats")
async def skill_stats(user: dict = Depends(get_current_user)):
    return get_user_state(user)["skill_registry"].get_skill_stats()


# ---------------------------------------------------------------------------
# Demo / one-click experience endpoints
# ---------------------------------------------------------------------------

EXAMPLES_DIR = Path(__file__).parent / "examples"

class DemoLoadBody(BaseModel):
    files: List[str]
    clear: bool = True


@app.post("/api/demo/load")
async def demo_load(body: DemoLoadBody, user: dict = Depends(get_current_user)):
    """Load example CSV files from the examples/ directory into the table store."""
    s = get_user_state(user)
    tables = s["tables"]
    chat_history = s["chat_history"]
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
async def demo_scenarios(user: dict = Depends(get_current_user)):
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
