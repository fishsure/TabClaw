import json
import re
import asyncio
from typing import AsyncGenerator, Dict, List

from agent.skill_distiller import SkillDistiller


class AgentExecutor:
    def __init__(self, llm, skill_registry, memory_manager):
        self.llm = llm
        self.skills = skill_registry
        self.memory = memory_manager
        self.distiller = SkillDistiller(llm, skill_registry)
        self.max_iterations = 12

    # ------------------------------------------------------------------
    # Public entry points
    # ------------------------------------------------------------------

    async def execute(
        self,
        message: str,
        tables: Dict,
        history: List,
        result_tables_store: Dict,
        code_tool: bool = False,
        auto_learn: bool = False,
    ) -> AsyncGenerator:
        """Execute user request directly (no plan)."""
        messages = self._build_messages(message, tables, history)
        tools = self.skills.get_tool_definitions(code_tool=code_tool)
        tool_calls_log: List[Dict] = []
        async for event in self._agent_stream(messages, tools, tables, result_tables_store):
            yield event
            if event["type"] == "tool_call":
                tool_calls_log.append({"name": event["skill"], "params": event["params"]})
            elif event["type"] == "tool_result" and tool_calls_log:
                tool_calls_log[-1]["result"] = event.get("text", "")[:200]
        await self._try_update_memory(message, tables)
        if auto_learn:
            skill = await self.distiller.try_distill(message, tool_calls_log)
            if skill:
                yield {"type": "skill_learned", "skill": skill}

    async def execute_plan(
        self,
        message: str,
        steps: List[Dict],
        tables: Dict,
        history: List,
        result_tables_store: Dict,
        code_tool: bool = False,
        auto_learn: bool = False,
    ) -> AsyncGenerator:
        """Execute a user-approved plan step by step."""
        base_messages = self._build_messages(
            f"Original user request: {message}\nExecuting a plan step by step.", tables, history
        )
        tools = self.skills.get_tool_definitions(code_tool=code_tool)
        conversation = list(base_messages)
        tool_calls_log: List[Dict] = []

        for i, step in enumerate(steps):
            desc = step.get("description", f"Step {i+1}")
            yield {"type": "step_start", "step_num": i + 1, "total": len(steps), "description": desc}

            step_msg = f"Execute step {i + 1} of {len(steps)}: {desc}"
            step_messages = conversation + [{"role": "user", "content": step_msg}]

            final_text = ""
            async for event in self._agent_stream(step_messages, tools, tables, result_tables_store):
                yield event
                if event["type"] == "tool_call":
                    tool_calls_log.append({"name": event["skill"], "params": event["params"]})
                elif event["type"] == "tool_result" and tool_calls_log:
                    tool_calls_log[-1]["result"] = event.get("text", "")[:200]
                elif event["type"] == "final_text":
                    final_text = event["content"]

            # Add step result to running conversation for context chaining
            conversation.append({"role": "user", "content": step_msg})
            if final_text:
                conversation.append({"role": "assistant", "content": final_text})

            yield {"type": "step_done", "step_num": i + 1}

        # Lightweight self-check: verify the original request was fully addressed
        yield {"type": "reflect_start"}
        reflect_msg = (
            f"Original user request: {message}\n\n"
            f"You just completed all {len(steps)} planned steps. "
            "Do a quick self-check:\n"
            "1. Was the original request fully addressed?\n"
            "2. Are there obvious errors, missing results, or gaps?\n\n"
            "If complete and correct: confirm in 1–2 sentences.\n"
            "If something is missing or wrong: fix it now by calling the appropriate tools."
        )
        async for event in self._agent_stream(
            conversation + [{"role": "user", "content": reflect_msg}],
            tools, tables, result_tables_store,
        ):
            yield event
        yield {"type": "reflect_done"}

        await self._try_update_memory(message, tables)
        if auto_learn:
            skill = await self.distiller.try_distill(message, tool_calls_log)
            if skill:
                yield {"type": "skill_learned", "skill": skill}

    # ------------------------------------------------------------------
    # Core streaming agent loop
    # ------------------------------------------------------------------

    async def _agent_stream(
        self, messages: List, tools: List, tables: Dict, result_tables_store: Dict
    ) -> AsyncGenerator:
        """ReAct streaming loop: stream LLM output, handle tool calls, repeat."""
        msgs = list(messages)

        # DeepSeek V3 leaks its raw tool-call syntax into delta.content.
        # Detect the marker and suppress those text chunks from the display.
        _TOOL_MARKER = "<\uff5ctool\u2581call\u2581begin\uff5c>"  # <｜tool▁call▁begin｜>
        _TOOL_RE = re.compile(r"<\uff5ctool[\s\S]*?(?:<\uff5ctool\u2581call\u2581end\uff5c>|$)", re.DOTALL)

        for iteration in range(self.max_iterations):
            # Accumulate streaming response
            full_content = ""
            # tool_calls_acc: index -> {id, name, arguments_str}
            tool_calls_acc: Dict[int, Dict] = {}
            _suppress_text = False  # True once we spot a tool-call marker in text

            try:
                async for chunk in self.llm.stream_chat(msgs, tools=tools if tools else None):
                    choice = chunk.choices[0] if chunk.choices else None
                    if not choice:
                        continue
                    delta = choice.delta

                    # Stream text content
                    if delta.content:
                        full_content += delta.content
                        # Suppress chunks that are actually tool-call markup
                        if _TOOL_MARKER in delta.content:
                            _suppress_text = True
                        if not _suppress_text:
                            yield {"type": "text_chunk", "content": delta.content}

                    # Accumulate tool call deltas
                    if delta.tool_calls:
                        for tc in delta.tool_calls:
                            idx = tc.index
                            if idx not in tool_calls_acc:
                                tool_calls_acc[idx] = {"id": "", "name": "", "arguments": ""}
                            if tc.id:
                                tool_calls_acc[idx]["id"] = tc.id
                            if tc.function:
                                if tc.function.name:
                                    tool_calls_acc[idx]["name"] += tc.function.name
                                if tc.function.arguments:
                                    tool_calls_acc[idx]["arguments"] += tc.function.arguments

                    # Check for stop reason
                    if choice.finish_reason in ("stop", "end_turn"):
                        break

            except Exception as e:
                yield {"type": "error", "content": f"LLM streaming error: {e}"}
                return

            # Strip any leaked tool-call markup from the visible text
            display_content = _TOOL_RE.sub("", full_content).strip()

            if tool_calls_acc:
                # Build proper tool_calls list for the assistant message
                tc_list = []
                for idx in sorted(tool_calls_acc):
                    tc = tool_calls_acc[idx]
                    tc_list.append({
                        "id": tc["id"] or f"call_{idx}",
                        "type": "function",
                        "function": {"name": tc["name"], "arguments": tc["arguments"]},
                    })

                msgs.append({
                    "role": "assistant",
                    "content": display_content or None,
                    "tool_calls": tc_list,
                })

                # Execute each tool call
                for tc in tc_list:
                    skill_name = tc["function"]["name"]
                    args_str = tc["function"]["arguments"]
                    try:
                        params = json.loads(args_str) if args_str else {}
                    except Exception:
                        params = {}

                    yield {"type": "tool_call", "skill": skill_name, "params": params}

                    result = await self._exec_skill(skill_name, params, tables, result_tables_store)

                    # Emit table event if skill returned one
                    if isinstance(result, dict) and "table" in result:
                        yield {"type": "table", "data": result["table"]}

                    result_text = result.get("text", str(result)) if isinstance(result, dict) else str(result)
                    yield {"type": "tool_result", "skill": skill_name, "text": result_text}

                    msgs.append({
                        "role": "tool",
                        "tool_call_id": tc["id"],
                        "content": result_text,
                    })
            else:
                # No tool calls — final answer
                yield {"type": "final_text", "content": display_content}
                return

        yield {"type": "error", "content": "Agent reached maximum iterations without completing."}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_messages(self, message: str, tables: Dict, history: List) -> List:
        system = self._system_prompt(tables)
        msgs = [{"role": "system", "content": system}]
        # Keep last 12 history turns to stay within context limits
        msgs.extend(history[-12:])
        msgs.append({"role": "user", "content": message})
        return msgs

    def _system_prompt(self, tables: Dict) -> str:
        table_lines = []
        for tid, t in tables.items():
            df = t["df"]
            cols = ", ".join(str(c) for c in df.columns[:12])
            extra = f" (+{len(df.columns)-12} more)" if len(df.columns) > 12 else ""
            table_lines.append(
                f"  - ID=`{tid}` name='{t['name']}' rows={len(df)} cols={len(df.columns)} | columns: {cols}{extra}"
            )
        tables_text = "\n".join(table_lines) or "  (no tables uploaded yet)"

        mem = self.memory.get_all()
        mem_lines = []
        for cat, items in mem.items():
            for k, entry in items.items():
                v = entry["value"] if isinstance(entry, dict) else entry
                mem_lines.append(f"  [{cat}] {k}: {v}")
        mem_text = "\n".join(mem_lines) or "  (empty)"

        custom = self.skills.list_custom()
        custom_text = ""
        if custom:
            custom_text = "\n## Custom Skills\n" + "\n".join(
                f"  - `{s['name']}`: {s['description']}" for s in custom
            )

        instruction_ctx = self.skills.get_instruction_context()
        instruction_text = ""
        if instruction_ctx:
            instruction_text = f"\n## Instruction Skills\n{instruction_ctx}"

        return f"""You are **TabClaw**, an expert AI assistant for table analysis and data manipulation.

## Available Tables
{tables_text}

## User Memory & Preferences
{mem_text}
{custom_text}
{instruction_text}

## Instructions
- Use the available tools to interact with tables. Always call `table_info` first to understand structure.
- For questions about data, retrieve and process the data with tools before answering.
- When your operation produces a new table, give it a descriptive `result_name`.
- Multiple tables can be referenced. Cross-table operations (merge, compare) are supported.
- Explain what you're doing at each step. Be concise but clear.
- Table results appear as interactive tables in the UI — don't repeat raw CSV in your final answer.
- If the user mentions preferences or important facts, they may be stored in memory automatically.

## ⚠️ Mandatory Output Format
You MUST end **every** response with one of the following clearly-marked sections.
Do NOT skip it, even for simple questions.

**For analysis / Q&A** (answering questions, finding patterns, making recommendations):
```
## ✅ 最终结论
- [key finding 1]
- [key finding 2]
- ...
```

**For table operations** (filter, aggregate, sort, merge, pivot, add column, etc. — anything that creates a new table):
```
## ✅ 操作结果
- 已生成结果表格：**[result_name]**（N 行 × M 列）
- [1–2 sentences describing what the table contains and its significance]
```

Rules:
- Keep it to 3–6 bullet points, no more.
- Be specific: include actual numbers, column names, table names.
- This section must come LAST in your reply, after all explanations.
"""

    async def _exec_skill(
        self, skill_name: str, params: Dict, tables: Dict, result_tables_store: Dict
    ) -> Dict:
        # Route custom skills to their own async handler
        custom = next((s for s in self.skills.list_custom() if s["name"] == skill_name), None)
        if custom:
            return await self._exec_custom_skill(custom, params, tables, result_tables_store)
        try:
            result = await asyncio.to_thread(self.skills.execute_sync, skill_name, params, tables)
            if isinstance(result, dict) and "df" in result:
                import uuid as _uuid
                rid = "r_" + _uuid.uuid4().hex[:6]
                rname = result.get("name", "Result")
                df = result["df"]
                result_tables_store[rid] = {
                    "name": rname,
                    "df": df,
                    "source": "computed",
                }
                preview = df.head(200).fillna("").to_dict("records")
                # Prepend any print output from execute_python
                extra = result.get("text", "") if isinstance(result, dict) else ""
                creation_msg = (
                    f"Created table '{rname}' (ID: `{rid}`) with "
                    f"{len(df)} rows × {len(df.columns)} columns."
                )
                return {
                    "text": (extra + "\n\n" + creation_msg).strip() if extra else creation_msg,
                    "table": {
                        "table_id": rid,
                        "name": rname,
                        "columns": df.columns.tolist(),
                        "rows": preview,
                        "total_rows": len(df),
                    },
                }
            return {"text": str(result)}
        except Exception as e:
            return {"text": f"Error in skill `{skill_name}`: {e}"}

    async def _exec_custom_skill(
        self, skill: Dict, params: Dict, tables: Dict, result_tables_store: Dict
    ) -> Dict:
        """Execute a custom skill — code-based or prompt-based."""
        table_id = params.get("table_id", "")
        user_request = params.get("user_request", "")

        # ── Code mode ────────────────────────────────────────────────────────
        if skill.get("code"):
            from skills.code_skill import execute_python
            result = await asyncio.to_thread(
                execute_python,
                {"code": skill["code"], "result_name": skill["name"]},
                tables,
            )
            if isinstance(result, dict) and "df" in result:
                import uuid as _uuid
                rid = "r_" + _uuid.uuid4().hex[:6]
                rname = result.get("name", skill["name"])
                df = result["df"]
                result_tables_store[rid] = {"name": rname, "df": df, "source": "computed"}
                preview = df.head(200).fillna("").to_dict("records")
                extra = result.get("text", "")
                creation_msg = f"Created table '{rname}' (ID: `{rid}`) with {len(df)} rows × {len(df.columns)} columns."
                return {
                    "text": (extra + "\n\n" + creation_msg).strip() if extra else creation_msg,
                    "table": {"table_id": rid, "name": rname, "columns": df.columns.tolist(),
                              "rows": preview, "total_rows": len(df)},
                }
            return result if isinstance(result, dict) else {"text": str(result)}

        # ── Prompt mode ───────────────────────────────────────────────────────
        prompt_template = skill.get("prompt") or skill.get("description", "")
        table_name = tables.get(table_id, {}).get("name", table_id) if table_id else ""
        system_prompt = prompt_template.replace("{table_name}", table_name).replace(
            "{user_request}", user_request
        )

        # Attach a preview of the relevant table as context
        context = system_prompt
        if table_id and table_id in tables:
            df = tables[table_id]["df"]
            preview_csv = df.head(30).fillna("").to_csv(index=False)
            context += f"\n\nTable '{table_name}' preview (first 30 rows):\n{preview_csv}"

        resp = await self.llm.chat([
            {"role": "system", "content": context},
            {"role": "user", "content": user_request or f"Execute the '{skill['name']}' skill."},
        ])
        return {"text": (resp.content or "").strip()}

    async def _try_update_memory(self, user_message: str, tables: Dict):
        """Lightweight background memory extraction — non-critical."""
        try:
            prompt = f"""From this user interaction, extract any preferences or important facts worth remembering.
User said: "{user_message}"

Return ONLY a compact JSON array (max 3 items) or [] if nothing notable:
[{{"category": "preferences|domain_knowledge|user_context|history_insights", "key": "short_key", "value": "value"}}]
Output ONLY the JSON array:"""
            resp = await self.llm.chat([{"role": "user", "content": prompt}])
            content = (resp.content or "").strip()
            match = re.search(r"\[.*?\]", content, re.DOTALL)
            if match:
                items = json.loads(match.group())
                for item in items[:3]:
                    if all(k in item for k in ["category", "key", "value"]) and item["value"]:
                        self.memory.set(item["category"], item["key"], item["value"])
        except Exception:
            pass
