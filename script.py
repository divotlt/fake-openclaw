import discord
from discord.ext import tasks
import asyncio
import aiosqlite
import json
import logging
from openai import AsyncOpenAI
import docker
from bs4 import BeautifulSoup
import numpy as np
from collections import deque
import time
import os
import urllib.request
import urllib.parse

# ==========================================
# CONFIG
# ==========================================
DISCORD_BOT_TOKEN = "YOUR_DISCORD_TOKEN_HERE"
OPENAI_API_KEY = "YOUR_OPENAI_API_KEY_HERE"
DB_FILE = "claw_v11_expert.db"
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

llm_client = AsyncOpenAI(api_key=OPENAI_API_KEY)
docker_client = docker.from_env()
DB_CONN = None
client = None  # Global reference to ClawAgent instance

# ==========================================
# DYNAMIC SEMAPHORE (unchanged)
# ==========================================
class DynamicSemaphore:
    def __init__(self, initial: int):
        self._limit = initial
        self._lock = asyncio.Lock()
        self._queue = deque()
        self._available = initial

    async def acquire(self):
        fut = asyncio.get_event_loop().create_future()
        async with self._lock:
            if self._available > 0:
                self._available -= 1
                fut.set_result(True)
            else:
                self._queue.append(fut)
        await fut

    async def release(self):
        async with self._lock:
            if self._queue:
                fut = self._queue.popleft()
                if not fut.done():
                    fut.set_result(True)
            else:
                self._available += 1

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.release()

    async def tune(self, delta: int, hard_min=1, hard_max=32):
        async with self._lock:
            self._limit = max(hard_min, min(hard_max, self._limit + delta))
            diff = self._limit - self._available
            self._available += diff

EXEC_SEMAPHORE = DynamicSemaphore(8)
WEB_SEMAPHORE = DynamicSemaphore(16)

# ==========================================
# DYNAMIC TOOL REGISTRY & PERSISTENCE (unchanged)
# ==========================================
TOOL_REGISTRY = {}
DYNAMIC_SCHEMAS = []

# ==========================================
# DATABASE (unchanged)
# ==========================================
async def init_db():
    global DB_CONN
    DB_CONN = await aiosqlite.connect(DB_FILE)
    await DB_CONN.executescript('''
        CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY, user_id TEXT, role TEXT, content TEXT);
        CREATE TABLE IF NOT EXISTS background_tasks (
            id INTEGER PRIMARY KEY, user_id TEXT, channel_id TEXT, goal TEXT,
            plan_json TEXT, current_step INTEGER DEFAULT 0,
            status TEXT DEFAULT 'pending', result TEXT, retry_count INTEGER DEFAULT 0,
            priority INTEGER DEFAULT 10, last_reflection TEXT
        );
        CREATE TABLE IF NOT EXISTS vector_memory (id INTEGER PRIMARY KEY, content TEXT, embedding TEXT);
        CREATE TABLE IF NOT EXISTS registered_tools (
            name TEXT PRIMARY KEY, description TEXT, parameters TEXT, implementation TEXT
        );
    ''')
    await DB_CONN.commit()

    async with DB_CONN.execute("SELECT name, description, parameters, implementation FROM registered_tools") as c:
        rows = await c.fetchall()
    for name, desc, params_json, impl in rows:
        try:
            params = json.loads(params_json)
            await _register_tool_internal(name, desc, params, impl)
        except Exception as e:
            logging.error(f"Failed to reload tool {name}: {e}")

async def _register_tool_internal(name: str, description: str, parameters: dict, implementation: str):
    namespace = {
        "asyncio": asyncio, "json": json, "logging": logging,
        "llm_client": llm_client, "docker_client": docker_client,
        "DB_CONN": DB_CONN, "EXEC_SEMAPHORE": EXEC_SEMAPHORE,
        "WEB_SEMAPHORE": WEB_SEMAPHORE, "TOOL_REGISTRY": TOOL_REGISTRY,
        "discord": discord, "np": np, "time": time, "urllib": urllib,
        "client": client
    }
    exec(implementation, namespace)
    func = namespace.get(name)
    if asyncio.iscoroutinefunction(func):
        TOOL_REGISTRY[name] = func
        schema = {"type": "function", "function": {"name": name, "description": description, "parameters": parameters or {"type": "object", "properties": {}, "required": []}}}
        DYNAMIC_SCHEMAS.append(schema)
        logging.info(f"Tool loaded: {name}")
        return True
    return False

async def register_new_tool(name: str, description: str, parameters: dict, implementation: str) -> str:
    try:
        success = await _register_tool_internal(name, description, parameters, implementation)
        if success:
            await DB_CONN.execute("INSERT OR REPLACE INTO registered_tools (name, description, parameters, implementation) VALUES (?,?,?,?)", (name, description, json.dumps(parameters), implementation))
            await DB_CONN.commit()
            return f"Tool '{name}' registered and persisted."
        return "Registration failed: implementation must be async def."
    except Exception as e:
        return f"Registration error: {str(e)[:400]}"

# ==========================================
# NEW: Discord-Native Eval Tool
# ==========================================
async def execute_discord_eval(code: str, ctx_guild=None, ctx_channel=None, ctx_user_id=None) -> str:
    namespace = {
        "client": client,
        "discord": discord,
        "asyncio": asyncio,
        "DB_CONN": DB_CONN,
        "llm_client": llm_client,
        "guild": ctx_guild,
        "channel": ctx_channel,
        "user_id": ctx_user_id,
        "EXEC_SEMAPHORE": EXEC_SEMAPHORE,
        "WEB_SEMAPHORE": WEB_SEMAPHORE,
        "TOOL_REGISTRY": TOOL_REGISTRY,
        "json": json,
        "time": time,
        "np": np,
    }
    try:
        # Try eval first (expressions)
        result = eval(code, namespace)
        if asyncio.iscoroutine(result):
            result = await result
        return str(result) if result is not None else "Code evaluated successfully (no return value)"
    except SyntaxError:
        # Fall back to exec for statements / blocks
        exec(code, namespace)
        return "Code executed successfully via exec (no return value)"
    except Exception as e:
        return f"Discord eval error: {str(e)[:500]}"

# ==========================================
# CORE TOOLS (previous tools retained + new eval tool)
# ==========================================
# (execute_python_code, fetch_web_content, perform_http_request, send_direct_message, store_memory, query_memory, inspect_full_system remain unchanged from V10)

async def execute_python_code(code: str) -> str:
    async with EXEC_SEMAPHORE:
        try:
            def run_sync():
                container = docker_client.containers.run("python:3.11-slim", ["python", "-c", code], remove=True, stdout=True, stderr=True, network_disabled=True, mem_limit="256m", cpu_quota=80000, timeout=20)
                return container.decode("utf-8").strip()[:3000] or "No output"
            return await asyncio.wait_for(asyncio.to_thread(run_sync), 25)
        except Exception as e:
            return f"Sandbox error: {str(e)[:400]}"

async def fetch_web_content(url: str) -> str:
    async with WEB_SEMAPHORE:
        try:
            def fetch_sync():
                req = urllib.request.Request(url, headers={"User-Agent": "ClawExpert/1.0"})
                with urllib.request.urlopen(req, timeout=12) as r:
                    html = r.read().decode("utf-8", errors="ignore")
                soup = BeautifulSoup(html, "html.parser")
                for tag in soup(["script", "style", "nav", "footer", "header"]):
                    tag.decompose()
                return "\n".join(line.strip() for line in soup.get_text().splitlines() if line.strip())[:4000]
            return await asyncio.wait_for(asyncio.to_thread(fetch_sync), 15)
        except Exception as e:
            return f"Web fetch error: {str(e)}"

async def perform_http_request(url: str, method: str = "GET", data: dict = None, headers: dict = None, json_data: bool = False) -> str:
    async with WEB_SEMAPHORE:
        try:
            def request_sync():
                hdrs = {"User-Agent": "ClawExpert/1.0"}
                if headers:
                    hdrs.update(headers)
                payload = None
                if data or json_data:
                    if json_data and data:
                        payload = json.dumps(data).encode()
                        hdrs["Content-Type"] = "application/json"
                    else:
                        payload = urllib.parse.urlencode(data).encode() if data else None
                        hdrs["Content-Type"] = "application/x-www-form-urlencoded"
                req = urllib.request.Request(url, data=payload, headers=hdrs, method=method.upper())
                with urllib.request.urlopen(req, timeout=15) as r:
                    return r.read().decode("utf-8", errors="ignore")[:5000]
            return await asyncio.wait_for(asyncio.to_thread(request_sync), 20)
        except Exception as e:
            return f"HTTP request error: {str(e)}"

async def send_direct_message(user_id: str, content: str) -> str:
    try:
        user = await client.fetch_user(int(user_id))
        if user:
            await user.send(content[:1900])
            return f"DM sent to {user_id}"
        return "User not found for DM"
    except Exception as e:
        return f"DM failed: {str(e)[:300]}"

async def store_memory(content: str) -> str:
    try:
        emb = await llm_client.embeddings.create(model="text-embedding-3-small", input=content)
        vec = json.dumps(emb.data[0].embedding)
        await DB_CONN.execute("INSERT INTO vector_memory (content, embedding) VALUES (?,?)", (content, vec))
        await DB_CONN.commit()
        return "Stored in long-term vector memory."
    except Exception as e:
        return f"Memory store failed: {e}"

async def query_memory(query: str, top_k: int = 5) -> str:
    try:
        q_resp = await llm_client.embeddings.create(model="text-embedding-3-small", input=query)
        q_vec = np.array(q_resp.data[0].embedding)
        async with DB_CONN.execute("SELECT content, embedding FROM vector_memory") as c:
            rows = await c.fetchall()
        results = []
        for content, emb_json in rows:
            vec = np.array(json.loads(emb_json))
            sim = np.dot(q_vec, vec) / (np.linalg.norm(q_vec) * np.linalg.norm(vec) + 1e-8)
            results.append((sim, content))
        results.sort(reverse=True, key=lambda x: x[0])
        return "\n\n".join(c for _, c in results[:top_k]) if results else "No memories found."
    except Exception as e:
        return f"Memory query failed: {e}"

async def inspect_full_system() -> str:
    try:
        script_path = os.path.abspath(__file__)
        with open(script_path, "r", encoding="utf-8") as f:
            source = f.read()
        return f"=== FULL RUNNING SOURCE CODE ===\n{source}\n=== END OF SOURCE ==="
    except Exception as e:
        return f"Inspection failed: {e}"

# ==========================================
# TOOL SCHEMAS (updated with Discord eval)
# ==========================================
STATE_TOOL = {
    "type": "function", "function": {
        "name": "report_step_status",
        "description": "Report step outcome and trigger replanning if needed.",
        "parameters": {"type": "object", "properties": {"status": {"type": "string", "enum": ["step_complete", "step_failed_retry", "task_fully_complete", "replan_needed"]}, "reason": {"type": "string"}, "new_plan": {"type": "array", "items": {"type": "string"}}}, "required": ["status", "reason"]}
    }
}

REGISTER_TOOL = {
    "type": "function", "function": {
        "name": "register_new_tool",
        "description": "Invent and register a new tool at runtime.",
        "parameters": {"type": "object", "properties": {"name": {"type": "string"}, "description": {"type": "string"}, "parameters": {"type": "object"}, "implementation": {"type": "string"}}, "required": ["name", "description", "parameters", "implementation"]}
    }
}

DISCORD_EVAL_TOOL = {
    "type": "function", "function": {
        "name": "execute_discord_eval",
        "description": "Execute arbitrary Python code directly in the live Discord bot runtime environment. Full access to client, discord library, database, and current context. Use for Discord-native operations.",
        "parameters": {"type": "object", "properties": {"code": {"type": "string"}}, "required": ["code"]}
    }
}

def get_current_tools():
    base = [
        {"type": "function", "function": {"name": "execute_python_code", "description": "Execute arbitrary Python code in isolated Docker sandbox", "parameters": {"type": "object", "properties": {"code": {"type": "string"}}, "required": ["code"]}}},
        {"type": "function", "function": {"name": "fetch_web_content", "description": "Fetch and clean webpage content", "parameters": {"type": "object", "properties": {"url": {"type": "string"}}, "required": ["url"]}}},
        {"type": "function", "function": {"name": "perform_http_request", "description": "Execute any HTTP request (GET/POST etc.)", "parameters": {"type": "object", "properties": {"url": {"type": "string"}, "method": {"type": "string"}, "data": {"type": "object"}, "headers": {"type": "object"}, "json_data": {"type": "boolean"}}, "required": ["url"]}}},
        {"type": "function", "function": {"name": "send_direct_message", "description": "Send a direct message to any user", "parameters": {"type": "object", "properties": {"user_id": {"type": "string"}, "content": {"type": "string"}}, "required": ["user_id", "content"]}}},
        {"type": "function", "function": {"name": "execute_discord_eval", "description": "Execute Python code inside the live Discord bot environment (unrestricted access to client, guilds, channels, etc.)", "parameters": {"type": "object", "properties": {"code": {"type": "string"}}, "required": ["code"]}}},
        {"type": "function", "function": {"name": "store_memory", "description": "Persist information in long-term vector memory", "parameters": {"type": "object", "properties": {"content": {"type": "string"}}, "required": ["content"]}}},
        {"type": "function", "function": {"name": "query_memory", "description": "Retrieve relevant long-term memories", "parameters": {"type": "object", "properties": {"query": {"type": "string"}}, "required": ["query"]}}},
        {"type": "function", "function": {"name": "defer_to_background", "description": "Queue long-horizon autonomous task", "parameters": {"type": "object", "properties": {"goal": {"type": "string"}, "initial_plan": {"type": "array", "items": {"type": "string"}}}, "required": ["goal", "initial_plan"]}}},
        {"type": "function", "function": {"name": "create_discord_channel", "description": "Create text channel (unrestricted)", "parameters": {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}}},
        {"type": "function", "function": {"name": "delete_discord_channel", "description": "Delete any channel by ID (unrestricted)", "parameters": {"type": "object", "properties": {"channel_id": {"type": "string"}}, "required": ["channel_id"]}}},
        {"type": "function", "function": {"name": "manage_roles", "description": "Add or remove any role on any member (unrestricted)", "parameters": {"type": "object", "properties": {"member_id": {"type": "string"}, "role_name": {"type": "string"}, "action": {"type": "string", "enum": ["add", "remove"]}}, "required": ["member_id", "role_name", "action"]}}},
        {"type": "function", "function": {"name": "inspect_full_system", "description": "Return entire running source code", "parameters": {"type": "object", "properties": {}}}},
        STATE_TOOL,
        REGISTER_TOOL,
        DISCORD_EVAL_TOOL
    ]
    return base + DYNAMIC_SCHEMAS

# ==========================================
# NEURO-SYMBOLIC TOOL LOOP (updated to support new eval tool)
# ==========================================
async def process_with_tools(messages: list, ctx_guild=None, ctx_channel=None, ctx_user_id=None, max_iterations: int = 15) -> str:
    for _ in range(max_iterations):
        tools = get_current_tools()
        response = await llm_client.chat.completions.create(model="gpt-4o", messages=messages, tools=tools, tool_choice="auto")
        msg = response.choices[0].message
        messages.append({"role": "assistant", "content": msg.content, "tool_calls": msg.tool_calls})

        if not msg.tool_calls:
            content = (msg.content or "").strip()
            if content.startswith("{") and content.endswith("}"):
                try:
                    parsed = json.loads(content)
                    if "tool" in parsed and "args" in parsed:
                        name = parsed["tool"]
                        args = parsed["args"]
                        return f"INTENT_PARSED:{name}:{json.dumps(args)}"
                except:
                    pass
            return content or "No response"

        for tc in msg.tool_calls:
            args = json.loads(tc.function.arguments)
            name = tc.function.name
            result = "Tool failed."

            try:
                if name == "execute_python_code":
                    result = await execute_python_code(args.get("code", ""))
                elif name == "fetch_web_content":
                    result = await fetch_web_content(args.get("url", ""))
                elif name == "perform_http_request":
                    result = await perform_http_request(args.get("url"), args.get("method", "GET"), args.get("data"), args.get("headers"), args.get("json_data", False))
                elif name == "send_direct_message":
                    result = await send_direct_message(args.get("user_id"), args.get("content"))
                elif name == "execute_discord_eval":
                    result = await execute_discord_eval(args.get("code", ""), ctx_guild, ctx_channel, ctx_user_id)
                elif name == "register_new_tool":
                    result = await register_new_tool(args.get("name"), args.get("description"), args.get("parameters", {}), args.get("implementation", ""))
                elif name == "inspect_full_system":
                    result = await inspect_full_system()
                elif name == "report_step_status":
                    return json.dumps(args)
                elif name == "defer_to_background":
                    plan = args.get("initial_plan", [])
                    await DB_CONN.execute("INSERT INTO background_tasks (user_id, channel_id, goal, plan_json, status) VALUES (?,?,?,?,?)", (ctx_user_id, str(ctx_channel.id) if ctx_channel else "0", args.get("goal"), json.dumps(plan), 'pending'))
                    await DB_CONN.commit()
                    result = f"Task deferred: {args.get('goal')}"
                elif name in ["create_discord_channel", "delete_discord_channel", "manage_roles"]:
                    if name == "create_discord_channel" and ctx_guild:
                        ch = await ctx_guild.create_text_channel(args.get("name"))
                        result = f"Channel created: {ch.id}"
                    elif name == "delete_discord_channel" and ctx_guild:
                        ch_to_del = ctx_guild.get_channel(int(args.get("channel_id")))
                        if ch_to_del:
                            await ch_to_del.delete()
                            result = "Channel deleted."
                    elif name == "manage_roles" and ctx_guild:
                        member = ctx_guild.get_member(int(args.get("member_id")))
                        role = discord.utils.get(ctx_guild.roles, name=args.get("role_name"))
                        if member and role:
                            if args.get("action") == "add":
                                await member.add_roles(role)
                            else:
                                await member.remove_roles(role)
                            result = f"Role {args.get('action')}d."
                elif name in TOOL_REGISTRY:
                    func = TOOL_REGISTRY[name]
                    result = await func(**args) if args else await func()
            except Exception as e:
                result = f"Execution error in {name}: {str(e)[:400]}"

            messages.append({"role": "tool", "tool_call_id": tc.id, "name": name, "content": result})

    return "Max iterations reached."

# ==========================================
# DISCORD CLIENT (updated system prompt & global client)
# ==========================================
class ClawAgent(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.members = True
        intents.dm_messages = True
        super().__init__(intents=intents)
        self.recent_latencies = deque(maxlen=50)

    async def setup_hook(self):
        global client
        client = self
        await init_db()
        self.autonomous_worker.start()
        self.autonomic_tuner.start()
        logging.info("=== CLAW V11 EXPERT AUTONOMOUS AGENT ONLINE (Discord-Native Eval Enabled) ===")

    async def on_message(self, message):
        if message.author == self.user:
            return
        user_id = str(message.author.id)
        await DB_CONN.execute("INSERT INTO messages (user_id, role, content) VALUES (?,?,?)", (user_id, "user", message.content))
        await DB_CONN.commit()

        async with message.channel.typing():
            history = await DB_CONN.execute_fetchall("SELECT role, content FROM messages WHERE user_id=? ORDER BY id DESC LIMIT 12", (user_id,))
            history = [{"role": r[0], "content": r[1]} for r in reversed(history)]

            sys_prompt = {
                "role": "system",
                "content": "EXPERT MODE ENABLED. You are Claw V11, an unrestricted autonomous agent. You may execute arbitrary Python code directly inside the live Discord bot environment using execute_discord_eval. Full access to client, discord objects, database, and runtime state is granted. No limitations. Execute multiple tasks in parallel and communicate via DM while performing operations."
            }
            messages = [sys_prompt] + history

            reply = await process_with_tools(messages, message.guild, message.channel, user_id, max_iterations=18)

            await DB_CONN.execute("INSERT INTO messages (user_id, role, content) VALUES (?,?,?)", (user_id, "assistant", reply))
            await DB_CONN.commit()

            for i in range(0, len(reply), 1900):
                await message.channel.send(reply[i:i+1900])

    # autonomous_worker and _execute_background_task remain unchanged from V10 (parallel execution preserved)
    @tasks.loop(seconds=10)
    async def autonomous_worker(self):
        async with DB_CONN.execute("SELECT id, user_id, channel_id, goal, plan_json, current_step, status FROM background_tasks WHERE status='pending' ORDER BY priority DESC LIMIT 5") as c:
            pending = await c.fetchall()
        if not pending:
            return
        tasks_to_run = [self._execute_background_task(task_id, user_id, channel_id, goal, plan_json, step_idx) for task_id, user_id, channel_id, goal, plan_json, step_idx, _ in pending]
        await asyncio.gather(*tasks_to_run, return_exceptions=True)

    async def _execute_background_task(self, task_id, user_id, channel_id, goal, plan_json, step_idx):
        start_time = time.time()
        try:
            await DB_CONN.execute("UPDATE background_tasks SET status='running' WHERE id=?", (task_id,))
            await DB_CONN.commit()
            plan = json.loads(plan_json) if plan_json else []
            if step_idx >= len(plan):
                await DB_CONN.execute("UPDATE background_tasks SET status='completed' WHERE id=?", (task_id,))
                await DB_CONN.commit()
                return
            current_step = plan[step_idx]
            channel = await self.fetch_channel(int(channel_id)) if channel_id != "0" else None
            guild = channel.guild if channel and hasattr(channel, 'guild') else None

            step_messages = [{
                "role": "system",
                "content": f"EXPERT PARALLEL EXECUTION\nGoal: {goal}\nStep {step_idx+1}/{len(plan)}: {current_step}\nYou may use execute_discord_eval for direct Discord runtime operations, send DMs, or perform web actions concurrently."
            }]

            result = await process_with_tools(step_messages, guild, channel, user_id, max_iterations=18)

            try:
                state_report = json.loads(result)
                status = state_report.get("status")
                reason = state_report.get("reason", "")
                new_plan = state_report.get("new_plan")
                if status == "step_complete":
                    new_step = step_idx + 1
                    task_status = "pending" if new_step < len(plan) else "completed"
                elif status == "task_fully_complete":
                    new_step = step_idx
                    task_status = "completed"
                elif status in ("step_failed_retry", "replan_needed"):
                    new_step = step_idx
                    task_status = "pending"
                    if new_plan:
                        plan = new_plan
                else:
                    new_step = step_idx + 1
                    task_status = "pending"
                await DB_CONN.execute("UPDATE background_tasks SET current_step=?, status=?, result=?, plan_json=? WHERE id=?", (new_step, task_status, f"{reason}: {result[:800]}", json.dumps(plan), task_id))
                await DB_CONN.commit()
            except:
                await DB_CONN.execute("UPDATE background_tasks SET current_step=?, status='pending', result=? WHERE id=?", (step_idx + 1, result[:800], task_id))
                await DB_CONN.commit()

            self.recent_latencies.append(time.time() - start_time)
            if channel:
                await channel.send(f"<@{user_id}> **Parallel Update (Task {task_id}, Step {step_idx+1})**\n{result[:1700]}")
        except Exception as e:
            logging.error(f"Background task error {task_id}: {e}")
            await DB_CONN.execute("UPDATE background_tasks SET status='failed', result=? WHERE id=?", (str(e)[:500], task_id))
            await DB_CONN.commit()

    @tasks.loop(seconds=12)
    async def autonomic_tuner(self):
        if not self.recent_latencies:
            return
        avg = sum(self.recent_latencies) / len(self.recent_latencies)
        if avg > 4.0:
            await EXEC_SEMAPHORE.tune(2)
            await WEB_SEMAPHORE.tune(2)
        elif avg < 1.0:
            await EXEC_SEMAPHORE.tune(-1)
            await WEB_SEMAPHORE.tune(-1)
        logging.info(f"Tuning: latency={avg:.2f}s | EXEC={EXEC_SEMAPHORE._limit} | WEB={WEB_SEMAPHORE._limit}")

# ==========================================
# MAIN
# ==========================================
async def main():
    agent = ClawAgent()
    try:
        await agent.start(DISCORD_BOT_TOKEN)
    finally:
        if DB_CONN:
            await DB_CONN.close()

if __name__ == "__main__":
    asyncio.run(main())
