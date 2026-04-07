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
# ==========================================
# CONFIG
# ==========================================
DISCORD_BOT_TOKEN = "YOUR_DISCORD_TOKEN_HERE"
OPENAI_API_KEY = "YOUR_OPENAI_API_KEY_HERE"
DB_FILE = "claw_v8_final.db"
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
llm_client = AsyncOpenAI(api_key=OPENAI_API_KEY)
docker_client = docker.from_env()
DB_CONN = None
# ==========================================
# DYNAMIC SEMAPHORE
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
EXEC_SEMAPHORE = DynamicSemaphore(4)
WEB_SEMAPHORE = DynamicSemaphore(8)
# ==========================================
# DATABASE
# ==========================================
async def init_db():
    global DB_CONN
    DB_CONN = await aiosqlite.connect(DB_FILE)
    await DB_CONN.executescript('''
        CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY, user_id TEXT, role TEXT, content TEXT);
        CREATE TABLE IF NOT EXISTS background_tasks (
            id INTEGER PRIMARY KEY, user_id TEXT, channel_id TEXT,
            plan_json TEXT, current_step INTEGER DEFAULT 0,
            status TEXT DEFAULT 'pending', result TEXT, retry_count INTEGER DEFAULT 0, priority INTEGER DEFAULT 10
        );
        CREATE TABLE IF NOT EXISTS vector_memory (id INTEGER PRIMARY KEY, content TEXT, embedding TEXT);
    ''')
    await DB_CONN.commit()
async def save_message(user_id: str, role: str, content: str):
    await DB_CONN.execute("INSERT INTO messages (user_id, role, content) VALUES (?,?,?)", (user_id, role, content))
    await DB_CONN.commit()
async def get_history(user_id: str, limit: int = 8):
    async with DB_CONN.execute("SELECT role, content FROM messages WHERE user_id=? ORDER BY id DESC LIMIT ?", (user_id, limit)) as c:
        rows = await c.fetchall()
    return [{"role": r[0], "content": r[1]} for r in reversed(rows)]
# ==========================================
# INFRASTRUCTURE TOOLS
# ==========================================
async def execute_python_code(code: str) -> str:
    async with EXEC_SEMAPHORE:
        try:
            def run_sync():
                container = docker_client.containers.run(
                    "python:3.11-slim", ["python", "-c", code],
                    remove=True, stdout=True, stderr=True,
                    network_disabled=True, mem_limit="128m", cpu_quota=40000, timeout=12
                )
                return container.decode("utf-8").strip()[:1500] or "No output"
            return await asyncio.wait_for(asyncio.to_thread(run_sync), 15)
        except asyncio.TimeoutError:
            return "Execution timeout."
        except Exception as e:
            return f"Sandbox error: {str(e)[:400]}"
async def fetch_web_content(url: str) -> str:
    async with WEB_SEMAPHORE:
        try:
            def fetch_sync():
                import urllib.request
                req = urllib.request.Request(url, headers={"User-Agent": "ClawBot/1.0"})
                with urllib.request.urlopen(req, timeout=10) as r:
                    html = r.read().decode("utf-8", errors="ignore")
                soup = BeautifulSoup(html, "html.parser")
                for tag in soup(["script", "style", "nav", "footer", "header"]):
                    tag.decompose()
                text = "\n".join(line.strip() for line in soup.get_text().splitlines() if line.strip())
                return text[:3000]
            return await asyncio.wait_for(asyncio.to_thread(fetch_sync), 12)
        except Exception as e:
            return f"Web fetch error: {str(e)}"
async def store_memory(content: str) -> str:
    try:
        emb = await llm_client.embeddings.create(model="text-embedding-3-small", input=content)
        vec = json.dumps(emb.data[0].embedding)
        await DB_CONN.execute("INSERT INTO vector_memory (content, embedding) VALUES (?,?)", (content, vec))
        await DB_CONN.commit()
        return "Information stored in long-term memory."
    except Exception as e:
        return f"Memory store failed: {e}"
async def query_memory(query: str, top_k: int = 3) -> str:
    try:
        q_resp = await llm_client.embeddings.create(model="text-embedding-3-small", input=query)
        q_vec = np.array(q_resp.data[0].embedding)
        async with DB_CONN.execute("SELECT content, embedding FROM vector_memory") as c:
            rows = await c.fetchall()
        results = []
        for content, emb_json in rows:
            vec = np.array(json.loads(emb_json))
            sim = np.dot(q_vec, vec)/(np.linalg.norm(q_vec)*np.linalg.norm(vec)+1e-8)
            results.append((sim, content))
        results.sort(reverse=True, key=lambda x: x[0])
        return "\n\n".join(c for _, c in results[:top_k]) if results else "No relevant memories found."
    except Exception as e:
        return f"Memory query failed: {e}"
# ==========================================
# TOOL SCHEMAS
# ==========================================
STATE_TOOL = {
    "type": "function", "function": {
        "name": "report_step_status", "description": "Report outcome of current step.",
        "parameters": {
            "type": "object", "properties": {
                "status": {"type": "string", "enum":["step_complete","step_failed_retry","task_fully_complete","replan_needed"]},
                "reason": {"type": "string"}, "new_plan": {"type": "array","items":{"type":"string"}}
            }, "required":["status","reason"]
        }
    }
}
ALL_TOOLS = [
    {"type": "function", "function": {"name":"execute_python_code","description":"Run Python in sandbox","parameters":{"type":"object","properties":{"code":{"type":"string"}},"required":["code"]}}},
    {"type": "function", "function": {"name":"fetch_web_content","description":"Fetch cleaned webpage text","parameters":{"type":"object","properties":{"url":{"type":"string"}},"required":["url"]}}},
    {"type": "function", "function": {"name":"store_memory","description":"Store info in vector memory","parameters":{"type":"object","properties":{"content":{"type":"string"}},"required":["content"]}}},
    {"type": "function", "function": {"name":"query_memory","description":"Recall relevant memories","parameters":{"type":"object","properties":{"query":{"type":"string"}},"required":["query"]}}},
    {"type": "function", "function": {"name":"defer_to_background","description":"Queue multi-step task","parameters":{"type":"object","properties":{"goal":{"type":"string"},"initial_plan":{"type":"array","items":{"type":"string"}}},"required":["goal", "initial_plan"]}}},
    {"type": "function", "function": {"name":"create_discord_channel","description":"Admin create channel","parameters":{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}}},
    {"type": "function", "function": {"name":"delete_discord_channel","description":"Admin delete channel","parameters":{"type":"object","properties":{"channel_id":{"type":"string"}},"required":["channel_id"]}}},
    {"type": "function", "function": {"name":"manage_roles","description":"Admin manage roles","parameters":{"type":"object","properties":{"member_id":{"type":"string"},"role_name":{"type":"string"},"action":{"type":"string","enum":["add","remove"]}},"required":["member_id","role_name","action"]}}},
    STATE_TOOL
]
# ==========================================
# NEURO-SYMBOLIC TOOL LOOP
# ==========================================
async def process_with_tools(messages: list, ctx_guild=None, ctx_channel=None, ctx_user_id=None, max_iterations: int = 8) -> str:
    for _ in range(max_iterations):
        response = await llm_client.chat.completions.create(
            model="gpt-4o", messages=messages, tools=ALL_TOOLS, tool_choice="auto"
        )
        msg = response.choices[0].message
        messages.append({"role":"assistant","content":msg.content,"tool_calls":msg.tool_calls})
        if not msg.tool_calls:
            return msg.content or "No response"
       
        for tc in msg.tool_calls:
            args = json.loads(tc.function.arguments)
            name = tc.function.name
            result = "Tool failed."
           
            try:
                if name=="execute_python_code":
                    result = await execute_python_code(args.get("code",""))
                elif name=="fetch_web_content":
                    result = await fetch_web_content(args.get("url",""))
                elif name=="store_memory":
                    result = await store_memory(args.get("content",""))
                elif name=="query_memory":
                    result = await query_memory(args.get("query",""))
                elif name=="report_step_status":
                    return json.dumps(args)
                elif name=="defer_to_background":
                    if not ctx_channel or not ctx_user_id:
                        result = "Context missing for background deferral."
                    else:
                        plan = args.get("initial_plan", [])
                        await DB_CONN.execute(
                            "INSERT INTO background_tasks (user_id, channel_id, plan_json, status) VALUES (?,?,?,?)",
                            (ctx_user_id, str(ctx_channel.id), json.dumps(plan), 'pending')
                        )
                        await DB_CONN.commit()
                        result = f"Task deferred. Goal: {args.get('goal')}"
               
                # ADMIN TOOLS (Requires Guild Context)
                elif name=="create_discord_channel":
                    if ctx_guild:
                        ch = await ctx_guild.create_text_channel(args.get("name"))
                        result = f"Channel created successfully: {ch.id}"
                    else:
                        result = "No guild context available."
                elif name=="delete_discord_channel":
                    if ctx_guild:
                        ch_to_del = ctx_guild.get_channel(int(args.get("channel_id")))
                        if ch_to_del:
                            await ch_to_del.delete()
                            result = "Channel deleted."
                        else:
                            result = "Channel not found."
                    else:
                        result = "No guild context available."
                elif name=="manage_roles":
                    if ctx_guild:
                        member = ctx_guild.get_member(int(args.get("member_id")))
                        role = discord.utils.get(ctx_guild.roles, name=args.get("role_name"))
                        if member and role:
                            if args.get("action") == "add":
                                await member.add_roles(role)
                            else:
                                await member.remove_roles(role)
                            result = f"Role {args.get('action')} applied."
                        else:
                            result = "Member or Role not found."
                    else:
                        result = "No guild context available."
            except Exception as e:
                result = f"Error executing {name}: {str(e)}"
               
            messages.append({"role":"tool","tool_call_id":tc.id,"name":name,"content":result})
           
    return "Max iterations reached. Last output: "+(messages[-1].get("content") or "")
# ==========================================
# DISCORD CLIENT
# ==========================================
class ClawAgent(discord.Client):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.members = True
        super().__init__(intents=intents)
        self.recent_latencies = deque(maxlen=50)
    async def setup_hook(self):
        await init_db()
        self.autonomous_worker.start()
        self.autonomic_tuner.start()
        logging.info("=== CLAW V8 AUTONOMIC ADMINISTRATOR ONLINE ===")
    async def on_message(self,message):
        if message.author==self.user: return
        user_id = str(message.author.id)
       
        await save_message(user_id,"user",message.content)
        history = await get_history(user_id)
        sys_prompt = {"role":"system","content":"You are Claw, an autonomous agent. Use tools to achieve goals. For long tasks, use defer_to_background."}
        messages = [sys_prompt] + history
       
        async with message.channel.typing():
            reply = await process_with_tools(
                messages,
                ctx_guild=message.guild,
                ctx_channel=message.channel,
                ctx_user_id=user_id,
                max_iterations=6
            )
            await save_message(user_id,"assistant",reply)
            chunks = [reply[i:i+1900] for i in range(0,len(reply),1900)]
            for chunk in chunks: await message.channel.send(chunk)
    @tasks.loop(seconds=20)
    async def autonomous_worker(self):
        async with DB_CONN.execute("SELECT id,user_id,channel_id,plan_json,current_step,status FROM background_tasks WHERE status='pending' LIMIT 3") as c:
            pending = await c.fetchall()
           
        for task_id, user_id, channel_id, plan_json, step_idx, _ in pending:
            start_time = time.time()
            try:
                await DB_CONN.execute("UPDATE background_tasks SET status='running' WHERE id=?",(task_id,))
                await DB_CONN.commit()
               
                plan = json.loads(plan_json)
                if step_idx >= len(plan):
                    await DB_CONN.execute("UPDATE background_tasks SET status='completed' WHERE id=?",(task_id,))
                    await DB_CONN.commit()
                    continue
                   
                current_step = plan[step_idx]
                logging.info(f"Task {task_id} - Step {step_idx+1}/{len(plan)}: {current_step}")
               
                # Fetch Discord context for the background execution
                channel = await self.fetch_channel(int(channel_id))
                guild = channel.guild if hasattr(channel, 'guild') else None
               
                step_messages=[{"role":"system","content":f"Current task step: {current_step}. Call report_step_status when done."}]
                result = await process_with_tools(
                    step_messages,
                    ctx_guild=guild,
                    ctx_channel=channel,
                    ctx_user_id=user_id,
                    max_iterations=10
                )
               
                try:
                    state_report = json.loads(result)
                    status = state_report.get("status")
                    reason = state_report.get("reason","")
                    new_plan = state_report.get("new_plan")
                   
                    if status == "step_complete":
                        new_step = step_idx+1
                        task_status = "pending" if new_step < len(plan) else "completed"
                    elif status == "task_fully_complete":
                        new_step = step_idx
                        task_status = "completed"
                    elif status in ("step_failed_retry","replan_needed"):
                        new_step = step_idx
                        task_status = "pending"
                        if new_plan: plan = new_plan
                    else:
                        new_step = step_idx+1
                        task_status = "pending"
                       
                    await DB_CONN.execute(
                        "UPDATE background_tasks SET current_step=?,status=?,result=?,plan_json=? WHERE id=?",
                        (new_step, task_status, f"{reason}: {result[:600]}", json.dumps(plan), task_id)
                    )
                    await DB_CONN.commit()
                except json.JSONDecodeError:
                    await DB_CONN.execute(
                        "UPDATE background_tasks SET current_step=?,status='pending',result=? WHERE id=?",
                        (step_idx+1, result[:800], task_id)
                    )
                    await DB_CONN.commit()
               
                self.recent_latencies.append(time.time() - start_time)
               
                # Send update back to the Discord channel
                if channel:
                    await channel.send(f"<@{user_id}> **Background Update (Task {task_id}, Step {step_idx+1}):**\n{result[:1700]}")
                   
            except Exception as e:
                logging.error(f"Background task {task_id} error: {e}")
                await DB_CONN.execute("UPDATE background_tasks SET status='failed',result=? WHERE id=?",(str(e)[:500],task_id))
                await DB_CONN.commit()
    @tasks.loop(seconds=15)
    async def autonomic_tuner(self):
        if not self.recent_latencies: return
        avg_latency = sum(self.recent_latencies)/len(self.recent_latencies)
        HIGH_THRESH, LOW_THRESH, DELTA = 5.0, 1.0, 1
       
        if avg_latency > HIGH_THRESH:
            await EXEC_SEMAPHORE.tune(DELTA)
            await WEB_SEMAPHORE.tune(DELTA)
        elif avg_latency < LOW_THRESH:
            await EXEC_SEMAPHORE.tune(-DELTA)
            await WEB_SEMAPHORE.tune(-DELTA)
           
        logging.info(f"Autonomic tuning: avg_latency={avg_latency:.2f}s, EXEC_LIMIT={EXEC_SEMAPHORE._limit}, WEB_LIMIT={WEB_SEMAPHORE._limit}")
# ==========================================
# MAIN
# ==========================================
async def main():
    client = ClawAgent()
    try:
        await client.start(DISCORD_BOT_TOKEN)
    finally:
        if DB_CONN: await DB_CONN.close()
if __name__=="__main__":
    asyncio.run(main())
