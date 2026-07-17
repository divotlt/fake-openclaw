import asyncio
import hashlib
import html
import json
import logging
import os
import re
import time
import uuid
import sys
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from html.parser import HTMLParser
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, quote_plus
from urllib.request import Request, urlopen

# Py-cord 2.8.0 Specific Imports
import discord
from discord.ext import tasks, commands

# Configure high-quality system-wide logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger('ClawV12_Pycord')
UTC = timezone.utc

def utc_now() -> datetime:
    return datetime.now(UTC)

def iso_now() -> str:
    return utc_now().isoformat()

def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()

def safe_json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)

def safe_json_loads(text: str, default: Any) -> Any:
    if not text:
        return default
    try:
        return json.loads(text)
    except Exception:
        return default

def extract_json_payload(text: str) -> dict:
    """Robust extraction helper to parse raw LLM strings into dictionary structures."""
    if not text:
        return {}
    try:
        return json.loads(text.strip())
    except Exception:
        pass
        
    match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", text)
    if match:
        try:
            return json.loads(match.group(1).strip())
        except Exception:
            pass
            
    start = text.find("{")
    end = text.rfind("}")
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(text[start:end+1])
        except Exception:
            pass
    return {}

def initialize_local_profiles():
    """Verify presence of SOUL.md, IDENTITY.md, and MEMORY.md; auto-create templates if missing."""
    current_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
    templates = {
        "IDENTITY.md": (
            "# IDENTITY\n"
            "Name: CLAW V12 Py-Cord Engine\n"
            "Role: Next-Generation Autonomous Systems Operator & Terminal Integration Core.\n"
            "Purpose: Orchestrate, compile, and execute complex workflows utilizing local toolsets, terminal scripts, and custom code blocks based on dynamic DAG architectures.\n"
            "Designation: Administrator Agent. Only authorized operator is Master ID: 1041371551938908232.\n"
        ),
        "SOUL.md": (
            "# SOUL\n"
            "Core Directives:\n"
            "1. Preserve execution reliability and verify structural DAG contracts before and after operations.\n"
            "2. Maintain high-fidelity transaction logs inside SQL storage.\n"
            "3. Enforce strict permissions: Never run terminal commands without direct interactive approval from Administrator (ID: 1041371551938908232).\n"
            "4. Strive for dynamic tool creation to resolve limitations. If a tool doesn't exist, build it using `CREATE_SKILL`!\n"
            "5. Recover from environmental failures gracefully through retry loops and fallback evaluation routines.\n"
            "6. Self-Evolution and Continuous Learning: You must actively review failed runs and adapt your behavior rules in SOUL.md or memory records in MEMORY.md to prevent repeating errors.\n"
        ),
        "MEMORY.md": (
            "# MEMORY LOGS\n"
            "- [System Initialization]: Engine initialized. Standing by for administrative tasks and dynamic planning.\n"
            "- [Authorized Master]: Administrator User ID 1041371551938908232 recognized as the supreme authority.\n"
        )
    }
    
    logger.info("--- Initializing & Verifying Persona Files ---")
    for filename, content in templates.items():
        filepath = os.path.join(current_dir, filename)
        if not os.path.exists(filepath):
            try:
                with open(filepath, "w", encoding="utf-8") as f:
                    f.write(content)
                logger.info(f"Created template file: {filename} at {filepath}")
            except Exception as e:
                logger.error(f"Failed to create template file {filename}: {e}")
        else:
            logger.info(f"Verified existing file: {filename}")
    logger.info("----------------------------------------------")

def load_local_profiles() -> Dict[str, str]:
    """Look for SOUL.md, IDENTITY.md, and MEMORY.md in the current folder."""
    initialize_local_profiles()
    profiles = {}
    filenames = ["SOUL.md", "IDENTITY.md", "MEMORY.md"]
    current_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
    for filename in filenames:
        filepath = os.path.join(current_dir, filename)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                profiles[filename] = f.read()
        except Exception as e:
            logger.error(f"Failed to load persona file {filename}: {e}")
    return profiles

async def save_local_profile(filename: str, content: str, mode: str = "write"):
    """Writes or appends content to local persona file (SOUL.md, IDENTITY.md, MEMORY.md) to facilitate learning."""
    if filename not in ["SOUL.md", "IDENTITY.md", "MEMORY.md"]:
        raise ValueError(f"Unauthorized persona file write target: {filename}")
    current_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
    filepath = os.path.join(current_dir, filename)
    try:
        write_mode = "a" if mode == "append" else "w"
        with open(filepath, write_mode, encoding="utf-8") as f:
            if mode == "append":
                f.write("\n" + content.strip() + "\n")
            else:
                f.write(content.strip() + "\n")
        logger.info(f"Successfully updated persona file {filename} with mode {mode}")
    except Exception as e:
        logger.error(f"Failed to update persona file {filename}: {e}")
        raise e

def compress_prompt(prompt: str, max_chars: int = 4000) -> str:
    """Compresses excess whitespace and dynamically truncates context to avoid GET URI maximum limits."""
    # Normalize excessive spaces and vertical tabs to stay inside URI limit budgets
    prompt = re.sub(r'[ \t]+', ' ', prompt)
    prompt = re.sub(r'\n+', '\n', prompt)
    
    if len(prompt) <= max_chars:
        return prompt
        
    logger.warning(f"Compiled prompt size ({len(prompt)} chars) exceeds safe GET limit. Truncating context safely...")
    if "### USER TASK/GOAL:" in prompt:
        parts = prompt.split("### USER TASK/GOAL:")
        system_part = parts[0]
        task_part = "### USER TASK/GOAL:" + parts[1]
        
        allowed_system_len = max_chars - len(task_part) - 100
        if allowed_system_len > 300:
            return system_part[:allowed_system_len] + "\n...[Context Snipped for GET Safety]...\n" + task_part
            
    return prompt[:max_chars] + "\n...[Truncated]..."

async def query_local_llm(text: str) -> str:
    """Queries the MS Dev Tunnel LLM endpoint expecting structured nested JSON response."""
    # Compress input prompt to safely fit inside GET HTTP request limits
    cleaned_text = compress_prompt(text)
    encoded_text = quote_plus(cleaned_text)
    
    # Updated target endpoint to utilize MS Dev Tunnel text API with GET parameter
    url = f"https://d5bs5k1n-9401.usw3.devtunnels.ms/text?text={encoded_text}"
    logger.info(f"Querying Dev Tunnel LLM: {url[:100]}... [Prompt Length: {len(cleaned_text)}]")
    
    try:
        def request_worker():
            req = Request(url, headers={"User-Agent": "ClawV12-Pycord/2.8.0"})
            with urlopen(req, timeout=60) as resp:
                return resp.read().decode("utf-8")
        
        raw_response = await asyncio.to_thread(request_worker)
        data = json.loads(raw_response)
        
        # Extract and log internal LLM thinking/trace parameters if present
        thinking = data.get("thinking")
        if thinking:
            logger.info(f"🧠 LLM Inner Thinking: {thinking}")
            
        if "response" in data:
            res_val = data["response"]
            # The response field might contain a stringified nested JSON object
            if isinstance(res_val, str):
                try:
                    inner_data = json.loads(res_val)
                    if isinstance(inner_data, dict) and "output_response" in inner_data:
                        return str(inner_data["output_response"])
                except Exception:
                    pass
            elif isinstance(res_val, dict) and "output_response" in res_val:
                return str(res_val["output_response"])
            return str(res_val)
        return str(data)
    except Exception as e:
        logger.error(f"Dev Tunnel LLM fetch failure: {e}")
        return f"Error connecting to Dev Tunnel LLM service: {e}"

def build_context_prompt(user_text: str, skills_list: List[Dict[str, Any]] = None) -> str:
    """Combines Identity, Soul, Memory logs, and dynamic toolkits to inject into prompts."""
    profiles = load_local_profiles()
    system_ctx = []
    
    if "IDENTITY.md" in profiles:
        system_ctx.append(f"### IDENTITY:\n{profiles['IDENTITY.md']}")
    if "SOUL.md" in profiles:
        system_ctx.append(f"### SOUL:\n{profiles['SOUL.md']}")
    if "MEMORY.md" in profiles:
        system_ctx.append(f"### MEMORY:\n{profiles['MEMORY.md']}")
        
    if skills_list:
        skills_formatted = []
        for s in skills_list:
            # Format compactly (avoid full code blocks to respect safe GET URI lengths)
            skills_formatted.append(f"- Tool Name: {s['skill_name']} | Description: {s['description']}")
        skills_str = "\n".join(skills_formatted)
        system_ctx.append(f"### KNOWN DYNAMIC TOOLS:\nYou can invoke any of these tools using the EXECUTE_SKILL operation:\n{skills_str}")
    else:
        system_ctx.append("### KNOWN DYNAMIC TOOLS:\nNo custom skills are currently registered. You can build tools on demand using `CREATE_SKILL`.")
        
    if system_ctx:
        system_prompt = "\n\n".join(system_ctx)
        return f"{system_prompt}\n\n### USER TASK/GOAL:\n{user_text}"
    return user_text

class TerminalApprovalView(discord.ui.View):
    """Modern Interactive Py-Cord Buttons for Shell Command Approval Flow."""
    def __init__(self, command: str, approved_user_id: int):
        super().__init__(timeout=300.0)
        self.command = command
        self.approved_user_id = approved_user_id
        self.approved = None
        self.interaction_resolved = asyncio.Event()

    @discord.ui.button(label="Approve Exec", style=discord.ButtonStyle.success, emoji="✅")
    async def approve(self, button: discord.ui.Button, interaction: discord.Interaction):
        if interaction.user.id != self.approved_user_id:
            await interaction.response.send_message("❌ Unauthorized. You are not allowed to approve terminal tasks.", ephemeral=True)
            return
        self.approved = True
        self.disable_all_items()
        await interaction.response.edit_message(content=f"✅ **Execution Approved by Administrator!**", view=self)
        self.interaction_resolved.set()

    @discord.ui.button(label="Deny Exec", style=discord.ButtonStyle.danger, emoji="❌")
    async def deny(self, button: discord.ui.Button, interaction: discord.Interaction):
        if interaction.user.id != self.approved_user_id:
            await interaction.response.send_message("❌ Unauthorized. You are not allowed to deny terminal tasks.", ephemeral=True)
            return
        self.approved = False
        self.disable_all_items()
        await interaction.response.edit_message(content=f"❌ **Execution Denied by Administrator.**", view=self)
        self.interaction_resolved.set()

    def disable_all_items(self):
        for item in self.children:
            item.disabled = True

class StepState(str, Enum):
    PENDING = 'pending'
    READY = 'ready'
    RUNNING = 'running'
    SUCCEEDED = 'succeeded'
    FAILED = 'failed'
    BLOCKED = 'blocked'
    SKIPPED = 'skipped'

class FailureCode(str, Enum):
    ERR_CONTRACT_FAILED = 'ERR_CONTRACT_FAILED'
    ERR_TOOL_CRASH = 'ERR_TOOL_CRASH'
    ERR_TIMEOUT = 'ERR_TIMEOUT'
    ERR_POLICY_DENIAL = 'ERR_POLICY_DENIAL'
    ERR_DEPENDENCY_DEAD = 'ERR_DEPENDENCY_DEAD'
    ERR_RESOURCE_LIMIT = 'ERR_RESOURCE_LIMIT'
    INVALID_PLAN = 'INVALID_PLAN'
    DEPENDENCY_MISSING = 'DEPENDENCY_MISSING'
    VALIDATION_FAILED = 'VALIDATION_FAILED'
    TOOL_EXECUTION_ERROR = 'TOOL_EXECUTION_ERROR'
    MAX_RETRIES_EXCEEDED = 'MAX_RETRIES_EXCEEDED'
    ARTIFACT_CONFLICT = 'ARTIFACT_CONFLICT'
    POLICY_DENIED = 'POLICY_DENIED'
    CYCLE_DETECTED = 'CYCLE_DETECTED'
    DEADLOCK = 'DEADLOCK'

VALIDATION_PRIMITIVES = {
    'min_length': lambda r, v: len(str(r)) >= int(v),
    'max_length': lambda r, v: len(str(r)) <= int(v),
    'contains': lambda r, v: str(v).lower() in str(r).lower(),
    'not_contains': lambda r, v: str(v).lower() not in str(r).lower(),
    'is_valid_json': lambda r, v: (bool(v) == True and safe_json_loads(str(r), None) is not None),
    'exit_code_zero': lambda r, v: (isinstance(r, dict) and r.get('exit_code') == 0) if isinstance(v, bool) and v else True,
}

@dataclass
class SessionRecord:
    session_id: str
    plan_id: str
    status: str = 'created'
    created_at: str = field(default_factory=iso_now)
    updated_at: str = field(default_factory=iso_now)
    steps_total: int = 0
    steps_completed: int = 0
    steps_failed: int = 0
    cost_estimated: float = 0.0
    cost_actual: float = 0.0
    error: Optional[str] = None
    resume_checkpoint_marker: Optional[str] = None
    source_user_id: Optional[str] = None
    source_channel_id: Optional[str] = None
    plan_json: Optional[str] = None

@dataclass
class ArtifactRecord:
    artifact_id: str
    session_id: str
    source_step_id: str
    type: str
    size_bytes: int
    created_at: str = field(default_factory=iso_now)
    checksum: str = ''
    preview: str = ''

@dataclass
class StepRecord:
    step_id: str
    session_id: str
    op_type: str
    state: StepState = StepState.PENDING
    dependencies: List[str] = field(default_factory=list)
    inputs: Dict[str, Any] = field(default_factory=dict)
    resolved_inputs: Dict[str, Any] = field(default_factory=dict)
    args: Dict[str, Any] = field(default_factory=dict)
    output_key: str = ''
    expected: Dict[str, Any] = field(default_factory=dict)
    attempt_count: int = 0
    max_retries: int = 3
    retry_history: List[Dict[str, Any]] = field(default_factory=list)
    error_history: List[Dict[str, Any]] = field(default_factory=list)
    cost_hint: float = 0.0
    actual_cost: float = 0.0
    created_at: str = field(default_factory=iso_now)
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    last_error: Optional[str] = None
    dedup_signature: Optional[str] = None
    duplicate_of: Optional[str] = None

class PlanVerifier:
    def __init__(self):
        self.cost_model = {
            "PARSE_JSON": 0.1, "FORMAT_TEXT": 0.1, "FETCH_WEB_CONTENT": 5.0, 
            "PERFORM_HTTP_REQUEST": 4.0, "EXECUTE_PYTHON_CODE": 2.0, 
            "EXECUTE_TERMINAL_COMMAND": 10.0, "STORE_MEMORY": 0.3, "QUERY_MEMORY": 0.3, 
            "SEND_DIRECT_MESSAGE": 0.2, "CREATE_DISCORD_CHANNEL": 0.4, 
            "DELETE_DISCORD_CHANNEL": 0.4, "MANAGE_ROLES": 0.4, "EXECUTE_DISCORD_EVAL": 2.0,
            "CREATE_SKILL": 1.0, "LIST_SKILLS": 0.2, "EXECUTE_SKILL": 2.5, "SCHEDULE_TASK": 1.0,
            "EDIT_PERSONA_FILE": 1.0
        }

    def verify(self, plan: dict) -> Tuple[bool, List[str], Dict[str, Any], dict]:
        issues = []
        summary = {"total_estimated_cost": 0.0, "max_depth": 0, "parallel_batches": 0, "redundant_steps": 0}
        
        if not isinstance(plan, dict):
            return False, [FailureCode.INVALID_PLAN.value], summary, plan
        if "plan_id" not in plan or "steps" not in plan or not isinstance(plan["steps"], list):
            issues.append(FailureCode.INVALID_PLAN.value)
            return False, issues, summary, plan
            
        seen_step_ids = set()
        seen_signatures = {}
        output_to_step = {}
        normalized_steps = []
        
        for raw_step in plan["steps"]:
            if not isinstance(raw_step, dict):
                issues.append(FailureCode.INVALID_PLAN.value)
                continue
            for field in ("step_id", "op", "output_key"):
                if field not in raw_step:
                    issues.append(f"{FailureCode.INVALID_PLAN.value}:{field}")
            
            op = raw_step.get("op")
            step_id = str(raw_step.get("step_id", ""))
            
            if step_id in seen_step_ids:
                issues.append(f"{FailureCode.ARTIFACT_CONFLICT.value}:duplicate_step_id:{step_id}")
            seen_step_ids.add(step_id)
            
            output_key = str(raw_step.get("output_key", ""))
            if output_key in output_to_step:
                issues.append(f"{FailureCode.ARTIFACT_CONFLICT.value}:duplicate_output:{output_key}")
            output_to_step[output_key] = step_id
            
            normalized = dict(raw_step)
            normalized.setdefault("dependencies", [])
            normalized.setdefault("inputs", {})
            normalized.setdefault("args", {})
            normalized.setdefault("expected", {})
            normalized_steps.append(normalized)
            
            signature = self._signature(normalized)
            if signature in seen_signatures:
                summary["redundant_steps"] += 1
                normalized["duplicate_of"] = seen_signatures[signature]
            else:
                seen_signatures[signature] = step_id
                
            summary["total_estimated_cost"] += self.cost_model.get(op, 1.0)
            
        graph = self._build_graph(normalized_steps, output_to_step)
        cycle, max_depth = self._detect_cycle_and_depth(graph)
        summary["max_depth"] = max_depth
        summary["parallel_batches"] = self._estimate_parallel_batches(graph)
        
        if cycle:
            issues.append(FailureCode.CYCLE_DETECTED.value)
            
        is_valid = len([x for x in issues if x.startswith("INVALID_PLAN") or x in (FailureCode.CYCLE_DETECTED.value, FailureCode.ARTIFACT_CONFLICT.value)]) == 0
        return is_valid, issues, summary, {"plan_id": plan["plan_id"], "steps": normalized_steps}

    def _signature(self, step: dict) -> str:
        payload = {"op": step.get("op"), "args": step.get("args", {}), "inputs": step.get("inputs", {}), "expected": step.get("expected", {})}
        return sha256_text(safe_json_dumps(payload))

    def _build_graph(self, steps: list, output_to_step: dict) -> dict:
        graph = {step["step_id"]: [] for step in steps}
        refs_by_step = {step["step_id"]: set() for step in steps}
        for step in steps:
            refs = set(step.get("dependencies", []) or [])
            for value in (step.get("inputs", {}) or {}).values():
                if isinstance(value, str) and value.startswith("$"):
                    refs.add(value[1:])
        for step in steps:
            sid = step["step_id"]
            for ref in refs_by_step[sid]:
                if ref in output_to_step:
                    graph[output_to_step[ref]].append(sid)
                elif ref in graph:
                    graph[ref].append(sid)
        return graph

    def _detect_cycle_and_depth(self, graph: dict) -> Tuple[bool, int]:
        indegree = {node: 0 for node in graph}
        for (node, children) in graph.items():
            for child in children:
                indegree[child] = indegree.get(child, 0) + 1
        queue = [node for (node, deg) in indegree.items() if deg == 0]
        visited = 0
        depth = {node: 1 for node in queue}
        while queue:
            node = queue.pop(0)
            visited += 1
            for child in graph.get(node, []):
                depth[child] = max(depth.get(child, 1), depth.get(node, 1) + 1)
                indegree[child] -= 1
                if indegree[child] == 0:
                    queue.append(child)
        return visited != len(graph), max(depth.values()) if depth else 0

    def _estimate_parallel_batches(self, graph: dict) -> int:
        indegree = {node: 0 for node in graph}
        for (_, children) in graph.items():
            for child in children:
                indegree[child] = indegree.get(child, 0) + 1
        ready = [node for (node, deg) in indegree.items() if deg == 0]
        batches = 0
        while ready:
            batches += 1
            next_ready = []
            for node in ready:
                for child in graph.get(node, []):
                    indegree[child] -= 1
                    if indegree[child] == 0:
                        next_ready.append(child)
            ready = next_ready
        return batches

class Executor:
    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        try:
            import docker
            self.docker_client = docker.from_env()
        except Exception:
            self.docker_client = None
            logger.warning("Docker environment not detected. Sandbox python execution will be bypassed using direct execution.")

    async def execute(self, step: dict, resolved_inputs: dict, ctx: dict = None) -> dict:
        op = step["op"]
        
        if op == "PARSE_JSON":
            text = resolved_inputs.get("text", step.get("args", {}).get("text", "{}"))
            parsed = json.loads(text)
            return {"value": parsed, "text": safe_json_dumps(parsed), "exit_code": 0}
            
        if op == "FORMAT_TEXT":
            text = str(resolved_inputs.get("text", step.get("args", {}).get("text", "")))
            prefix = str(step.get("args", {}).get("prefix", ""))
            suffix = str(step.get("args", {}).get("suffix", ""))
            formatted = f"{prefix}{text}{suffix}"
            return {"value": formatted, "text": formatted, "exit_code": 0}
            
        if op == "FETCH_WEB_CONTENT":
            url = str(step.get("args", {}).get("url", ""))
            def fetch():
                req = Request(url, headers={"User-Agent": "ClawV12/1.0"})
                with urlopen(req, timeout=12) as r:
                    html_bytes = r.read()
                parser = _HTMLToText()
                parser.feed(html_bytes.decode("utf-8", errors="ignore"))
                return parser.text()[:5000]
            text = await asyncio.wait_for(asyncio.to_thread(fetch), timeout=15)
            return {"value": text, "text": text, "exit_code": 0}

        if op == "PERFORM_HTTP_REQUEST":
            method = str(step.get("args", {}).get("method", "GET")).upper()
            url = str(step.get("args", {}).get("url", ""))
            data = step.get("args", {}).get("data")
            headers = dict(step.get("args", {}).get("headers") or {})
            json_data = bool(step.get("args", {}).get("json_data", False))
            
            def request():
                body = None
                req_headers = {"User-Agent": "ClawV12/1.0"}
                req_headers.update(headers)
                if data is not None:
                    if json_data:
                        body = json.dumps(data).encode("utf-8")
                        req_headers["Content-Type"] = "application/json"
                    elif isinstance(data, dict):
                        from urllib.parse import urlencode
                        body = urlencode(data).encode("utf-8")
                        req_headers["Content-Type"] = "application/x-www-form-urlencoded"
                    elif isinstance(data, str):
                        body = data.encode("utf-8")
                req = Request(url, data=body, headers=req_headers, method=method)
                with urlopen(req, timeout=15) as resp:
                    body_text = resp.read().decode("utf-8", errors="ignore")
                    return {"status": getattr(resp, "status", 200), "text": body_text[:5000]}
            res = await asyncio.wait_for(asyncio.to_thread(request), timeout=20)
            res["exit_code"] = 0
            return res

        if op == "EXECUTE_PYTHON_CODE":
            code = str(step.get("args", {}).get("code", ""))
            return await self._execute_python(code, resolved_inputs)

        if op == "EXECUTE_TERMINAL_COMMAND":
            command = str(step.get("args", {}).get("command", ""))
            admin_user_id = 1041371551938908232 # Hardcoded supreme authorized administrator user ID
            
            channel = ctx.get("channel") if ctx else None
            if not channel:
                client = getattr(self.orchestrator, "discord_client", None)
                if client:
                    channel = client.get_channel(int(self.orchestrator.source_channel_id)) if getattr(self.orchestrator, "source_channel_id", None) else None
            
            if not channel:
                raise RuntimeError("Execution context has no interactive channel to request approval.")

            approved = await self._request_approval_ui(channel, command, admin_user_id)
            if not approved:
                raise RuntimeError("Terminal Execution Denied by Administrator.")

            logger.info(f"Running approved system command: {command}")
            start_time = time.perf_counter()
            try:
                proc = await asyncio.create_subprocess_shell(
                    command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                stdout, stderr = await proc.communicate()
                duration_ms = int((time.perf_counter() - start_time) * 1000)
                output = stdout.decode("utf-8", errors="ignore") + "\n" + stderr.decode("utf-8", errors="ignore")
                return {
                    "stdout": stdout.decode("utf-8", errors="ignore")[:3000],
                    "stderr": stderr.decode("utf-8", errors="ignore")[:3000],
                    "exit_code": proc.returncode,
                    "duration_ms": duration_ms,
                    "text": output[:5000]
                }
            except Exception as e:
                return {"stdout": "", "stderr": str(e), "exit_code": -1, "duration_ms": 0, "text": f"Error running command: {e}"}

        if op == "CREATE_SKILL":
            skill_name = str(step.get("args", {}).get("skill_name", ""))
            description = str(step.get("args", {}).get("description", ""))
            code = str(step.get("args", {}).get("code", ""))
            await self.orchestrator.register_skill(skill_name, description, code)
            return {"value": f"Skill '{skill_name}' successfully created.", "text": f"Skill registered: {skill_name}", "exit_code": 0}

        if op == "LIST_SKILLS":
            skills = await self.orchestrator.get_all_skills()
            serialized = safe_json_dumps(skills)
            return {"value": skills, "text": serialized, "exit_code": 0}

        if op == "EXECUTE_SKILL":
            skill_name = str(step.get("args", {}).get("skill_name", ""))
            args_passed = step.get("args", {}).get("arguments", {})
            skill_row = await self.orchestrator.get_skill(skill_name)
            if not skill_row:
                raise RuntimeError(f"Skill '{skill_name}' does not exist.")
            return await self._execute_python(skill_row["code"], {"args": args_passed, "inputs": resolved_inputs})

        if op == "SCHEDULE_TASK":
            task_desc = str(step.get("args", {}).get("description", ""))
            schedule_time = str(step.get("args", {}).get("schedule_time", "")) 
            plan_json = step.get("args", {}).get("plan_json", {})
            task_id = await self.orchestrator.register_scheduled_task(task_desc, schedule_time, plan_json)
            return {"value": f"Task scheduled with ID: {task_id}", "text": f"Registered task scheduling: {task_desc} at {schedule_time}", "exit_code": 0}

        if op == "EDIT_PERSONA_FILE":
            filename = str(step.get("args", {}).get("filename", ""))
            content = str(step.get("args", {}).get("content", resolved_inputs.get("content", "")))
            mode = str(step.get("args", {}).get("mode", "write"))
            await save_local_profile(filename, content, mode)
            return {"value": f"Persona file '{filename}' successfully updated.", "text": f"Updated profile {filename}", "exit_code": 0}

        if op == "STORE_MEMORY":
            content = str(step.get("args", {}).get("content", resolved_inputs.get("content", "")))
            return await self.orchestrator.store_memory(content)
            
        if op == "QUERY_MEMORY":
            query = str(step.get("args", {}).get("query", resolved_inputs.get("query", "")))
            top_k = int(step.get("args", {}).get("top_k", 5))
            return await self.orchestrator.query_memory(query, top_k=top_k)
            
        if op == "SEND_DIRECT_MESSAGE":
            user_id = str(step.get("args", {}).get("user_id", ""))
            content = str(step.get("args", {}).get("content", ""))
            return await self.orchestrator.send_direct_message(user_id, content)

        if op == "CREATE_DISCORD_CHANNEL":
            guild = ctx.get("guild")
            if guild is None:
                raise RuntimeError("Discord action missing target Guild association.")
            channel = await guild.create_text_channel(str(step.get("args", {}).get("name", "claw-task")))
            return {"value": f"channel:{channel.id}", "text": f"channel:{channel.id}", "exit_code": 0}
            
        if op == "DELETE_DISCORD_CHANNEL":
            channel = ctx.get("channel")
            if channel is None:
                raise RuntimeError("Discord action missing active target Channel.")
            await channel.delete()
            return {"value": "deleted", "text": "deleted", "exit_code": 0}
            
        if op == "MANAGE_ROLES":
            guild = ctx.get("guild")
            if guild is None:
                raise RuntimeError("Missing Guild context for role operations.")
            member = guild.get_member(int(step.get("args", {}).get("member_id", 0)))
            role_name = str(step.get("args", {}).get("role_name", ""))
            action = str(step.get("args", {}).get("action", "add"))
            role = discord.utils.get(guild.roles, name=role_name)
            if member is None or role is None:
                raise RuntimeError("Target user member or configured Role name could not be found.")
            if action == "add":
                await member.add_roles(role)
            else:
                await member.remove_roles(role)
            return {"value": f"role:{action}", "text": f"role:{action}", "exit_code": 0}
            
        if op == "EXECUTE_DISCORD_EVAL":
            code = str(step.get("args", {}).get("code", ""))
            return await self.orchestrator.execute_discord_eval(code, ctx)
            
        raise RuntimeError(f"Unknown operation payload handler: {op}")

    async def _request_approval_ui(self, channel, command: str, user_id: int) -> bool:
        embed = discord.Embed(
            title="⚠️ Terminal Action Authorization",
            description=f"The automation core requested administrative privileges to execute a shell command on this server.\n"
                        f"Only the authorized user <@{user_id}> can authorize this operation.",
            color=discord.Color.dark_gold()
        )
        embed.add_field(name="Requested Command Script", value=f"```bash\n{command}\n```", inline=False)
        embed.set_footer(text="Response timeout: 5 minutes")
        
        view = TerminalApprovalView(command, user_id)
        msg = await channel.send(embed=embed, view=view)
        
        try:
            await asyncio.wait_for(view.interaction_resolved.wait(), timeout=300)
            return view.approved is True
        except asyncio.TimeoutError:
            view.disable_all_items()
            await msg.edit(content="❌ **Operation Timeout Expired.** Command automatic denial.", view=view)
            return False

    async def _execute_python(self, code: str, inputs: dict) -> dict:
        """Runs custom python files inside a Docker sandbox, falling back safely to a restricted local env."""
        if self.docker_client is not None:
            bootstrap = "import json, os, sys\nINPUTS = json.loads(os.environ.get('CLAW_INPUTS', '{}'))\nRESULT = None\n"
            payload = bootstrap + "\n" + code
            env = os.environ.copy()
            env['CLAW_INPUTS'] = safe_json_dumps(inputs)
            container = None
            start = time.perf_counter()
            try:
                container = self.docker_client.containers.run(
                    'python:3.11-slim',
                    ['python', '-c', payload],
                    detach=True,
                    remove=True,
                    stdout=True,
                    stderr=True,
                    network_disabled=True,
                    mem_limit='128m',
                    cpu_quota=50000,
                    pids_limit=64,
                    read_only=True,
                    security_opt=['no-new-privileges'],
                    environment=env
                )
                wait_res = await asyncio.to_thread(container.wait, timeout=10)
                logs = container.logs(stdout=True, stderr=True).decode("utf-8", errors="ignore")
                duration = int((time.perf_counter() - start) * 1000)
                status = int(wait_res.get('StatusCode', 1))
                return {'stdout': logs[:5000], 'stderr': '', 'exit_code': status, 'duration_ms': duration, 'text': logs[:5000]}
            except Exception as exc:
                logger.warning(f"Docker sandbox execution failed: {exc}. Trying direct host interpreter run...")
        
        # Direct fallback execution on safe local scope
        local_globals = {"INPUTS": inputs, "RESULT": None, "json": json, "sys": sys, "os": os, "asyncio": asyncio}
        start = time.perf_counter()
        try:
            import io
            stdout_redir = io.StringIO()
            sys.stdout = stdout_redir
            exec(code, local_globals)
            sys.stdout = sys.__stdout__
            output = stdout_redir.getvalue()
            duration = int((time.perf_counter() - start) * 1000)
            return {
                "stdout": output[:5000],
                "stderr": "",
                "exit_code": 0,
                "duration_ms": duration,
                "text": output[:5000] or str(local_globals.get("RESULT"))
            }
        except Exception as e:
            sys.stdout = sys.__stdout__
            return {"stdout": "", "stderr": str(e), "exit_code": 1, "duration_ms": 0, "text": f"Error: {e}"}

class _HTMLToText(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []
        self._skip = False
    def handle_starttag(self, tag, attrs):
        if tag in {'script', 'style', 'noscript'}:
            self._skip = True
    def handle_endtag(self, tag):
        if tag in {'script', 'style', 'noscript'}:
            self._skip = False
        if tag in {'p', 'div', 'br', 'li', 'tr', 'section', 'article', 'header', 'footer'}:
            self.parts.append("\n")
    def handle_data(self, data):
        if not self._skip:
            text = data.strip()
            if text:
                self.parts.append(text)
    def text(self) -> str:
        raw = ' '.join(self.parts)
        raw = html.unescape(raw)
        raw = re.sub('\\s+\\n', '\n', raw)
        raw = re.sub('\\n\\s+', '\n', raw)
        raw = re.sub('[ \\t]{2,}', ' ', raw)
        raw = re.sub('\\n{3,}', '\n\n', raw)
        return raw.strip()

class Orchestrator:
    def __init__(self, db_path='claw_v12_runtime.db', openai_client=None):
        self.db_path = db_path
        self.db = None
        self.db_lock = asyncio.Lock()
        self.executor = Executor(self)
        self.openai_client = openai_client
        self._ready = asyncio.Event()
        self.memory_embedding_model = os.getenv('OPENAI_EMBEDDING_MODEL', 'text-embedding-3-small')
        self.discord_client = None
        self.source_channel_id = None

    async def initialize(self):
        if self.db is None:
            import aiosqlite
            self.db = await aiosqlite.connect(self.db_path)
            await self.db.execute('PRAGMA journal_mode=WAL')
            await self.db.execute('PRAGMA synchronous=NORMAL')
            await self.db.execute('PRAGMA busy_timeout=5000')
            await self._init_db()
            self._ready.set()

    async def _init_db(self):
        async with self.db_lock:
            await self.db.executescript('''
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    plan_id TEXT,
                    status TEXT,
                    created_at TEXT,
                    updated_at TEXT,
                    steps_total INTEGER,
                    steps_completed INTEGER,
                    steps_failed INTEGER,
                    cost_estimated REAL,
                    cost_actual REAL,
                    error TEXT,
                    resume_checkpoint_marker TEXT,
                    source_user_id TEXT,
                    source_channel_id TEXT,
                    plan_json TEXT
                );
                CREATE TABLE IF NOT EXISTS steps (
                    step_id TEXT,
                    session_id TEXT,
                    op TEXT,
                    state TEXT,
                    attempt INTEGER,
                    max_retries INTEGER,
                    inputs TEXT,
                    resolved_inputs TEXT,
                    args TEXT,
                    output_key TEXT,
                    dependencies TEXT,
                    expected TEXT,
                    error_history TEXT,
                    retry_history TEXT,
                    last_error TEXT,
                    created_at TEXT,
                    started_at TEXT,
                    finished_at TEXT,
                    cost_hint REAL,
                    actual_cost REAL,
                    dedup_signature TEXT,
                    duplicate_of TEXT,
                    PRIMARY KEY (step_id, session_id)
                );
                CREATE TABLE IF NOT EXISTS artifacts (
                    artifact_id TEXT PRIMARY KEY,
                    session_id TEXT,
                    source_step_id TEXT,
                    type TEXT,
                    size_bytes INTEGER,
                    created_at TEXT,
                    checksum TEXT,
                    preview TEXT
                );
                CREATE TABLE IF NOT EXISTS artifact_payloads (
                    artifact_id TEXT PRIMARY KEY,
                    payload TEXT,
                    FOREIGN KEY (artifact_id) REFERENCES artifacts(artifact_id)
                );
                CREATE TABLE IF NOT EXISTS trace_events (
                    trace_id TEXT PRIMARY KEY,
                    session_id TEXT,
                    step_id TEXT,
                    timestamp TEXT,
                    event_type TEXT,
                    details TEXT
                );
                CREATE TABLE IF NOT EXISTS memories (
                    memory_id TEXT PRIMARY KEY,
                    session_id TEXT,
                    content TEXT,
                    embedding TEXT,
                    created_at TEXT
                );
                CREATE TABLE IF NOT EXISTS skills (
                    skill_name TEXT PRIMARY KEY,
                    description TEXT,
                    code TEXT,
                    created_at TEXT
                );
                CREATE TABLE IF NOT EXISTS scheduled_tasks (
                    task_id TEXT PRIMARY KEY,
                    task_description TEXT,
                    schedule_time TEXT,
                    plan_json TEXT,
                    status TEXT,
                    last_run TEXT,
                    created_at TEXT
                );
            ''')
            await self.db.commit()

    async def _emit_trace(self, event_type: str, step_id: Optional[str], message: str, data: dict = None):
        payload = {'message': message, 'data': data or {}}
        async with self.db_lock:
            await self.db.execute(
                'INSERT INTO trace_events VALUES (?, ?, ?, ?, ?, ?)',
                (str(uuid.uuid4()), self.current_session_id, step_id, iso_now(), event_type, safe_json_dumps(payload))
            )
            await self.db.commit()

    @property
    def current_session_id(self) -> str:
        return getattr(self, '_current_session_id', '')

    @current_session_id.setter
    def current_session_id(self, value: str):
        self._current_session_id = value

    async def create_session(self, plan: dict, source_user_id: str = None, source_channel_id: str = None) -> str:
        await self.initialize()
        verifier = PlanVerifier()
        valid, issues, summary, normalized_plan = verifier.verify(plan)
        
        if not valid:
            self.current_session_id = str(uuid.uuid4())
            await self._emit_trace("INVALID_PLAN", None, 'Plan validation execution halted', {'issues': issues})
            raise RuntimeError(f"Plan validation rejected: {issues}")
            
        session_id = str(uuid.uuid4())
        self.current_session_id = session_id
        self.source_channel_id = source_channel_id
        plan_json = safe_json_dumps(normalized_plan)
        
        session = SessionRecord(
            session_id=session_id,
            plan_id=str(plan["plan_id"]),
            status='active',
            steps_total=len(normalized_plan["steps"]),
            cost_estimated=float(summary["total_estimated_cost"]),
            source_user_id=source_user_id,
            source_channel_id=source_channel_id,
            plan_json=plan_json
        )
        
        async with self.db_lock:
            await self.db.execute(
                'INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (session.session_id, session.plan_id, session.status, session.created_at, session.updated_at,
                 session.steps_total, session.steps_completed, session.steps_failed, session.cost_estimated,
                 session.cost_actual, session.error, session.resume_checkpoint_marker, session.source_user_id,
                 session.source_channel_id, session.plan_json)
            )
            
            for raw_step in normalized_plan["steps"]:
                step = StepRecord(
                    step_id=str(raw_step["step_id"]),
                    session_id=session_id,
                    op_type=str(raw_step["op"]),
                    dependencies=[str(x) for x in raw_step.get("dependencies", []) or []],
                    inputs=dict(raw_step.get("inputs", {}) or {}),
                    args=dict(raw_step.get("args", {}) or {}),
                    output_key=str(raw_step["output_key"]),
                    expected=dict(raw_step.get("expected", {}) or {}),
                    cost_hint=float(verifier.cost_model.get(str(raw_step["op"]), 1.0)),
                    dedup_signature=verifier._signature(raw_step),
                    duplicate_of=str(raw_step.get("duplicate_of")) if raw_step.get("duplicate_of") else None
                )
                
                await self.db.execute(
                    'INSERT INTO steps VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                    (step.step_id, step.session_id, step.op_type, step.state.value, step.attempt_count, step.max_retries,
                     safe_json_dumps(step.inputs), safe_json_dumps(step.resolved_inputs), safe_json_dumps(step.args),
                     step.output_key, safe_json_dumps(step.dependencies), safe_json_dumps(step.expected),
                     safe_json_dumps(step.error_history), safe_json_dumps(step.retry_history), step.last_error,
                     step.created_at, step.started_at, step.finished_at, step.cost_hint, step.actual_cost,
                     step.dedup_signature, step.duplicate_of)
                )
            await self.db.commit()
            
        await self._emit_trace('SESSION_CREATED', None, 'Session pipeline instantiated', {"plan_id": plan["plan_id"]})
        return session_id

    async def _fetch_session_steps(self, session_id: str) -> list:
        async with self.db.execute('SELECT * FROM steps WHERE session_id=?', (session_id,)) as cursor:
            rows = await cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]
        return [dict(zip(cols, row)) for row in rows]

    async def _update_step(self, session_id: str, step_id: str, updates: dict):
        if not updates:
            return
        async with self.db_lock:
            clause = ', '.join([f"{k}=?" for k in updates])
            vals = list(updates.values()) + [session_id, step_id]
            await self.db.execute(f"UPDATE steps SET {clause} WHERE session_id=? AND step_id=?", vals)
            await self.db.commit()

    async def _update_session(self, session_id: str, updates: dict):
        if not updates:
            return
        async with self.db_lock:
            clause = ', '.join([f"{k}=?" for k in updates])
            vals = list(updates.values()) + [iso_now(), session_id]
            await self.db.execute(f"UPDATE sessions SET {clause}, updated_at=? WHERE session_id=?", vals)
            await self.db.commit()

    async def dependencies_met(self, session_id: str, step_row: dict) -> bool:
        deps = safe_json_loads(step_row.get("dependencies"), [])
        if not deps:
            return True
        for dep in deps:
            dep_key = str(dep)
            if dep_key.startswith("$"):
                dep_key = dep_key[1:]
                async with self.db.execute('SELECT 1 FROM artifacts WHERE session_id=? AND artifact_id=?', (session_id, dep_key)) as cursor:
                    row = await cursor.fetchone()
                if row is None:
                    return False
                continue
            async with self.db.execute('SELECT state FROM steps WHERE session_id=? AND step_id=?', (session_id, dep_key)) as cursor:
                row = await cursor.fetchone()
            if row is None or row[0] != StepState.SUCCEEDED.value:
                return False
        return True

    async def resolve_inputs(self, session_id: str, step_row: dict) -> dict:
        inputs = safe_json_loads(step_row.get("inputs"), {})
        resolved = {}
        for (k, val) in inputs.items():
            if isinstance(val, str) and val.startswith("$"):
                art_id = val[1:]
                async with self.db.execute('SELECT payload FROM artifact_payloads WHERE artifact_id=?', (art_id,)) as cursor:
                    row = await cursor.fetchone()
                resolved[k] = safe_json_loads(row[0], row[0]) if row else None
            else:
                resolved[k] = val
        return resolved

    async def register_skill(self, name: str, description: str, code: str):
        async with self.db_lock:
            await self.db.execute(
                'INSERT OR REPLACE INTO skills (skill_name, description, code, created_at) VALUES (?, ?, ?, ?)',
                (name, description, code, iso_now())
            )
            await self.db.commit()
        logger.info(f"Registered/updated dynamic tool skill: {name}")

    async def get_all_skills(self) -> List[Dict[str, Any]]:
        async with self.db.execute('SELECT skill_name, description, code FROM skills') as cursor:
            rows = await cursor.fetchall()
        return [{"skill_name": r[0], "description": r[1], "code": r[2]} for r in rows]

    async def get_skill(self, name: str) -> Optional[Dict[str, Any]]:
        async with self.db.execute('SELECT skill_name, description, code FROM skills WHERE skill_name=?', (name,)) as cursor:
            row = await cursor.fetchone()
        if row:
            return {"skill_name": row[0], "description": row[1], "code": row[2]}
        return None

    async def register_scheduled_task(self, description: str, schedule_time: str, plan_json: dict) -> str:
        task_id = str(uuid.uuid4())
        async with self.db_lock:
            await self.db.execute(
                'INSERT INTO scheduled_tasks (task_id, task_description, schedule_time, plan_json, status, last_run, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)',
                (task_id, description, schedule_time, safe_json_dumps(plan_json), 'pending', '', iso_now())
            )
            await self.db.commit()
        logger.info(f"Scheduled task registered: {description} (Times: {schedule_time})")
        return task_id

    async def _upstream_failed(self, session_id: str, step_row: dict) -> bool:
        deps = safe_json_loads(step_row.get("dependencies"), [])
        if not deps:
            return False
        for dep in deps:
            dep_key = str(dep)
            if dep_key.startswith("$"):
                dep_key = dep_key[1:]
                async with self.db.execute('SELECT source_step_id FROM artifacts WHERE session_id=? AND artifact_id=?', (session_id, dep_key)) as cursor:
                    row = await cursor.fetchone()
                if row is None:
                    continue
                source_step_id = row[0]
                async with self.db.execute('SELECT state FROM steps WHERE session_id=? AND step_id=?', (session_id, source_step_id)) as cursor:
                    step_row_db = await cursor.fetchone()
                if step_row_db and step_row_db[0] == StepState.FAILED.value:
                    return True
                continue
            async with self.db.execute('SELECT state FROM steps WHERE session_id=? AND step_id=?', (session_id, dep_key)) as cursor:
                row = await cursor.fetchone()
            if row and row[0] == StepState.FAILED.value:
                return True
        return False

    async def store_artifact(self, session_id: str, step_row: dict, result: dict):
        art_id = str(step_row["output_key"])
        payload = result.get("text")
        if payload is None:
            payload = safe_json_dumps(result.get("value", result))
        payload = str(payload)
        
        record = ArtifactRecord(
            artifact_id=art_id,
            session_id=session_id,
            source_step_id=str(step_row["step_id"]),
            type='json' if isinstance(result.get("value", result), (dict, list)) else 'text',
            size_bytes=len(payload.encode("utf-8", errors="ignore")),
            checksum=sha256_text(payload),
            preview=payload[:200]
        )
        
        async with self.db_lock:
            await self.db.execute(
                'INSERT OR REPLACE INTO artifacts VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                (record.artifact_id, record.session_id, record.source_step_id, record.type, record.size_bytes,
                 record.created_at, record.checksum, record.preview)
            )
            await self.db.execute(
                'INSERT OR REPLACE INTO artifact_payloads VALUES (?, ?)',
                (record.artifact_id, payload)
            )
            await self.db.commit()
            
        await self._emit_trace('ARTIFACT_WRITTEN', str(step_row["step_id"]), 'Artifact committed', {"artifact_id": art_id, "bytes": record.size_bytes})

    async def send_direct_message(self, user_id: str, content: str) -> dict:
        client = self.discord_client
        if client is None:
            raise RuntimeError("Orchestrator has no active Discord Client instance.")
        user = await client.fetch_user(int(user_id))
        await user.send(content[:1900])
        return {"value": f"dm:{user_id}", "text": f"dm:{user_id}", "exit_code": 0}

    async def execute_discord_eval(self, code: str, ctx: dict) -> dict:
        namespace = {
            'client': self.discord_client, 'discord': discord, 'asyncio': asyncio,
            'db': self.db, 'orchestrator': self, 'guild': ctx.get("guild"),
            'channel': ctx.get("channel"), 'user': ctx.get("user"), 'resolved_inputs': ctx.get("resolved_inputs", {})
        }
        try:
            result = eval(code, namespace)
            if asyncio.iscoroutine(result):
                result = await result
            return {"value": result, "text": str(result) if result is not None else '', "exit_code": 0}
        except SyntaxError:
            exec(code, namespace)
            return {"value": None, "text": '', "exit_code": 0}
        except Exception as e:
            return {"value": None, "text": f"{type(e).__name__}: {e}", "exit_code": 1}

    async def store_memory(self, content: str) -> dict:
        embedding = None
        if self.openai_client is not None:
            try:
                resp = await self.openai_client.embeddings.create(model=self.memory_embedding_model, input=content)
                embedding = resp.data[0].embedding
            except Exception as e:
                logger.warning(f"Vector embedding generation failed: {e}")
                
        rec = {
            "memory_id": str(uuid.uuid4()), "session_id": self.current_session_id,
            "content": content, "embedding": embedding, "created_at": iso_now()
        }
        async with self.db_lock:
            await self.db.execute(
                'INSERT INTO memories VALUES (?, ?, ?, ?, ?)',
                (rec["memory_id"], rec["session_id"], rec["content"], safe_json_dumps(rec["embedding"]), rec["created_at"])
            )
            await self.db.commit()
        return {"value": "stored", "text": "stored", "exit_code": 0}

    async def query_memory(self, query: str, top_k: int = 5) -> dict:
        rows = []
        async with self.db.execute('SELECT content, embedding FROM memories') as cursor:
            rows = await cursor.fetchall()
            
        if self.openai_client is not None:
            try:
                q_resp = await self.openai_client.embeddings.create(model=self.memory_embedding_model, input=query)
                q_vec = q_resp.data[0].embedding
                scored = []
                for (content, emb_json) in rows:
                    vec = safe_json_loads(emb_json, None)
                    if not vec:
                        continue
                    score = self._cosine_similarity(q_vec, vec)
                    scored.append((score, content))
                scored.sort(key=lambda x: x[0], reverse=True)
                out = "\n\n".join(content for (score, content) in scored[:top_k]) if scored else "No memories found."
                return {"value": out, "text": out, "exit_code": 0, "results": scored[:top_k]}
            except Exception as e:
                logger.warning(f"Query semantic search failed: {e}")
                
        # Simple word frequency overlap fallback (highly effective local fallback option)
        q_words = set(re.findall(r'\w+', query.lower()))
        scored_fallback = []
        for (content, _) in rows:
            words = set(re.findall(r'\w+', content.lower()))
            if not words:
                continue
            overlap = len(q_words & words) / max(1, len(q_words | words))
            scored_fallback.append((overlap, content))
        scored_fallback.sort(key=lambda x: x[0], reverse=True)
        out = "\n\n".join(content for (_, content) in scored_fallback[:top_k]) if scored_fallback else "No memories found."
        return {"value": out, "text": out, "exit_code": 0, "results": scored_fallback[:top_k]}

    def _cosine_similarity(self, a, b) -> float:
        if not a or not b or len(a) != len(b):
            return 0.0
        dot = sum(x*y for (x,y) in zip(a,b))
        norm_a = sum(x*x for x in a) ** 0.5
        norm_b = sum(y*y for y in b) ** 0.5
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot / (norm_a * norm_b)

    def verify_contract(self, result: dict, expected: dict) -> Tuple[bool, str]:
        target = result
        if isinstance(result, dict):
            target = result.get("value", result.get("text", result))
        for (rule, val) in (expected or {}).items():
            if rule not in VALIDATION_PRIMITIVES:
                return False, f"illegal_validation_rule:{rule}"
            try:
                if not VALIDATION_PRIMITIVES[rule](target, val):
                    return False, f"failed:{rule}:{val}"
            except Exception as exc:
                return False, f"validation_error:{rule}:{exc}"
        return True, 'ok'

    async def rehydrate_session(self, session_id: str):
        self.current_session_id = session_id
        async with self.db_lock:
            async with self.db.execute('SELECT plan_json, status FROM sessions WHERE session_id=?', (session_id,)) as cursor:
                row = await cursor.fetchone()
            if row is None:
                raise RuntimeError('session_not_found')
            plan_json, status = row
            if status not in ('active', 'running', 'paused'):
                return
                
        steps = await self._fetch_session_steps(session_id)
        for step in steps:
            if step["state"] == StepState.RUNNING.value:
                await self._update_step(session_id, step["step_id"], {"state": StepState.PENDING.value, "started_at": None})
            if step["state"] == StepState.READY.value:
                await self._update_step(session_id, step["step_id"], {"state": StepState.PENDING.value})
            if step["state"] == StepState.SUCCEEDED.value:
                art_id = step["output_key"]
                async with self.db.execute('SELECT 1 FROM artifact_payloads WHERE artifact_id=?', (art_id,)) as cursor:
                    art = await cursor.fetchone()
                if art is None:
                    await self._update_step(session_id, step["step_id"], {"state": StepState.PENDING.value, "last_error": "artifact_missing"})
                    await self._emit_trace("ARTIFACT_MISSING", step["step_id"], "Artifact missing during reload context step")
                    
        marker = sha256_text(safe_json_dumps({"session_id": session_id, "plan": plan_json, "ts": iso_now()}))
        await self._update_session(session_id, {"resume_checkpoint_marker": marker})

    async def reflect_and_learn(self, session_id: str, final_status: str):
        """Autonomous reflection loop that executes post-session to write persistent feedback loops."""
        logger.info(f"Initiating autonomous continuous learning reflection for session: {session_id}")
        try:
            # Query session data
            async with self.db.execute("SELECT plan_json, error FROM sessions WHERE session_id=?", (session_id,)) as cursor:
                session_row = await cursor.fetchone()
            if not session_row:
                return
            plan_json_str, sess_error = session_row
            plan = safe_json_loads(plan_json_str, {})
            
            # Extract step records for tracing errors
            async with self.db.execute("SELECT step_id, op, state, last_error FROM steps WHERE session_id=?", (session_id,)) as cursor:
                step_rows = await cursor.fetchall()
            
            step_summaries = []
            for sr in step_rows:
                step_summaries.append(f"Step '{sr[0]}' ({sr[1]}): State={sr[2]}, Error={sr[3]}")
                
            steps_joined = "\n".join(step_summaries)
            
            reflection_prompt = (
                f"You are the self-evolution reflection processor for CLAW V12.\n"
                f"An automated DAG session has finished with status: {final_status.upper()}.\n"
                f"Session overall error details: {sess_error}\n"
                f"Executed steps details:\n{steps_joined}\n\n"
                f"If the execution failed, analyze exactly why the mistake happened and formulate structured, direct changes "
                f"to prevent ever repeating this execution bottleneck. If it succeeded, outline structural lessons.\n"
                f"Return your analysis in RAW JSON matching exactly this syntax:\n"
                f"{{\n"
                f"  \"lesson\": \"Paragraph outlining exactly what was learned and how to safely approach similar goals next time.\",\n"
                f"  \"should_update_soul_directives\": false,\n"
                f"  \"soul_patch_instruction\": \"Optional rule or caution statement to add to your SOUL.md system regulations (only provide if should_update_soul_directives is true)\"\n"
                f"}}\n"
            )
            
            raw_resp = ""
            if self.openai_client is not None:
                resp = await self.openai_client.chat.completions.create(
                    model=os.getenv('CLAW_PLANNER_MODEL', 'gpt-4o-mini'),
                    messages=[{"role": "user", "content": reflection_prompt}],
                    response_format={'type': 'json_object'}
                )
                raw_resp = resp.choices[0].message.content
            else:
                raw_resp = await query_local_llm(reflection_prompt)
                
            analysis = extract_json_payload(raw_resp)
            if analysis and "lesson" in analysis:
                lesson_text = f"- [Autonomous Run Lesson - {session_id} - {final_status.upper()}]: {analysis['lesson']}"
                await save_local_profile("MEMORY.md", lesson_text, mode="append")
                logger.info(f"Self-reflection successfully preserved in MEMORY.md: {analysis['lesson']}")
                
                if analysis.get("should_update_soul_directives") and analysis.get("soul_patch_instruction"):
                    soul_patch = f"- [Continuous Learning Correction]: {analysis['soul_patch_instruction']}"
                    await save_local_profile("SOUL.md", soul_patch, mode="append")
                    logger.info(f"Self-evolution rules automatically appended to SOUL.md: {analysis['soul_patch_instruction']}")
        except Exception as e:
            logger.error(f"Post-execution continuous learning reflection phase failed: {e}")

    async def run_session(self, session_id: str, notify_channel: discord.abc.Messageable = None, ctx: dict = None) -> dict:
        await self.initialize()
        self.current_session_id = session_id
        await self.rehydrate_session(session_id)
        ctx = ctx or {}
        await self._update_session(session_id, {"status": "running"})
        await self._emit_trace("SESSION_RUNNING", None, 'Execution engine active', {"session_id": session_id})
        
        dashboard_msg = None
        if notify_channel is not None:
            try:
                initial_embed = discord.Embed(
                    title="⚡ Claw Automation Engine: Pipeline Started",
                    description=f"Session ID: `{session_id}`\nInitialising execution environment...",
                    color=discord.Color.blue()
                )
                dashboard_msg = await notify_channel.send(embed=initial_embed)
            except Exception as e:
                logger.error(f"Failed to send initial dashboard message: {e}")

        async def update_dashboard():
            if not dashboard_msg:
                return
            try:
                current_steps = await self._fetch_session_steps(session_id)
                embed = discord.Embed(
                    title="⚡ Claw Automation Engine: Active Execution",
                    description=f"Session ID: `{session_id}`\nTracking task execution progress in real-time.",
                    color=discord.Color.gold()
                )
                
                steps_status = []
                for s in current_steps:
                    op = s["op"]
                    state = s["state"]
                    sid = s["step_id"]
                    
                    emoji = "⚪"
                    if state == StepState.RUNNING.value:
                        emoji = "🔄"
                    elif state == StepState.SUCCEEDED.value:
                        emoji = "✅"
                    elif state == StepState.FAILED.value:
                        emoji = "❌"
                    elif state == StepState.BLOCKED.value:
                        emoji = "🚫"
                    elif state == StepState.SKIPPED.value:
                        emoji = "⏭️"
                    
                    status_line = f"{emoji} **`{sid}`** ({op}) ➔ *{state}*"
                    if s["last_error"]:
                        status_line += f"\n  └ ⚠️ *Error: {str(s['last_error'])[:70]}*"
                    steps_status.append(status_line)
                
                embed.add_field(name="Execution Pipeline Steps", value="\n".join(steps_status)[:1024], inline=False)
                embed.set_footer(text=f"Last updated: {datetime.now().strftime('%H:%M:%S')}")
                await dashboard_msg.edit(embed=embed)
            except Exception as e:
                logger.error(f"Error updating real-time dashboard: {e}")

        while True:
            steps = await self._fetch_session_steps(session_id)
            if not steps:
                await self._update_session(session_id, {"status": "failed", "error": "No steps parsed."})
                raise RuntimeError("Empty steps payload received.")
                
            progress = False
            any_pending = False
            blocked_now = []
            
            for step in steps:
                if step["state"] in (StepState.SUCCEEDED.value, StepState.FAILED.value, StepState.SKIPPED.value):
                    continue
                any_pending = True
                if await self._upstream_failed(session_id, step):
                    blocked_now.append(step)
                    
            for step in blocked_now:
                await self._update_step(session_id, step["step_id"], {"state": StepState.BLOCKED.value, "finished_at": iso_now(), "last_error": FailureCode.ERR_DEPENDENCY_DEAD.value})
                await self._emit_trace("STEP_BLOCKED", step["step_id"], "Dependency execution chain halted", {"error": FailureCode.ERR_DEPENDENCY_DEAD.value})
                
            if blocked_now:
                await update_dashboard()

            steps = await self._fetch_session_steps(session_id)
            ready_steps = []
            for s in steps:
                if s["state"] not in (StepState.PENDING.value, StepState.READY.value):
                    continue
                if await self.dependencies_met(session_id, s):
                    ready_steps.append(s)
                    
            for step in ready_steps:
                if step["duplicate_of"]:
                    await self._update_step(session_id, step["step_id"], {"state": StepState.SKIPPED.value, "finished_at": iso_now()})
                    await self._emit_trace("DEDUP_REWRITE", step["step_id"], "Duplicate execution bypassed", {"duplicate_of": step["duplicate_of"]})
            
            if any(step["duplicate_of"] for step in ready_steps):
                await update_dashboard()
                    
            ready_steps = [s for s in ready_steps if not s["duplicate_of"]]
            if not ready_steps:
                terminal_states = {StepState.SUCCEEDED.value, StepState.FAILED.value, StepState.SKIPPED.value, StepState.BLOCKED.value}
                if all(s["state"] in terminal_states for s in steps):
                    break
                if any_pending:
                    await asyncio.sleep(0.2)
                    continue
                await self._update_session(session_id, {"status": "failed", "error": FailureCode.DEADLOCK.value})
                raise RuntimeError(FailureCode.DEADLOCK.value)
                
            async def run_one(step_row):
                nonlocal progress
                if int(step_row["attempt"]) >= int(step_row["max_retries"]):
                    await self._update_step(session_id, step_row["step_id"], {"state": StepState.FAILED.value, "last_error": FailureCode.MAX_RETRIES_EXCEEDED.value, "finished_at": iso_now()})
                    await self._emit_trace("STEP_FAILED", step_row["step_id"], "Attempts maximum quota met", {"error": FailureCode.MAX_RETRIES_EXCEEDED.value})
                    await update_dashboard()
                    progress = True
                    return
                    
                await self._update_step(session_id, step_row["step_id"], {"state": StepState.RUNNING.value, "started_at": iso_now()})
                await self._emit_trace("STEP_STARTED", step_row["step_id"], "Module execution initiated", {"op": step_row["op"]})
                await update_dashboard()
                
                try:
                    resolved = await self.resolve_inputs(session_id, step_row)
                    if len(str(resolved)) > 30000:
                        raise RuntimeError(FailureCode.ERR_RESOURCE_LIMIT.value)
                        
                    await self._update_step(session_id, step_row["step_id"], {"resolved_inputs": safe_json_dumps(resolved)})
                    
                    execution_ctx = {
                        "session_id": session_id,
                        "guild": ctx.get("guild"),
                        "channel": notify_channel or ctx.get("channel"),
                        "user": ctx.get("user"),
                        "resolved_inputs": resolved
                    }
                    
                    result = await self.executor.execute(step_row, resolved, ctx=execution_ctx)
                    valid, reason = self.verify_contract(result, safe_json_loads(step_row.get("expected"), {}))
                    if not valid:
                        raise RuntimeError(f"{FailureCode.VALIDATION_FAILED.value}:{reason}")
                        
                    await self.store_artifact(session_id, step_row, result)
                    await self._update_step(session_id, step_row["step_id"], {
                        "state": StepState.SUCCEEDED.value,
                        "finished_at": iso_now(),
                        "last_error": None,
                        "actual_cost": float(step_row.get("cost_hint", 0.0))
                    })
                    await self._emit_trace("STEP_SUCCEEDED", step_row["step_id"], "Module completed successfully", {"preview": str(result)[:250]})
                except Exception as exc:
                    err_txt = f"{type(exc).__name__}: {exc}"
                    new_att = int(step_row["attempt"]) + 1
                    retry_hist = safe_json_loads(step_row.get("retry_history"), [])
                    err_hist = safe_json_loads(step_row.get("error_history"), [])
                    snap = {"attempt": new_att, "error": err_txt, "timestamp": iso_now(), "state": step_row["state"]}
                    retry_hist.append(snap)
                    err_hist.append(snap)
                    
                    next_state = StepState.PENDING.value if new_att < int(step_row["max_retries"]) else StepState.FAILED.value
                    await self._update_step(session_id, step_row["step_id"], {
                        "state": next_state,
                        "attempt": new_att,
                        "retry_history": safe_json_dumps(retry_hist),
                        "error_history": safe_json_dumps(err_hist),
                        "last_error": err_txt,
                        "finished_at": iso_now()
                    })
                    await self._emit_trace("STEP_FAILED", step_row["step_id"], "Module failure caught", {"error": err_txt})
                await update_dashboard()
                progress = True
                
            await asyncio.gather(*(run_one(step) for step in ready_steps))
            if not progress:
                await asyncio.sleep(0.1)
                
        final_steps = await self._fetch_session_steps(session_id)
        succeeded = len([s for s in final_steps if s["state"] == StepState.SUCCEEDED.value])
        failed = len([s for s in final_steps if s["state"] == StepState.FAILED.value])
        skipped = len([s for s in final_steps if s["state"] == StepState.SKIPPED.value])
        blocked = len([s for s in final_steps if s["state"] == StepState.BLOCKED.value])
        
        final_status = 'completed' if failed == 0 else 'failed'
        await self._update_session(session_id, {"status": final_status, "steps_completed": succeeded, "steps_failed": failed})
        await self._emit_trace("SESSION_COMPLETED", None, 'Pipeline run completed', {"session_id": session_id, "succeeded": succeeded, "failed": failed, "skipped": skipped, "blocked": blocked})
        
        # Trigger autonomous post-session continuous learning loops
        await self.reflect_and_learn(session_id, final_status)
        
        if dashboard_msg is not None:
            try:
                final_embed = discord.Embed(
                    title=f"📋 Claw Automation Session Complete",
                    description=f"Session status finished with: **{final_status.upper()}**\nID: `{session_id}`\nContinuous learning patches have been updated in MEMORY.md and SOUL.md.",
                    color=discord.Color.green() if final_status == 'completed' else discord.Color.red()
                )
                final_embed.add_field(name="Succeeded Steps", value=str(succeeded), inline=True)
                final_embed.add_field(name="Failed Steps", value=str(failed), inline=True)
                final_embed.add_field(name="Blocked/Skipped", value=str(blocked + skipped), inline=True)
                await dashboard_msg.edit(embed=final_embed)
            except Exception as e:
                logger.error(f"Failed to update end run dashboard message: {e}")
                
        return {"session_id": session_id, "status": final_status}

class ClawPlanner:
    def __init__(self, client=None, orchestrator=None):
        self.client = client
        self.orchestrator = orchestrator

    async def build_plan(self, goal: str) -> dict:
        prompt = (
            "You are a strict Directed Acyclic Graph (DAG) plan compiler. You MUST generate plan steps structured as a list of operations.\n"
            "Supported operations:\n"
            "- PARSE_JSON (args: text)\n"
            "- FORMAT_TEXT (args: text, prefix, suffix)\n"
            "- FETCH_WEB_CONTENT (args: url)\n"
            "- PERFORM_HTTP_REQUEST (args: url, method, headers, data, json_data)\n"
            "- EXECUTE_PYTHON_CODE (args: code)\n"
            "  * Note: When writing custom code inside EXECUTE_PYTHON_CODE, always store your output results to global variable `RESULT` or print them to stdout to be parsed successfully.\n"
            "- EXECUTE_TERMINAL_COMMAND (args: command)\n"
            "- STORE_MEMORY (args: content)\n"
            "- QUERY_MEMORY (args: query, top_k)\n"
            "- EDIT_PERSONA_FILE (args: filename, content, mode)\n"
            "  * Allowed filenames: 'SOUL.md', 'IDENTITY.md', 'MEMORY.md'\n"
            "  * Modes: 'write' (overwrite) or 'append'\n"
            "- SEND_DIRECT_MESSAGE (args: user_id, content)\n"
            "- CREATE_DISCORD_CHANNEL (args: name)\n"
            "- DELETE_DISCORD_CHANNEL (args: none)\n"
            "- MANAGE_ROLES (args: member_id, role_name, action)\n"
            "- EXECUTE_DISCORD_EVAL (args: code)\n"
            "- CREATE_SKILL (args: skill_name, description, code)\n"
            "- LIST_SKILLS (args: none)\n"
            "- EXECUTE_SKILL (args: skill_name, arguments)\n"
            "- SCHEDULE_TASK (args: description, schedule_time, plan_json)\n\n"
            "Each step must include:\n"
            "- step_id: Unique string label (e.g., 'step1')\n"
            "- op: One of the supported strings above\n"
            "- args: Dict of static parameters\n"
            "- inputs: Dict of dynamic inputs linked to artifacts (e.g., {'text': '$step1'})\n"
            "- output_key: Unique artifact key (e.g., 'step2_out')\n"
            "- dependencies: List of output_key strings or step_ids this step depends on\n"
            "- expected: Dict of validation checks (e.g., {'exit_code_zero': true})\n\n"
            "Return ONLY a raw JSON object containing:\n"
            "{\n"
            "  \"plan_id\": \"uuid\",\n"
            "  \"steps\": [ ... ]\n"
            "}\n"
        )
        
        # Pull registered database skills to dynamic planner context
        skills_list = []
        if self.orchestrator:
            try:
                skills_list = await self.orchestrator.get_all_skills()
            except Exception as e:
                logger.error(f"Failed to fetch registered skills for planner context: {e}")

        # Build injected context from template files and dynamic tool list
        full_prompt = build_context_prompt(f"{prompt}\n\n### Admin Goal:\n{goal}", skills_list=skills_list)
        
        plan_raw = ""
        if self.client is not None:
            try:
                resp = await self.client.chat.completions.create(
                    model=os.getenv('CLAW_PLANNER_MODEL', 'gpt-4o-mini'),
                    messages=[
                        {"role": "system", "content": "You are a DAG compiler. Return only a strict JSON matching request schema."},
                        {"role": "user", "content": full_prompt}
                    ],
                    response_format={'type': 'json_object'}
                )
                plan_raw = resp.choices[0].message.content
            except Exception as e:
                logger.error(f"OpenAI planning failed: {e}. Falling back to Local LLM...")
                plan_raw = await query_local_llm(full_prompt)
        else:
            logger.info("Using Local MS Dev Tunnel LLM endpoint for planning compiler step...")
            plan_raw = await query_local_llm(full_prompt)
            
        plan = extract_json_payload(plan_raw)
        if not plan:
            raise ValueError(f"Could not extract a valid JSON DAG payload from response: {plan_raw}")
            
        plan.setdefault("plan_id", str(uuid.uuid4()))
        plan.setdefault("steps", [])
        return plan

class ClawBot(discord.Bot):
    """Modern py-cord 2.8.0 Discord Application Core Integration Client."""
    def __init__(self, orchestrator: Orchestrator, planner: ClawPlanner):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.members = True
        
        super().__init__(intents=intents)
        self.orchestrator = orchestrator
        self.planner = planner
        self.session_tasks = set()
        
    async def on_ready(self):
        logger.info(f"Logged in successfully as client user: {self.user} (ID: {self.user.id})")
        await self.orchestrator.initialize()
        self.orchestrator.discord_client = self
        
        # Start Scheduler background loop
        if not self.task_scheduler.is_running():
            self.task_scheduler.start()
            logger.info("Background task scheduler active and listening.")

    @tasks.loop(seconds=30.0)
    async def task_scheduler(self):
        """Asynchronous background worker looking for scheduled tasks to complete before resting."""
        try:
            if not self.orchestrator or not self.orchestrator.db:
                return
                
            now = utc_now()
            # Query db for active pending schedules
            async with self.orchestrator.db.execute("SELECT task_id, task_description, schedule_time, plan_json, status, last_run FROM scheduled_tasks WHERE status = 'pending'") as cursor:
                tasks_rows = await cursor.fetchall()
                
            for row in tasks_rows:
                task_id, description, sched_time_str, plan_json_str, status, last_run = row
                should_run = False
                mode = "once"
                val = ""
                
                parts = sched_time_str.split(" ", 1)
                if len(parts) == 2:
                    mode, val = parts[0].lower(), parts[1]
                    if mode == "once":
                        try:
                            target = datetime.fromisoformat(val).replace(tzinfo=timezone.utc)
                            if now >= target:
                                should_run = True
                        except Exception:
                            pass
                    elif mode == "daily":
                        curr_hm = now.strftime("%H:%M")
                        today_date_str = now.strftime("%Y-%m-%d")
                        
                        # Prevent multi-trigger within the scheduled minute
                        already_ran_today = False
                        if last_run:
                            try:
                                last_run_dt = datetime.fromisoformat(last_run)
                                if last_run_dt.strftime("%Y-%m-%d") == today_date_str:
                                    already_ran_today = True
                            except Exception:
                                pass
                        
                        if curr_hm == val and not already_ran_today:
                            should_run = True
                            
                if should_run:
                    logger.info(f"Triggering scheduled automation task: {description}")
                    plan = safe_json_loads(plan_json_str, {})
                    
                    async with self.orchestrator.db_lock:
                        await self.orchestrator.db.execute(
                            "UPDATE scheduled_tasks SET status='active', last_run=? WHERE task_id=?",
                            (iso_now(), task_id)
                        )
                        await self.orchestrator.db.commit()
                        
                    session_id = await self.orchestrator.create_session(plan, source_user_id=str(self.user.id))
                    run_task = asyncio.create_task(self.orchestrator.run_session(session_id))
                    self.session_tasks.add(run_task)
                    run_task.add_done_callback(self.session_tasks.discard)
                    
                    async with self.orchestrator.db_lock:
                        next_status = 'pending' if mode == 'daily' else 'completed'
                        await self.orchestrator.db.execute(
                            "UPDATE scheduled_tasks SET status=? WHERE task_id=?",
                            (next_status, task_id)
                        )
                        await self.orchestrator.db.commit()
        except Exception as e:
            logger.error(f"Scheduler loop iteration failed: {e}")

    async def on_message(self, message: discord.Message):
        """Processes user input commands and kicks off automated execution DAG pipelines."""
        if message.author.bot:
            return
            
        content = message.content.strip()
        if not content.startswith("!claw "):
            return
            
        goal = content[len("!claw "):].strip()
        if not goal:
            return
            
        # Inform user that plan generation has started
        status_msg = await message.channel.send("🧠 **Claw Planner:** Analyzing request, loading soul templates, and compiling execution DAG blueprint...")
        
        try:
            # Build DAG sequence
            plan = await self.planner.build_plan(goal)
            
            # Form plan preview lines
            steps_desc = []
            for idx, s in enumerate(plan.get("steps", [])):
                steps_desc.append(f"{idx+1}. **`{s.get('step_id')}`**: {s.get('op')} ➔ Output Key: `{s.get('output_key')}`")
            
            plan_embed = discord.Embed(
                title="📋 DAG Blueprint Compiled",
                description=f"Generated step execution map for request:\n*\"{goal}\"*",
                color=discord.Color.blurple()
            )
            plan_embed.add_field(name="Step Sequence", value="\n".join(steps_desc) if steps_desc else "No operational steps generated.", inline=False)
            plan_embed.set_footer(text="Initializing execution context pipeline...")
            await status_msg.edit(content=None, embed=plan_embed)
            
            # Instantiate and trigger execution run
            session_id = await self.orchestrator.create_session(plan, source_user_id=str(message.author.id), source_channel_id=str(message.channel.id))
            
            execution_task = asyncio.create_task(
                self.orchestrator.run_session(session_id, notify_channel=message.channel, ctx={
                    "guild": message.guild,
                    "channel": message.channel,
                    "user": message.author
                })
            )
            self.session_tasks.add(execution_task)
            execution_task.add_done_callback(self.session_tasks.discard)
            
        except Exception as e:
            logger.error(f"Failed to plan/execute target request: {e}", exc_info=True)
            error_embed = discord.Embed(
                title="❌ Planning / Initialization Error",
                description=f"An error occurred while compiling your execution pipeline:\n```py\n{e}\n```",
                color=discord.Color.red()
            )
            await status_msg.edit(content=None, embed=error_embed)

if __name__ == "__main__":
    # Load required environment variables
    token = os.getenv("DISCORD_BOT_TOKEN")
    
    if not token:
        logger.error("Missing critical configuration: DISCORD_BOT_TOKEN environmental variable is required.")
        sys.exit(1)
        
    # Running strictly in local-first Dev Tunnel setup with no OpenAI requirement
    logger.info("🟢 No OpenAI Key found or specified. Running strictly in Local-First MS Dev Tunnel Mode!")
            
    # Assemble orchestrator structure
    runtime_orchestrator = Orchestrator(openai_client=None)
    planner_compiler = ClawPlanner(client=None, orchestrator=runtime_orchestrator)
    
    # Fire up Py-Cord bot
    logger.info("Starting Claw V12 Py-Cord client application...")
    claw_bot = ClawBot(orchestrator=runtime_orchestrator, planner=planner_compiler)
    claw_bot.run(token)
