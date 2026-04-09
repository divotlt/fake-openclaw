from __future__ import annotations

import asyncio
import hashlib
import html
import json
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from html.parser import HTMLParser
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import aiosqlite
import discord
import docker
from docker.errors import DockerException

try:
    from openai import AsyncOpenAI
except Exception:  # pragma: no cover
    AsyncOpenAI = None  # type: ignore


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("claw_v12")

UTC = timezone.utc


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_now() -> str:
    return utc_now().isoformat()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", errors="ignore")).hexdigest()


def safe_json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def safe_json_loads(text: Optional[str], default: Any) -> Any:
    if not text:
        return default
    try:
        return json.loads(text)
    except Exception:
        return default


class StepState(str, Enum):
    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    BLOCKED = "blocked"
    SKIPPED = "skipped"


class FailureCode(str, Enum):
    ERR_CONTRACT_FAILED = "ERR_CONTRACT_FAILED"
    ERR_TOOL_CRASH = "ERR_TOOL_CRASH"
    ERR_TIMEOUT = "ERR_TIMEOUT"
    ERR_POLICY_DENIAL = "ERR_POLICY_DENIAL"
    ERR_DEPENDENCY_DEAD = "ERR_DEPENDENCY_DEAD"
    ERR_RESOURCE_LIMIT = "ERR_RESOURCE_LIMIT"
    INVALID_PLAN = "INVALID_PLAN"
    DEPENDENCY_MISSING = "DEPENDENCY_MISSING"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    TOOL_EXECUTION_ERROR = "TOOL_EXECUTION_ERROR"
    MAX_RETRIES_EXCEEDED = "MAX_RETRIES_EXCEEDED"
    ARTIFACT_CONFLICT = "ARTIFACT_CONFLICT"
    POLICY_DENIED = "POLICY_DENIED"
    CYCLE_DETECTED = "CYCLE_DETECTED"
    DEADLOCK = "DEADLOCK"


VALIDATION_PRIMITIVES = {
    "min_length": lambda result, val: len(str(result)) >= int(val),
    "max_length": lambda result, val: len(str(result)) <= int(val),
    "contains": lambda result, val: str(val).lower() in str(result).lower(),
    "not_contains": lambda result, val: str(val).lower() not in str(result).lower(),
    "is_valid_json": lambda result, val: _is_valid_json(result) == bool(val),
    "exit_code_zero": lambda result, val: _exit_code_zero(result) == bool(val),
    "min_results_found": lambda result, val: _min_results_found(result) >= int(val),
}


TIER_0_OPS = {"PARSE_JSON", "FORMAT_TEXT"}
TIER_1_OPS = {"FETCH_WEB_CONTENT", "PERFORM_HTTP_REQUEST"}
TIER_2_OPS = {"EXECUTE_PYTHON_CODE"}
DISCORD_NATIVE_OPS = {
    "SEND_DIRECT_MESSAGE",
    "CREATE_DISCORD_CHANNEL",
    "DELETE_DISCORD_CHANNEL",
    "MANAGE_ROLES",
    "STORE_MEMORY",
    "QUERY_MEMORY",
    "EXECUTE_DISCORD_EVAL",
}
ALL_OPS = TIER_0_OPS | TIER_1_OPS | TIER_2_OPS | DISCORD_NATIVE_OPS

ALLOWED_DOMAINS = {
    "api.openai.com",
    "example.com",
}

MAX_INPUT_SIZE = 20000
SANDBOX_TIMEOUT = 10
OUTPUT_TRUNCATE = 5000
MAX_RETRIES = 3


class _HTMLToText(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: List[str] = []
        self._skip = False

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if tag in {"script", "style", "noscript"}:
            self._skip = True

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style", "noscript"}:
            self._skip = False
        if tag in {"p", "div", "br", "li", "tr", "section", "article", "header", "footer"}:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self._skip:
            text = data.strip()
            if text:
                self.parts.append(text)

    def text(self) -> str:
        raw = " ".join(self.parts)
        raw = html.unescape(raw)
        raw = re.sub(r"\s+\n", "\n", raw)
        raw = re.sub(r"\n\s+", "\n", raw)
        raw = re.sub(r"[ \t]{2,}", " ", raw)
        raw = re.sub(r"\n{3,}", "\n\n", raw)
        return raw.strip()


@dataclass
class SessionRecord:
    session_id: str
    plan_id: str
    status: str = "created"
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
    checksum: str = ""
    preview: str = ""


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
    output_key: str = ""
    expected: Dict[str, Any] = field(default_factory=dict)
    attempt_count: int = 0
    max_retries: int = MAX_RETRIES
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


@dataclass
class TraceEvent:
    trace_id: str
    session_id: str
    step_id: Optional[str]
    timestamp: str
    event_type: str
    details: Dict[str, Any]


class PlanVerifier:
    def __init__(self) -> None:
        self.cost_model = {
            "PARSE_JSON": 0.1,
            "FORMAT_TEXT": 0.1,
            "FETCH_WEB_CONTENT": 5.0,
            "PERFORM_HTTP_REQUEST": 4.0,
            "EXECUTE_PYTHON_CODE": 2.0,
            "STORE_MEMORY": 0.3,
            "QUERY_MEMORY": 0.3,
            "SEND_DIRECT_MESSAGE": 0.2,
            "CREATE_DISCORD_CHANNEL": 0.4,
            "DELETE_DISCORD_CHANNEL": 0.4,
            "MANAGE_ROLES": 0.4,
            "EXECUTE_DISCORD_EVAL": 2.0,
        }

    def verify(self, plan: Dict[str, Any]) -> Tuple[bool, List[str], Dict[str, Any], Dict[str, Any]]:
        issues: List[str] = []
        summary = {
            "total_estimated_cost": 0.0,
            "max_depth": 0,
            "parallel_batches": 0,
            "redundant_steps": 0,
        }

        if not isinstance(plan, dict):
            return False, [FailureCode.INVALID_PLAN.value], summary, plan

        if "plan_id" not in plan or "steps" not in plan or not isinstance(plan["steps"], list):
            issues.append(FailureCode.INVALID_PLAN.value)
            return False, issues, summary, plan

        seen_step_ids: set[str] = set()
        seen_signatures: Dict[str, str] = {}
        output_to_step: Dict[str, str] = {}
        normalized_steps: List[Dict[str, Any]] = []

        for raw_step in plan["steps"]:
            if not isinstance(raw_step, dict):
                issues.append(FailureCode.INVALID_PLAN.value)
                continue
            for field in ("step_id", "op", "output_key"):
                if field not in raw_step:
                    issues.append(f"{FailureCode.INVALID_PLAN.value}:{field}")
            if raw_step.get("op") not in ALL_OPS:
                issues.append(f"{FailureCode.INVALID_PLAN.value}:unknown_op:{raw_step.get('op')}")
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

            summary["total_estimated_cost"] += self.cost_model.get(normalized["op"], 1.0)

        graph = self._build_graph(normalized_steps, output_to_step)
        cycle, max_depth = self._detect_cycle_and_depth(graph)
        summary["max_depth"] = max_depth
        summary["parallel_batches"] = self._estimate_parallel_batches(graph)
        if cycle:
            issues.append(FailureCode.CYCLE_DETECTED.value)

        used_outputs = self._used_outputs(normalized_steps)
        for output_key, producer in output_to_step.items():
            if output_key not in used_outputs and len(normalized_steps) > 1:
                issues.append(f"unused_output:{output_key}:{producer}")

        is_valid = len([x for x in issues if x.startswith("INVALID_PLAN") or x in {FailureCode.CYCLE_DETECTED.value, FailureCode.ARTIFACT_CONFLICT.value}]) == 0
        if not is_valid:
            return False, issues, summary, {"plan_id": plan["plan_id"], "steps": normalized_steps}

        return True, issues, summary, {"plan_id": plan["plan_id"], "steps": normalized_steps}

    def _signature(self, step: Dict[str, Any]) -> str:
        payload = {
            "op": step.get("op"),
            "args": step.get("args", {}),
            "inputs": step.get("inputs", {}),
            "expected": step.get("expected", {}),
        }
        return sha256_text(safe_json_dumps(payload))

    def _build_graph(self, steps: List[Dict[str, Any]], output_to_step: Dict[str, str]) -> Dict[str, List[str]]:
        graph = {step["step_id"]: [] for step in steps}
        refs_by_step: Dict[str, set[str]] = {step["step_id"]: set() for step in steps}
        for step in steps:
            refs = set(step.get("dependencies", []) or [])
            for value in (step.get("inputs", {}) or {}).values():
                if isinstance(value, str) and value.startswith("$"):
                    refs.add(value[1:])
            refs_by_step[step["step_id"]] = refs
        for step in steps:
            sid = step["step_id"]
            for ref in refs_by_step[sid]:
                if ref in output_to_step:
                    graph[output_to_step[ref]].append(sid)
                elif ref in graph:
                    graph[ref].append(sid)
        return graph

    def _detect_cycle_and_depth(self, graph: Dict[str, List[str]]) -> Tuple[bool, int]:
        indegree: Dict[str, int] = {node: 0 for node in graph}
        for node, children in graph.items():
            for child in children:
                indegree[child] = indegree.get(child, 0) + 1
        queue = [node for node, deg in indegree.items() if deg == 0]
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

    def _estimate_parallel_batches(self, graph: Dict[str, List[str]]) -> int:
        indegree: Dict[str, int] = {node: 0 for node in graph}
        for _, children in graph.items():
            for child in children:
                indegree[child] = indegree.get(child, 0) + 1
        ready = [node for node, deg in indegree.items() if deg == 0]
        batches = 0
        while ready:
            batches += 1
            next_ready: List[str] = []
            for node in ready:
                for child in graph.get(node, []):
                    indegree[child] -= 1
                    if indegree[child] == 0:
                        next_ready.append(child)
            ready = next_ready
        return batches

    def _used_outputs(self, steps: List[Dict[str, Any]]) -> set[str]:
        used: set[str] = set()
        for step in steps:
            for dep in step.get("dependencies", []) or []:
                if isinstance(dep, str) and dep.startswith("$"):
                    used.add(dep)
                elif isinstance(dep, str):
                    used.add(dep if dep.startswith("$") else f"${dep}")
            for value in (step.get("inputs", {}) or {}).values():
                if isinstance(value, str) and value.startswith("$"):
                    used.add(value)
        return used


class Executor:
    def __init__(self, orchestrator: "Orchestrator") -> None:
        self.orchestrator = orchestrator
        self.docker_client = docker.from_env()

    def policy_gate(self, step: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        op = step["op"]
        if op == "FETCH_WEB_CONTENT":
            url = str(step.get("args", {}).get("url", ""))
            if not url:
                return False, FailureCode.ERR_POLICY_DENIAL.value
            domain = urlparse(url).netloc.lower()
            if domain not in ALLOWED_DOMAINS:
                return False, FailureCode.ERR_POLICY_DENIAL.value
        return True, None

    async def execute(self, step: Dict[str, Any], resolved_inputs: Dict[str, Any], ctx: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        op = step["op"]
        allowed, denial = self.policy_gate(step)
        if not allowed:
            raise RuntimeError(denial or FailureCode.ERR_POLICY_DENIAL.value)

        if op in TIER_0_OPS:
            return await self._execute_tier_0(step, resolved_inputs)
        if op in TIER_1_OPS:
            return await self._execute_tier_1(step, resolved_inputs)
        if op in TIER_2_OPS:
            return await self._execute_tier_2(step, resolved_inputs)
        if op in DISCORD_NATIVE_OPS:
            return await self._execute_discord_native(step, resolved_inputs, ctx or {})
        raise RuntimeError(f"Unknown op: {op}")

    async def _execute_tier_0(self, step: Dict[str, Any], resolved_inputs: Dict[str, Any]) -> Dict[str, Any]:
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
        raise RuntimeError(FailureCode.TOOL_EXECUTION_ERROR.value)

    async def _execute_tier_1(self, step: Dict[str, Any], resolved_inputs: Dict[str, Any]) -> Dict[str, Any]:
        op = step["op"]
        if op == "FETCH_WEB_CONTENT":
            url = str(step.get("args", {}).get("url", ""))

            def fetch() -> str:
                req = Request(url, headers={"User-Agent": "ClawV12/1.0"})
                with urlopen(req, timeout=12) as resp:
                    html_bytes = resp.read()
                parser = _HTMLToText()
                parser.feed(html_bytes.decode("utf-8", errors="ignore"))
                text = parser.text()
                return text[:OUTPUT_TRUNCATE]

            text = await asyncio.wait_for(asyncio.to_thread(fetch), timeout=SANDBOX_TIMEOUT)
            return {"value": text, "text": text, "exit_code": 0}

        if op == "PERFORM_HTTP_REQUEST":
            method = str(step.get("args", {}).get("method", "GET")).upper()
            url = str(step.get("args", {}).get("url", ""))
            data = step.get("args", {}).get("data")
            headers = dict(step.get("args", {}).get("headers") or {})
            json_data = bool(step.get("args", {}).get("json_data", False))

            def request() -> Dict[str, Any]:
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
                    return {"status": getattr(resp, "status", 200), "text": body_text[:OUTPUT_TRUNCATE]}

            response = await asyncio.wait_for(asyncio.to_thread(request), timeout=SANDBOX_TIMEOUT)
            response["exit_code"] = 0
            return response

        raise RuntimeError(FailureCode.TOOL_EXECUTION_ERROR.value)

    async def _execute_tier_2(self, step: Dict[str, Any], resolved_inputs: Dict[str, Any]) -> Dict[str, Any]:
        code = str(step.get("args", {}).get("code", ""))
        bootstrap = (
            "import json, os, sys\n"
            "INPUTS = json.loads(os.environ.get('CLAW_INPUTS', '{}'))\n"
            "RESULT = None\n"
        )
        payload = bootstrap + "\n" + code
        env = os.environ.copy()
        env["CLAW_INPUTS"] = safe_json_dumps(resolved_inputs)
        container = None
        start = time.perf_counter()
        try:
            container = self.docker_client.containers.run(
                "python:3.11-slim",
                ["python", "-c", payload],
                detach=True,
                remove=True,
                stdout=True,
                stderr=True,
                network_disabled=True,
                mem_limit="128m",
                cpu_quota=50000,
                pids_limit=64,
                read_only=True,
                security_opt=["no-new-privileges"],
                environment=env,
            )
            wait_result = await asyncio.wait_for(asyncio.to_thread(container.wait), timeout=SANDBOX_TIMEOUT)
            logs = container.logs(stdout=True, stderr=True).decode("utf-8", errors="ignore")
            duration_ms = int((time.perf_counter() - start) * 1000)
            status = int(wait_result.get("StatusCode", 1))
            return {
                "stdout": logs[:OUTPUT_TRUNCATE],
                "stderr": "",
                "exit_code": status,
                "duration_ms": duration_ms,
                "text": logs[:OUTPUT_TRUNCATE],
            }
        except asyncio.TimeoutError:
            if container is not None:
                try:
                    container.kill()
                except Exception:
                    pass
            raise RuntimeError(FailureCode.ERR_TIMEOUT.value)
        except DockerException as exc:
            raise RuntimeError(f"{FailureCode.TOOL_EXECUTION_ERROR.value}: {exc}")

    async def _execute_discord_native(
        self,
        step: Dict[str, Any],
        resolved_inputs: Dict[str, Any],
        ctx: Dict[str, Any],
    ) -> Dict[str, Any]:
        op = step["op"]
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
                raise RuntimeError(FailureCode.TOOL_EXECUTION_ERROR.value)
            channel = await guild.create_text_channel(str(step.get("args", {}).get("name", "claw-task")))
            return {"value": f"channel:{channel.id}", "text": f"channel:{channel.id}", "exit_code": 0}
        if op == "DELETE_DISCORD_CHANNEL":
            channel = ctx.get("channel")
            if channel is None:
                raise RuntimeError(FailureCode.TOOL_EXECUTION_ERROR.value)
            await channel.delete()
            return {"value": "deleted", "text": "deleted", "exit_code": 0}
        if op == "MANAGE_ROLES":
            guild = ctx.get("guild")
            if guild is None:
                raise RuntimeError(FailureCode.TOOL_EXECUTION_ERROR.value)
            member = guild.get_member(int(step.get("args", {}).get("member_id", 0)))
            role_name = str(step.get("args", {}).get("role_name", ""))
            action = str(step.get("args", {}).get("action", "add"))
            role = discord.utils.get(guild.roles, name=role_name)
            if member is None or role is None:
                raise RuntimeError(FailureCode.TOOL_EXECUTION_ERROR.value)
            if action == "add":
                await member.add_roles(role)
            else:
                await member.remove_roles(role)
            return {"value": f"role:{action}", "text": f"role:{action}", "exit_code": 0}
        if op == "EXECUTE_DISCORD_EVAL":
            code = str(step.get("args", {}).get("code", ""))
            return await self.orchestrator.execute_discord_eval(code, ctx)
        raise RuntimeError(FailureCode.TOOL_EXECUTION_ERROR.value)


class Orchestrator:
    def __init__(self, db_path: str = "claw_v12_runtime.db", openai_client: Optional[Any] = None) -> None:
        self.db_path = db_path
        self.db: Optional[aiosqlite.Connection] = None
        self.db_lock = asyncio.Lock()
        self.executor = Executor(self)
        self.openai_client = openai_client
        self._ready = asyncio.Event()
        self._session_lock = asyncio.Lock()
        self.memory_embedding_model = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")

    async def initialize(self) -> None:
        if self.db is None:
            self.db = await aiosqlite.connect(self.db_path)
            await self.db.execute("PRAGMA journal_mode=WAL")
            await self.db.execute("PRAGMA synchronous=NORMAL")
            await self.db.execute("PRAGMA busy_timeout=5000")
            await self._init_db()
            self._ready.set()

    async def _init_db(self) -> None:
        assert self.db is not None
        async with self.db_lock:
            await self.db.executescript(
                """
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
                """
            )
            await self.db.commit()

    async def _emit_trace(self, event_type: str, step_id: Optional[str], message: str, data: Optional[Dict[str, Any]] = None) -> None:
        assert self.db is not None
        payload = {"message": message, "data": data or {}}
        async with self.db_lock:
            await self.db.execute(
                "INSERT INTO trace_events VALUES (?, ?, ?, ?, ?, ?)",
                (str(uuid.uuid4()), self.current_session_id, step_id, iso_now(), event_type, safe_json_dumps(payload)),
            )
            await self.db.commit()

    @property
    def current_session_id(self) -> str:
        return getattr(self, "_current_session_id", "")

    @current_session_id.setter
    def current_session_id(self, value: str) -> None:
        self._current_session_id = value

    async def create_session(self, plan: Dict[str, Any], source_user_id: Optional[str] = None, source_channel_id: Optional[str] = None) -> str:
        await self.initialize()
        verifier = PlanVerifier()
        valid, issues, summary, normalized_plan = verifier.verify(plan)
        if not valid:
            self.current_session_id = str(uuid.uuid4())
            await self._emit_trace("INVALID_PLAN", None, "Plan verification failed", {"issues": issues})
            raise RuntimeError(f"Plan rejected: {issues}")

        session_id = str(uuid.uuid4())
        self.current_session_id = session_id
        plan_json = safe_json_dumps(normalized_plan)
        session = SessionRecord(
            session_id=session_id,
            plan_id=str(plan["plan_id"]),
            status="active",
            steps_total=len(normalized_plan["steps"]),
            cost_estimated=float(summary["total_estimated_cost"]),
            source_user_id=source_user_id,
            source_channel_id=source_channel_id,
            plan_json=plan_json,
        )
        async with self.db_lock:
            await self.db.execute(
                "INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    session.session_id,
                    session.plan_id,
                    session.status,
                    session.created_at,
                    session.updated_at,
                    session.steps_total,
                    session.steps_completed,
                    session.steps_failed,
                    session.cost_estimated,
                    session.cost_actual,
                    session.error,
                    session.resume_checkpoint_marker,
                    session.source_user_id,
                    session.source_channel_id,
                    session.plan_json,
                ),
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
                    duplicate_of=str(raw_step.get("duplicate_of")) if raw_step.get("duplicate_of") else None,
                )
                await self.db.execute(
                    "INSERT INTO steps VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        step.step_id,
                        step.session_id,
                        step.op_type,
                        step.state.value,
                        step.attempt_count,
                        step.max_retries,
                        safe_json_dumps(step.inputs),
                        safe_json_dumps(step.resolved_inputs),
                        safe_json_dumps(step.args),
                        step.output_key,
                        safe_json_dumps(step.dependencies),
                        safe_json_dumps(step.expected),
                        safe_json_dumps(step.error_history),
                        safe_json_dumps(step.retry_history),
                        step.last_error,
                        step.created_at,
                        step.started_at,
                        step.finished_at,
                        step.cost_hint,
                        step.actual_cost,
                        step.dedup_signature,
                        step.duplicate_of,
                    ),
                )
            await self.db.commit()
        await self._emit_trace("SESSION_CREATED", None, "Session initialized", {"plan_id": plan["plan_id"]})
        return session_id

    async def _fetch_session_steps(self, session_id: str) -> List[Dict[str, Any]]:
        assert self.db is not None
        async with self.db.execute("SELECT * FROM steps WHERE session_id=?", (session_id,)) as cursor:
            rows = await cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]
        return [dict(zip(cols, row)) for row in rows]

    async def _fetch_step(self, session_id: str, step_id: str) -> Optional[Dict[str, Any]]:
        assert self.db is not None
        async with self.db.execute("SELECT * FROM steps WHERE session_id=? AND step_id=?", (session_id, step_id)) as cursor:
            row = await cursor.fetchone()
            if row is None:
                return None
            cols = [desc[0] for desc in cursor.description]
            return dict(zip(cols, row))

    async def _update_step(self, session_id: str, step_id: str, updates: Dict[str, Any]) -> None:
        assert self.db is not None
        if not updates:
            return
        async with self.db_lock:
            set_clause = ", ".join([f"{k}=?" for k in updates])
            values = list(updates.values()) + [session_id, step_id]
            await self.db.execute(
                f"UPDATE steps SET {set_clause} WHERE session_id=? AND step_id=?",
                values,
            )
            await self.db.commit()

    async def _update_session(self, session_id: str, updates: Dict[str, Any]) -> None:
        assert self.db is not None
        if not updates:
            return
        async with self.db_lock:
            set_clause = ", ".join([f"{k}=?" for k in updates])
            values = list(updates.values()) + [session_id]
            await self.db.execute(
                f"UPDATE sessions SET {set_clause}, updated_at=? WHERE session_id=?",
                list(updates.values()) + [iso_now(), session_id],
            )
            await self.db.commit()

    async def dependencies_met(self, session_id: str, step_row: Dict[str, Any]) -> bool:
        deps = safe_json_loads(step_row.get("dependencies"), [])
        if not deps:
            return True
        assert self.db is not None
        for dep in deps:
            dep_key = str(dep)
            if dep_key.startswith("$"):
                dep_key = dep_key[1:]
                async with self.db.execute(
                    "SELECT 1 FROM artifacts WHERE session_id=? AND artifact_id=?",
                    (session_id, dep_key),
                ) as cursor:
                    row = await cursor.fetchone()
                if row is None:
                    return False
                continue
            async with self.db.execute(
                "SELECT state FROM steps WHERE session_id=? AND step_id=?",
                (session_id, dep_key),
            ) as cursor:
                row = await cursor.fetchone()
            if row is None or row[0] != StepState.SUCCEEDED.value:
                return False
        return True

    async def resolve_inputs(self, session_id: str, step_row: Dict[str, Any]) -> Dict[str, Any]:
        inputs = safe_json_loads(step_row.get("inputs"), {})
        resolved: Dict[str, Any] = {}
        assert self.db is not None
        for key, value in inputs.items():
            if isinstance(value, str) and value.startswith("$"):
                artifact_id = value[1:]
                async with self.db.execute(
                    "SELECT payload FROM artifact_payloads WHERE artifact_id=?",
                    (artifact_id,),
                ) as cursor:
                    row = await cursor.fetchone()
                resolved[key] = safe_json_loads(row[0], row[0]) if row else None
            else:
                resolved[key] = value
        return resolved


    async def _upstream_failed(self, session_id: str, step_row: Dict[str, Any]) -> bool:
        deps = safe_json_loads(step_row.get("dependencies"), [])
        if not deps:
            return False
        assert self.db is not None
        for dep in deps:
            dep_key = str(dep)
            if dep_key.startswith("$"):
                dep_key = dep_key[1:]
                async with self.db.execute(
                    "SELECT source_step_id FROM artifacts WHERE session_id=? AND artifact_id=?",
                    (session_id, dep_key),
                ) as cursor:
                    row = await cursor.fetchone()
                if row is None:
                    continue
                source_step_id = row[0]
                async with self.db.execute(
                    "SELECT state FROM steps WHERE session_id=? AND step_id=?",
                    (session_id, source_step_id),
                ) as cursor:
                    step_row_db = await cursor.fetchone()
                if step_row_db and step_row_db[0] == StepState.FAILED.value:
                    return True
                continue
            async with self.db.execute(
                "SELECT state FROM steps WHERE session_id=? AND step_id=?",
                (session_id, dep_key),
            ) as cursor:
                row = await cursor.fetchone()
            if row and row[0] == StepState.FAILED.value:
                return True
        return False

    async def store_artifact(self, session_id: str, step_row: Dict[str, Any], result: Dict[str, Any]) -> None:
        assert self.db is not None
        artifact_id = str(step_row["output_key"])
        payload_text = result.get("text")
        if payload_text is None:
            payload_text = safe_json_dumps(result.get("value", result))
        payload_text = str(payload_text)
        record = ArtifactRecord(
            artifact_id=artifact_id,
            session_id=session_id,
            source_step_id=str(step_row["step_id"]),
            type="json" if isinstance(result.get("value", result), (dict, list)) else "text",
            size_bytes=len(payload_text.encode("utf-8", errors="ignore")),
            checksum=sha256_text(payload_text),
            preview=payload_text[:200],
        )
        async with self.db_lock:
            await self.db.execute(
                "INSERT OR REPLACE INTO artifacts VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    record.artifact_id,
                    record.session_id,
                    record.source_step_id,
                    record.type,
                    record.size_bytes,
                    record.created_at,
                    record.checksum,
                    record.preview,
                ),
            )
            await self.db.execute(
                "INSERT OR REPLACE INTO artifact_payloads VALUES (?, ?)",
                (record.artifact_id, payload_text),
            )
            await self.db.commit()
        await self._emit_trace("ARTIFACT_WRITTEN", str(step_row["step_id"]), "Artifact persisted", {"artifact_id": artifact_id, "bytes": record.size_bytes})

    async def send_direct_message(self, user_id: str, content: str) -> Dict[str, Any]:
        client = getattr(self, "discord_client", None)
        if client is None:
            raise RuntimeError(FailureCode.TOOL_EXECUTION_ERROR.value)
        user = await client.fetch_user(int(user_id))
        await user.send(content[:1900])
        return {"value": f"dm:{user_id}", "text": f"dm:{user_id}", "exit_code": 0}

    async def execute_discord_eval(self, code: str, ctx: Dict[str, Any]) -> Dict[str, Any]:
        namespace = {
            "client": getattr(self, "discord_client", None),
            "discord": discord,
            "asyncio": asyncio,
            "db": self.db,
            "orchestrator": self,
            "guild": ctx.get("guild"),
            "channel": ctx.get("channel"),
            "user": ctx.get("user"),
            "resolved_inputs": ctx.get("resolved_inputs", {}),
        }
        try:
            result = eval(code, namespace)
            if asyncio.iscoroutine(result):
                result = await result
            return {"value": result, "text": str(result) if result is not None else "", "exit_code": 0}
        except SyntaxError:
            exec(code, namespace)
            return {"value": None, "text": "", "exit_code": 0}
        except Exception as exc:
            return {"value": None, "text": f"{type(exc).__name__}: {exc}", "exit_code": 1}

    async def store_memory(self, content: str) -> Dict[str, Any]:
        embedding = None
        if self.openai_client is not None:
            try:
                resp = await self.openai_client.embeddings.create(model=self.memory_embedding_model, input=content)
                embedding = resp.data[0].embedding
            except Exception as exc:
                logger.warning("embedding failed: %s", exc)
        record = {
            "memory_id": str(uuid.uuid4()),
            "session_id": self.current_session_id,
            "content": content,
            "embedding": embedding,
            "created_at": iso_now(),
        }
        assert self.db is not None
        async with self.db_lock:
            await self.db.execute(
                "INSERT INTO memories VALUES (?, ?, ?, ?, ?)",
                (record["memory_id"], record["session_id"], record["content"], safe_json_dumps(record["embedding"]), record["created_at"]),
            )
            await self.db.commit()
        return {"value": "stored", "text": "stored", "exit_code": 0}

    async def query_memory(self, query: str, top_k: int = 5) -> Dict[str, Any]:
        assert self.db is not None
        rows: List[Tuple[str, str]] = []
        async with self.db.execute("SELECT content, embedding FROM memories") as cursor:
            rows = await cursor.fetchall()

        if self.openai_client is not None:
            try:
                q_resp = await self.openai_client.embeddings.create(model=self.memory_embedding_model, input=query)
                q_vec = q_resp.data[0].embedding
                scored: List[Tuple[float, str]] = []
                for content, emb_json in rows:
                    vec = safe_json_loads(emb_json, None)
                    if not vec:
                        continue
                    score = self._cosine_similarity(q_vec, vec)
                    scored.append((score, content))
                scored.sort(key=lambda x: x[0], reverse=True)
                out = "\n\n".join(content for score, content in scored[:top_k]) if scored else "No memories found."
                return {"value": out, "text": out, "exit_code": 0, "results": scored[:top_k]}
            except Exception as exc:
                logger.warning("memory query embedding failed: %s", exc)

        q_words = set(re.findall(r"\w+", query.lower()))
        scored_fallback: List[Tuple[float, str]] = []
        for content, _emb_json in rows:
            words = set(re.findall(r"\w+", content.lower()))
            if not words:
                continue
            overlap = len(q_words & words) / max(1, len(q_words | words))
            scored_fallback.append((overlap, content))
        scored_fallback.sort(key=lambda x: x[0], reverse=True)
        out = "\n\n".join(content for score, content in scored_fallback[:top_k]) if scored_fallback else "No memories found."
        return {"value": out, "text": out, "exit_code": 0, "results": scored_fallback[:top_k]}

    def _cosine_similarity(self, a: List[float], b: List[float]) -> float:
        if not a or not b or len(a) != len(b):
            return 0.0
        dot = sum(x * y for x, y in zip(a, b))
        norm_a = sum(x * x for x in a) ** 0.5
        norm_b = sum(y * y for y in b) ** 0.5
        if norm_a == 0 or norm_b == 0:
            return 0.0
        return dot / (norm_a * norm_b)

    def verify_contract(self, result: Dict[str, Any], expected: Dict[str, Any]) -> Tuple[bool, str]:
        target = result
        if isinstance(result, dict):
            target = result.get("value", result.get("text", result))
        for rule, required in (expected or {}).items():
            if rule not in VALIDATION_PRIMITIVES:
                return False, f"illegal_rule:{rule}"
            try:
                if not VALIDATION_PRIMITIVES[rule](target, required):
                    return False, f"failed:{rule}:{required}"
            except Exception as exc:
                return False, f"validation_error:{rule}:{exc}"
        return True, "ok"

    async def rehydrate_session(self, session_id: str) -> None:
        assert self.db is not None
        self.current_session_id = session_id
        async with self.db_lock:
            async with self.db.execute("SELECT plan_json, status FROM sessions WHERE session_id=?", (session_id,)) as cursor:
                row = await cursor.fetchone()
            if row is None:
                raise RuntimeError("session_not_found")
            plan_json, status = row
            if status not in {"active", "running", "paused"}:
                return
        steps = await self._fetch_session_steps(session_id)
        for step in steps:
            if step["state"] == StepState.RUNNING.value:
                await self._update_step(session_id, step["step_id"], {"state": StepState.PENDING.value, "started_at": None})
            if step["state"] == StepState.READY.value:
                await self._update_step(session_id, step["step_id"], {"state": StepState.PENDING.value})
            if step["state"] == StepState.SUCCEEDED.value:
                artifact_id = step["output_key"]
                async with self.db.execute("SELECT 1 FROM artifact_payloads WHERE artifact_id=?", (artifact_id,)) as cursor:
                    art = await cursor.fetchone()
                if art is None:
                    await self._update_step(session_id, step["step_id"], {"state": StepState.PENDING.value, "last_error": "artifact_missing"})
                    await self._emit_trace("ARTIFACT_MISSING", step["step_id"], "Artifact missing during resume", {"artifact_id": artifact_id})
        marker = sha256_text(safe_json_dumps({"session_id": session_id, "plan": plan_json, "ts": iso_now()}))
        await self._update_session(session_id, {"resume_checkpoint_marker": marker})

    async def run_session(self, session_id: str, notify_channel: Optional[discord.abc.Messageable] = None, ctx: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        await self.initialize()
        self.current_session_id = session_id
        await self.rehydrate_session(session_id)
        ctx = ctx or {}
        await self._update_session(session_id, {"status": "running"})
        await self._emit_trace("SESSION_RUNNING", None, "Session execution started", {"session_id": session_id})
        while True:
            steps = await self._fetch_session_steps(session_id)
            if not steps:
                await self._update_session(session_id, {"status": "failed", "error": "no_steps"})
                raise RuntimeError("no_steps")

            progress = False
            any_pending = False
            step_ids = {s["step_id"] for s in steps}
            blocked_now: List[Dict[str, Any]] = []
            for step in steps:
                if step["state"] in {StepState.SUCCEEDED.value, StepState.FAILED.value, StepState.SKIPPED.value}:
                    continue
                any_pending = True
                if await self._upstream_failed(session_id, step):
                    blocked_now.append(step)
                    continue

            for step in blocked_now:
                await self._update_step(session_id, step["step_id"], {"state": StepState.BLOCKED.value, "finished_at": iso_now(), "last_error": FailureCode.ERR_DEPENDENCY_DEAD.value})
                await self._emit_trace("STEP_BLOCKED", step["step_id"], "Upstream dependency failed", {"code": FailureCode.ERR_DEPENDENCY_DEAD.value})

            steps = await self._fetch_session_steps(session_id)
            ready_steps = []
            for s in steps:
                if s["state"] not in {StepState.PENDING.value, StepState.READY.value}:
                    continue
                if await self.dependencies_met(session_id, s):
                    ready_steps.append(s)

            for step in ready_steps:
                if step["duplicate_of"]:
                    await self._update_step(session_id, step["step_id"], {"state": StepState.SKIPPED.value, "finished_at": iso_now()})
                    await self._emit_trace("DEDUP_REWRITE", step["step_id"], "Duplicate step skipped", {"duplicate_of": step["duplicate_of"]})
            ready_steps = [s for s in ready_steps if not s["duplicate_of"]]

            if not ready_steps:
                terminal = {StepState.SUCCEEDED.value, StepState.FAILED.value, StepState.SKIPPED.value, StepState.BLOCKED.value}
                if all(s["state"] in terminal for s in steps):
                    break
                if any_pending:
                    await asyncio.sleep(0.1)
                    continue
                await self._update_session(session_id, {"status": "failed", "error": FailureCode.DEADLOCK.value})
                raise RuntimeError(FailureCode.DEADLOCK.value)

            async def run_one(step_row: Dict[str, Any]) -> None:
                nonlocal progress
                if int(step_row["attempt"]) >= int(step_row["max_retries"]):
                    await self._update_step(session_id, step_row["step_id"], {"state": StepState.FAILED.value, "last_error": FailureCode.MAX_RETRIES_EXCEEDED.value, "finished_at": iso_now()})
                    await self._emit_trace("STEP_FAILED", step_row["step_id"], "Max retries exceeded", {"code": FailureCode.MAX_RETRIES_EXCEEDED.value})
                    progress = True
                    return
                await self._update_step(session_id, step_row["step_id"], {"state": StepState.RUNNING.value, "started_at": iso_now()})
                await self._emit_trace("STEP_STARTED", step_row["step_id"], "Execution started", {"op": step_row["op"]})
                try:
                    resolved = await self.resolve_inputs(session_id, step_row)
                    if len(str(resolved)) > MAX_INPUT_SIZE:
                        raise RuntimeError(FailureCode.ERR_RESOURCE_LIMIT.value)
                    await self._update_step(session_id, step_row["step_id"], {"resolved_inputs": safe_json_dumps(resolved)})
                    result = await self.executor.execute(step_row, resolved, ctx={"session_id": session_id, "guild": ctx.get("guild"), "channel": notify_channel, "user": ctx.get("user"), "resolved_inputs": resolved})
                    valid, reason = self.verify_contract(result, safe_json_loads(step_row.get("expected"), {}))
                    if not valid:
                        raise RuntimeError(f"{FailureCode.VALIDATION_FAILED.value}:{reason}")
                    await self.store_artifact(session_id, step_row, result)
                    await self._update_step(session_id, step_row["step_id"], {"state": StepState.SUCCEEDED.value, "finished_at": iso_now(), "last_error": None, "actual_cost": float(step_row.get("cost_hint", 0.0))})
                    await self._emit_trace("STEP_SUCCEEDED", step_row["step_id"], "Step completed", {"result_preview": str(result)[:250]})
                except Exception as exc:
                    error_text = f"{type(exc).__name__}:{exc}"
                    new_attempt = int(step_row["attempt"]) + 1
                    retry_history = safe_json_loads(step_row.get("retry_history"), [])
                    error_history = safe_json_loads(step_row.get("error_history"), [])
                    snapshot = {
                        "attempt": new_attempt,
                        "error": error_text,
                        "timestamp": iso_now(),
                        "state": step_row["state"],
                    }
                    retry_history.append(snapshot)
                    error_history.append(snapshot)
                    new_state = StepState.PENDING.value if new_attempt < int(step_row["max_retries"]) else StepState.FAILED.value
                    await self._update_step(session_id, step_row["step_id"], {
                        "state": new_state,
                        "attempt": new_attempt,
                        "retry_history": safe_json_dumps(retry_history),
                        "error_history": safe_json_dumps(error_history),
                        "last_error": error_text,
                        "finished_at": iso_now(),
                    })
                    await self._emit_trace("STEP_FAILED", step_row["step_id"], "Step failed", {"error": error_text})
                progress = True

            await asyncio.gather(*(run_one(step) for step in ready_steps))

            if not progress:
                await asyncio.sleep(0.1)

        final_steps = await self._fetch_session_steps(session_id)
        succeeded = len([s for s in final_steps if s["state"] == StepState.SUCCEEDED.value])
        failed = len([s for s in final_steps if s["state"] == StepState.FAILED.value])
        skipped = len([s for s in final_steps if s["state"] == StepState.SKIPPED.value])
        blocked = len([s for s in final_steps if s["state"] == StepState.BLOCKED.value])
        final_status = "completed" if failed == 0 else "failed"
        await self._update_session(session_id, {"status": final_status, "steps_completed": succeeded, "steps_failed": failed})
        await self._emit_trace("SESSION_COMPLETED", None, "Session finished", {"session_id": session_id, "succeeded": succeeded, "failed": failed, "skipped": skipped, "blocked": blocked})
        if notify_channel is not None:
            try:
                await notify_channel.send(f"Claw V12 session {final_status}: `{session_id}`")
            except Exception:
                pass
        return {"session_id": session_id, "status": final_status}


class ClawPlanner:
    def __init__(self, client: Optional[Any]) -> None:
        self.client = client

    async def build_plan(self, goal: str) -> Dict[str, Any]:
        if self.client is None:
            raise RuntimeError("OPENAI client unavailable")
        prompt = (
            "Return only JSON. Build a strict DAG plan for the goal. "
            "Supported ops: PARSE_JSON, FORMAT_TEXT, FETCH_WEB_CONTENT, PERFORM_HTTP_REQUEST, "
            "EXECUTE_PYTHON_CODE, STORE_MEMORY, QUERY_MEMORY, SEND_DIRECT_MESSAGE, "
            "CREATE_DISCORD_CHANNEL, DELETE_DISCORD_CHANNEL, MANAGE_ROLES, EXECUTE_DISCORD_EVAL. "
            "Each step must include: step_id, op, args, inputs, output_key, dependencies, expected. "
            f"Goal: {goal}"
        )
        resp = await self.client.chat.completions.create(
            model=os.getenv("CLAW_PLANNER_MODEL", "gpt-4o-mini"),
            messages=[
                {"role": "system", "content": "You are a DAG compiler. Return only a strict JSON object."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        plan = json.loads(resp.choices[0].message.content)
        if "plan_id" not in plan:
            plan["plan_id"] = str(uuid.uuid4())
        if "steps" not in plan:
            plan["steps"] = []
        return plan


class ClawBot(discord.Client):
    def __init__(self, orchestrator: Orchestrator, planner: ClawPlanner) -> None:
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        intents.members = True
        intents.dm_messages = True
        super().__init__(intents=intents)
        self.orchestrator = orchestrator
        self.planner = planner
        self.session_tasks: set[asyncio.Task] = set()

    async def setup_hook(self) -> None:
        await self.orchestrator.initialize()
        self.orchestrator.discord_client = self
        logger.info("Claw V12 online")

    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot:
            return
        content = message.content.strip()
        if not content.startswith("!claw "):
            return
        goal = content[len("!claw "):].strip()
        if not goal:
            return
        async with message.channel.typing():
            try:
                plan = await self.planner.build_plan(goal)
                session_id = await self.orchestrator.create_session(
                    plan,
                    source_user_id=str(message.author.id),
                    source_channel_id=str(message.channel.id),
                )
                task = asyncio.create_task(self.orchestrator.run_session(session_id, notify_channel=message.channel, ctx={"guild": message.guild, "user": message.author, "channel": message.channel}))
                self.session_tasks.add(task)
                task.add_done_callback(self.session_tasks.discard)
                await message.channel.send(f"Session queued: `{session_id}`")
            except Exception as exc:
                await message.channel.send(f"Plan rejected or execution failed to start: `{exc}`")


async def main() -> None:
    token = os.getenv("DISCORD_BOT_TOKEN", "")
    openai_key = os.getenv("OPENAI_API_KEY", "")
    if not token:
        raise RuntimeError("DISCORD_BOT_TOKEN is required")
    client = AsyncOpenAI(api_key=openai_key) if (AsyncOpenAI is not None and openai_key) else None
    orchestrator = Orchestrator(openai_client=client)
    planner = ClawPlanner(client)
    bot = ClawBot(orchestrator, planner)
    await bot.start(token)


if __name__ == "__main__":
    asyncio.run(main())
