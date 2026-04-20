from __future__ import annotations
_A3='started_at'
_A2='discord_client'
_A1='artifact_id'
_A0='EXECUTE_PYTHON_CODE'
_z='skipped'
_y='blocked'
_x='succeeded'
_w='SELECT state FROM steps WHERE session_id=? AND step_id=?'
_v='url'
_u='total_estimated_cost'
_t='\n\n'
_s='EXECUTE_DISCORD_EVAL'
_r='QUERY_MEMORY'
_q='STORE_MEMORY'
_p='MANAGE_ROLES'
_o='DELETE_DISCORD_CHANNEL'
_n='CREATE_DISCORD_CHANNEL'
_m='SEND_DIRECT_MESSAGE'
_l='PERFORM_HTTP_REQUEST'
_k='FORMAT_TEXT'
_j='PARSE_JSON'
_i='INVALID_PLAN'
_h='running'
_g='resolved_inputs'
_f='code'
_e='\n'
_d='FETCH_WEB_CONTENT'
_c='last_error'
_b='channel'
_a='expected'
_Z='output_key'
_Y='failed'
_X='ignore'
_W='user'
_V='dependencies'
_U='session_id'
_T='guild'
_S='content'
_R='status'
_Q='$'
_P='duplicate_of'
_O='inputs'
_N=.0
_M='utf-8'
_L='steps'
_K='plan_id'
_J='exit_code'
_I='op'
_H='value'
_G=False
_F='state'
_E='text'
_D='args'
_C='step_id'
_B=True
_A=None
import asyncio,hashlib,html,json,logging,os,re,time,uuid
from dataclasses import dataclass,field
from datetime import datetime,timezone
from enum import Enum
from html.parser import HTMLParser
from typing import Any,Dict,List,Optional,Tuple
from urllib.parse import urlparse
from urllib.request import Request,urlopen
import aiosqlite,discord,docker
from docker.errors import DockerException
try:from openai import AsyncOpenAI
except Exception:AsyncOpenAI=_A
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger=logging.getLogger('claw_v12')
UTC=timezone.utc
def utc_now():return datetime.now(UTC)
def iso_now():return utc_now().isoformat()
def sha256_text(text):return hashlib.sha256(text.encode(_M,errors=_X)).hexdigest()
def safe_json_dumps(value):return json.dumps(value,ensure_ascii=_G,sort_keys=_B,default=str)
def safe_json_loads(text,default):
	if not text:return default
	try:return json.loads(text)
	except Exception:return default
class StepState(str,Enum):PENDING='pending';READY='ready';RUNNING=_h;SUCCEEDED=_x;FAILED=_Y;BLOCKED=_y;SKIPPED=_z
class FailureCode(str,Enum):ERR_CONTRACT_FAILED='ERR_CONTRACT_FAILED';ERR_TOOL_CRASH='ERR_TOOL_CRASH';ERR_TIMEOUT='ERR_TIMEOUT';ERR_POLICY_DENIAL='ERR_POLICY_DENIAL';ERR_DEPENDENCY_DEAD='ERR_DEPENDENCY_DEAD';ERR_RESOURCE_LIMIT='ERR_RESOURCE_LIMIT';INVALID_PLAN=_i;DEPENDENCY_MISSING='DEPENDENCY_MISSING';VALIDATION_FAILED='VALIDATION_FAILED';TOOL_EXECUTION_ERROR='TOOL_EXECUTION_ERROR';MAX_RETRIES_EXCEEDED='MAX_RETRIES_EXCEEDED';ARTIFACT_CONFLICT='ARTIFACT_CONFLICT';POLICY_DENIED='POLICY_DENIED';CYCLE_DETECTED='CYCLE_DETECTED';DEADLOCK='DEADLOCK'
VALIDATION_PRIMITIVES={'min_length':lambda result,val:len(str(result))>=int(val),'max_length':lambda result,val:len(str(result))<=int(val),'contains':lambda result,val:str(val).lower()in str(result).lower(),'not_contains':lambda result,val:str(val).lower()not in str(result).lower(),'is_valid_json':lambda result,val:_is_valid_json(result)==bool(val),'exit_code_zero':lambda result,val:_exit_code_zero(result)==bool(val),'min_results_found':lambda result,val:_min_results_found(result)>=int(val)}
TIER_0_OPS={_j,_k}
TIER_1_OPS={_d,_l}
TIER_2_OPS={_A0}
DISCORD_NATIVE_OPS={_m,_n,_o,_p,_q,_r,_s}
ALL_OPS=TIER_0_OPS|TIER_1_OPS|TIER_2_OPS|DISCORD_NATIVE_OPS
ALLOWED_DOMAINS={'api.openai.com','example.com'}
MAX_INPUT_SIZE=20000
SANDBOX_TIMEOUT=10
OUTPUT_TRUNCATE=5000
MAX_RETRIES=3
class _HTMLToText(HTMLParser):
	def __init__(self):super().__init__();self.parts=[];self._skip=_G
	def handle_starttag(self,tag,attrs):
		if tag in{'script','style','noscript'}:self._skip=_B
	def handle_endtag(self,tag):
		if tag in{'script','style','noscript'}:self._skip=_G
		if tag in{'p','div','br','li','tr','section','article','header','footer'}:self.parts.append(_e)
	def handle_data(self,data):
		if not self._skip:
			text=data.strip()
			if text:self.parts.append(text)
	def text(self):raw=' '.join(self.parts);raw=html.unescape(raw);raw=re.sub('\\s+\\n',_e,raw);raw=re.sub('\\n\\s+',_e,raw);raw=re.sub('[ \\t]{2,}',' ',raw);raw=re.sub('\\n{3,}',_t,raw);return raw.strip()
@dataclass
class SessionRecord:session_id:str;plan_id:str;status:str='created';created_at:str=field(default_factory=iso_now);updated_at:str=field(default_factory=iso_now);steps_total:int=0;steps_completed:int=0;steps_failed:int=0;cost_estimated:float=_N;cost_actual:float=_N;error:Optional[str]=_A;resume_checkpoint_marker:Optional[str]=_A;source_user_id:Optional[str]=_A;source_channel_id:Optional[str]=_A;plan_json:Optional[str]=_A
@dataclass
class ArtifactRecord:artifact_id:str;session_id:str;source_step_id:str;type:str;size_bytes:int;created_at:str=field(default_factory=iso_now);checksum:str='';preview:str=''
@dataclass
class StepRecord:step_id:str;session_id:str;op_type:str;state:StepState=StepState.PENDING;dependencies:List[str]=field(default_factory=list);inputs:Dict[str,Any]=field(default_factory=dict);resolved_inputs:Dict[str,Any]=field(default_factory=dict);args:Dict[str,Any]=field(default_factory=dict);output_key:str='';expected:Dict[str,Any]=field(default_factory=dict);attempt_count:int=0;max_retries:int=MAX_RETRIES;retry_history:List[Dict[str,Any]]=field(default_factory=list);error_history:List[Dict[str,Any]]=field(default_factory=list);cost_hint:float=_N;actual_cost:float=_N;created_at:str=field(default_factory=iso_now);started_at:Optional[str]=_A;finished_at:Optional[str]=_A;last_error:Optional[str]=_A;dedup_signature:Optional[str]=_A;duplicate_of:Optional[str]=_A
@dataclass
class TraceEvent:trace_id:str;session_id:str;step_id:Optional[str];timestamp:str;event_type:str;details:Dict[str,Any]
class PlanVerifier:
	def __init__(self):self.cost_model={_j:.1,_k:.1,_d:5.,_l:4.,_A0:2.,_q:.3,_r:.3,_m:.2,_n:.4,_o:.4,_p:.4,_s:2.}
	def verify(self,plan):
		C='redundant_steps';B='parallel_batches';A='max_depth';issues=[];summary={_u:_N,A:0,B:0,C:0}
		if not isinstance(plan,dict):return _G,[FailureCode.INVALID_PLAN.value],summary,plan
		if _K not in plan or _L not in plan or not isinstance(plan[_L],list):issues.append(FailureCode.INVALID_PLAN.value);return _G,issues,summary,plan
		seen_step_ids=set();seen_signatures={};output_to_step={};normalized_steps=[]
		for raw_step in plan[_L]:
			if not isinstance(raw_step,dict):issues.append(FailureCode.INVALID_PLAN.value);continue
			for field in(_C,_I,_Z):
				if field not in raw_step:issues.append(f"{FailureCode.INVALID_PLAN.value}:{field}")
			if raw_step.get(_I)not in ALL_OPS:issues.append(f"{FailureCode.INVALID_PLAN.value}:unknown_op:{raw_step.get(_I)}")
			step_id=str(raw_step.get(_C,''))
			if step_id in seen_step_ids:issues.append(f"{FailureCode.ARTIFACT_CONFLICT.value}:duplicate_step_id:{step_id}")
			seen_step_ids.add(step_id);output_key=str(raw_step.get(_Z,''))
			if output_key in output_to_step:issues.append(f"{FailureCode.ARTIFACT_CONFLICT.value}:duplicate_output:{output_key}")
			output_to_step[output_key]=step_id;normalized=dict(raw_step);normalized.setdefault(_V,[]);normalized.setdefault(_O,{});normalized.setdefault(_D,{});normalized.setdefault(_a,{});normalized_steps.append(normalized);signature=self._signature(normalized)
			if signature in seen_signatures:summary[C]+=1;normalized[_P]=seen_signatures[signature]
			else:seen_signatures[signature]=step_id
			summary[_u]+=self.cost_model.get(normalized[_I],1.)
		graph=self._build_graph(normalized_steps,output_to_step);cycle,max_depth=self._detect_cycle_and_depth(graph);summary[A]=max_depth;summary[B]=self._estimate_parallel_batches(graph)
		if cycle:issues.append(FailureCode.CYCLE_DETECTED.value)
		used_outputs=self._used_outputs(normalized_steps)
		for(output_key,producer)in output_to_step.items():
			if output_key not in used_outputs and len(normalized_steps)>1:issues.append(f"unused_output:{output_key}:{producer}")
		is_valid=len([x for x in issues if x.startswith(_i)or x in{FailureCode.CYCLE_DETECTED.value,FailureCode.ARTIFACT_CONFLICT.value}])==0
		if not is_valid:return _G,issues,summary,{_K:plan[_K],_L:normalized_steps}
		return _B,issues,summary,{_K:plan[_K],_L:normalized_steps}
	def _signature(self,step):payload={_I:step.get(_I),_D:step.get(_D,{}),_O:step.get(_O,{}),_a:step.get(_a,{})};return sha256_text(safe_json_dumps(payload))
	def _build_graph(self,steps,output_to_step):
		graph={step[_C]:[]for step in steps};refs_by_step={step[_C]:set()for step in steps}
		for step in steps:
			refs=set(step.get(_V,[])or[])
			for value in(step.get(_O,{})or{}).values():
				if isinstance(value,str)and value.startswith(_Q):refs.add(value[1:])
			refs_by_step[step[_C]]=refs
		for step in steps:
			sid=step[_C]
			for ref in refs_by_step[sid]:
				if ref in output_to_step:graph[output_to_step[ref]].append(sid)
				elif ref in graph:graph[ref].append(sid)
		return graph
	def _detect_cycle_and_depth(self,graph):
		indegree={node:0 for node in graph}
		for(node,children)in graph.items():
			for child in children:indegree[child]=indegree.get(child,0)+1
		queue=[node for(node,deg)in indegree.items()if deg==0];visited=0;depth={node:1 for node in queue}
		while queue:
			node=queue.pop(0);visited+=1
			for child in graph.get(node,[]):
				depth[child]=max(depth.get(child,1),depth.get(node,1)+1);indegree[child]-=1
				if indegree[child]==0:queue.append(child)
		return visited!=len(graph),max(depth.values())if depth else 0
	def _estimate_parallel_batches(self,graph):
		indegree={node:0 for node in graph}
		for(_,children)in graph.items():
			for child in children:indegree[child]=indegree.get(child,0)+1
		ready=[node for(node,deg)in indegree.items()if deg==0];batches=0
		while ready:
			batches+=1;next_ready=[]
			for node in ready:
				for child in graph.get(node,[]):
					indegree[child]-=1
					if indegree[child]==0:next_ready.append(child)
			ready=next_ready
		return batches
	def _used_outputs(self,steps):
		used=set()
		for step in steps:
			for dep in step.get(_V,[])or[]:
				if isinstance(dep,str)and dep.startswith(_Q):used.add(dep)
				elif isinstance(dep,str):used.add(dep if dep.startswith(_Q)else f"${dep}")
			for value in(step.get(_O,{})or{}).values():
				if isinstance(value,str)and value.startswith(_Q):used.add(value)
		return used
class Executor:
	def __init__(self,orchestrator):self.orchestrator=orchestrator;self.docker_client=docker.from_env()
	def policy_gate(self,step):
		op=step[_I]
		if op==_d:
			url=str(step.get(_D,{}).get(_v,''))
			if not url:return _G,FailureCode.ERR_POLICY_DENIAL.value
			domain=urlparse(url).netloc.lower()
			if domain not in ALLOWED_DOMAINS:return _G,FailureCode.ERR_POLICY_DENIAL.value
		return _B,_A
	async def execute(self,step,resolved_inputs,ctx=_A):
		op=step[_I];allowed,denial=self.policy_gate(step)
		if not allowed:raise RuntimeError(denial or FailureCode.ERR_POLICY_DENIAL.value)
		if op in TIER_0_OPS:return await self._execute_tier_0(step,resolved_inputs)
		if op in TIER_1_OPS:return await self._execute_tier_1(step,resolved_inputs)
		if op in TIER_2_OPS:return await self._execute_tier_2(step,resolved_inputs)
		if op in DISCORD_NATIVE_OPS:return await self._execute_discord_native(step,resolved_inputs,ctx or{})
		raise RuntimeError(f"Unknown op: {op}")
	async def _execute_tier_0(self,step,resolved_inputs):
		op=step[_I]
		if op==_j:text=resolved_inputs.get(_E,step.get(_D,{}).get(_E,'{}'));parsed=json.loads(text);return{_H:parsed,_E:safe_json_dumps(parsed),_J:0}
		if op==_k:text=str(resolved_inputs.get(_E,step.get(_D,{}).get(_E,'')));prefix=str(step.get(_D,{}).get('prefix',''));suffix=str(step.get(_D,{}).get('suffix',''));formatted=f"{prefix}{text}{suffix}";return{_H:formatted,_E:formatted,_J:0}
		raise RuntimeError(FailureCode.TOOL_EXECUTION_ERROR.value)
	async def _execute_tier_1(self,step,resolved_inputs):
		B='ClawV12/1.0';A='User-Agent';op=step[_I]
		if op==_d:
			url=str(step.get(_D,{}).get(_v,''))
			def fetch():
				req=Request(url,headers={A:B})
				with urlopen(req,timeout=12)as resp:html_bytes=resp.read()
				parser=_HTMLToText();parser.feed(html_bytes.decode(_M,errors=_X));text=parser.text();return text[:OUTPUT_TRUNCATE]
			text=await asyncio.wait_for(asyncio.to_thread(fetch),timeout=SANDBOX_TIMEOUT);return{_H:text,_E:text,_J:0}
		if op==_l:
			method=str(step.get(_D,{}).get('method','GET')).upper();url=str(step.get(_D,{}).get(_v,''));data=step.get(_D,{}).get('data');headers=dict(step.get(_D,{}).get('headers')or{});json_data=bool(step.get(_D,{}).get('json_data',_G))
			def request():
				C='Content-Type';body=_A;req_headers={A:B};req_headers.update(headers)
				if data is not _A:
					if json_data:body=json.dumps(data).encode(_M);req_headers[C]='application/json'
					elif isinstance(data,dict):from urllib.parse import urlencode;body=urlencode(data).encode(_M);req_headers[C]='application/x-www-form-urlencoded'
					elif isinstance(data,str):body=data.encode(_M)
				req=Request(url,data=body,headers=req_headers,method=method)
				with urlopen(req,timeout=15)as resp:body_text=resp.read().decode(_M,errors=_X);return{_R:getattr(resp,_R,200),_E:body_text[:OUTPUT_TRUNCATE]}
			response=await asyncio.wait_for(asyncio.to_thread(request),timeout=SANDBOX_TIMEOUT);response[_J]=0;return response
		raise RuntimeError(FailureCode.TOOL_EXECUTION_ERROR.value)
	async def _execute_tier_2(self,step,resolved_inputs):
		code=str(step.get(_D,{}).get(_f,''));bootstrap="import json, os, sys\nINPUTS = json.loads(os.environ.get('CLAW_INPUTS', '{}'))\nRESULT = None\n";payload=bootstrap+_e+code;env=os.environ.copy();env['CLAW_INPUTS']=safe_json_dumps(resolved_inputs);container=_A;start=time.perf_counter()
		try:container=self.docker_client.containers.run('python:3.11-slim',['python','-c',payload],detach=_B,remove=_B,stdout=_B,stderr=_B,network_disabled=_B,mem_limit='128m',cpu_quota=50000,pids_limit=64,read_only=_B,security_opt=['no-new-privileges'],environment=env);wait_result=await asyncio.wait_for(asyncio.to_thread(container.wait),timeout=SANDBOX_TIMEOUT);logs=container.logs(stdout=_B,stderr=_B).decode(_M,errors=_X);duration_ms=int((time.perf_counter()-start)*1000);status=int(wait_result.get('StatusCode',1));return{'stdout':logs[:OUTPUT_TRUNCATE],'stderr':'',_J:status,'duration_ms':duration_ms,_E:logs[:OUTPUT_TRUNCATE]}
		except asyncio.TimeoutError:
			if container is not _A:
				try:container.kill()
				except Exception:pass
			raise RuntimeError(FailureCode.ERR_TIMEOUT.value)
		except DockerException as exc:raise RuntimeError(f"{FailureCode.TOOL_EXECUTION_ERROR.value}: {exc}")
	async def _execute_discord_native(self,step,resolved_inputs,ctx):
		C='add';B='deleted';A='query';op=step[_I]
		if op==_q:content=str(step.get(_D,{}).get(_S,resolved_inputs.get(_S,'')));return await self.orchestrator.store_memory(content)
		if op==_r:query=str(step.get(_D,{}).get(A,resolved_inputs.get(A,'')));top_k=int(step.get(_D,{}).get('top_k',5));return await self.orchestrator.query_memory(query,top_k=top_k)
		if op==_m:user_id=str(step.get(_D,{}).get('user_id',''));content=str(step.get(_D,{}).get(_S,''));return await self.orchestrator.send_direct_message(user_id,content)
		if op==_n:
			guild=ctx.get(_T)
			if guild is _A:raise RuntimeError(FailureCode.TOOL_EXECUTION_ERROR.value)
			channel=await guild.create_text_channel(str(step.get(_D,{}).get('name','claw-task')));return{_H:f"channel:{channel.id}",_E:f"channel:{channel.id}",_J:0}
		if op==_o:
			channel=ctx.get(_b)
			if channel is _A:raise RuntimeError(FailureCode.TOOL_EXECUTION_ERROR.value)
			await channel.delete();return{_H:B,_E:B,_J:0}
		if op==_p:
			guild=ctx.get(_T)
			if guild is _A:raise RuntimeError(FailureCode.TOOL_EXECUTION_ERROR.value)
			member=guild.get_member(int(step.get(_D,{}).get('member_id',0)));role_name=str(step.get(_D,{}).get('role_name',''));action=str(step.get(_D,{}).get('action',C));role=discord.utils.get(guild.roles,name=role_name)
			if member is _A or role is _A:raise RuntimeError(FailureCode.TOOL_EXECUTION_ERROR.value)
			if action==C:await member.add_roles(role)
			else:await member.remove_roles(role)
			return{_H:f"role:{action}",_E:f"role:{action}",_J:0}
		if op==_s:code=str(step.get(_D,{}).get(_f,''));return await self.orchestrator.execute_discord_eval(code,ctx)
		raise RuntimeError(FailureCode.TOOL_EXECUTION_ERROR.value)
class Orchestrator:
	def __init__(self,db_path='claw_v12_runtime.db',openai_client=_A):self.db_path=db_path;self.db=_A;self.db_lock=asyncio.Lock();self.executor=Executor(self);self.openai_client=openai_client;self._ready=asyncio.Event();self._session_lock=asyncio.Lock();self.memory_embedding_model=os.getenv('OPENAI_EMBEDDING_MODEL','text-embedding-3-small')
	async def initialize(self):
		if self.db is _A:self.db=await aiosqlite.connect(self.db_path);await self.db.execute('PRAGMA journal_mode=WAL');await self.db.execute('PRAGMA synchronous=NORMAL');await self.db.execute('PRAGMA busy_timeout=5000');await self._init_db();self._ready.set()
	async def _init_db(self):
		async with self.db_lock:await self.db.executescript('\n                CREATE TABLE IF NOT EXISTS sessions (\n                    session_id TEXT PRIMARY KEY,\n                    plan_id TEXT,\n                    status TEXT,\n                    created_at TEXT,\n                    updated_at TEXT,\n                    steps_total INTEGER,\n                    steps_completed INTEGER,\n                    steps_failed INTEGER,\n                    cost_estimated REAL,\n                    cost_actual REAL,\n                    error TEXT,\n                    resume_checkpoint_marker TEXT,\n                    source_user_id TEXT,\n                    source_channel_id TEXT,\n                    plan_json TEXT\n                );\n\n                CREATE TABLE IF NOT EXISTS steps (\n                    step_id TEXT,\n                    session_id TEXT,\n                    op TEXT,\n                    state TEXT,\n                    attempt INTEGER,\n                    max_retries INTEGER,\n                    inputs TEXT,\n                    resolved_inputs TEXT,\n                    args TEXT,\n                    output_key TEXT,\n                    dependencies TEXT,\n                    expected TEXT,\n                    error_history TEXT,\n                    retry_history TEXT,\n                    last_error TEXT,\n                    created_at TEXT,\n                    started_at TEXT,\n                    finished_at TEXT,\n                    cost_hint REAL,\n                    actual_cost REAL,\n                    dedup_signature TEXT,\n                    duplicate_of TEXT,\n                    PRIMARY KEY (step_id, session_id)\n                );\n\n                CREATE TABLE IF NOT EXISTS artifacts (\n                    artifact_id TEXT PRIMARY KEY,\n                    session_id TEXT,\n                    source_step_id TEXT,\n                    type TEXT,\n                    size_bytes INTEGER,\n                    created_at TEXT,\n                    checksum TEXT,\n                    preview TEXT\n                );\n\n                CREATE TABLE IF NOT EXISTS artifact_payloads (\n                    artifact_id TEXT PRIMARY KEY,\n                    payload TEXT,\n                    FOREIGN KEY (artifact_id) REFERENCES artifacts(artifact_id)\n                );\n\n                CREATE TABLE IF NOT EXISTS trace_events (\n                    trace_id TEXT PRIMARY KEY,\n                    session_id TEXT,\n                    step_id TEXT,\n                    timestamp TEXT,\n                    event_type TEXT,\n                    details TEXT\n                );\n\n                CREATE TABLE IF NOT EXISTS memories (\n                    memory_id TEXT PRIMARY KEY,\n                    session_id TEXT,\n                    content TEXT,\n                    embedding TEXT,\n                    created_at TEXT\n                );\n                ');await self.db.commit()
	async def _emit_trace(self,event_type,step_id,message,data=_A):
		payload={'message':message,'data':data or{}}
		async with self.db_lock:await self.db.execute('INSERT INTO trace_events VALUES (?, ?, ?, ?, ?, ?)',(str(uuid.uuid4()),self.current_session_id,step_id,iso_now(),event_type,safe_json_dumps(payload)));await self.db.commit()
	@property
	def current_session_id(self):return getattr(self,'_current_session_id','')
	@current_session_id.setter
	def current_session_id(self,value):self._current_session_id=value
	async def create_session(self,plan,source_user_id=_A,source_channel_id=_A):
		await self.initialize();verifier=PlanVerifier();valid,issues,summary,normalized_plan=verifier.verify(plan)
		if not valid:self.current_session_id=str(uuid.uuid4());await self._emit_trace(_i,_A,'Plan verification failed',{'issues':issues});raise RuntimeError(f"Plan rejected: {issues}")
		session_id=str(uuid.uuid4());self.current_session_id=session_id;plan_json=safe_json_dumps(normalized_plan);session=SessionRecord(session_id=session_id,plan_id=str(plan[_K]),status='active',steps_total=len(normalized_plan[_L]),cost_estimated=float(summary[_u]),source_user_id=source_user_id,source_channel_id=source_channel_id,plan_json=plan_json)
		async with self.db_lock:
			await self.db.execute('INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',(session.session_id,session.plan_id,session.status,session.created_at,session.updated_at,session.steps_total,session.steps_completed,session.steps_failed,session.cost_estimated,session.cost_actual,session.error,session.resume_checkpoint_marker,session.source_user_id,session.source_channel_id,session.plan_json))
			for raw_step in normalized_plan[_L]:step=StepRecord(step_id=str(raw_step[_C]),session_id=session_id,op_type=str(raw_step[_I]),dependencies=[str(x)for x in raw_step.get(_V,[])or[]],inputs=dict(raw_step.get(_O,{})or{}),args=dict(raw_step.get(_D,{})or{}),output_key=str(raw_step[_Z]),expected=dict(raw_step.get(_a,{})or{}),cost_hint=float(verifier.cost_model.get(str(raw_step[_I]),1.)),dedup_signature=verifier._signature(raw_step),duplicate_of=str(raw_step.get(_P))if raw_step.get(_P)else _A);await self.db.execute('INSERT INTO steps VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',(step.step_id,step.session_id,step.op_type,step.state.value,step.attempt_count,step.max_retries,safe_json_dumps(step.inputs),safe_json_dumps(step.resolved_inputs),safe_json_dumps(step.args),step.output_key,safe_json_dumps(step.dependencies),safe_json_dumps(step.expected),safe_json_dumps(step.error_history),safe_json_dumps(step.retry_history),step.last_error,step.created_at,step.started_at,step.finished_at,step.cost_hint,step.actual_cost,step.dedup_signature,step.duplicate_of))
			await self.db.commit()
		await self._emit_trace('SESSION_CREATED',_A,'Session initialized',{_K:plan[_K]});return session_id
	async def _fetch_session_steps(self,session_id):
		async with self.db.execute('SELECT * FROM steps WHERE session_id=?',(session_id,))as cursor:rows=await cursor.fetchall();cols=[desc[0]for desc in cursor.description]
		return[dict(zip(cols,row))for row in rows]
	async def _fetch_step(self,session_id,step_id):
		async with self.db.execute('SELECT * FROM steps WHERE session_id=? AND step_id=?',(session_id,step_id))as cursor:
			row=await cursor.fetchone()
			if row is _A:return
			cols=[desc[0]for desc in cursor.description];return dict(zip(cols,row))
	async def _update_step(self,session_id,step_id,updates):
		if not updates:return
		async with self.db_lock:set_clause=', '.join([f"{k}=?"for k in updates]);values=list(updates.values())+[session_id,step_id];await self.db.execute(f"UPDATE steps SET {set_clause} WHERE session_id=? AND step_id=?",values);await self.db.commit()
	async def _update_session(self,session_id,updates):
		if not updates:return
		async with self.db_lock:set_clause=', '.join([f"{k}=?"for k in updates]);values=list(updates.values())+[session_id];await self.db.execute(f"UPDATE sessions SET {set_clause}, updated_at=? WHERE session_id=?",list(updates.values())+[iso_now(),session_id]);await self.db.commit()
	async def dependencies_met(self,session_id,step_row):
		deps=safe_json_loads(step_row.get(_V),[])
		if not deps:return _B
		for dep in deps:
			dep_key=str(dep)
			if dep_key.startswith(_Q):
				dep_key=dep_key[1:]
				async with self.db.execute('SELECT 1 FROM artifacts WHERE session_id=? AND artifact_id=?',(session_id,dep_key))as cursor:row=await cursor.fetchone()
				if row is _A:return _G
				continue
			async with self.db.execute(_w,(session_id,dep_key))as cursor:row=await cursor.fetchone()
			if row is _A or row[0]!=StepState.SUCCEEDED.value:return _G
		return _B
	async def resolve_inputs(self,session_id,step_row):
		inputs=safe_json_loads(step_row.get(_O),{});resolved={}
		for(key,value)in inputs.items():
			if isinstance(value,str)and value.startswith(_Q):
				artifact_id=value[1:]
				async with self.db.execute('SELECT payload FROM artifact_payloads WHERE artifact_id=?',(artifact_id,))as cursor:row=await cursor.fetchone()
				resolved[key]=safe_json_loads(row[0],row[0])if row else _A
			else:resolved[key]=value
		return resolved
	async def _upstream_failed(self,session_id,step_row):
		deps=safe_json_loads(step_row.get(_V),[])
		if not deps:return _G
		for dep in deps:
			dep_key=str(dep)
			if dep_key.startswith(_Q):
				dep_key=dep_key[1:]
				async with self.db.execute('SELECT source_step_id FROM artifacts WHERE session_id=? AND artifact_id=?',(session_id,dep_key))as cursor:row=await cursor.fetchone()
				if row is _A:continue
				source_step_id=row[0]
				async with self.db.execute(_w,(session_id,source_step_id))as cursor:step_row_db=await cursor.fetchone()
				if step_row_db and step_row_db[0]==StepState.FAILED.value:return _B
				continue
			async with self.db.execute(_w,(session_id,dep_key))as cursor:row=await cursor.fetchone()
			if row and row[0]==StepState.FAILED.value:return _B
		return _G
	async def store_artifact(self,session_id,step_row,result):
		artifact_id=str(step_row[_Z]);payload_text=result.get(_E)
		if payload_text is _A:payload_text=safe_json_dumps(result.get(_H,result))
		payload_text=str(payload_text);record=ArtifactRecord(artifact_id=artifact_id,session_id=session_id,source_step_id=str(step_row[_C]),type='json'if isinstance(result.get(_H,result),(dict,list))else _E,size_bytes=len(payload_text.encode(_M,errors=_X)),checksum=sha256_text(payload_text),preview=payload_text[:200])
		async with self.db_lock:await self.db.execute('INSERT OR REPLACE INTO artifacts VALUES (?, ?, ?, ?, ?, ?, ?, ?)',(record.artifact_id,record.session_id,record.source_step_id,record.type,record.size_bytes,record.created_at,record.checksum,record.preview));await self.db.execute('INSERT OR REPLACE INTO artifact_payloads VALUES (?, ?)',(record.artifact_id,payload_text));await self.db.commit()
		await self._emit_trace('ARTIFACT_WRITTEN',str(step_row[_C]),'Artifact persisted',{_A1:artifact_id,'bytes':record.size_bytes})
	async def send_direct_message(self,user_id,content):
		client=getattr(self,_A2,_A)
		if client is _A:raise RuntimeError(FailureCode.TOOL_EXECUTION_ERROR.value)
		user=await client.fetch_user(int(user_id));await user.send(content[:1900]);return{_H:f"dm:{user_id}",_E:f"dm:{user_id}",_J:0}
	async def execute_discord_eval(self,code,ctx):
		namespace={'client':getattr(self,_A2,_A),'discord':discord,'asyncio':asyncio,'db':self.db,'orchestrator':self,_T:ctx.get(_T),_b:ctx.get(_b),_W:ctx.get(_W),_g:ctx.get(_g,{})}
		try:
			result=eval(code,namespace)
			if asyncio.iscoroutine(result):result=await result
			return{_H:result,_E:str(result)if result is not _A else'',_J:0}
		except SyntaxError:exec(code,namespace);return{_H:_A,_E:'',_J:0}
		except Exception as exc:return{_H:_A,_E:f"{type(exc).__name__}: {exc}",_J:1}
	async def store_memory(self,content):
		D='stored';C='created_at';B='embedding';A='memory_id';embedding=_A
		if self.openai_client is not _A:
			try:resp=await self.openai_client.embeddings.create(model=self.memory_embedding_model,input=content);embedding=resp.data[0].embedding
			except Exception as exc:logger.warning('embedding failed: %s',exc)
		record={A:str(uuid.uuid4()),_U:self.current_session_id,_S:content,B:embedding,C:iso_now()}
		async with self.db_lock:await self.db.execute('INSERT INTO memories VALUES (?, ?, ?, ?, ?)',(record[A],record[_U],record[_S],safe_json_dumps(record[B]),record[C]));await self.db.commit()
		return{_H:D,_E:D,_J:0}
	async def query_memory(self,query,top_k=5):
		C='\\w+';B='results';A='No memories found.';rows=[]
		async with self.db.execute('SELECT content, embedding FROM memories')as cursor:rows=await cursor.fetchall()
		if self.openai_client is not _A:
			try:
				q_resp=await self.openai_client.embeddings.create(model=self.memory_embedding_model,input=query);q_vec=q_resp.data[0].embedding;scored=[]
				for(content,emb_json)in rows:
					vec=safe_json_loads(emb_json,_A)
					if not vec:continue
					score=self._cosine_similarity(q_vec,vec);scored.append((score,content))
				scored.sort(key=lambda x:x[0],reverse=_B);out=_t.join(content for(score,content)in scored[:top_k])if scored else A;return{_H:out,_E:out,_J:0,B:scored[:top_k]}
			except Exception as exc:logger.warning('memory query embedding failed: %s',exc)
		q_words=set(re.findall(C,query.lower()));scored_fallback=[]
		for(content,_emb_json)in rows:
			words=set(re.findall(C,content.lower()))
			if not words:continue
			overlap=len(q_words&words)/max(1,len(q_words|words));scored_fallback.append((overlap,content))
		scored_fallback.sort(key=lambda x:x[0],reverse=_B);out=_t.join(content for(score,content)in scored_fallback[:top_k])if scored_fallback else A;return{_H:out,_E:out,_J:0,B:scored_fallback[:top_k]}
	def _cosine_similarity(self,a,b):
		if not a or not b or len(a)!=len(b):return _N
		dot=sum(x*y for(x,y)in zip(a,b));norm_a=sum(x*x for x in a)**.5;norm_b=sum(y*y for y in b)**.5
		if norm_a==0 or norm_b==0:return _N
		return dot/(norm_a*norm_b)
	def verify_contract(self,result,expected):
		target=result
		if isinstance(result,dict):target=result.get(_H,result.get(_E,result))
		for(rule,required)in(expected or{}).items():
			if rule not in VALIDATION_PRIMITIVES:return _G,f"illegal_rule:{rule}"
			try:
				if not VALIDATION_PRIMITIVES[rule](target,required):return _G,f"failed:{rule}:{required}"
			except Exception as exc:return _G,f"validation_error:{rule}:{exc}"
		return _B,'ok'
	async def rehydrate_session(self,session_id):
		self.current_session_id=session_id
		async with self.db_lock:
			async with self.db.execute('SELECT plan_json, status FROM sessions WHERE session_id=?',(session_id,))as cursor:row=await cursor.fetchone()
			if row is _A:raise RuntimeError('session_not_found')
			plan_json,status=row
			if status not in{'active',_h,'paused'}:return
		steps=await self._fetch_session_steps(session_id)
		for step in steps:
			if step[_F]==StepState.RUNNING.value:await self._update_step(session_id,step[_C],{_F:StepState.PENDING.value,_A3:_A})
			if step[_F]==StepState.READY.value:await self._update_step(session_id,step[_C],{_F:StepState.PENDING.value})
			if step[_F]==StepState.SUCCEEDED.value:
				artifact_id=step[_Z]
				async with self.db.execute('SELECT 1 FROM artifact_payloads WHERE artifact_id=?',(artifact_id,))as cursor:art=await cursor.fetchone()
				if art is _A:await self._update_step(session_id,step[_C],{_F:StepState.PENDING.value,_c:'artifact_missing'});await self._emit_trace('ARTIFACT_MISSING',step[_C],'Artifact missing during resume',{_A1:artifact_id})
		marker=sha256_text(safe_json_dumps({_U:session_id,'plan':plan_json,'ts':iso_now()}));await self._update_session(session_id,{'resume_checkpoint_marker':marker})
	async def run_session(self,session_id,notify_channel=_A,ctx=_A):
		C='no_steps';B='error';A='finished_at';await self.initialize();self.current_session_id=session_id;await self.rehydrate_session(session_id);ctx=ctx or{};await self._update_session(session_id,{_R:_h});await self._emit_trace('SESSION_RUNNING',_A,'Session execution started',{_U:session_id})
		while _B:
			steps=await self._fetch_session_steps(session_id)
			if not steps:await self._update_session(session_id,{_R:_Y,B:C});raise RuntimeError(C)
			progress=_G;any_pending=_G;step_ids={s[_C]for s in steps};blocked_now=[]
			for step in steps:
				if step[_F]in{StepState.SUCCEEDED.value,StepState.FAILED.value,StepState.SKIPPED.value}:continue
				any_pending=_B
				if await self._upstream_failed(session_id,step):blocked_now.append(step);continue
			for step in blocked_now:await self._update_step(session_id,step[_C],{_F:StepState.BLOCKED.value,A:iso_now(),_c:FailureCode.ERR_DEPENDENCY_DEAD.value});await self._emit_trace('STEP_BLOCKED',step[_C],'Upstream dependency failed',{_f:FailureCode.ERR_DEPENDENCY_DEAD.value})
			steps=await self._fetch_session_steps(session_id);ready_steps=[]
			for s in steps:
				if s[_F]not in{StepState.PENDING.value,StepState.READY.value}:continue
				if await self.dependencies_met(session_id,s):ready_steps.append(s)
			for step in ready_steps:
				if step[_P]:await self._update_step(session_id,step[_C],{_F:StepState.SKIPPED.value,A:iso_now()});await self._emit_trace('DEDUP_REWRITE',step[_C],'Duplicate step skipped',{_P:step[_P]})
			ready_steps=[s for s in ready_steps if not s[_P]]
			if not ready_steps:
				terminal={StepState.SUCCEEDED.value,StepState.FAILED.value,StepState.SKIPPED.value,StepState.BLOCKED.value}
				if all(s[_F]in terminal for s in steps):break
				if any_pending:await asyncio.sleep(.1);continue
				await self._update_session(session_id,{_R:_Y,B:FailureCode.DEADLOCK.value});raise RuntimeError(FailureCode.DEADLOCK.value)
			async def run_one(step_row):
				G='error_history';F='retry_history';E='STEP_FAILED';D='max_retries';C='attempt';nonlocal progress
				if int(step_row[C])>=int(step_row[D]):await self._update_step(session_id,step_row[_C],{_F:StepState.FAILED.value,_c:FailureCode.MAX_RETRIES_EXCEEDED.value,A:iso_now()});await self._emit_trace(E,step_row[_C],'Max retries exceeded',{_f:FailureCode.MAX_RETRIES_EXCEEDED.value});progress=_B;return
				await self._update_step(session_id,step_row[_C],{_F:StepState.RUNNING.value,_A3:iso_now()});await self._emit_trace('STEP_STARTED',step_row[_C],'Execution started',{_I:step_row[_I]})
				try:
					resolved=await self.resolve_inputs(session_id,step_row)
					if len(str(resolved))>MAX_INPUT_SIZE:raise RuntimeError(FailureCode.ERR_RESOURCE_LIMIT.value)
					await self._update_step(session_id,step_row[_C],{_g:safe_json_dumps(resolved)});result=await self.executor.execute(step_row,resolved,ctx={_U:session_id,_T:ctx.get(_T),_b:notify_channel,_W:ctx.get(_W),_g:resolved});valid,reason=self.verify_contract(result,safe_json_loads(step_row.get(_a),{}))
					if not valid:raise RuntimeError(f"{FailureCode.VALIDATION_FAILED.value}:{reason}")
					await self.store_artifact(session_id,step_row,result);await self._update_step(session_id,step_row[_C],{_F:StepState.SUCCEEDED.value,A:iso_now(),_c:_A,'actual_cost':float(step_row.get('cost_hint',_N))});await self._emit_trace('STEP_SUCCEEDED',step_row[_C],'Step completed',{'result_preview':str(result)[:250]})
				except Exception as exc:error_text=f"{type(exc).__name__}:{exc}";new_attempt=int(step_row[C])+1;retry_history=safe_json_loads(step_row.get(F),[]);error_history=safe_json_loads(step_row.get(G),[]);snapshot={C:new_attempt,B:error_text,'timestamp':iso_now(),_F:step_row[_F]};retry_history.append(snapshot);error_history.append(snapshot);new_state=StepState.PENDING.value if new_attempt<int(step_row[D])else StepState.FAILED.value;await self._update_step(session_id,step_row[_C],{_F:new_state,C:new_attempt,F:safe_json_dumps(retry_history),G:safe_json_dumps(error_history),_c:error_text,A:iso_now()});await self._emit_trace(E,step_row[_C],'Step failed',{B:error_text})
				progress=_B
			await asyncio.gather(*(run_one(step)for step in ready_steps))
			if not progress:await asyncio.sleep(.1)
		final_steps=await self._fetch_session_steps(session_id);succeeded=len([s for s in final_steps if s[_F]==StepState.SUCCEEDED.value]);failed=len([s for s in final_steps if s[_F]==StepState.FAILED.value]);skipped=len([s for s in final_steps if s[_F]==StepState.SKIPPED.value]);blocked=len([s for s in final_steps if s[_F]==StepState.BLOCKED.value]);final_status='completed'if failed==0 else _Y;await self._update_session(session_id,{_R:final_status,'steps_completed':succeeded,'steps_failed':failed});await self._emit_trace('SESSION_COMPLETED',_A,'Session finished',{_U:session_id,_x:succeeded,_Y:failed,_z:skipped,_y:blocked})
		if notify_channel is not _A:
			try:await notify_channel.send(f"Claw V12 session {final_status}: `{session_id}`")
			except Exception:pass
		return{_U:session_id,_R:final_status}
class ClawPlanner:
	def __init__(self,client):self.client=client
	async def build_plan(self,goal):
		A='role'
		if self.client is _A:raise RuntimeError('OPENAI client unavailable')
		prompt=f"Return only JSON. Build a strict DAG plan for the goal. Supported ops: PARSE_JSON, FORMAT_TEXT, FETCH_WEB_CONTENT, PERFORM_HTTP_REQUEST, EXECUTE_PYTHON_CODE, STORE_MEMORY, QUERY_MEMORY, SEND_DIRECT_MESSAGE, CREATE_DISCORD_CHANNEL, DELETE_DISCORD_CHANNEL, MANAGE_ROLES, EXECUTE_DISCORD_EVAL. Each step must include: step_id, op, args, inputs, output_key, dependencies, expected. Goal: {goal}";resp=await self.client.chat.completions.create(model=os.getenv('CLAW_PLANNER_MODEL','gpt-4o-mini'),messages=[{A:'system',_S:'You are a DAG compiler. Return only a strict JSON object.'},{A:_W,_S:prompt}],response_format={'type':'json_object'});plan=json.loads(resp.choices[0].message.content)
		if _K not in plan:plan[_K]=str(uuid.uuid4())
		if _L not in plan:plan[_L]=[]
		return plan
class ClawBot(discord.Client):
	def __init__(self,orchestrator,planner):intents=discord.Intents.default();intents.message_content=_B;intents.guilds=_B;intents.members=_B;intents.dm_messages=_B;super().__init__(intents=intents);self.orchestrator=orchestrator;self.planner=planner;self.session_tasks=set()
	async def setup_hook(self):await self.orchestrator.initialize();self.orchestrator.discord_client=self;logger.info('Claw V12 online')
	async def on_message(self,message):
		A='!claw '
		if message.author.bot:return
		content=message.content.strip()
		if not content.startswith(A):return
		goal=content[len(A):].strip()
		if not goal:return
		async with message.channel.typing():
			try:plan=await self.planner.build_plan(goal);session_id=await self.orchestrator.create_session(plan,source_user_id=str(message.author.id),source_channel_id=str(message.channel.id));task=asyncio.create_task(self.orchestrator.run_session(session_id,notify_channel=message.channel,ctx={_T:message.guild,_W:message.author,_b:message.channel}));self.session_tasks.add(task);task.add_done_callback(self.session_tasks.discard);await message.channel.send(f"Session queued: `{session_id}`")
			except Exception as exc:await message.channel.send(f"Plan rejected or execution failed to start: `{exc}`")
async def main():
	token=os.getenv('DISCORD_BOT_TOKEN','');openai_key=os.getenv('OPENAI_API_KEY','')
	if not token:raise RuntimeError('DISCORD_BOT_TOKEN is required')
	client=AsyncOpenAI(api_key=openai_key)if AsyncOpenAI is not _A and openai_key else _A;orchestrator=Orchestrator(openai_client=client);planner=ClawPlanner(client);bot=ClawBot(orchestrator,planner);await bot.start(token)
if __name__=='__main__':asyncio.run(main())
