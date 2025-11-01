"""
AgentOps API - FastAPI server for autonomous agent system
"""
import os
import json
from datetime import datetime
from typing import List, Optional, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ConfigDict
from uuid import UUID
import psycopg
from psycopg.rows import dict_row
import nats
from nats.aio.client import Client as NATS

from app.events import event_system


# Configuration
DATABASE_URL = os.getenv("DB_HOST", "")
DATABASE_PASSWORD = os.getenv("DB_PASSWORD", "")
NATS_URL = os.getenv("NATS_URL", "")
NATS_CREDENTIALS = os.getenv("NATS_CREDENTIALS", "")

# Global NATS client
nc: Optional[NATS] = None


# Pydantic Models
class ProjectCreate(BaseModel):
    name: str
    description: Optional[str] = None
    priority: int = Field(default=50, ge=0, le=100)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class ProjectUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    priority: Optional[int] = Field(default=None, ge=0, le=100)
    metadata: Optional[Dict[str, Any]] = None


class Project(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    name: str
    description: Optional[str]
    status: str
    priority: int
    created_at: datetime
    updated_at: datetime
    completed_at: Optional[datetime]
    metadata: Dict[str, Any]


class TaskCreate(BaseModel):
    project_id: Optional[str] = None
    title: str
    description: Optional[str] = None
    priority: int = Field(default=50, ge=0, le=100)
    assigned_to: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class TaskUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    assigned_to: Optional[str] = None
    priority: Optional[int] = Field(default=None, ge=0, le=100)
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


class Task(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    project_id: Optional[UUID]
    title: str
    description: Optional[str]
    status: str
    assigned_to: Optional[str]
    priority: int
    created_at: datetime
    updated_at: datetime
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    result: Optional[Dict[str, Any]]
    error: Optional[str]
    metadata: Dict[str, Any]


class TaskUpdateCreate(BaseModel):
    task_id: str
    agent_name: str
    update_type: str
    message: str
    metadata: Dict[str, Any] = Field(default_factory=dict)


class TaskUpdateResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    task_id: UUID
    agent_name: str
    update_type: str
    message: str
    created_at: datetime
    metadata: Dict[str, Any]


class AgentRegister(BaseModel):
    name: str
    type: str
    capabilities: List[str] = Field(default_factory=list)
    created_by: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


class Agent(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    name: str
    type: str
    status: str
    capabilities: List[str]
    created_by: Optional[str]
    created_at: datetime
    last_active_at: datetime
    metadata: Dict[str, Any]


class EventCreate(BaseModel):
    event_type: str
    source_agent: Optional[str] = None
    target_agent: Optional[str] = None
    payload: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class Event(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: UUID
    event_type: str
    source_agent: Optional[str]
    target_agent: Optional[str]
    payload: Dict[str, Any]
    created_at: datetime
    processed_at: Optional[datetime]
    metadata: Dict[str, Any]


# Database connection
def get_db_connection():
    """Get database connection with proper credentials"""
    # Parse DATABASE_URL to extract components
    # Format: postgres://host:port/dbname?sslmode=require
    conn_str = DATABASE_URL.replace("postgres://", "")
    if "?" in conn_str:
        conn_str = conn_str.split("?")[0]
    
    parts = conn_str.split("/")
    host_port = parts[0].split(":")
    host = host_port[0]
    port = host_port[1] if len(host_port) > 1 else "5432"
    dbname = parts[1] if len(parts) > 1 else "prod-agentops-db"
    
    # Get IAM user ID from environment or use default
    user_id = os.getenv("IAM_USER_ID", "7f9d2892-bd2b-48f9-9f37-67a737607304")
    
    conn = psycopg.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user_id,
        password=DATABASE_PASSWORD,
        sslmode="require",
        row_factory=dict_row
    )
    return conn


# Lifespan context manager for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan - startup and shutdown"""
    global nc
    
    # Startup
    print("🚀 Starting AgentOps API...")
    
    # Connect event system (Redis + NATS)
    await event_system.connect()
    
    # Test database connection
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) as count FROM agents")
            result = cur.fetchone()
            print(f"✅ Database connected: {result['count']} agents registered")
        conn.close()
    except Exception as e:
        print(f"⚠️  Database connection test failed: {e}")
    
    yield
    
    # Shutdown
    print("👋 Shutting down AgentOps API...")
    await event_system.disconnect()
    if nc:
        await nc.close()


# Create FastAPI app
app = FastAPI(
    title="AgentOps API",
    description="REST API for autonomous agent system",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Health check
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "nats_connected": nc is not None and nc.is_connected if nc else False
    }


# Projects endpoints
@app.post("/projects", response_model=Project)
async def create_project(project: ProjectCreate):
    """Create a new project"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO projects (name, description, priority, metadata)
                VALUES (%s, %s, %s, %s)
                RETURNING *
                """,
                (project.name, project.description, project.priority, json.dumps(project.metadata))
            )
            result = cur.fetchone()
            conn.commit()
            
            # Publish event to NATS
            if nc:
                await nc.publish(
                    "agentops.projects.created",
                    json.dumps({"project_id": result["id"], "name": result["name"]}).encode()
                )
            
            return result
    finally:
        conn.close()


@app.get("/projects", response_model=List[Project])
async def list_projects(
    status: Optional[str] = Query(None),
    limit: int = Query(100, le=1000)
):
    """List all projects"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            if status:
                cur.execute(
                    "SELECT * FROM projects WHERE status = %s ORDER BY priority DESC, created_at DESC LIMIT %s",
                    (status, limit)
                )
            else:
                cur.execute(
                    "SELECT * FROM projects ORDER BY priority DESC, created_at DESC LIMIT %s",
                    (limit,)
                )
            return cur.fetchall()
    finally:
        conn.close()


@app.get("/projects/{project_id}", response_model=Project)
async def get_project(project_id: str):
    """Get a specific project"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
            result = cur.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail="Project not found")
            return result
    finally:
        conn.close()


@app.patch("/projects/{project_id}", response_model=Project)
async def update_project(project_id: str, project: ProjectUpdate):
    """Update a project"""
    conn = get_db_connection()
    try:
        updates = []
        values = []
        
        if project.name is not None:
            updates.append("name = %s")
            values.append(project.name)
        if project.description is not None:
            updates.append("description = %s")
            values.append(project.description)
        if project.status is not None:
            updates.append("status = %s")
            values.append(project.status)
            if project.status == "completed":
                updates.append("completed_at = NOW()")
        if project.priority is not None:
            updates.append("priority = %s")
            values.append(project.priority)
        if project.metadata is not None:
            updates.append("metadata = %s")
            values.append(json.dumps(project.metadata))
        
        if not updates:
            raise HTTPException(status_code=400, detail="No fields to update")
        
        values.append(project_id)
        
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE projects SET {', '.join(updates)} WHERE id = %s RETURNING *",
                values
            )
            result = cur.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail="Project not found")
            conn.commit()
            return result
    finally:
        conn.close()


# Tasks endpoints
@app.post("/tasks", response_model=Task)
async def create_task(task: TaskCreate):
    """Create a new task"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tasks (project_id, title, description, priority, assigned_to, metadata)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING *
                """,
                (task.project_id, task.title, task.description, task.priority, task.assigned_to, json.dumps(task.metadata))
            )
            result = cur.fetchone()
            conn.commit()
            
            # Publish event to NATS
            if nc:
                await nc.publish(
                    "agentops.tasks.created",
                    json.dumps({
                        "task_id": result["id"],
                        "title": result["title"],
                        "assigned_to": result["assigned_to"]
                    }).encode()
                )
            
            return result
    finally:
        conn.close()


@app.get("/tasks", response_model=List[Task])
async def list_tasks(
    status: Optional[str] = Query(None),
    assigned_to: Optional[str] = Query(None),
    project_id: Optional[str] = Query(None),
    limit: int = Query(100, le=1000)
):
    """List tasks with optional filters"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            query = "SELECT * FROM tasks WHERE 1=1"
            params = []
            
            if status:
                query += " AND status = %s"
                params.append(status)
            if assigned_to:
                query += " AND assigned_to = %s"
                params.append(assigned_to)
            if project_id:
                query += " AND project_id = %s"
                params.append(project_id)
            
            query += " ORDER BY priority DESC, created_at DESC LIMIT %s"
            params.append(limit)
            
            cur.execute(query, params)
            return cur.fetchall()
    finally:
        conn.close()


@app.get("/tasks/{task_id}", response_model=Task)
async def get_task(task_id: str):
    """Get a specific task"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM tasks WHERE id = %s", (task_id,))
            result = cur.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail="Task not found")
            return result
    finally:
        conn.close()


@app.patch("/tasks/{task_id}", response_model=Task)
async def update_task(task_id: str, task: TaskUpdate):
    """Update a task"""
    conn = get_db_connection()
    try:
        updates = []
        values = []
        
        if task.title is not None:
            updates.append("title = %s")
            values.append(task.title)
        if task.description is not None:
            updates.append("description = %s")
            values.append(task.description)
        if task.status is not None:
            updates.append("status = %s")
            values.append(task.status)
            if task.status == "in_progress" and not updates.__contains__("started_at = NOW()"):
                updates.append("started_at = NOW()")
            elif task.status == "completed":
                updates.append("completed_at = NOW()")
        if task.assigned_to is not None:
            updates.append("assigned_to = %s")
            values.append(task.assigned_to)
        if task.priority is not None:
            updates.append("priority = %s")
            values.append(task.priority)
        if task.result is not None:
            updates.append("result = %s")
            values.append(json.dumps(task.result))
        if task.error is not None:
            updates.append("error = %s")
            values.append(task.error)
        if task.metadata is not None:
            updates.append("metadata = %s")
            values.append(json.dumps(task.metadata))
        
        if not updates:
            raise HTTPException(status_code=400, detail="No fields to update")
        
        values.append(task_id)
        
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE tasks SET {', '.join(updates)} WHERE id = %s RETURNING *",
                values
            )
            result = cur.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail="Task not found")
            conn.commit()
            
            # Publish event to NATS
            if nc and task.status:
                await nc.publish(
                    f"agentops.tasks.{task.status}",
                    json.dumps({"task_id": task_id, "status": task.status}).encode()
                )
            
            return result
    finally:
        conn.close()


# Task Updates endpoints
@app.post("/task-updates", response_model=TaskUpdateResponse)
async def create_task_update(update: TaskUpdateCreate):
    """Create a task update"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO task_updates (task_id, agent_name, update_type, message, metadata)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING *
                """,
                (update.task_id, update.agent_name, update.update_type, update.message, json.dumps(update.metadata))
            )
            result = cur.fetchone()
            conn.commit()
            
            # Publish event to NATS
            if nc:
                await nc.publish(
                    f"agentops.updates.{update.update_type}",
                    json.dumps({
                        "task_id": update.task_id,
                        "agent": update.agent_name,
                        "message": update.message
                    }).encode()
                )
            
            return result
    finally:
        conn.close()


@app.get("/task-updates/{task_id}", response_model=List[TaskUpdateResponse])
async def get_task_updates(task_id: str):
    """Get all updates for a task"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM task_updates WHERE task_id = %s ORDER BY created_at ASC",
                (task_id,)
            )
            return cur.fetchall()
    finally:
        conn.close()


# Agents endpoints
@app.post("/agents", response_model=Agent)
async def register_agent(agent: AgentRegister):
    """Register a new agent"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO agents (name, type, capabilities, created_by, metadata)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (name) DO UPDATE
                SET last_active_at = NOW()
                RETURNING *
                """,
                (agent.name, agent.type, json.dumps(agent.capabilities), agent.created_by, json.dumps(agent.metadata))
            )
            result = cur.fetchone()
            conn.commit()
            
            # Publish event to NATS
            if nc:
                await nc.publish(
                    "agentops.agents.registered",
                    json.dumps({"agent_name": result["name"], "type": result["type"]}).encode()
                )
            
            return result
    finally:
        conn.close()


@app.get("/agents", response_model=List[Agent])
async def list_agents(
    status: Optional[str] = Query(None),
    type: Optional[str] = Query(None)
):
    """List all agents"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            query = "SELECT * FROM agents WHERE 1=1"
            params = []
            
            if status:
                query += " AND status = %s"
                params.append(status)
            if type:
                query += " AND type = %s"
                params.append(type)
            
            query += " ORDER BY last_active_at DESC"
            
            cur.execute(query, params)
            return cur.fetchall()
    finally:
        conn.close()


@app.get("/agents/{agent_name}", response_model=Agent)
async def get_agent(agent_name: str):
    """Get a specific agent"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM agents WHERE name = %s", (agent_name,))
            result = cur.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail="Agent not found")
            return result
    finally:
        conn.close()


@app.patch("/agents/{agent_name}/status")
async def update_agent_status(agent_name: str, status: str):
    """Update agent status"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE agents
                SET status = %s, last_active_at = NOW()
                WHERE name = %s
                RETURNING *
                """,
                (status, agent_name)
            )
            result = cur.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail="Agent not found")
            conn.commit()
            return result
    finally:
        conn.close()


# Events endpoints
@app.post("/events", response_model=Event)
async def create_event(event: EventCreate):
    """Create an event"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO events (event_type, source_agent, target_agent, payload, metadata)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING *
                """,
                (event.event_type, event.source_agent, event.target_agent, json.dumps(event.payload), json.dumps(event.metadata))
            )
            result = cur.fetchone()
            conn.commit()
            
            # Publish event to NATS
            if nc:
                await nc.publish(
                    f"agentops.events.{event.event_type}",
                    json.dumps(result, default=str).encode()
                )
            
            return result
    finally:
        conn.close()


@app.get("/events", response_model=List[Event])
async def list_events(
    event_type: Optional[str] = Query(None),
    source_agent: Optional[str] = Query(None),
    target_agent: Optional[str] = Query(None),
    limit: int = Query(100, le=1000)
):
    """List events"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            query = "SELECT * FROM events WHERE 1=1"
            params = []
            
            if event_type:
                query += " AND event_type = %s"
                params.append(event_type)
            if source_agent:
                query += " AND source_agent = %s"
                params.append(source_agent)
            if target_agent:
                query += " AND target_agent = %s"
                params.append(target_agent)
            
            query += " ORDER BY created_at DESC LIMIT %s"
            params.append(limit)
            
            cur.execute(query, params)
            return cur.fetchall()
    finally:
        conn.close()


# Statistics endpoints
@app.get("/stats/overview")
async def get_stats_overview():
    """Get system overview statistics"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            # Get project stats
            cur.execute("SELECT * FROM project_progress")
            projects = cur.fetchall()
            
            # Get agent stats
            cur.execute("SELECT * FROM agent_activity")
            agents = cur.fetchall()
            
            # Get active tasks
            cur.execute("SELECT COUNT(*) as count FROM tasks WHERE status IN ('pending', 'in_progress')")
            active_tasks = cur.fetchone()["count"]
            
            return {
                "projects": projects,
                "agents": agents,
                "active_tasks": active_tasks,
                "timestamp": datetime.utcnow().isoformat()
            }
    finally:
        conn.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
