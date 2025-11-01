"""
Event System for AgentOps
Integrates Redis (caching/state) and NATS (messaging)
"""
import os
import json
import asyncio
from typing import Dict, Any, Optional, Callable, List
from datetime import datetime, timedelta
import redis.asyncio as redis
import nats
from nats.aio.client import Client as NATS

class EventSystem:
    """
    Unified event system combining Redis and NATS
    
    Redis: Fast caching, state management, distributed locks
    NATS: Pub/sub messaging, agent communication
    """
    
    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self.nats_client: Optional[NATS] = None
        self.subscriptions: Dict[str, Any] = {}
        
        # Configuration
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("REDIS_PORT", "6379"))
        self.redis_password = os.getenv("REDIS_PASSWORD")
        self.redis_db = int(os.getenv("REDIS_DB", "0"))
        
        self.nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
        self.nats_credentials = os.getenv("NATS_CREDENTIALS")
    
    async def connect(self):
        """Connect to Redis and NATS"""
        # Connect to Redis
        try:
            self.redis_client = await asyncio.wait_for(
                redis.from_url(
                    f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}",
                    password=self.redis_password,
                    encoding="utf-8",
                    decode_responses=True,
                    socket_connect_timeout=5
                ),
                timeout=5.0
            )
            await asyncio.wait_for(self.redis_client.ping(), timeout=2.0)
            print(f"✅ Connected to Redis at {self.redis_host}:{self.redis_port}")
        except (asyncio.TimeoutError, Exception) as e:
            print(f"⚠️  Redis connection failed: {e}")
            self.redis_client = None
        
        # Connect to NATS
        try:
            self.nats_client = await asyncio.wait_for(
                nats.connect(self.nats_url),
                timeout=5.0
            )
            print(f"✅ Connected to NATS at {self.nats_url}")
        except (asyncio.TimeoutError, Exception) as e:
            print(f"⚠️  NATS connection failed: {e}")
            self.nats_client = None
    
    async def disconnect(self):
        """Disconnect from Redis and NATS"""
        if self.redis_client:
            await self.redis_client.close()
        
        if self.nats_client:
            await self.nats_client.close()
    
    # ==================== Redis Operations ====================
    
    async def cache_set(self, key: str, value: Any, ttl: int = 3600):
        """Set a value in Redis cache with TTL"""
        if not self.redis_client:
            return False
        
        try:
            if isinstance(value, (dict, list)):
                value = json.dumps(value)
            await self.redis_client.setex(key, ttl, value)
            return True
        except Exception as e:
            print(f"Redis cache_set error: {e}")
            return False
    
    async def cache_get(self, key: str) -> Optional[Any]:
        """Get a value from Redis cache"""
        if not self.redis_client:
            return None
        
        try:
            value = await self.redis_client.get(key)
            if value:
                try:
                    return json.loads(value)
                except json.JSONDecodeError:
                    return value
            return None
        except Exception as e:
            print(f"Redis cache_get error: {e}")
            return None
    
    async def cache_delete(self, key: str):
        """Delete a key from Redis cache"""
        if not self.redis_client:
            return False
        
        try:
            await self.redis_client.delete(key)
            return True
        except Exception as e:
            print(f"Redis cache_delete error: {e}")
            return False
    
    async def acquire_lock(self, lock_name: str, timeout: int = 10) -> bool:
        """
        Acquire a distributed lock
        Used to prevent multiple agents from claiming the same task
        """
        if not self.redis_client:
            return True  # If Redis is down, allow operation
        
        try:
            # Try to set lock with NX (only if not exists) and EX (expiry)
            result = await self.redis_client.set(
                f"lock:{lock_name}",
                "locked",
                nx=True,
                ex=timeout
            )
            return result is not None
        except Exception as e:
            print(f"Redis acquire_lock error: {e}")
            return True  # Fail open
    
    async def release_lock(self, lock_name: str):
        """Release a distributed lock"""
        if not self.redis_client:
            return
        
        try:
            await self.redis_client.delete(f"lock:{lock_name}")
        except Exception as e:
            print(f"Redis release_lock error: {e}")
    
    async def increment_counter(self, key: str, amount: int = 1) -> int:
        """Increment a counter (for rate limiting, stats)"""
        if not self.redis_client:
            return 0
        
        try:
            return await self.redis_client.incrby(key, amount)
        except Exception as e:
            print(f"Redis increment_counter error: {e}")
            return 0
    
    async def set_agent_status(self, agent_name: str, status: Dict[str, Any]):
        """Cache agent status in Redis"""
        await self.cache_set(f"agent:status:{agent_name}", status, ttl=300)  # 5 min TTL
    
    async def get_agent_status(self, agent_name: str) -> Optional[Dict[str, Any]]:
        """Get cached agent status"""
        return await self.cache_get(f"agent:status:{agent_name}")
    
    async def get_all_agent_statuses(self) -> List[Dict[str, Any]]:
        """Get all cached agent statuses"""
        if not self.redis_client:
            return []
        
        try:
            keys = await self.redis_client.keys("agent:status:*")
            statuses = []
            for key in keys:
                status = await self.cache_get(key)
                if status:
                    statuses.append(status)
            return statuses
        except Exception as e:
            print(f"Redis get_all_agent_statuses error: {e}")
            return []
    
    # ==================== NATS Operations ====================
    
    async def publish(self, subject: str, message: Dict[str, Any]):
        """Publish a message to NATS"""
        if not self.nats_client:
            print(f"⚠️  NATS not connected, skipping publish to {subject}")
            return False
        
        try:
            await self.nats_client.publish(
                subject,
                json.dumps(message).encode()
            )
            return True
        except Exception as e:
            print(f"NATS publish error: {e}")
            return False
    
    async def subscribe(self, subject: str, callback: Callable):
        """Subscribe to a NATS subject"""
        if not self.nats_client:
            print(f"⚠️  NATS not connected, skipping subscribe to {subject}")
            return None
        
        try:
            async def message_handler(msg):
                try:
                    data = json.loads(msg.data.decode())
                    await callback(data)
                except Exception as e:
                    print(f"Error in message handler: {e}")
            
            sub = await self.nats_client.subscribe(subject, cb=message_handler)
            self.subscriptions[subject] = sub
            return sub
        except Exception as e:
            print(f"NATS subscribe error: {e}")
            return None
    
    async def request(self, subject: str, message: Dict[str, Any], timeout: float = 5.0) -> Optional[Dict[str, Any]]:
        """Send a request and wait for response (request/reply pattern)"""
        if not self.nats_client:
            return None
        
        try:
            response = await self.nats_client.request(
                subject,
                json.dumps(message).encode(),
                timeout=timeout
            )
            return json.loads(response.data.decode())
        except asyncio.TimeoutError:
            print(f"NATS request timeout for {subject}")
            return None
        except Exception as e:
            print(f"NATS request error: {e}")
            return None
    
    # ==================== High-Level Event Operations ====================
    
    async def emit_task_created(self, task: Dict[str, Any]):
        """Emit task created event"""
        await self.publish("tasks.created", {
            "event": "task_created",
            "task_id": str(task["id"]),
            "title": task["title"],
            "assigned_to": task.get("assigned_to"),
            "priority": task.get("priority"),
            "timestamp": datetime.utcnow().isoformat()
        })
    
    async def emit_task_updated(self, task_id: str, updates: Dict[str, Any]):
        """Emit task updated event"""
        await self.publish("tasks.updated", {
            "event": "task_updated",
            "task_id": task_id,
            "updates": updates,
            "timestamp": datetime.utcnow().isoformat()
        })
    
    async def emit_task_completed(self, task_id: str, result: Dict[str, Any]):
        """Emit task completed event"""
        await self.publish("tasks.completed", {
            "event": "task_completed",
            "task_id": task_id,
            "result": result,
            "timestamp": datetime.utcnow().isoformat()
        })
    
    async def emit_agent_status_changed(self, agent_name: str, status: str, metadata: Dict[str, Any] = None):
        """Emit agent status changed event"""
        status_data = {
            "agent_name": agent_name,
            "status": status,
            "timestamp": datetime.utcnow().isoformat()
        }
        if metadata:
            status_data["metadata"] = metadata
        
        # Cache in Redis
        await self.set_agent_status(agent_name, status_data)
        
        # Publish to NATS
        await self.publish(f"agents.{agent_name}.status", {
            "event": "agent_status_changed",
            **status_data
        })

# Global event system instance
event_system = EventSystem()
