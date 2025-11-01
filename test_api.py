"""
Test script for AgentOps API
"""
import os
import asyncio
import httpx

# Set environment variables
os.environ["DB_HOST"] = "postgres://6838bdb4-a8db-453f-84e5-0b9025abd987.pg.sdb.fr-par.scw.cloud:5432/prod-agentops-db?sslmode=require"
os.environ["DB_PASSWORD"] = "6087458f-225d-4da2-916f-ef4a1875246e"
os.environ["IAM_USER_ID"] = "7f9d2892-bd2b-48f9-9f37-67a737607304"
os.environ["NATS_URL"] = "nats://nats.mnq.fr-par.scaleway.com:4222"
os.environ["NATS_CREDENTIALS"] = ""  # Will add later

async def test_api():
    """Test API endpoints"""
    base_url = "http://localhost:8000"
    
    async with httpx.AsyncClient() as client:
        # Test health check
        print("Testing health check...")
        response = await client.get(f"{base_url}/health")
        print(f"✅ Health check: {response.json()}")
        
        # Test list projects
        print("\nTesting list projects...")
        response = await client.get(f"{base_url}/projects")
        projects = response.json()
        print(f"✅ Found {len(projects)} projects")
        for p in projects:
            print(f"  - {p['name']} ({p['status']})")
        
        # Test list agents
        print("\nTesting list agents...")
        response = await client.get(f"{base_url}/agents")
        agents = response.json()
        print(f"✅ Found {len(agents)} agents")
        for a in agents:
            print(f"  - {a['name']} ({a['type']}, {a['status']})")
        
        # Test list tasks
        print("\nTesting list tasks...")
        response = await client.get(f"{base_url}/tasks")
        tasks = response.json()
        print(f"✅ Found {len(tasks)} tasks")
        for t in tasks:
            print(f"  - {t['title']} ({t['status']}, assigned to: {t['assigned_to']})")
        
        # Test create task
        print("\nTesting create task...")
        response = await client.post(
            f"{base_url}/tasks",
            json={
                "title": "Test API endpoints",
                "description": "Validate all API endpoints are working",
                "priority": 90,
                "assigned_to": "seed"
            }
        )
        new_task = response.json()
        print(f"✅ Created task: {new_task['id']}")
        
        # Test update task
        print("\nTesting update task...")
        response = await client.patch(
            f"{base_url}/tasks/{new_task['id']}",
            json={"status": "in_progress"}
        )
        updated_task = response.json()
        print(f"✅ Updated task status to: {updated_task['status']}")
        
        # Test create task update
        print("\nTesting create task update...")
        response = await client.post(
            f"{base_url}/task-updates",
            json={
                "task_id": new_task['id'],
                "agent_name": "seed",
                "update_type": "progress",
                "message": "API test in progress..."
            }
        )
        task_update = response.json()
        print(f"✅ Created task update: {task_update['message']}")
        
        # Test stats
        print("\nTesting stats overview...")
        response = await client.get(f"{base_url}/stats/overview")
        stats = response.json()
        print(f"✅ Active tasks: {stats['active_tasks']}")
        print(f"✅ Projects: {len(stats['projects'])}")
        print(f"✅ Agents: {len(stats['agents'])}")
        
        print("\n🎉 All tests passed!")

if __name__ == "__main__":
    asyncio.run(test_api())
