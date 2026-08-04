import asyncio
import json
import logging
import os
from pathlib import Path

import httpx
from logfmter import Logfmter
from pydantic_settings import BaseSettings
from cloudevents.conversion import to_structured
from envds.sampling.event import SamplingEvent

# Setup standard JSON logging
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger("envops-core")
L.setLevel(logging.INFO)

class Settings(BaseSettings):
    knative_broker: str = "http://kafka-broker-ingress.knative-eventing.svc.cluster.local/default/default"
    sync_interval: int = 300  # Sync every 5 minutes
    daq_id: str = "server"    # The identifier for the central hub

    class Config:
        env_prefix = "ENVOPS_"
        case_sensitive = False

config = Settings()

def load_json_dir(dir_path_str: str) -> list:
    """Scans a directory for JSON files, expands env vars, and returns the parsed list."""
    results = []
    dir_path = Path(dir_path_str)
    
    if dir_path.exists() and dir_path.is_dir():
        for file_path in dir_path.glob("*.json"):
            try:
                with open(file_path, "r") as f:
                    expanded_content = os.path.expandvars(f.read())
                    data = json.loads(expanded_content)
                    if isinstance(data, list):
                        results.extend(data)
                    else:
                        results.append(data)
            except Exception as e:
                L.error(f"Failed to parse {file_path.name}", extra={"reason": str(e)})
    return results

async def send_to_broker(client: httpx.AsyncClient, ce: dict):
    """Packages and sends a CloudEvent to the Knative Broker."""
    try:
        headers, body = to_structured(ce)
        resp = await client.post(config.knative_broker, headers=headers, data=body, timeout=5.0)
        resp.raise_for_status()
    except Exception as e:
        L.error("Broker publish failed", extra={"reason": str(e)})

async def sync_loop():
    """Main GitOps reconciliation loop for organizational metadata."""
    L.info("Starting envops-core GitOps reconciliation loop...")
    
    # Use connection pooling to prevent port exhaustion during bulk syncs
    limits = httpx.Limits(max_keepalive_connections=10, max_connections=20)
    
    async with httpx.AsyncClient(limits=limits) as client:
        while True:
            try:
                # 1. Load the directories (Mounted via Kubernetes ConfigMaps or Volumes)
                projects = load_json_dir("/app/config/projects")
                contacts = load_json_dir("/app/config/contacts")
                allocations = load_json_dir("/app/config/allocations")

                # 2. Publish Projects
                for proj in projects:
                    ce = SamplingEvent.create_definition_registry_update(
                        resource="project",
                        source=f"envds.{config.daq_id}.envops-core",
                        data={"project-definition": proj}
                    )
                    ce["destpath"] = f"envds/{config.daq_id}/project-definition/registry/update"
                    await send_to_broker(client, ce)

                # 3. Publish Contacts
                for contact in contacts:
                    ce = SamplingEvent.create_definition_registry_update(
                        resource="contact",
                        source=f"envds.{config.daq_id}.envops-core",
                        data={"contact-definition": contact}
                    )
                    ce["destpath"] = f"envds/{config.daq_id}/contact-definition/registry/update"
                    await send_to_broker(client, ce)

                # 4. Publish ProjectAllocations
                for alloc in allocations:
                    ce = SamplingEvent.create_definition_registry_update(
                        resource="projectallocation",
                        source=f"envds.{config.daq_id}.envops-core",
                        data={"projectallocation-definition": alloc}
                    )
                    ce["destpath"] = f"envds/{config.daq_id}/projectallocation-definition/registry/update"
                    await send_to_broker(client, ce)

                L.info("GitOps sync completed.", extra={
                    "projects": len(projects),
                    "contacts": len(contacts),
                    "allocations": len(allocations)
                })

            except Exception as e:
                L.error("Sync loop encountered an error", extra={"reason": str(e)})

            await asyncio.sleep(config.sync_interval)

if __name__ == "__main__":
    asyncio.run(sync_loop())