import os
import asyncio
import logging
import httpx
from fastapi import FastAPI, Request, status, Response, BackgroundTasks
from cloudevents.http import from_http
from pydantic_settings import BaseSettings
from logfmter import Logfmter

class Settings(BaseSettings):
    host: str = "0.0.0.0"
    service_port: int = 8080
    log_level: str = "INFO"
    daq_id: str = "default"

    @property
    def storage_delete_url(self) -> str:
        """Dynamically build the storage vault delete URL"""
        return f"http://dataset-storage.{self.daq_id}-system.svc.cluster.local:80/delete/"

    class Config:
        env_prefix = "PNNL_STS_"
        case_sensitive = False

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger("pnnl-sts")

config = Settings()
L.setLevel(config.log_level.upper())

app = FastAPI()

# ---> UPDATED: The hardcoded path required by the STS binary <---
STS_OUTBOX_DIR = "/datasets/beacons/sts"

@app.on_event("startup")
async def startup():
    os.makedirs(STS_OUTBOX_DIR, exist_ok=True)
    L.info("PNNL STS Mock Service initialized.", extra={"outbox": STS_OUTBOX_DIR})

async def process_sts_transfer(filename: str, download_url: str):
    """Mocks downloading a b1 file, running the STS binary, and cleaning up."""
    local_path = os.path.join(STS_OUTBOX_DIR, filename)
    
    try:
        # 1. Download the 'b1' file into the designated STS outbox folder
        L.info("Staging file into STS outbox...", extra={"processed_file": filename, "path": local_path})
        async with httpx.AsyncClient() as client:
            resp = await client.get(download_url, timeout=30.0)
            resp.raise_for_status()
            with open(local_path, "wb") as f:
                f.write(resp.content)

        # 2. MOCK THE PNNL STS BINARY PROCESS
        L.info("Executing mock PNNL STS binary...", extra={"processed_file": filename})
        # In reality, this would be: subprocess.run(["/path/to/sts_binary", local_path])
        await asyncio.sleep(4) 
        L.info("STS Transfer COMPLETE.", extra={"processed_file": filename})

        # 3. Clean up the central Storage PVC so we don't run out of disk space
        delete_url = f"{config.storage_delete_url}{filename}"
        L.info("Sending cleanup request to storage vault...", extra={"processed_file": filename})
        async with httpx.AsyncClient() as client:
            del_resp = await client.delete(delete_url, timeout=10.0)
            del_resp.raise_for_status()
            L.info("Storage PVC cleanup confirmed.")

    except Exception as e:
        L.error("STS processing failed", extra={"processed_file": filename, "reason": str(e)})
    finally:
        # Clean up the local outbox
        if os.path.exists(local_path):
            os.remove(local_path)
            L.debug("Local STS outbox cleaned.", extra={"processed_file": filename})

@app.post("/")
async def handle_event(request: Request, background_tasks: BackgroundTasks):
    try:
        ce = from_http(request.headers, await request.body())
        if ce.get("type") == "envds.dataset.stored":
            data = ce.data
            dataset_id = data.get("dataset_id", "")
            
            # Only transmit 'b1' (Gold/QC'd) datasets to the PNNL
            if ".b1" in dataset_id:
                L.info(f"PNNL STS Service triggered", extra={"dataset": dataset_id})
                background_tasks.add_task(
                    process_sts_transfer, 
                    data["filename"], 
                    data["download_url"]
                )
                
    except Exception as e:
        L.error("Event handling failed", extra={"reason": str(e)})
        
    return Response(status_code=status.HTTP_204_NO_CONTENT)