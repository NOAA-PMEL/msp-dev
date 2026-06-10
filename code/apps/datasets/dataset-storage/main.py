import os
import logging
import httpx
from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from cloudevents.http import CloudEvent, to_structured
from pydantic_settings import BaseSettings
from logfmter import Logfmter

class Settings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8000
    log_level: str = "INFO"
    daq_id: str = "default"
    storage_dir: str = "/app/data/storage"
    knative_broker: str = "http://default-broker.envds.svc.cluster.local"

    class Config:
        env_prefix = "DATASET_STORAGE_"
        case_sensitive = False

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger("dataset-storage")

config = Settings()
L.setLevel(config.log_level.upper())
app = FastAPI()

@app.on_event("startup")
async def startup():
    os.makedirs(config.storage_dir, exist_ok=True)
    L.info("Dataset Storage Service initialized.")

async def fire_stored_event(filename: str, dataset_id: str, stage: str):
    """Fires a Knative CloudEvent telling the cluster a file is ready at a specific stage."""
    attributes = {
        "type": "envds.dataset.stored",
        "source": "envds.dataset-storage",
    }
    
    base_url = f"http://dataset-storage.{config.daq_id}-system.svc.cluster.local"
    
    data = {
        "filename": filename,
        "dataset_id": dataset_id,
        "stage": stage,  # <-- NEW: The data stage
        "download_url": f"{base_url}:80/download/{stage}/{filename}", # <-- NEW: Stage in URL
        "delete_url": f"{base_url}:80/delete/{stage}/{filename}"     # <-- NEW: Explicit delete URL
    }
    
    event = CloudEvent(attributes, data)
    headers, body = to_structured(event)

    try:
        async with httpx.AsyncClient() as client:
            await client.post(config.knative_broker, headers=headers, data=body)
            L.info("Event fired successfully", extra={"stage": stage, "saved_file": filename})
    except Exception as e:
        L.error("Failed to fire CloudEvent", extra={"reason": str(e)})

@app.post("/upload/{stage}")
async def upload_dataset(stage: str, background_tasks: BackgroundTasks, dataset_id: str, file: UploadFile = File(...)):
    if not file.filename.endswith(".nc"):
        raise HTTPException(status_code=400, detail="Only .nc files are allowed.")

    # Save into a stage-specific subfolder (e.g., /app/data/storage/raw)
    stage_dir = os.path.join(config.storage_dir, stage)
    os.makedirs(stage_dir, exist_ok=True)
    
    file_path = os.path.join(stage_dir, file.filename)
    try:
        with open(file_path, "wb") as f:
            f.write(await file.read())
            
        L.info(f"File saved to {stage} PVC", extra={"saved_file": file.filename})
        background_tasks.add_task(fire_stored_event, file.filename, dataset_id, stage)
        return {"status": "success", "stage": stage, "filename": file.filename}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal file save error.")

@app.get("/download/{stage}/{filename}")
async def download_dataset(stage: str, filename: str):
    file_path = os.path.join(config.storage_dir, stage, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found.")
    return FileResponse(path=file_path, filename=filename, media_type='application/x-netcdf')

@app.delete("/delete/{stage}/{filename}")
async def delete_dataset(stage: str, filename: str):
    file_path = os.path.join(config.storage_dir, stage, filename)
    if os.path.exists(file_path):
        os.remove(file_path)
        L.info(f"File deleted from {stage} PVC", extra={"filename": filename})
        return {"status": "success"}
    raise HTTPException(status_code=404, detail="File not found.")