# datasets/dataset-storage/main.py
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
    L.info("Dataset Storage Service initialized.", extra={"storage_dir": config.storage_dir})

async def fire_stored_event(filename: str, dataset_id: str):
    """Fires a Knative CloudEvent telling the cluster a file is ready."""
    attributes = {
        "type": "envds.dataset.stored",
        "source": "envds.dataset-storage",
    }
    data = {
        "filename": filename,
        "dataset_id": dataset_id,
        "download_url": f"http://dataset-storage.{config.daq_id}-system.svc.cluster.local/download/{filename}"
    }
    event = CloudEvent(attributes, data)
    headers, body = to_structured(event)

    try:
        async with httpx.AsyncClient() as client:
            await client.post(config.broker_url, headers=headers, data=body)
            # CHANGE 'filename' to 'saved_file'
            L.info("Event fired successfully", extra={"event_type": attributes["type"], "saved_file": filename})
    except Exception as e:
        L.error("Failed to fire CloudEvent", extra={"reason": str(e)})

@app.post("/upload/")
async def upload_dataset(background_tasks: BackgroundTasks, dataset_id: str, file: UploadFile = File(...)):
    """Receives a NetCDF file, saves it to the PVC, and fires an event."""
    if not file.filename.endswith(".nc"):
        raise HTTPException(status_code=400, detail="Only .nc files are allowed.")

    file_path = os.path.join(config.storage_dir, file.filename)
    
    try:
        with open(file_path, "wb") as f:
            f.write(await file.read())
            
        # CHANGE 'filename' to 'saved_file'
        L.info("File successfully saved to PVC", extra={"saved_file": file.filename, "size_bytes": os.path.getsize(file_path)})
        
        # Fire the event in the background so the uploader gets a fast HTTP 200 response
        background_tasks.add_task(fire_stored_event, file.filename, dataset_id)
        
        return {"status": "success", "filename": file.filename, "message": "File stored and event dispatched."}
    except Exception as e:
        L.error("Failed to save file", extra={"reason": str(e)})
        raise HTTPException(status_code=500, detail="Internal file save error.")

@app.get("/download/{filename}")
async def download_dataset(filename: str):
    """Allows downstream services (like STS or QA) to download the file."""
    file_path = os.path.join(config.storage_dir, filename)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found.")
    return FileResponse(path=file_path, filename=filename, media_type='application/x-netcdf')

@app.delete("/delete/{filename}")
async def delete_dataset(filename: str):
    """Allows STS wrapper to clean up the PVC after successful upload to DOE."""
    file_path = os.path.join(config.storage_dir, filename)
    if os.path.exists(file_path):
        os.remove(file_path)
        L.info("File deleted from PVC", extra={"filename": filename})
        return {"status": "success", "message": "File deleted."}
    raise HTTPException(status_code=404, detail="File not found.")