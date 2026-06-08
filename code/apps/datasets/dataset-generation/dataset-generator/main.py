# datasets/dataset-generation/dataset-generator/main.py
import asyncio
import logging
import os
import json
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, Request, status, Response
from cloudevents.http import from_http
from logfmter import Logfmter
from pydantic_settings import BaseSettings

from dataset_generator import DatasetGenerator

class Settings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = False
    daq_id: str = "default"
    log_level: str = "INFO"

    class Config:
        env_prefix = "DATASET_GENERATOR_"
        case_sensitive = False

# Setup logging using envds standard Logfmter
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger("dataset-generator")

config = Settings()
L.setLevel(config.log_level.upper())

app = FastAPI()

# In-memory dictionary to hold our definitions
dataset_definitions = {}

# Global reference for our processing pipeline
generator = None

@app.on_event("startup")
async def start_system():
    global generator
    generator = DatasetGenerator(daq_id=config.daq_id)
    L.info("Dataset Generator initialized and starting up.")

    # --- FIX 1: Pre-populate registry from the mounted GitOps folder on boot ---
    definitions_dir = "/app/config/definitions"
    if os.path.exists(definitions_dir):
        for fname in os.listdir(definitions_dir):
            if fname.endswith(".json"):
                path = os.path.join(definitions_dir, fname)
                try:
                    with open(path, "r") as f:
                        def_data = json.load(f)
                        d_id = def_data.get("id")
                        if d_id:
                            dataset_definitions[d_id] = def_data
                            L.info("Pre-loaded DatasetDefinition from disk", extra={"dataset_id": d_id})
                except Exception as os_e:
                    L.error(f"Failed to pre-load definition file {fname}", extra={"reason": str(os_e)})
    L.debug("Startup Registry State", extra={"configs_loaded": list(dataset_definitions.keys())})

@app.on_event("shutdown")
async def shutdown_system():
    global generator
    if generator:
        await generator.close()
        L.info("Dataset Generator HTTP client closed safely.")

@app.get("/")
async def root():
    return {"message": "Hello World from Dataset Generator"}

@app.post("/dataset-definition/registry/update/")
async def dataset_definition_update(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        L.debug("dataset_definition_update: RECEIVED", extra={"ce_type": ce.get("type")})
        
        data = ce.data
        dataset_id = data.get("id")
        
        L.info("Registering DatasetDefinition", extra={"dataset_id": dataset_id})
        dataset_definitions[dataset_id] = data
        
        L.debug("Current Registry", extra={"configs_loaded": list(dataset_definitions.keys())})
        return Response(status_code=status.HTTP_204_NO_CONTENT)
        
    except Exception as e:
        L.error("dataset_definition_update: CRITICAL FAILURE", extra={"reason": str(e)}, exc_info=True)
        return Response(status_code=status.HTTP_204_NO_CONTENT)

@app.post("/dataset/generate/request/")
async def dataset_generate_request(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        data = ce.data
        
        dataset_id = data.get("dataset_id")
        start_str = data.get("start_time")
        end_str = data.get("end_time")
        time_window = data.get("time_window")
        
        # --- FIX 2: If missing from memory cache, check the disk as a fallback ---
        if dataset_id not in dataset_definitions:
            config_path = f"/app/config/definitions/{dataset_id}.json"
            if os.path.exists(config_path):
                try:
                    with open(config_path, "r") as f:
                        dataset_definitions[dataset_id] = json.load(f)
                    L.info("Loaded missing dataset definition via disk fallback", extra={"dataset_id": dataset_id})
                except Exception as read_e:
                    L.error("Failed to read definition file via disk fallback", extra={"dataset_id": dataset_id, "reason": str(read_e)})

        if dataset_id not in dataset_definitions:
            L.error("Cannot generate: Unknown dataset definition", extra={"dataset_id": dataset_id})
            return Response(status_code=status.HTTP_204_NO_CONTENT)

        dataset_config = dataset_definitions[dataset_id]
        
        # PATH A: Explicit Backfill (User provided exact ISO timestamps)
        if start_str and end_str:
            start_time = start_str
            end_time = end_str

        # PATH B: Automated Cron (Trigger sends "auto")
        elif time_window == "auto":
            freq = dataset_config.get("timebase", {}).get("file_frequency", "hourly")
            now = datetime.now(timezone.utc)
            
            if freq == "hourly":
                end_dt = now.replace(minute=0, second=0, microsecond=0)
                start_dt = end_dt - timedelta(hours=1)
            elif freq == "daily":
                end_dt = now.replace(hour=0, minute=0, second=0, microsecond=0)
                start_dt = end_dt - timedelta(days=1)
            elif freq == "5min":
                # 1. Round down to nearest 5-minute boundary
                minute_rounded = now.minute - (now.minute % 5)
                base_dt = now.replace(minute=minute_rounded, second=0, microsecond=0)
                
                # 2. APPLY THE 5 MINUTE DELAY
                end_dt = base_dt - timedelta(minutes=5)
                start_dt = end_dt - timedelta(minutes=5)
            else:
                L.error(f"Unknown file_frequency '{freq}' in {dataset_id}")
                return Response(status_code=status.HTTP_204_NO_CONTENT)
                
            start_time = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            end_time = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            
        else:
            L.error("Invalid request: Must provide start_time/end_time or time_window='auto'")
            return Response(status_code=status.HTTP_204_NO_CONTENT)

        L.info("Dataset Generation Triggered", extra={
            "dataset_id": dataset_id, 
            "start": start_time, 
            "end": end_time
        })
        
        L.debug("Config found. Proceeding to pipeline...", extra={"dataset_id": dataset_id})
        
        # Fire and forget the pipeline task so we don't block the Knative Eventing Broker
        asyncio.create_task(generator.generate_dataset(dataset_config, start_time, end_time))
            
        return Response(status_code=status.HTTP_204_NO_CONTENT)
        
    except Exception as e:
        L.error("dataset_generate_request: FAILURE", extra={"reason": str(e)}, exc_info=True)
        return Response(status_code=status.HTTP_204_NO_CONTENT)