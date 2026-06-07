# datasets/dataset-generation/dataset-generator/main.py
import asyncio
import logging
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
        start_time = data.get("start_time")
        end_time = data.get("end_time")
        
        L.info("Dataset Generation Triggered", extra={
            "dataset_id": dataset_id, 
            "start": start_time, 
            "end": end_time
        })
        
        if dataset_id not in dataset_definitions:
            L.error("Cannot generate: Unknown dataset definition", extra={"dataset_id": dataset_id})
        else:
            L.debug("Config found. Proceeding to pipeline...", extra={"dataset_id": dataset_id})
            dataset_config = dataset_definitions[dataset_id]
            
            # Fire and forget the pipeline task so we don't block the Knative Eventing Broker
            asyncio.create_task(generator.generate_dataset(dataset_config, start_time, end_time))
            
        return Response(status_code=status.HTTP_204_NO_CONTENT)
        
    except Exception as e:
        L.error("dataset_generate_request: FAILURE", extra={"reason": str(e)}, exc_info=True)
        return Response(status_code=status.HTTP_204_NO_CONTENT)