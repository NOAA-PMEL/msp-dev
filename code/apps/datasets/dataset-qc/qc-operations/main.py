import os
import logging
import httpx
import numpy as np
import pandas as pd
import xarray as xr
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
    def datastore_url(self) -> str:
        """Dynamically build the datastore URL based on the namespace DAQ ID"""
        return f"http://datastore.{self.daq_id}-system.svc.cluster.local:80"

    @property
    def storage_url(self) -> str:
        """Dynamically build the storage vault upload URL"""
        return f"http://dataset-storage.{self.daq_id}-system.svc.cluster.local:80/upload/"

    class Config:
        env_prefix = "QC_OPERATIONS_"
        case_sensitive = False

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger("default-qc")

config = Settings()
L.setLevel(config.log_level.upper())
app = FastAPI()
WORK_DIR = "/app/data/work"

@app.on_event("startup")
async def startup():
    os.makedirs(WORK_DIR, exist_ok=True)

async def fetch_operational_status(start_time: str, end_time: str, dataset_id: str):
    """
    Queries the sampling-operations service to find any status events 
    (maintenance, sensor offline, calibration) during this hour.
    """
    # MOCK FETCH: You would hit your actual operations API here
    # e.g., await client.get(f"{config.ops_url}/status/get/", params={...})
    L.info(f"Querying sampling-operations for hardware status between {start_time} and {end_time}...")
    
    # Simulating a response: The 'air_temperature' sensor was offline for maintenance 
    # for a specific 5-minute window during this dataset's hour.
    mock_status_events = [
        {
            "variable": "air_temperature",
            "start": "2026-06-07T20:15:00Z",
            "end": "2026-06-07T20:20:00Z",
            "condition": "maintenance_offline",
            "qc_flag": 4 # 4 = sensor offline
        }
    ]
    return mock_status_events

async def process_default_qc(filename: str, dataset_id: str, download_url: str, delete_url: str):
    local_path = os.path.join(WORK_DIR, filename)
    
    try:
        # 1. Download the raw a1 dataset
        async with httpx.AsyncClient() as client:
            resp = await client.get(download_url, timeout=30.0)
            resp.raise_for_status()
            with open(local_path, "wb") as f:
                f.write(resp.content)

        # 2-4. Open Dataset, Fetch Status, Apply Flags... (Same as before)
        ds = xr.open_dataset(local_path)
        start_time, end_time = str(ds.time.values[0]), str(ds.time.values[-1])
        ops_events = await fetch_operational_status(start_time, end_time, dataset_id)

        if ops_events:
            for event in ops_events:
                var_name = event["variable"]
                qc_var = f"qc_{var_name}"
                if qc_var in ds.data_vars:
                    start_dt = np.datetime64(event["start"].replace("Z", ""))
                    end_dt = np.datetime64(event["end"].replace("Z", ""))
                    time_mask = (ds.time >= start_dt) & (ds.time <= end_dt)
                    ds[qc_var].values[time_mask] |= event["qc_flag"]

        # 5. Overwrite the file LOCALLY (Keep it as .a1)
        ds.to_netcdf(local_path, engine="netcdf4", format="NETCDF4")
        ds.close()

        # 6. Push back to Storage Vault to the "qc" stage!
        upload_url = f"{config.storage_url}qc" 
        async with httpx.AsyncClient() as client:
            with open(local_path, "rb") as f:
                files = {"file": (filename, f, "application/x-netcdf")}
                params = {"dataset_id": dataset_id}
                upload_resp = await client.post(upload_url, files=files, params=params, timeout=30.0)
                upload_resp.raise_for_status()
                
        L.info(f"QC complete. Pushed {filename} to 'qc' stage.")

        # 7. Delete the raw file from storage to save space
        if delete_url:
            async with httpx.AsyncClient() as client:
                await client.delete(delete_url, timeout=10.0)

    except Exception as e:
        L.error("Default QC failed", extra={"reason": str(e)})
    finally:
        if os.path.exists(local_path): os.remove(local_path)

@app.post("/")
async def handle_event(request: Request, background_tasks: BackgroundTasks):
    try:
        ce = from_http(request.headers, await request.body())
        if ce.get("type") == "envds.dataset.stored":
            data = ce.data
            
            # STRICT FILTER: Only grab raw .a1 files
            if ".a1" in data.get("dataset_id", "") and data.get("stage") == "raw":
                background_tasks.add_task(
                    process_default_qc, 
                    data["filename"], 
                    data["dataset_id"], 
                    data["download_url"],
                    data.get("delete_url")
                )
    except Exception as e:
        L.error("Event handling failed", extra={"reason": str(e)})
    return Response(status_code=status.HTTP_204_NO_CONTENT)