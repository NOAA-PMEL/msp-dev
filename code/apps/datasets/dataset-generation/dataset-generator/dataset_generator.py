# datasets/dataset-generation/dataset-generator/dataset_generator.py
import asyncio
import logging
import httpx
from datetime import datetime, timezone
import pandas as pd
import xarray as xr
import numpy as np

L = logging.getLogger("dataset-service")

class DatasetGenerator:
    def __init__(self, daq_id: str):
        self.daq_id = daq_id
        self.datastore_url = f"http://datastore.{self.daq_id}-system.svc.cluster.local:80"
        self.client = httpx.AsyncClient(base_url=self.datastore_url, timeout=30.0)

    async def close(self):
        await self.client.aclose()

    async def generate_dataset(self, config: dict, start_time: str, end_time: str):
        """
        Executes the data pipeline: fetch -> align -> qc -> format -> transmit
        """
        dataset_id = config["id"]
        freq_sec = config["timebase"]["record_frequency_sec"]
        
        L.info("Starting generation pipeline", extra={"dataset_id": dataset_id, "start": start_time, "end": end_time})
        
        try:
            # 1. Fetch & Build DataArrays
            data_arrays = []
            
            for var in config.get("variables", []):
                var_name = var["name"]
                
                # Handle static values (e.g., fixed altitude = 20.0m)
                if "static_value" in var:
                    L.debug(f"Applying static value for {var_name}")
                    # We will apply this after we know the time index of the merged dataset
                    continue 
                
                variableset_id = var.get("variableset_id")
                if not variableset_id:
                    L.warning(f"Variable {var_name} missing variableset_id or static_value. Skipping.")
                    continue
                    
                # Fetch data from Datastore
                records = await self.fetch_variableset_data(variableset_id, start_time, end_time)
                
                if not records:
                    L.debug(f"No data found for {var_name} ({variableset_id})")
                    continue
                
                # Parse to xarray
                times = [datetime.fromisoformat(r["time"].replace("Z", "+00:00")) for r in records]
                values = [r["data"] for r in records]
                
                da = xr.DataArray(
                    data=values,
                    coords={"time": times},
                    dims=["time"],
                    name=var_name
                )
                data_arrays.append(da)
                
            if not data_arrays:
                L.warning("No data retrieved for any variables. Aborting dataset generation.")
                return None
                
            # 2. Merge Data
            L.debug("Merging fetched variables into dataset")
            ds = xr.merge(data_arrays)
            
            # 3. Time Alignment (Resampling)
            L.debug(f"Resampling dataset to {freq_sec}S")
            aligned_ds = ds.resample(time=f"{freq_sec}S").mean()
            
            # 4. Apply Static Variables (Now that we have our resampled time index)
            for var in config.get("variables", []):
                if "static_value" in var:
                    var_name = var["name"]
                    val = var["static_value"]
                    # Create an array of the static value shaped like the time dimension
                    aligned_ds[var_name] = xr.DataArray(
                        data=np.full(aligned_ds.sizes["time"], val),
                        coords={"time": aligned_ds.time},
                        dims=["time"]
                    )

            # --- Next steps (to be built): QC Flagging & Formatting ---
            L.info("Dataset alignment complete", extra={"dataset_id": dataset_id, "size": len(aligned_ds.time)})
            
            # Temporary debug output to watch it work!
            print(aligned_ds)
            
            return aligned_ds
            
        except Exception as e:
            L.error("Pipeline failure", extra={"reason": str(e)}, exc_info=True)
            raise e

    async def fetch_variableset_data(self, variableset_id: str, start_time: str, end_time: str):
        """Queries the Datastore service for historical telemetry."""
        params = {
            "variableset_id": variableset_id,
            "start_time": start_time,
            "end_time": end_time
        }
        
        try:
            L.debug(f"Requesting datastore history for {variableset_id}")
            # Based on your datastore/main.py routes
            response = await self.client.get("/variableset/data/get/", params=params)
            response.raise_for_status()
            
            data = response.json()
            return data.get("data", []) # Adjust depending on datastore exact JSON return structure
            
        except httpx.HTTPError as e:
            L.error("Datastore fetch failed", extra={"variableset_id": variableset_id, "error": str(e)})
            return []