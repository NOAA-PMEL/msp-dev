import os
import logging
import httpx
import importlib
import numpy as np
import xarray as xr
from datetime import datetime

L = logging.getLogger(__name__)

class DatasetGenerator:
    def __init__(self, daq_id: str):
        self.daq_id = daq_id
        # Dynamic internal URL matching sampling-system
        self.datastore_url = f"http://datastore.{self.daq_id}-system.svc.cluster.local:80"
        self.client = httpx.AsyncClient(base_url=self.datastore_url, timeout=30.0)
        
        # Ensure our output directory exists
        self.output_dir = "/app/data/output"
        os.makedirs(self.output_dir, exist_ok=True)

    async def fetch_variableset_data(self, variableset_id: str, start_time: str, end_time: str):
        """Fetch raw historical telemetry arrays from the Datastore."""
        params = {"variableset_id": variableset_id, "start_time": start_time, "end_time": end_time}
        try:
            response = await self.client.get("/variableset/data/get/", params=params)
            response.raise_for_status()
            return response.json().get("results", [])
        except httpx.HTTPError as e:
            L.error(f"Datastore fetch failed for data {variableset_id}", extra={"error": str(e)})
            return []

    async def fetch_variableset_def(self, variableset_id: str):
        """Fetch the VariableSet definition to inherit native array shapes and coordinates."""
        try:
            # Note: Adjust this endpoint to match your actual datastore registry API
            response = await self.client.get(f"/variableset/registry/get/{variableset_id}")
            response.raise_for_status()
            return response.json().get("data", {})
        except httpx.HTTPError as e:
            L.error(f"Datastore fetch failed for definition {variableset_id}", extra={"error": str(e)})
            return {}

    async def execute_calculation(self, action_module: str, action_def: str, params: dict):
        """
        Dynamically load the GitOps math script and run it on our arrays.
        Example: action_module='calculations.default', action_def='calculate_true_wind_speed'
        """
        try:
            # Import the module mounted from our GitOps ConfigMap
            module = importlib.import_module(action_module)
            calc_func = getattr(module, action_def)
            
            # Unpack the DataArrays into the function and pass 'self' for logging
            result = await calc_func(self, **params)
            return result
        except Exception as e:
            L.error(f"Failed to execute {action_def} from {action_module}", extra={"error": str(e)})
            return None

    async def generate_dataset(self, config: dict, start_time: str, end_time: str):
        """Main pipeline to extract, compile, align, and export NetCDF datasets."""
        dataset_id = config.get("id", "unknown_dataset")
        freq_sec = config.get("timebase", {}).get("record_frequency_sec", 60)
        
        L.info("Starting generation pipeline", extra={"dataset_id": dataset_id})
        
        try:
            data_arrays = []
            
            for var in config.get("variables", []):
                out_name = var["name"]
                
                # --- Handle purely static variables ---
                if "static_value" in var:
                    continue 
                
                source_def = var.get("source", {})
                is_calculated = "calculate_method" in source_def
                
                # If calculated, fetch multiple inputs. Otherwise, fetch just the primary source.
                fetch_list = source_def.get("inputs", {}) if is_calculated else {"primary": source_def}
                
                input_arrays = {}
                unique_sources = set()
                
                # --- STEP 1: Fetch Data & Track ALL Sources ---
                for param_name, input_source in fetch_list.items():
                    vs_id = input_source.get("variableset_id")
                    vs_var = input_source.get("variable_name")
                    
                    if not vs_id or not vs_var:
                        continue
                        
                    records = await self.fetch_variableset_data(vs_id, start_time, end_time)
                    if not records: 
                        continue
                    
                    times, values = [], []
                    for r in records:
                        v_dict = r.get("variables", {})
                        if "time" in v_dict and vs_var in v_dict:
                            # 1. Time parsing
                            t_str = v_dict["time"]["data"]
                            times.append(datetime.fromisoformat(t_str.replace("Z", "+00:00")))
                            
                            # 2. Extract Value
                            target_var = v_dict[vs_var]
                            values.append(target_var["data"])
                            
                            # 3. Harvest Source ID (handles mid-hour sensor swaps)
                            hw_source = target_var.get("attributes", {}).get("source_id", {}).get("data")
                            if hw_source:
                                unique_sources.add(hw_source)
                                
                    if times:
                        input_arrays[param_name] = {"values": values, "times": times, "vs_id": vs_id, "vs_var": vs_var}
                
                if not input_arrays:
                    continue

                # --- STEP 2: Execute Vectorized Math (If Applicable) ---
                if is_calculated:
                    action_module = source_def["calculate_method"]["action_module"]
                    action_def = source_def["calculate_method"]["action_def"]
                    
                    # Prepare params dictionary of raw values to pass to numpy math
                    math_params = {k: v["values"] for k, v in input_arrays.items()}
                    calc_result = await self.execute_calculation(action_module, action_def, math_params)
                    
                    if not calc_result:
                        continue
                        
                    final_values = calc_result.get(out_name)
                    # Grab time axis from the first valid input
                    primary_input = list(input_arrays.values())[0]
                    final_times = primary_input["times"]
                    native_vs_id = primary_input["vs_id"]
                    native_vs_var = primary_input["vs_var"]
                else:
                    primary_input = input_arrays["primary"]
                    final_values = primary_input["values"]
                    final_times = primary_input["times"]
                    native_vs_id = primary_input["vs_id"]
                    native_vs_var = primary_input["vs_var"]

                # --- STEP 3: Build, Inherit, and Rebin ---
                
                # Fetch the source variableset definition to get native coordinates
                vs_def = await self.fetch_variableset_def(native_vs_id)
                native_vars = vs_def.get("variables", {})
                
                # Determine Dimensions (Defaults to 1D ["time"])
                dims = native_vars.get(native_vs_var, {}).get("shape", ["time"])
                
                # Build Native Coordinates (Inheriting arrays like 60-bin diameter)
                coords = {"time": final_times}
                for dim in dims:
                    if dim == "time": continue
                    if dim in native_vars:
                        coords[dim] = native_vars[dim].get("data", [])
                
                # Build Native DataArray
                da = xr.DataArray(
                    data=final_values, 
                    coords=coords, 
                    dims=dims, 
                    name=out_name
                )
                
                # OVERRIDE & REBIN: Check Dataset Definition for a custom grid
                if "coordinates" in var:
                    for custom_dim, custom_grid in var["coordinates"].items():
                        if custom_dim in da.dims:
                            L.info(f"Rebinning {out_name} along {custom_dim} to new custom grid.")
                            
                            # TODO: Replace linear interpolation with a conservative 
                            # rebinning algorithm (area-under-curve) for dN/dlogDp parameters.
                            da = da.interp(
                                {custom_dim: custom_grid}, 
                                method="linear", 
                                kwargs={"fill_value": np.nan}
                            )

                # --- STEP 4: Apply Attributes & Hardware Provenance ---
                for attr_key, attr_val in var.get("attributes", {}).items():
                    da.attrs[attr_key] = attr_val
                    
                if unique_sources:
                    da.attrs["sources"] = ", ".join(sorted(list(unique_sources)))
                
                data_arrays.append(da)

            if not data_arrays:
                L.warning("No data retrieved. Aborting dataset generation.")
                return None
                
            # --- STEP 5: Merge, Time-Align, and Resample ---
            ds = xr.merge(data_arrays)
            
            # Resample exactly to the requested frequency. This handles mid-file NaNs
            # and naturally averages 2D matrices across the time axis.
            aligned_ds = ds.resample(time=f"{freq_sec}S").mean()
            
            # Apply Static Variables (e.g., nominal sensor heights, site locations)
            for var in config.get("variables", []):
                if "static_value" in var:
                    out_name = var["name"]
                    da = xr.DataArray(
                        data=np.full(aligned_ds.sizes["time"], var["static_value"]),
                        coords={"time": aligned_ds.time},
                        dims=["time"]
                    )
                    for attr_key, attr_val in var.get("attributes", {}).items():
                        da.attrs[attr_key] = attr_val
                    aligned_ds[out_name] = da

            # --- STEP 6: Global File Metadata ---
            aligned_ds.attrs["title"] = f"Dataset: {dataset_id}"
            aligned_ds.attrs["history"] = f"Generated {datetime.utcnow().isoformat()}Z"
            if "conventions" in config:
                aligned_ds.attrs["Conventions"] = config["conventions"].get("name", "CF-1.8")
                aligned_ds.attrs["featureType"] = config["conventions"].get("featureType", "timeSeries")

            # --- STEP 7: Export to NetCDF ---
            safe_start = start_time.replace(":", "").replace("-", "")
            filename = f"{dataset_id}.{safe_start}.nc"
            filepath = os.path.join(self.output_dir, filename)
            
            aligned_ds.to_netcdf(filepath, engine="netcdf4", format="NETCDF4")
            
            L.info(f"Successfully generated NetCDF: {filepath}")
            return filepath
            
        except Exception as e:
            L.error("Pipeline failure", extra={"reason": str(e)}, exc_info=True)
            raise e