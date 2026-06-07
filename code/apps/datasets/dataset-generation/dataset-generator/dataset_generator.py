import os
import logging
import httpx
import importlib
import numpy as np
import xarray as xr
from datetime import datetime

import pint
ureg = pint.UnitRegistry()
# Optional but recommended: Tell pint to fall back to standard naming if there are slight variations
ureg.default_format = "~"

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

    async def fetch_variableset_data(self, variableset_id: str, variable_name: str, start_time: str, end_time: str):
        """
        Reconstructs VariableSet data historically by looking up the mapping 
        and fetching the raw telemetry from ERDDAP-backed device records.
        """
        try:
            # 1. Parse the requested ID (e.g., 'payload_03::main')
            parts = variableset_id.split("::")
            if len(parts) >= 2:
                vmap_name = parts[0]
                vs_name = parts[1]
            else:
                vmap_name = variableset_id
                vs_name = "unknown"

            # 2. Fetch the VariableMap Definition valid at start_time
            vmap_resp = await self.client.get(
                "/variablemap-definition/registry/get/", 
                params={"variablemap": vmap_name}
            )
            vmap_resp.raise_for_status()
            vmaps = vmap_resp.json().get("results", [])
            
            # Find the active vmap for our start_time
            query_time = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
            active_vmap = None
            
            # Sort maps by config time descending, pick the first one older than our data
            for vmap in sorted(vmaps, key=lambda x: x.get("valid_config_time", "2020-01-01T00:00:00Z"), reverse=True):
                cfg_time_str = vmap.get("valid_config_time", "2020-01-01T00:00:00Z")
                cfg_time = datetime.fromisoformat(cfg_time_str.replace("Z", "+00:00"))
                if cfg_time <= query_time:
                    active_vmap = vmap
                    break
            
            if not active_vmap:
                L.error(f"No valid VariableMap found for {vmap_name} at {start_time}")
                return []

            # 3. Trace the Variable back to the Raw Hardware
            # Look inside the raw variablemap definition to find the data source
            variables_def = active_vmap.get("variables", {})
            target_var_def = variables_def.get(variable_name, {})
            
            if not target_var_def:
                L.error(f"Variable '{variable_name}' not found in map {variableset_id}")
                return []
                
            map_type = target_var_def.get("map_type", "")
            if map_type != "direct":
                L.warning(f"Cannot historically fetch non-direct variable: {variable_name}")
                return []

            # Traverse the nested source dict matching the sampling_system architecture
            direct_var = target_var_def.get("direct_value", {}).get("source_variable", variable_name)
            src_info = target_var_def.get("source", {}).get(direct_var, {})
            
            raw_source_type = src_info.get("source_type", "device") # device or controller
            raw_device_id = src_info.get("source_id")
            raw_variable = src_info.get("source_variable")

            if not raw_device_id or not raw_variable:
                L.error(f"Mapping for '{variable_name}' is missing source_id or source_variable")
                return []

            # 4. Fetch the Raw Data (This routes to ERDDAP!)
            endpoint = f"/{raw_source_type}/data/get/"
            params = {
                f"{raw_source_type}_id": raw_device_id, 
                "start_time": start_time, 
                "end_time": end_time
            }
            
            data_resp = await self.client.get(endpoint, params=params)
            data_resp.raise_for_status()
            raw_records = data_resp.json().get("results", [])

            # 5. Repackage the raw data so it "looks" like Variableset data 
            repackaged_records = []
            for record in raw_records:
                record_vars = record.get("variables", {})
                
                # Check if the requested variable actually exists in this raw record
                if "time" in record_vars and raw_variable in record_vars:
                    repackaged_records.append({
                        "timestamp": record.get("timestamp"),
                        "variablemap_id": active_vmap.get("variablemap_definition_id"), # Apply the exact mapping stamp
                        "variables": {
                            "time": record_vars["time"],
                            variable_name: record_vars[raw_variable] # Rename it back to the mapped name
                        }
                    })
                
            return repackaged_records

        except Exception as e:
            L.error(f"Failed to reconstruct variableset data for {variableset_id}", extra={"error": str(e)}, exc_info=True)
            return []
        
    async def fetch_variableset_def(self, variableset_id: str, data_time: datetime = None, exact_vmap_id: str = None):
        """Fetch the VariableSet definition using exact mapping or time-based fallback."""
        try:
            vs_name = variableset_id.split("::")[-1]

            # --- PATH A: THE FAST PATH (Exact match from the data record) ---
            if exact_vmap_id:
                resp = await self.client.get(
                    "/variableset-definition/registry/get/", 
                    params={"variablemap_definition_id": exact_vmap_id, "variableset": vs_name}
                )
                resp.raise_for_status()
                defs = resp.json().get("results", [])
                if defs:
                    return defs[0]

            # --- PATH B: TIME COMPARISON FALLBACK ---
            resp = await self.client.get(
                "/variableset-definition/registry/get/", 
                params={"variableset": vs_name}
            )
            resp.raise_for_status()
            defs = resp.json().get("results", [])
            
            valid_defs = []
            vmap_prefix = variableset_id.split("::")[-2] if "::" in variableset_id else None

            for d in defs:
                vmap_id = d.get("variablemap_definition_id", "")
                
                # Make sure it matches our expected platform/payload (e.g., 'payload_03')
                if vmap_prefix and vmap_prefix not in vmap_id:
                    continue
                
                # Extract the valid_config_time
                vmap_parts = vmap_id.split("::")
                if len(vmap_parts) >= 3:
                    config_time_str = vmap_parts[-1]
                else:
                    config_time_str = d.get("attributes", {}).get("valid_config_time", {}).get("data", "2020-01-01T00:00:00Z")
                
                try:
                    config_time = datetime.fromisoformat(config_time_str.replace("Z", "+00:00"))
                    # The definition must be active AT or BEFORE the data was collected
                    if data_time and config_time <= data_time:
                        valid_defs.append((config_time, d))
                except ValueError:
                    continue
            
            if valid_defs:
                # Sort descending to get the most recent valid configuration
                valid_defs.sort(key=lambda x: x[0], reverse=True)
                return valid_defs[0][1]
            
            L.warning(f"No valid definition found for {variableset_id} at {data_time}")
            return {}
            
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
        """Main pipeline to extract, compile, align, convert units, and export NetCDF datasets."""
        dataset_id = config.get("id", "unknown_dataset")
        freq_sec = config.get("timebase", {}).get("record_frequency_sec", 60)
        
        L.info("Starting generation pipeline", extra={"dataset_id": dataset_id, "start": start_time, "end": end_time})
        
        try:
            data_arrays = []
            
            for var in config.get("variables", []):
                out_name = var["name"]
                
                # --- Handle purely static variables ---
                # These are applied later after the time axis is finalized
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
                        
                    # --- FIX: Pass the specific variable_name so the fetcher can trace it! ---
                    records = await self.fetch_variableset_data(vs_id, vs_var, start_time, end_time)
                    if not records: 
                        continue
                    
                    times, values = [], []
                    exact_vmap_id = None # NEW: Track the mapping ID
                    
                    for r in records:
                        v_dict = r.get("variables", {})
                        if "time" in v_dict and vs_var in v_dict:
                            # Time parsing
                            t_str = v_dict["time"]["data"]
                            times.append(datetime.fromisoformat(t_str.replace("Z", "+00:00")))
                            
                            # Extract Value
                            target_var = v_dict[vs_var]
                            values.append(target_var["data"])
                            
                            # NEW: Harvest Exact Mapping ID if present
                            if not exact_vmap_id and "variablemap_id" in r:
                                exact_vmap_id = r.get("variablemap_id")

                            # Harvest Hardware Source ID
                            hw_source = target_var.get("attributes", {}).get("source_id", {}).get("data")
                            if hw_source:
                                unique_sources.add(hw_source)
                                
                    if times:
                        # NEW: Include exact_vmap_id in the dictionary
                        input_arrays[param_name] = {
                            "values": values, 
                            "times": times, 
                            "vs_id": vs_id, 
                            "vs_var": vs_var,
                            "exact_vmap_id": exact_vmap_id 
                        }
                
                if not input_arrays:
                    continue

                # --- STEP 2: Execute Vectorized Math (If Applicable) ---
                if is_calculated:
                    action_module = source_def["calculate_method"]["action_module"]
                    action_def = source_def["calculate_method"]["action_def"]
                    
                    math_params = {k: v["values"] for k, v in input_arrays.items()}
                    calc_result = await self.execute_calculation(action_module, action_def, math_params)
                    
                    if not calc_result:
                        continue
                        
                    final_values = calc_result.get(out_name)
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
                
                # Fetch native dataset definition (Temporally Aware)
                native_vmap_id = primary_input.get("exact_vmap_id")
                vs_def = await self.fetch_variableset_def(
                    native_vs_id, 
                    data_time=final_times[0] if final_times else None,
                    exact_vmap_id=native_vmap_id
                )
                native_vars = vs_def.get("variables", {})
                
                # Determine Dimensions and Coordinates
                dims = native_vars.get(native_vs_var, {}).get("shape", ["time"])
                coords = {"time": final_times}
                for dim in dims:
                    if dim == "time": continue
                    if dim in native_vars:
                        coords[dim] = native_vars[dim].get("data", [])
                
                # Build initial Xarray DataArray
                da = xr.DataArray(
                    data=final_values, 
                    coords=coords, 
                    dims=dims, 
                    name=out_name
                )
                
                # Rebin: Check Dataset Definition for a custom grid
                if "coordinates" in var:
                    for custom_dim, custom_grid in var["coordinates"].items():
                        if custom_dim in da.dims:
                            L.info(f"Rebinning {out_name} along {custom_dim}")
                            da = da.interp(
                                {custom_dim: custom_grid}, 
                                method="linear", 
                                kwargs={"fill_value": np.nan}
                            )

                # --- STEP 4: Apply Attributes & Unit Conversion ---
                
                # 1. Inherit native attributes
                native_attrs = native_vars.get(native_vs_var, {}).get("attributes", {})
                for attr_key, attr_val in native_attrs.items():
                    da.attrs[attr_key] = attr_val
                
                native_units = da.attrs.get("units")
                target_units = None

                # 2. Extract explicit attributes from Dataset Definition JSON
                for attr_key, attr_val in var.get("attributes", {}).items():
                    if attr_key == "units":
                        target_units = attr_val
                    da.attrs[attr_key] = attr_val
                    
                # 3. Pint Unit Conversion
                if native_units and target_units and (native_units != target_units):
                    L.info(f"Unit mismatch for {out_name}: Attempting conversion from '{native_units}' to '{target_units}'")
                    try:
                        data_quantity = ureg.Quantity(da.values, native_units)
                        converted_quantity = data_quantity.to(target_units)
                        da.values = converted_quantity.magnitude
                        da.attrs["units"] = target_units
                        L.debug(f"Successfully converted {out_name} to {target_units}")
                    except pint.errors.DimensionalityError as e:
                        L.error(f"Dimensionality mismatch for {out_name}. Cannot convert '{native_units}' to '{target_units}'. Error: {e}")
                        da.attrs["units"] = f"{native_units} (CONVERSION FAILED)"
                    except pint.errors.UndefinedUnitError as e:
                        L.error(f"Undefined unit found for {out_name}. Error: {e}")
                        da.attrs["units"] = f"{native_units} (CONVERSION FAILED)"
                    except Exception as e:
                        L.error(f"Unexpected error converting units for {out_name}: {e}")

                # 4. Attach Hardware Provenance
                if unique_sources:
                    da.attrs["sources"] = ", ".join(sorted(list(unique_sources)))
                
                data_arrays.append(da)

            if not data_arrays:
                L.warning("No data retrieved for any variables. Aborting dataset generation.")
                return None
                
            # --- STEP 5: Merge, Time-Align, and Resample ---
            ds = xr.merge(data_arrays, join='outer')
            aligned_ds = ds.resample(time=f"{freq_sec}s").mean() # Lowercase 's' applied here
            
            # Apply Static Variables across the new time axis
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

            # --- STEP 8: Push to Dataset Storage ---
            storage_url = "http://dataset-storage.pmel-dev-system.svc.cluster.local/upload/"
            try:
                async with httpx.AsyncClient() as client:
                    with open(filepath, "rb") as f:
                        files = {"file": (filename, f, "application/x-netcdf")}
                        data = {"dataset_id": dataset_id}
                        resp = await client.post(storage_url, files=files, data=data, timeout=30.0)
                        resp.raise_for_status()
                L.info(f"Successfully pushed {filename} to central dataset-storage.")
                
                # Clean up local ephemeral file since it's safe in the vault now
                os.remove(filepath)
            except Exception as e:
                L.error(f"Failed to push {filename} to storage. Kept locally.", extra={"reason": str(e)})

            return filepath
            
        except Exception as e:
            L.error("Pipeline failure", extra={"reason": str(e)}, exc_info=True)
            raise e