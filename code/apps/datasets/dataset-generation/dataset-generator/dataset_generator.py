import os
import logging
import httpx
import importlib
import numpy as np
import pandas as pd
import xarray as xr
from datetime import datetime

import pint
ureg = pint.UnitRegistry()
ureg.default_format = "~"

L = logging.getLogger("dataset-generator")

class DatasetGenerator:
    def __init__(self, daq_id: str):
        self.daq_id = daq_id
        self.datastore_url = f"http://datastore.{self.daq_id}-system.svc.cluster.local:80"
        self.client = httpx.AsyncClient(base_url=self.datastore_url, timeout=30.0)
        self.output_dir = "/app/data/output"
        os.makedirs(self.output_dir, exist_ok=True)

    async def fetch_variableset_def(self, variableset_id: str, data_time: datetime = None, exact_vmap_id: str = None):
        """Fetch the VariableSet definition from the datastore registry."""
        try:
            vs_name = variableset_id.split("::")[-1]

            if exact_vmap_id:
                resp = await self.client.get(
                    "/variableset-definition/registry/get/", 
                    params={"variablemap_definition_id": exact_vmap_id, "variableset": vs_name}
                )
                resp.raise_for_status()
                defs = resp.json().get("results", [])
                if defs:
                    return defs[0]

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
                if vmap_prefix and vmap_prefix not in vmap_id:
                    continue
                
                vmap_parts = vmap_id.split("::")
                if len(vmap_parts) >= 3:
                    config_time_str = vmap_parts[-1]
                else:
                    config_time_str = d.get("attributes", {}).get("valid_config_time", {}).get("data", "2020-01-01T00:00:00Z")
                
                try:
                    config_time = datetime.fromisoformat(config_time_str.replace("Z", "+00:00"))
                    if data_time and config_time <= data_time:
                        valid_defs.append((config_time, d))
                except ValueError:
                    continue
            
            if valid_defs:
                valid_defs.sort(key=lambda x: x[0], reverse=True)
                return valid_defs[0][1]
            
            return {}
        except Exception as e:
            L.error(f"Failed to fetch variableset definition for {variableset_id}: {e}")
            return {}

    async def execute_calculation(self, action_module: str, action_def: str, params: dict):
        """Dynamically load calculation scripts."""
        try:
            module = importlib.import_module(action_module)
            calc_func = getattr(module, action_def)
            return await calc_func(self, **params)
        except Exception as e:
            L.error(f"Failed to execute {action_def}: {e}")
            return None

    async def generate_dataset(self, config: dict, start_time: str, end_time: str):
        """
        Highly optimized pipeline that resolves mappings, fetches telemetry, 
        and extracts schemas exactly once per unique resource.
        """
        dataset_id = config.get("id", "unknown_dataset")
        freq_sec = config.get("timebase", {}).get("record_frequency_sec", 60)
        
        L.info("Starting batch-optimized pipeline", extra={"dataset_id": dataset_id, "start": start_time, "end": end_time})
        
        try:
            # -----------------------------------------------------------------
            # PASS 1: Identify Unique VariableSets & Resolve Mappings ONCE
            # -----------------------------------------------------------------
            unique_vs_ids = set()
            for var in config.get("variables", []):
                if "static_value" in var: continue
                source_def = var.get("source", {})
                fetch_list = source_def.get("inputs", {}) if "calculate_method" in source_def else {"primary": source_def}
                for input_source in fetch_list.values():
                    vs_id = input_source.get("variableset_id")
                    if vs_id: unique_vs_ids.add(vs_id)

            L.info("Deduplicated VariableSets discovered", extra={"unique_variablesets": list(unique_vs_ids)})

            # Query the mapping definition registry exactly ONCE per unique variableset_id
            vs_to_hardware_map = {} # Maps: vs_id -> active mapping details dictionary
            for vs_id in unique_vs_ids:
                vmap_name = vs_id.split("::")[0]
                L.debug(f"Fetching variablemap mapping definition for: {vmap_name}")
                vmap_resp = await self.client.get("/variablemap-definition/registry/get/", params={"variablemap": vmap_name})
                vmap_resp.raise_for_status()
                vmaps = vmap_resp.json().get("results", [])
                
                # Locate active variablemap valid for this specific time window
                query_time = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                active_vmap = None
                for vmap in sorted(vmaps, key=lambda x: x.get("valid_config_time", "2020-01-01T00:00:00Z"), reverse=True):
                    cfg_time = datetime.fromisoformat(vmap.get("valid_config_time", "2020-01-01T00:00:00Z").replace("Z", "+00:00"))
                    if cfg_time <= query_time:
                        active_vmap = vmap
                        break
                
                if active_vmap:
                    vs_to_hardware_map[vs_id] = active_vmap
                    L.debug(f"Successfully cached mapping schema for {vs_id}", extra={
                        "vmap_def_id": active_vmap.get("variablemap_definition_id")
                    })
                else:
                    L.error(f"No active variablemap mapping found in registry for {vs_id}")

            # -----------------------------------------------------------------
            # PASS 2: Deduplicate and Bulk Fetch Raw Telemetry Sources ONCE
            # -----------------------------------------------------------------
            # Trace target variables to extract their exact hardware source configurations
            variable_tracing_registry = {} # Maps: (vs_id, vs_var) -> tracking dict
            telemetry_sources_to_fetch = {} # Maps: source_id -> {source_type, variables_set}

            for var in config.get("variables", []):
                if "static_value" in var: continue
                source_def = var.get("source", {})
                fetch_list = source_def.get("inputs", {}) if "calculate_method" in source_def else {"primary": source_def}
                
                for input_source in fetch_list.values():
                    vs_id = input_source.get("variableset_id")
                    vs_var = input_source.get("variable_name")
                    if not vs_id or not vs_var: continue
                    
                    active_vmap = vs_to_hardware_map.get(vs_id)
                    if active_vmap and vs_var in active_vmap.get("variables", {}):
                        target_var_def = active_vmap["variables"][vs_var]
                        direct_var = target_var_def.get("direct_value", {}).get("source_variable", vs_var)
                        src_info = target_var_def.get("source", {}).get(direct_var, {})
                        
                        s_id = src_info.get("source_id")
                        s_type = src_info.get("source_type", "device")
                        raw_var_name = src_info.get("source_variable", vs_var)
                        
                        if s_id:
                            # Cache tracing paths for easy Pass 4 lookups
                            variable_tracing_registry[(vs_id, vs_var)] = {
                                "source_id": s_id,
                                "raw_variable_name": raw_var_name,
                                "vmap_def_id": active_vmap.get("variablemap_definition_id")
                            }
                            # Group required fields under single physical stream queries
                            if s_id not in telemetry_sources_to_fetch:
                                telemetry_sources_to_fetch[s_id] = {"source_type": s_type, "fields": set()}
                            telemetry_sources_to_fetch[s_id]["fields"].add(raw_var_name)

            L.info("Deduplicated Telemetry Sources discovered", extra={"unique_sources": list(telemetry_sources_to_fetch.keys())})

            # Execute exactly ONE bulk HTTP download request per unique telemetry source stream
            bulk_telemetry_cache = {}
            for s_id, source_meta in telemetry_sources_to_fetch.items():
                endpoint = f"/{source_meta['source_type']}/data/get/"
                params = {f"{source_meta['source_type']}_id": s_id, "start_time": start_time, "end_time": end_time}
                
                L.info(f"Bulk-retrieving historical telemetry stream from: {s_id}", extra={"endpoint": endpoint})
                resp = await self.client.get(endpoint, params=params)
                resp.raise_for_status()
                records = resp.json().get("results", [])
                bulk_telemetry_cache[s_id] = records
                
                # --- DEEP DIAGNOSTIC DEBUG LOG ---
                # Exposes exactly what dictionary keys ERDDAP/Datastore returned vs what fields we need
                sample_hardware_keys = list(records[0].get("variables", {}).keys()) if records else []
                L.debug("Datastore telemetry payload structural check", extra={
                    "source_id": s_id,
                    "records_downloaded": len(records),
                    "expected_fields_for_nc": list(source_meta["fields"]),
                    "actual_keys_in_payload": sample_hardware_keys
                })

            # -----------------------------------------------------------------
            # PASS 3: Fetch and Cache VariableSet Schema Definitions ONCE
            # -----------------------------------------------------------------
            vs_defs_cache = {}
            for (vs_id, vs_var), trace in variable_tracing_registry.items():
                if vs_id not in vs_defs_cache:
                    records = bulk_telemetry_cache.get(trace["source_id"], [])
                    sample_time = None
                    if records and "variables" in records[0] and "time" in records[0]["variables"]:
                        try:
                            sample_time = pd.to_datetime(records[0]["variables"]["time"]["data"].replace("Z", "")).to_pydatetime()
                        except Exception:
                            pass
                    
                    L.info(f"Caching definition schema file for variableset: {vs_id}")
                    vs_defs_cache[vs_id] = await self.fetch_variableset_def(
                        vs_id, data_time=sample_time, exact_vmap_id=trace["vmap_def_id"]
                    )

            # -----------------------------------------------------------------
            # PASS 4: Compile Xarray & Output NetCDF entirely from In-Memory Cache
            # -----------------------------------------------------------------
            data_arrays = []

            for var in config.get("variables", []):
                out_name = var["name"]
                if "static_value" in var: continue
                
                source_def = var.get("source", {})
                is_calculated = "calculate_method" in source_def
                fetch_list = source_def.get("inputs", {}) if is_calculated else {"primary": source_def}
                
                input_arrays = {}
                unique_sources = set()
                primary_vs_id = None
                primary_vs_var = None
                
                for param_name, input_source in fetch_list.items():
                    vs_id = input_source.get("variableset_id")
                    vs_var = input_source.get("variable_name")
                    
                    trace_key = (vs_id, vs_var)
                    if trace_key not in variable_tracing_registry: continue
                    trace = variable_tracing_registry[trace_key]
                    
                    if not primary_vs_id:
                        primary_vs_id = vs_id
                        primary_vs_var = vs_var
                    
                    records = bulk_telemetry_cache.get(trace["source_id"], [])
                    times, values = [], []
                    raw_key = trace["raw_variable_name"]
                    
                    # Unpack timelines entirely out of local RAM cache
                    for r in records:
                        r_vars = r.get("variables", {})
                        if "time" in r_vars and raw_key in r_vars:
                            rounded_dt = pd.to_datetime(r_vars["time"]["data"].replace("Z", "")).round("1s")
                            times.append(rounded_dt.to_datetime64())
                            values.append(r_vars[raw_key]["data"])
                            
                            hw_source = r_vars[raw_key].get("attributes", {}).get("source_id", {}).get("data")
                            if hw_source: unique_sources.add(hw_source)
                    
                    # --- DEEP DIAGNOSTIC DEBUG LOG ---
                    L.debug("Variable extraction timeline metrics", extra={
                        "target_nc_field": out_name,
                        "extracted_from_key": raw_key,
                        "data_points_parsed": len(times),
                        "first_sample_values": values[:3] if values else []
                    })
                    
                    if times:
                        input_arrays[param_name] = {"values": values, "times": times}

                if not input_arrays: continue

                # Vector math transformations
                if is_calculated:
                    action_module = source_def["calculate_method"]["action_module"]
                    action_def = source_def["calculate_method"]["action_def"]
                    math_params = {k: v["values"] for k, v in input_arrays.items()}
                    calc_result = await self.execute_calculation(action_module, action_def, math_params)
                    if not calc_result: continue
                    final_values = calc_result.get(out_name)
                    final_times = list(input_arrays.values())[0]["times"]
                else:
                    final_values = input_arrays["primary"]["values"]
                    final_times = input_arrays["primary"]["times"]

                # Extract cached grid configurations
                vs_def = vs_defs_cache.get(primary_vs_id, {})
                native_vars = vs_def.get("variables", {})
                
                dims = native_vars.get(primary_vs_var, {}).get("shape", ["time"])
                coords = {"time": final_times}
                for dim in dims:
                    if dim != "time" and dim in native_vars:
                        coords[dim] = native_vars[dim].get("data", [])
                
                da = xr.DataArray(data=final_values, coords=coords, dims=dims, name=out_name)
                
                # --- RE-ADDED 2D INTERPOLATION BLOCK ---
                if "coordinates" in var:
                    for custom_dim, custom_grid in var["coordinates"].items():
                        if custom_dim in da.dims:
                            L.info(f"Rebinning {out_name} along {custom_dim}")
                            da = da.interp(
                                {custom_dim: custom_grid}, 
                                method="linear", 
                                kwargs={"fill_value": np.nan}
                            )
                # ---------------------------------------

                # Global mappings, attributes and pint conversions
                native_attrs = native_vars.get(primary_vs_var, {}).get("attributes", {})
                for attr_key, attr_val in native_attrs.items(): da.attrs[attr_key] = attr_val
                
                native_units = da.attrs.get("units")
                target_units = None
                for attr_key, attr_val in var.get("attributes", {}).items():
                    if attr_key == "units": target_units = attr_val
                    da.attrs[attr_key] = attr_val
                    
                if native_units and target_units and (native_units != target_units):
                    try:
                        data_quantity = ureg.Quantity(da.values, native_units)
                        da.values = data_quantity.to(target_units).magnitude
                        da.attrs["units"] = target_units
                    except Exception as e:
                        L.error(f"Unit conversion failed for {out_name}: {e}")
                        da.attrs["units"] = f"{native_units} (CONVERSION FAILED)"

                if unique_sources: da.attrs["sources"] = ", ".join(sorted(list(unique_sources)))
                data_arrays.append(da)

            # Assemble merged structure
            if not data_arrays:
                L.warning("No data extracted for any target variables. Building blank schema.", extra={"dataset_id": dataset_id})
                ds = xr.Dataset()
            else:
                ds = xr.merge(data_arrays, join='outer')
                
            if "time" in ds.dims:
                ds = ds.groupby("time").mean(dim="time")
            
            # Rebin time centered averages
            half_base = freq_sec / 2.0
            if "time" in ds.dims and len(ds.time) > 0:
                aligned_ds = ds.resample(time=f"{freq_sec}s", closed="left", label="right", offset=f"{half_base}s").mean(dim="time")
                if len(aligned_ds.time) > 0:
                    aligned_ds.coords["time"] = aligned_ds.time - pd.Timedelta(seconds=half_base)
            else:
                aligned_ds = ds

            # Force complete continuous bounds template
            master_time = pd.date_range(start=start_time.replace("Z", ""), end=end_time.replace("Z", ""), freq=f"{freq_sec}s", inclusive="left")
            aligned_ds = aligned_ds.reindex(time=master_time)

            # Allocate stable NaN matrices for missing streams
            for var in config.get("variables", []):
                if "static_value" in var: continue
                out_name = var["name"]
                if out_name not in aligned_ds.data_vars:
                    L.warning(f"Variable '{out_name}' missing from telemetry. Injecting NaN placeholder array.", extra={"dataset_id": dataset_id})
                    dims = ["time"]
                    coords = {"time": aligned_ds.time}
                    shape = [aligned_ds.sizes["time"]]
                    
                    if "coordinates" in var:
                        for custom_dim, custom_grid in var["coordinates"].items():
                            dims.append(custom_dim)
                            coords[custom_dim] = custom_grid
                            shape.append(len(custom_grid))
                            
                    empty_da = xr.DataArray(data=np.full(shape, np.nan, dtype=np.float32), coords=coords, dims=dims, name=out_name)
                    for attr_key, attr_val in var.get("attributes", {}).items(): empty_da.attrs[attr_key] = attr_val
                    aligned_ds[out_name] = empty_da

            # Pre-allocate clean quality control byte masks
            for var_name in list(aligned_ds.data_vars.keys()):
                if var_name in ["time", "latitude", "longitude", "altitude"] or var_name.startswith("qc_"): continue
                
                qc_da = xr.DataArray(data=np.zeros(aligned_ds.sizes["time"], dtype=np.int32), coords={"time": aligned_ds.time}, dims=["time"], name=f"qc_{var_name}")
                qc_da.attrs["long_name"] = f"Quality check results on field: {aligned_ds[var_name].attrs.get('long_name', var_name)}"
                qc_da.attrs["units"] = "1"
                qc_da.attrs["standard_name"] = "quality_flag"
                qc_da.attrs["flag_masks"] = [1, 2, 4, 8]
                qc_da.attrs["flag_meanings"] = "value_less_than_valid_min value_greater_than_valid_max sensor_offline flatline_detected"
                aligned_ds[f"qc_{var_name}"] = qc_da

            # Append static metrics
            for var in config.get("variables", []):
                if "static_value" in var:
                    out_name = var["name"]
                    da = xr.DataArray(data=np.full(aligned_ds.sizes["time"], var["static_value"]), coords={"time": aligned_ds.time}, dims=["time"])
                    for attr_key, attr_val in var.get("attributes", {}).items(): da.attrs[attr_key] = attr_val
                    aligned_ds[out_name] = da

            # Write out to NetCDF disk
            aligned_ds.attrs["title"] = f"Dataset: {dataset_id}"
            aligned_ds.attrs["history"] = f"Generated {datetime.utcnow().isoformat()}Z"
            if "conventions" in config:
                aligned_ds.attrs["Conventions"] = config["conventions"].get("name", "CF-1.8")
                aligned_ds.attrs["featureType"] = config["conventions"].get("featureType", "timeSeries")

            safe_start = start_time.replace(":", "").replace("-", "")
            filename = f"{dataset_id}.{safe_start}.nc"
            filepath = os.path.join(self.output_dir, filename)
            aligned_ds.to_netcdf(filepath, engine="netcdf4", format="NETCDF4")

            # Push to storage vault
            storage_url = f"http://dataset-storage.{self.daq_id}-system.svc.cluster.local:80/upload/"
            try:
                async with httpx.AsyncClient() as client:
                    with open(filepath, "rb") as f:
                        files = {"file": (filename, f, "application/x-netcdf")}
                        resp = await client.post(storage_url, files=files, params={"dataset_id": dataset_id}, timeout=30.0)
                        resp.raise_for_status()
                L.info(f"Successfully pushed {filename} to central dataset-storage.")
                os.remove(filepath)
            except Exception as e:
                L.error("Failed to push to storage.", extra={"out_file": filename, "attempted_url": storage_url, "reason": str(e)})

            return filepath
        except Exception as e:
            L.error("Pipeline failure", extra={"reason": str(e)}, exc_info=True)
            raise e