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

    def normalize_unit_string(self, unit_str: str) -> str:
        """Translates strict UDUNITS into Pint-compatible formats and maps edge cases."""
        if not unit_str or not isinstance(unit_str, str):
            return unit_str
        import re
        s = re.sub(r'([a-zA-Z]+)([-+]?\d+)', r'\1**\2', unit_str)
        s = s.replace("kilometers/hour", "km/h")  # <-- Added Furuno exact match
        s = s.replace("km/hr", "km/h")
        s = s.replace("m/sec", "m/s")
        s = s.replace("knots", "knot")
        return s

    async def fetch_variableset_def(self, variableset_id: str, data_time: datetime = None, exact_vmap_id: str = None):
        """Fetch the VariableSet definition from the datastore registry."""
        try:
            vs_name = variableset_id.split("::")[-1]

            if exact_vmap_id:
                # ---> THE FIX: Use the strict "name" parameter to hit the DB cache fast-path! <---
                target_name = f"{exact_vmap_id}::{vs_name}"
                resp = await self.client.get(
                    "/variableset-definition/registry/get/", 
                    params={"name": target_name}
                )
                resp.raise_for_status()
                defs = resp.json().get("results", [])
                if defs:
                    return defs[0]

            # Generic fallback (if no exact vmap is provided)
            resp = await self.client.get(
                "/variableset-definition/registry/get/", 
                params={"name": variableset_id}
            )
            resp.raise_for_status()
            defs = resp.json().get("results", [])
            
            if not defs:
                return None
                
            return defs[0]
            
        except Exception as e:
            L.error(f"Failed to fetch VariableSet definition {variableset_id}: {e}")
            return None

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
        and extracts schemas, gracefully populating available streams and injecting NaN placeholders for missing sources.
        """
        import asyncio
        import json
        from datetime import timedelta
        dataset_id = config.get("id", "unknown_dataset")
        freq_sec = config.get("timebase", {}).get("record_frequency_sec", 60)
        
        L.info("Starting batch-optimized pipeline", extra={"dataset_id": dataset_id, "start": start_time, "end": end_time})
        
        def _extract_vars_dict(schema_doc):
            """Polymorphic helper to find 'variables' dictionary anywhere in schema responses."""
            if not schema_doc or not isinstance(schema_doc, dict):
                return {}
            if "variables" in schema_doc and isinstance(schema_doc["variables"], dict):
                return schema_doc["variables"]
            if "data" in schema_doc and isinstance(schema_doc["data"], dict):
                d = schema_doc["data"]
                if "variables" in d and isinstance(d["variables"], dict):
                    return d["variables"]
            for k, v in schema_doc.items():
                if isinstance(v, dict) and "data" in v and isinstance(v["data"], dict):
                    if "variables" in v["data"] and isinstance(v["data"]["variables"], dict):
                        return v["data"]["variables"]
                if isinstance(v, dict) and "variables" in v and isinstance(v["variables"], dict):
                    return v["variables"]
            return {}

        def _find_var_def(vars_dict, target_name):
            """Strict finder matching exact names or lowercase directly against definitions."""
            if not vars_dict or not isinstance(vars_dict, dict):
                return None, None
            if target_name in vars_dict:
                return target_name, vars_dict[target_name]
            target_lower = target_name.lower()
            for k, v in vars_dict.items():
                if k.lower() == target_lower:
                    return k, v
            return None, None

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

            vs_to_hardware_map = {} 
            for vs_id in unique_vs_ids:
                vmap_name = vs_id.split("::")[0]
                L.debug(f"Fetching variablemap mapping definition for: {vmap_name}")

                try:
                    vmap_resp = await self.client.get("/variablemap-definition/registry/get/", params={"variablemap": vmap_name})
                    vmap_resp.raise_for_status()
                    vmaps = vmap_resp.json().get("results", [])
                except Exception as e:
                    L.error(f"Failed to fetch variablemap mapping for {vmap_name}: {e}")
                    vmaps = []
                
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
            # PASS 2: Fetch Hardware Context and Validate Telemetry Sources
            # -----------------------------------------------------------------
            variable_tracing_registry = {} 
            telemetry_sources_to_fetch = {} 
            hw_defs_cache = {}

            for var in config.get("variables", []):
                if "static_value" in var: continue
                source_def = var.get("source", {})
                fetch_list = source_def.get("inputs", {}) if "calculate_method" in source_def else {"primary": source_def}
                
                for input_source in fetch_list.values():
                    vs_id = input_source.get("variableset_id")
                    vs_var = input_source.get("variable_name")
                    if not vs_id or not vs_var: continue
                    
                    active_vmap = vs_to_hardware_map.get(vs_id)
                    vmap_vars = _extract_vars_dict(active_vmap)
                    _, target_var_def = _find_var_def(vmap_vars, vs_var)
                    
                    if active_vmap and target_var_def:
                        sources = target_var_def.get("source", {})
                        src_info = next(iter(sources.values())) if sources else {}
                            
                        s_id = src_info.get("source_id")
                        s_type = src_info.get("source_type", "device")
                        raw_var_name = src_info.get("source_variable", vs_var)
                        
                        is_coordinate = False
                        static_data = []
                        true_raw_var_name = raw_var_name
                        hw_shape = target_var_def.get("shape")
                        hw_var = {}
                        
                        if s_id and len(s_id.split("::")) >= 2:
                            parts = s_id.split("::")
                            make, model = parts[0], parts[1]
                            hw_cache_key = f"{s_type}::{make}::{model}"
                            
                            if hw_cache_key not in hw_defs_cache:
                                try:
                                    hw_resp = await self.client.get(f"/{s_type}-definition/registry/get/", params={"make": make, "model": model})
                                    if hw_resp.status_code == 200 and hw_resp.json().get("results"):
                                        hw_defs_cache[hw_cache_key] = hw_resp.json()["results"][0]
                                    else:
                                        hw_defs_cache[hw_cache_key] = {}
                                except Exception as e:
                                    L.warning(f"Failed to fetch {hw_cache_key}", extra={"reason": str(e)})
                                    hw_defs_cache[hw_cache_key] = {}
                                    
                            hw_def = hw_defs_cache[hw_cache_key]
                            hw_vars = _extract_vars_dict(hw_def)
                            
                            matched_key, hw_var = _find_var_def(hw_vars, true_raw_var_name)
                            if matched_key:
                                true_raw_var_name = matched_key
                            else:
                                hw_var = {}

                            hw_var_type = hw_var.get("attributes", {}).get("variable_type", {}).get("data", "")
                            
                            if not hw_shape:
                                hw_shape = hw_var.get("shape", ["time"])
                            
                            if hw_var_type == "coordinate":
                                is_coordinate = True
                                static_data = hw_var.get("data", [])
                                L.debug(f"DEBUG PASS 2: Discovered coordinate '{true_raw_var_name}' from hardware. Data length: {len(static_data)}")
                                
                        var_type = target_var_def.get("attributes", {}).get("variable_type", {}).get("data", "")
                        
                        if var_type == "coordinate" or is_coordinate:
                            variable_tracing_registry[(vs_id, vs_var)] = {
                                "source_id": "STATIC_COORDINATE",
                                "raw_variable_name": true_raw_var_name,
                                "vmap_def_id": active_vmap.get("variablemap_definition_id"),
                                "is_coordinate": True,
                                "static_data": static_data,
                                "shape": hw_shape,
                                "hw_attrs": hw_var.get("attributes", {}),
                                "vmap_attrs": target_var_def.get("attributes", {})
                            }
                            continue
                        
                        if s_id:
                            variable_tracing_registry[(vs_id, vs_var)] = {
                                "source_id": s_id,
                                "raw_variable_name": true_raw_var_name,
                                "vmap_def_id": active_vmap.get("variablemap_definition_id"),
                                "is_coordinate": False,
                                "shape": hw_shape,
                                "hw_attrs": hw_var.get("attributes", {}),
                                "vmap_attrs": target_var_def.get("attributes", {})
                            }
                            if s_id not in telemetry_sources_to_fetch:
                                telemetry_sources_to_fetch[s_id] = {"source_type": s_type, "fields": set()}
                            telemetry_sources_to_fetch[s_id]["fields"].add(true_raw_var_name)

            L.info("Deduplicated Telemetry Sources discovered", extra={"unique_sources": list(telemetry_sources_to_fetch.keys())})

            bulk_telemetry_cache = {}
            for s_id, source_meta in telemetry_sources_to_fetch.items():
                endpoint = f"/{source_meta['source_type']}/data/get/"
                
                L.info(f"Bulk-retrieving historical telemetry stream from: {s_id}", extra={"endpoint": endpoint})
                
                start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                end_dt = datetime.fromisoformat(end_time.replace("Z", "+00:00"))
                chunk_duration = timedelta(hours=1)
                
                all_records = []
                current_start = start_dt
                
                while current_start < end_dt:
                    current_end = current_start + chunk_duration
                    if current_end > end_dt:
                        current_end = end_dt
                        
                    params = {
                        f"{source_meta['source_type']}_id": s_id, 
                        "start_time": current_start.isoformat().replace("+00:00", "Z"), 
                        "end_time": current_end.isoformat().replace("+00:00", "Z"),
                        "force_archive": True 
                    }
                    
                    max_retries = 3
                    for attempt in range(max_retries):
                        try:
                            resp = await self.client.get(endpoint, params=params, headers={"Connection": "close"}, timeout=180.0)
                            resp.raise_for_status()
                            records = resp.json().get("results", [])
                            all_records.extend(records)
                            break
                        except Exception as e:
                            if attempt == max_retries - 1:
                                L.error(f"Failed to fetch telemetry stream for {s_id} chunk {current_start} after {max_retries} attempts. Proceeding with available data...", extra={"error": str(e)})
                                break
                            L.warning(f"Fetch failed for {s_id} chunk {current_start} on attempt {attempt+1}. Retrying...", extra={"error": str(e)})
                            await asyncio.sleep(2)
                            
                    current_start = current_end

                if all_records:
                    raw_times = [r["variables"]["time"]["data"].replace("Z", "") for r in all_records if "variables" in r and "time" in r["variables"]]
                    if raw_times:
                        try:
                            parsed_times = pd.to_datetime(raw_times).round("1s").values
                            idx = 0
                            for r in all_records:
                                if "variables" in r and "time" in r["variables"]:
                                    r["_parsed_time"] = parsed_times[idx]
                                    idx += 1
                        except Exception as e:
                            L.warning("Vectorized time parse failed, falling back to slow loop", extra={"reason": str(e)})
                            for r in all_records:
                                if "variables" in r and "time" in r["variables"]:
                                    r["_parsed_time"] = pd.to_datetime(r["variables"]["time"]["data"].replace("Z", "")).round("1s").to_datetime64()

                bulk_telemetry_cache[s_id] = all_records

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
            vs_dim_map = {}
            compiled_coords = {}
            saved_var_attrs = {}  
            
            def build_master_attrs(trace, var_obj, config_var, unique_srcs=None):
                mattrs = {}
                for k, v in trace.get("hw_attrs", {}).items():
                    mattrs[k] = v.get("data") if isinstance(v, dict) else v
                for k, v in trace.get("vmap_attrs", {}).items():
                    mattrs[k] = v.get("data") if isinstance(v, dict) else v
                if var_obj:
                    for k, v in var_obj.get("attributes", {}).items():
                        mattrs[k] = v.get("data") if isinstance(v, dict) else v
                
                native_u = mattrs.get("native_units") or mattrs.get("units")
                target_u = None
                
                for k, v in config_var.get("attributes", {}).items():
                    unpacked_val = v.get("data") if isinstance(v, dict) else v
                    if k == "units": target_u = unpacked_val
                    mattrs[k] = unpacked_val
                    
                mattrs["instrument_source"] = trace.get("source_id", "Unknown")
                mattrs["variablemap_source"] = trace.get("vmap_def_id", "Unknown")
                mattrs["raw_variable_name"] = trace.get("raw_variable_name", "Unknown")
                if unique_srcs: mattrs["sources"] = ", ".join(sorted(list(unique_srcs)))
                
                return mattrs, native_u, target_u

            for var in config.get("variables", []):
                if "static_value" in var: continue
                out_name = var["name"]
                source_def = var.get("source", {})
                fetch_list = source_def.get("inputs", {}) if "calculate_method" in source_def else {"primary": source_def}
                primary_source = fetch_list.get("primary", next(iter(fetch_list.values()), {}))
                vs_id = primary_source.get("variableset_id")
                vs_var = primary_source.get("variable_name")
                trace = variable_tracing_registry.get((vs_id, vs_var), {})
                
                # Maps natively discovered hardware dimensions to defined dataset coordinates
                if trace.get("is_coordinate"):
                    raw_name = trace.get("raw_variable_name", vs_var)
                    vs_dim_map[(vs_id, raw_name)] = out_name
                    vs_dim_map[raw_name] = out_name
                    vs_dim_map[vs_var] = out_name

            data_arrays = []
            for var in config.get("variables", []):
                out_name = var["name"]
                if "static_value" in var: continue
                
                source_def = var.get("source", {})
                is_calculated = "calculate_method" in source_def
                fetch_list = source_def.get("inputs", {}) if is_calculated else {"primary": source_def}
                
                primary_source = fetch_list.get("primary", next(iter(fetch_list.values()), {}))
                primary_vs_id = primary_source.get("variableset_id")
                primary_vs_var = primary_source.get("variable_name")
                trace_key = (primary_vs_id, primary_vs_var)
                trace = variable_tracing_registry.get(trace_key, {})
                
                if trace.get("is_coordinate") and not is_calculated:
                    vs_def = vs_defs_cache.get(primary_vs_id, {})
                    native_vars = _extract_vars_dict(vs_def)
                    
                    static_data = trace.get("static_data", [])
                    if not static_data:
                        _, coord_var_obj = _find_var_def(native_vars, primary_vs_var)
                        if coord_var_obj:
                            static_data = coord_var_obj.get("data", [])
                        
                    _, coord_var_obj = _find_var_def(native_vars, primary_vs_var)
                    raw_dims = coord_var_obj.get("shape", [out_name]) if coord_var_obj else [out_name]
                    dims = [vs_dim_map.get((primary_vs_id, d), vs_dim_map.get(d, d)) for d in raw_dims]
                    
                    L.debug(f"DEBUG PASS 4 [COORD]: Compiling Coordinate '{out_name}'. Dims={dims}. static_data length={len(static_data)}")
                    
                    try:
                        mattrs, native_units, target_units = build_master_attrs(trace, coord_var_obj, var)
                        
                        # --- THE FIX: Perform conversion before Xarray builds the Pandas.Index ---
                        if native_units and target_units and (native_units != target_units):
                            try:
                                norm_native = self.normalize_unit_string(native_units)
                                norm_target = self.normalize_unit_string(target_units)
                                data_quantity = ureg.Quantity(static_data, norm_native)
                                converted = data_quantity.to(norm_target).magnitude
                                
                                # Store as a strict float32 numpy array to prevent precision drift
                                static_data = np.array(converted, dtype=np.float32)
                                mattrs["units"] = target_units
                            except Exception as e:
                                L.error(f"Unit conversion failed for coordinate {out_name}: {e}")
                                mattrs["units"] = f"{native_units} (CONVERSION FAILED)"
                        else:
                            static_data = np.array(static_data, dtype=np.float32)
                        # -------------------------------------------------------------------------
                        
                        da_coord = xr.DataArray(data=static_data, dims=dims)
                        
                        if "time" in da_coord.dims and not pd.Index(da_coord.time.values).is_unique:
                            da_coord = da_coord.groupby("time").mean(dim="time", keep_attrs=True)

                        da_coord.attrs.update(mattrs)

                        compiled_coords[out_name] = da_coord.values

                        saved_var_attrs[out_name] = da_coord.attrs.copy()
                        ds_coord = xr.Dataset(coords={out_name: da_coord})
                        data_arrays.append(ds_coord)
                    except Exception as coord_err:
                        L.error(f"DEBUG PASS 4 [COORD ERROR]: Failed to construct DataArray for {out_name}. Error: {coord_err}")

                    continue

                input_arrays = {}
                unique_sources = set()
                for param_name, input_source in fetch_list.items():
                    vs_id = input_source.get("variableset_id")
                    vs_var = input_source.get("variable_name")
                    
                    trace_key = (vs_id, vs_var)
                    if trace_key not in variable_tracing_registry: continue
                    trace = variable_tracing_registry[trace_key]
                    
                    records = bulk_telemetry_cache.get(trace["source_id"], [])
                    times, values = [], []
                    raw_key = trace["raw_variable_name"]
                    v_type = var.get("type", "float")

                    for r in records:
                        r_vars = r.get("variables", {})
                        
                        target_key = raw_key
                        if target_key not in r_vars:
                            for k in r_vars.keys():
                                if k.lower() == raw_key.lower():
                                    target_key = k
                                    break

                        if "time" in r_vars and target_key in r_vars:
                            val = r_vars[target_key].get("data")
                            
                            if len(values) == 0:
                                val_preview = str(val)[:200] if val is not None else "None"
                                L.info(f"CHECKPOINT 1 [RAW TELEMETRY INGEST] {out_name} -> target_key='{target_key}': type={type(val).__name__}, preview={val_preview}")

                            if val is None or val == "":
                                val = np.nan
                            else:
                                if isinstance(val, str):
                                    val_s = val.strip()
                                    if val_s.startswith("[") and val_s.endswith("]"):
                                        try:
                                            val = json.loads(val_s)
                                        except Exception:
                                            pass
                                    elif "," in val_s:
                                        val = val_s.split(",")
                                        
                                if isinstance(val, list):
                                    clean_val = []
                                    for v in val:
                                        if v is None or str(v).strip() == "":
                                            clean_val.append(np.nan)
                                        else:
                                            try:
                                                clean_val.append(float(v))
                                            except (ValueError, TypeError):
                                                clean_val.append(np.nan)
                                    val = clean_val
                                    
                                elif not isinstance(val, (list, np.ndarray)):
                                    if v_type in ["float", "double"] and not isinstance(val, float):
                                        try:
                                            val = float(val)
                                        except (ValueError, TypeError):
                                            val = np.nan
                                    elif v_type in ["int", "integer"] and not isinstance(val, int):
                                        try:
                                            val = int(float(val))
                                        except (ValueError, TypeError):
                                            val = np.nan

                            parsed_time = r.get("_parsed_time")
                            if parsed_time is not None:
                                times.append(parsed_time)
                                values.append(val)
                            
                            hw_source = r_vars[target_key].get("attributes", {}).get("source_id", {}).get("data")
                            if hw_source: unique_sources.add(hw_source)

                    if times:
                        input_arrays[param_name] = {"values": values, "times": times}

                if not input_arrays: continue

                if is_calculated:
                    action_module = source_def["calculate_method"]["action_module"]
                    action_def = source_def["calculate_method"]["action_def"]
                    
                    # --- THE FIX: Align independent telemetry streams by time before calculating ---
                    df_inputs = pd.DataFrame()
                    for param_k, param_v in input_arrays.items():
                        s = pd.Series(data=param_v["values"], index=param_v["times"], name=param_k)
                        # Drop duplicate times to prevent reindexing issues
                        s = s[~s.index.duplicated(keep='first')]
                        if df_inputs.empty:
                            df_inputs = s.to_frame()
                        else:
                            df_inputs = df_inputs.join(s, how="outer")
                            
                    # Missing gaps are intentionally left as NaNs to prevent averaging bias
                    
                    math_params = {col: df_inputs[col].tolist() for col in df_inputs.columns}
                    aligned_times = df_inputs.index.values

                    calc_result = await self.execute_calculation(action_module, action_def, math_params)
                    if not calc_result: continue
                    final_values = calc_result.get(out_name)
                    final_times = aligned_times
                else:
                    final_values = input_arrays["primary"]["values"]
                    final_times = input_arrays["primary"]["times"]

                vs_def = vs_defs_cache.get(primary_vs_id, {})
                native_vars = _extract_vars_dict(vs_def)
                
                _, primary_var_obj = _find_var_def(native_vars, primary_vs_var)
                if not primary_var_obj and primary_vs_id in vs_to_hardware_map:
                    vmap_vars = _extract_vars_dict(vs_to_hardware_map[primary_vs_id])
                    _, primary_var_obj = _find_var_def(vmap_vars, primary_vs_var)

                raw_dims = primary_var_obj.get("shape") if primary_var_obj else None
                if not raw_dims:
                    raw_dims = trace.get("shape", ["time"])
                    
                dims = [vs_dim_map.get((primary_vs_id, d), vs_dim_map.get(d, d)) for d in raw_dims]

                coords = {"time": final_times}
                for dim in dims:
                    if dim != "time":
                        if dim in compiled_coords:
                            coords[dim] = compiled_coords[dim]
                        else:
                            L.warning(f"Coordinate '{dim}' not found in compiled_coords for {out_name}.")
                            coords[dim] = []

                if len(dims) > 1 and "time" in dims:
                    second_dim_name = dims[1] if dims[0] == "time" else dims[0]
                    expected_dim_len = len(coords.get(second_dim_name, []))
                    
                    if isinstance(final_values, (list, np.ndarray)) and len(final_values) > 0:
                        try:
                            padded = []
                            for v in final_values:
                                if isinstance(v, (list, np.ndarray)):
                                    v_list = list(v) if isinstance(v, np.ndarray) else v
                                    if expected_dim_len > 0 and len(v_list) < expected_dim_len:
                                        v_padded = v_list + [np.nan] * (expected_dim_len - len(v_list))
                                    elif expected_dim_len > 0 and len(v_list) > expected_dim_len:
                                        v_padded = v_list[:expected_dim_len]
                                    else:
                                        v_padded = v_list
                                    padded.append(v_padded)
                                else:
                                    fill_len = expected_dim_len if expected_dim_len > 0 else 1
                                    padded.append([np.nan] * fill_len)
                            final_values = np.array(padded, dtype=np.float32)
                        except Exception as pad_err:
                            L.warning(f"Padding failed for {out_name}", extra={"error": str(pad_err)})
                    else:
                        final_values = np.empty((len(final_values), expected_dim_len), dtype=np.float32)
                        final_values[:] = np.nan
                else:
                    try:
                        final_values = np.array(final_values, dtype=np.float32)
                    except ValueError:
                        pass

                if isinstance(final_values, np.ndarray):
                    v_size = final_values.size
                    v_non_nan = np.count_nonzero(~np.isnan(final_values))
                    v_non_zero = np.count_nonzero((~np.isnan(final_values)) & (final_values != 0))
                    
                    active_slice = []
                    if final_values.ndim > 1:
                        valid_row_idxs = np.where(~np.isnan(final_values).all(axis=1))[0]
                        if len(valid_row_idxs) > 0:
                            target_row = final_values[valid_row_idxs[0]]
                            nz = target_row[(~np.isnan(target_row)) & (target_row != 0)]
                            active_slice = nz[:10].tolist() if len(nz) > 0 else target_row[:10].tolist()
                    else:
                        nz = final_values[(~np.isnan(final_values)) & (final_values != 0)]
                        active_slice = nz[:10].tolist() if len(nz) > 0 else final_values[:10].tolist()

                    L.info(f"CHECKPOINT 2 [PRE-XARRAY MATRIX] {out_name}: shape={final_values.shape}, non_nan={v_non_nan}/{v_size}, non_zero={v_non_zero}, active_sample={active_slice}")

                try:
                    da = xr.DataArray(data=final_values, coords=coords, dims=dims, name=out_name)
                    
                    if "time" in da.dims and not pd.Index(da.time.values).is_unique:
                        da = da.groupby("time").mean(dim="time", keep_attrs=True)
                    
                    if "coordinates" in var:
                        for custom_dim, custom_grid in var["coordinates"].items():
                            if custom_dim in da.dims:
                                L.info(f"Rebinning {out_name} along {custom_dim}")
                                da = da.interp(
                                    {custom_dim: custom_grid}, 
                                    method="linear", 
                                    kwargs={"fill_value": np.nan}
                                )

                    mattrs, native_units, target_units = build_master_attrs(trace, primary_var_obj, var, unique_sources)
                    da.attrs.update(mattrs)
                    
                    if native_units and target_units and (native_units != target_units):
                        try:
                            norm_native = self.normalize_unit_string(native_units)
                            norm_target = self.normalize_unit_string(target_units)
                            data_quantity = ureg.Quantity(da.values, norm_native)
                            da.values = data_quantity.to(norm_target).magnitude
                            da.attrs["units"] = target_units
                        except Exception as e:
                            L.error(f"Unit conversion failed for {out_name}: {e}")
                            da.attrs["units"] = f"{native_units} (CONVERSION FAILED)"

                    saved_var_attrs[out_name] = da.attrs.copy()
                    data_arrays.append(da)
                except Exception as array_err:
                    L.error(f"DEBUG PASS 4 [VAR ERROR]: Failed to construct DataArray for {out_name}. Mismatch between coords and final_values shape. Error: {array_err}")

            if not data_arrays:
                L.warning("No data extracted for any target variables. Building blank schema.", extra={"dataset_id": dataset_id})
                ds = xr.Dataset()
            else:
                ds = xr.merge(data_arrays, join='outer', combine_attrs='drop')
            
            static_vars = {k: v for k, v in ds.variables.items() if "time" not in v.dims}
            
            if "time" in ds.dims:
                ds = ds.groupby("time").mean(dim="time", keep_attrs=True)
            
            half_base = freq_sec / 2.0
            if "time" in ds.dims and len(ds.time) > 0:
                aligned_ds = ds.resample(time=f"{freq_sec}s", closed="left", label="right", offset=f"{half_base}s").mean(dim="time", keep_attrs=True)
                if len(aligned_ds.time) > 0:
                    aligned_ds.coords["time"] = aligned_ds.time - pd.Timedelta(seconds=half_base)
            else:
                aligned_ds = ds

            for k, v in static_vars.items():
                if k not in aligned_ds.variables:
                    aligned_ds[k] = v

            master_time = pd.date_range(start=start_time.replace("Z", ""), end=end_time.replace("Z", ""), freq=f"{freq_sec}s", inclusive="left")
            aligned_ds = aligned_ds.reindex(time=master_time)

            for var in config.get("variables", []):
                if "static_value" in var: continue
                out_name = var["name"]
                
                if out_name not in aligned_ds.variables:
                    L.warning(f"Variable '{out_name}' missing from telemetry. Injecting NaN placeholder array.", extra={"dataset_id": dataset_id})
                    
                    source_def = var.get("source", {})
                    is_calc = "calculate_method" in source_def
                    fetch_list = source_def.get("inputs", {}) if is_calc else {"primary": source_def}
                    primary_source = fetch_list.get("primary", next(iter(fetch_list.values()), {}))
                    primary_vs_id = primary_source.get("variableset_id")
                    primary_vs_var = primary_source.get("variable_name")
                    
                    trace_key = (primary_vs_id, primary_vs_var)
                    trace = variable_tracing_registry.get(trace_key, {})
                    
                    vs_def = vs_defs_cache.get(primary_vs_id, {})
                    native_vars = _extract_vars_dict(vs_def)
                    
                    _, primary_var_obj = _find_var_def(native_vars, primary_vs_var)
                    if not primary_var_obj and primary_vs_id in vs_to_hardware_map:
                        vmap_vars = _extract_vars_dict(vs_to_hardware_map[primary_vs_id])
                        _, primary_var_obj = _find_var_def(vmap_vars, primary_vs_var)

                    raw_dims = primary_var_obj.get("shape") if primary_var_obj else None
                    if not raw_dims:
                        raw_dims = trace.get("shape", ["time"])
                        
                    dims = [vs_dim_map.get((primary_vs_id, d), vs_dim_map.get(d, d)) for d in raw_dims]
                    
                    coords = {}
                    shape = []
                    for dim in dims:
                        if dim == "time":
                            coords["time"] = master_time
                            shape.append(len(master_time))
                        elif dim in compiled_coords:
                            coords[dim] = compiled_coords[dim]
                            shape.append(len(compiled_coords[dim]))
                        else:
                            coords[dim] = [0]
                            shape.append(1)
                            
                    if "coordinates" in var:
                        for custom_dim, custom_grid in var["coordinates"].items():
                            if custom_dim not in dims:
                                dims.append(custom_dim)
                                coords[custom_dim] = custom_grid
                                shape.append(len(custom_grid))
                            else:
                                idx = dims.index(custom_dim)
                                coords[custom_dim] = custom_grid
                                shape[idx] = len(custom_grid)
                            
                    empty_da = xr.DataArray(data=np.full(shape, np.nan, dtype=np.float32), coords=coords, dims=dims, name=out_name)
                    
                    mattrs, _, _ = build_master_attrs(trace, primary_var_obj, var)
                    empty_da.attrs.update(mattrs)
                        
                    saved_var_attrs[out_name] = empty_da.attrs.copy()
                    aligned_ds[out_name] = empty_da

            for var_name in list(aligned_ds.data_vars.keys()):
                if var_name in ["time", "latitude", "longitude", "altitude"] or var_name.startswith("qc_"): continue
                
                qc_da = xr.DataArray(data=np.zeros(len(master_time), dtype=np.int32), coords={"time": master_time}, dims=["time"], name=f"qc_{var_name}")
                qc_da.attrs["long_name"] = f"Quality check results on field: {aligned_ds[var_name].attrs.get('long_name', var_name)}"
                qc_da.attrs["units"] = "1"
                qc_da.attrs["standard_name"] = "quality_flag"
                qc_da.attrs["flag_masks"] = [1, 2, 4, 8]
                qc_da.attrs["flag_meanings"] = "value_less_than_valid_min value_greater_than_valid_max sensor_offline flatline_detected"
                saved_var_attrs[f"qc_{var_name}"] = qc_da.attrs.copy()
                aligned_ds[f"qc_{var_name}"] = qc_da

            for var in config.get("variables", []):
                if "static_value" in var:
                    out_name = var["name"]
                    da = xr.DataArray(data=np.full(len(master_time), var["static_value"]), coords={"time": master_time}, dims=["time"])
                    for attr_key, attr_val in var.get("attributes", {}).items(): da.attrs[attr_key] = attr_val
                    saved_var_attrs[out_name] = da.attrs.copy()
                    aligned_ds[out_name] = da

            # STRICT ATTRIBUTE RESTORATION: Re-inject saved variable attributes back into aligned_ds
            for var_name in aligned_ds.variables:
                if var_name in saved_var_attrs:
                    aligned_ds[var_name].attrs.update(saved_var_attrs[var_name])

            # -----------------------------------------------------------------
            # PASS 5: Resolve GitOps Context & Global Metadata
            # -----------------------------------------------------------------
            primary_platform = None
            for vmap in vs_to_hardware_map.values():
                p_ref = vmap.get("variablemap_type_id")
                if not p_ref:
                    p_ref = vmap.get("data", {}).get("attributes", {}).get("platform")
                if isinstance(p_ref, dict): 
                    p_ref = p_ref.get("data")
                if p_ref:
                    primary_platform = p_ref
                    break

            resolved_project = "Unknown Project"
            resolved_project_ref = "Unallocated"
            resolved_deployment = "Unknown Deployment"
            resolved_deployment_ref = "Unallocated"

            try:
                target_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                
                ids_resp = await self.client.get("/deployment-definition/registry/ids/get/")
                if ids_resp.status_code == 200 and "results" in ids_resp.json():
                    dep_ids = ids_resp.json()["results"]
                    
                    async def fetch_dep(dep_id):
                        return await self.client.get("/deployment-definition/registry/get/", params={"name": dep_id})
                    
                    dep_responses = await asyncio.gather(*(fetch_dep(did) for did in dep_ids))
                    
                    valid_deps = []
                    for resp in dep_responses:
                        if resp.status_code == 200 and resp.json().get("results"):
                            dep = resp.json()["results"][0]
                            d_data = dep.get("data", {})
                            
                            p_ref_from_dep = d_data.get("platform_ref")
                            if primary_platform and p_ref_from_dep != primary_platform:
                                continue

                            d_start = d_data.get("planned_start_time", "1970-01-01T00:00:00Z")
                            d_end = d_data.get("actual_end_time", d_data.get("planned_end_time", "9999-12-31T23:59:59Z"))
                            try:
                                dt_s = datetime.fromisoformat(d_start.replace("Z", "+00:00"))
                                dt_e = datetime.fromisoformat(d_end.replace("Z", "+00:00"))
                                if dt_s <= target_dt <= dt_e:
                                    valid_deps.append(dep)
                            except ValueError:
                                pass
                    
                    if valid_deps:
                        active_dep = valid_deps[0]
                        dep_data = active_dep.get("data", {})
                        resolved_deployment = dep_data.get("display_name", "Unknown Deployment")
                        resolved_deployment_ref = active_dep.get("metadata", {}).get("name", "Unallocated")
                        
                        proj_ref = dep_data.get("project_ref")
                        if proj_ref:
                            proj_resp = await self.client.get("/project-definition/registry/get/", params={"name": proj_ref})
                            if proj_resp.status_code == 200 and proj_resp.json().get("results"):
                                p_data = proj_resp.json()["results"][0].get("data", {})
                                resolved_project = p_data.get("display_name", proj_ref)
                                resolved_project_ref = proj_ref
                        else:
                            proj_ids_resp = await self.client.get("/project-definition/registry/ids/get/")
                            if proj_ids_resp.status_code == 200 and "results" in proj_ids_resp.json():
                                proj_ids = proj_ids_resp.json()["results"]
                                async def fetch_proj(pid):
                                    return await self.client.get("/project-definition/registry/get/", params={"name": pid})
                                proj_responses = await asyncio.gather(*(fetch_proj(pid) for pid in proj_ids))
                                
                                for resp in proj_responses:
                                    if resp.status_code == 200 and resp.json().get("results"):
                                        proj = resp.json()["results"][0]
                                        p_data = proj.get("data", {})
                                        p_start = p_data.get("planned_start_time", "1970-01-01T00:00:00Z")
                                        p_end = p_data.get("actual_end_time", p_data.get("planned_end_time", "9999-12-31T23:59:59Z"))
                                        try:
                                            dt_s = datetime.fromisoformat(p_start.replace("Z", "+00:00"))
                                            dt_e = datetime.fromisoformat(p_end.replace("Z", "+00:00"))
                                            if dt_s <= target_dt <= dt_e:
                                                resolved_project = p_data.get("display_name", proj.get("metadata", {}).get("name"))
                                                resolved_project_ref = proj.get("metadata", {}).get("name", "Unallocated")
                                                break
                                        except ValueError:
                                            pass

            except Exception as e:
                L.error("Failed to resolve GitOps metadata", extra={"reason": str(e)})

            # RESET GLOBAL ATTRIBUTES: Clear out all variable-level residue
            aligned_ds.attrs = {}

            # Collect global deduplicated sources and variablemaps
            all_sources = sorted(list(telemetry_sources_to_fetch.keys()))
            all_vmaps = sorted(list(set(vmap.get("variablemap_definition_id") for vmap in vs_to_hardware_map.values() if vmap.get("variablemap_definition_id"))))

            # Compile strictly clean global dataset metadata
            aligned_ds.attrs["title"] = config.get("attributes", {}).get("title", f"Dataset: {dataset_id}")
            aligned_ds.attrs["project"] = resolved_project
            aligned_ds.attrs["project_ref"] = resolved_project_ref
            aligned_ds.attrs["platform"] = primary_platform if primary_platform else "Unknown Platform"
            aligned_ds.attrs["deployment"] = resolved_deployment
            aligned_ds.attrs["deployment_ref"] = resolved_deployment_ref
            aligned_ds.attrs["variablemaps"] = ", ".join(all_vmaps) if all_vmaps else "None"
            aligned_ds.attrs["sources"] = ", ".join(all_sources) if all_sources else "None"
            aligned_ds.attrs["history"] = f"Generated {datetime.utcnow().isoformat()}Z"
            
            conv_cfg = config.get("conventions", {})
            aligned_ds.attrs["Conventions"] = conv_cfg.get("name", "CF-1.8")
            
            # Enforce CF featureType: trajectory for mobile lat/lon paths, timeSeries for static
            if "featureType" in conv_cfg:
                aligned_ds.attrs["featureType"] = conv_cfg["featureType"]
            elif "latitude" in aligned_ds.variables or "longitude" in aligned_ds.variables:
                aligned_ds.attrs["featureType"] = "trajectory"
            else:
                aligned_ds.attrs["featureType"] = "timeSeries"

            # Pass through non-variable explicit config global attributes
            excluded_global_attrs = {"evaluate_by", "units", "standard_name", "instrument_source", "variablemap_source", "raw_variable_name"}
            for attr_key, attr_val in config.get("attributes", {}).items():
                if attr_key not in excluded_global_attrs:
                    unpacked_val = attr_val.get("data") if isinstance(attr_val, dict) else attr_val
                    aligned_ds.attrs[attr_key] = unpacked_val

            safe_start = start_time.replace(":", "").replace("-", "")
            filename = f"{dataset_id}.{safe_start}.nc"
            filepath = os.path.join(self.output_dir, filename)

            aligned_ds.to_netcdf(filepath, engine="netcdf4", format="NETCDF4")

            # Force the output to go to the 'raw' stage so QC picks it up!
            storage_url = f"http://dataset-storage.{self.daq_id}-system.svc.cluster.local:80/upload/raw"
            
            try:
                async with httpx.AsyncClient() as client:
                    with open(filepath, "rb") as f:
                        files = {"file": (filename, f, "application/x-netcdf")}
                        resp = await client.post(storage_url, files=files, params={"dataset_id": dataset_id}, timeout=180.0)
                        resp.raise_for_status()
                L.info(f"Successfully pushed {filename} to central dataset-storage.")
                os.remove(filepath)
            except Exception as e:
                L.error("Failed to push to storage.", extra={"out_file": filename, "attempted_url": storage_url, "reason": str(e)})
            
            return filepath

        except Exception as e:
            L.error("Pipeline failure", extra={"reason": str(e)}, exc_info=True)
            raise e