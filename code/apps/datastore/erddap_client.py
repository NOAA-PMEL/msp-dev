import httpx
import logging
import json
import asyncio
import urllib.parse
from typing import List, Dict, Any

from datastore_requests import (
    DataRequest,
    ControllerDataRequest,
    VariableSetDataRequest,
    DeviceDefinitionRequest,
    ControllerDefinitionRequest,
    VariableMapDefinitionRequest,
    VariableSetDefinitionRequest
)
from db_client import DBClientConfig

class ErddapClient:
    def __init__(self, config: DBClientConfig):
        self.config = config.config
        self.base_url = self.config.get("erddap_http_connection", "http://erddap-sidecar:8000/erddap")
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(self.config.get("log_level", "INFO").upper())
        self.http = httpx.AsyncClient(timeout=30.0)

    async def _fetch_tabledap(self, dataset_id: str, query_args: List[str]) -> Dict[str, Any]:
        """Executes a query against ERDDAP's tabledap endpoint."""
        query_string = "&".join(query_args)
        url = f"{self.base_url}/tabledap/{dataset_id}.json?&{query_string}"
        
        self.logger.debug(f"ERDDAP Query: {url}")
        try:
            resp = await self.http.get(url)
            if resp.status_code == 404:
                return {"dataset_id": dataset_id, "results": []}
                
            resp.raise_for_status()
            data = resp.json()
            
            table = data.get("table", {})
            cols = table.get("columnNames", [])
            rows = table.get("rows", [])
            
            return {"dataset_id": dataset_id, "results": [dict(zip(cols, row)) for row in rows]}
            
        except Exception as e:
            self.logger.error("ERDDAP fetch failed", extra={"url": url, "reason": str(e)})
            return {"dataset_id": dataset_id, "results": []}

    async def _discover_and_fetch_all_versions(self, make: str, model: str, query_args: List[str]) -> List[dict]:
        """Dynamically discovers all versioned and shape-split datasets for a device and fetches them concurrently."""
        safe_make = make.replace("-", "_")
        safe_model = model.replace("-", "_")
        search_url = f"{self.base_url}/tabledap/allDatasets.json?datasetID&datasetID=~%22telemetry_{safe_make}_{safe_model}_v.*%22"
        dataset_ids = []
        
        try:
            resp = await self.http.get(search_url)
            if resp.status_code == 200:
                rows = resp.json().get("table", {}).get("rows", [])
                dataset_ids = [row[0] for row in rows]
        except Exception as e:
            self.logger.warning(f"Dataset discovery failed for {make} {model}. Reason: {e}")
            
        if not dataset_ids:
            dataset_ids = [f"telemetry_{safe_make}_{safe_model}_v1_time"]

        tasks = [self._fetch_tabledap(ds_id, query_args) for ds_id in dataset_ids]
        return await asyncio.gather(*tasks)
    
    # ---------------------------------------------------------
    # TELEMETRY QUERIES
    # ---------------------------------------------------------
    async def device_data_get(self, request: DataRequest, definition: dict = None) -> dict:
        """Fetches historical device telemetry natively from ERDDAP and explicitly un-flattens dimensions."""
        make = request.make
        model = request.model
        sn = request.serial_number
        
        if request.device_id and "::" in request.device_id:
            parts = request.device_id.split("::")
            if not make and len(parts) > 0: make = parts[0]
            if not model and len(parts) > 1: model = parts[1]
            if not sn and len(parts) > 2: sn = parts[2]

        self.logger.warning(f"BUILDING DATASET ID: make={make}, model={model}, sn={sn}, raw_device_id={request.device_id}")

        query_args = []
        if sn:
            query_args.append(f'serial_number=%22{sn}%22')
            
        if request.start_time:
            safe_start = urllib.parse.quote(request.start_time)
            query_args.append(f"time%3E={safe_start}")
            
        if request.end_time:
            safe_end = urllib.parse.quote(request.end_time)
            query_args.append(f"time%3C={safe_end}")
            
        query_args.append("orderBy(%22time%22)")

        # Fetch all shape dataset slices. Returns a list of dicts: {"dataset_id": str, "results": list}
        datasets_data = await self._discover_and_fetch_all_versions(make, model, query_args)

        merged_records = {}
        for ds_data in datasets_data:
            dataset_id = ds_data.get("dataset_id")
            rows = ds_data.get("results", [])
            if not rows: continue
            
            # Parse shape dimensions exactly from the ERDDAP dataset ID (e.g., telemetry_TSI_APS3321_v5_time_diameter)
            parts = dataset_id.split("_v")
            if len(parts) < 2: continue
            shape_str = parts[1].split("_", 1)[1] if "_" in parts[1] else "time"
            shape_dims = shape_str.split("_")
            
            sys_cols = {"timestamp", "author", "command", "make", "model", "format_version", "serial_number"}
            all_cols = list(rows[0].keys())
            var_cols = [c for c in all_cols if c not in sys_cols and c not in shape_dims and not c.endswith("_dim")]
            
            # 1. Group rows perfectly using the known shape dimensions
            grouped = {}
            for row in rows:
                time_key = row.get("time")
                if not time_key: continue
                
                if time_key not in merged_records:
                    merged_records[time_key] = {"variables": {}}
                    for sc in sys_cols:
                        if sc in row:
                            merged_records[time_key]["variables"][sc] = {"data": row[sc]}
                            
                current_level = grouped
                for i, dim in enumerate(shape_dims):
                    dim_val = row.get(dim)
                    if i == len(shape_dims) - 1:
                        current_level[dim_val] = row 
                    else:
                        if dim_val not in current_level:
                            current_level[dim_val] = {}
                        current_level = current_level[dim_val]
            
            # 2. Extract perfectly unflattened N-dimensional arrays
            def extract_array(node, var_name, dims_left):
                if not dims_left:
                    val = node.get(var_name)
                    if val == "NaN" or val == "": return None
                    if isinstance(val, str) and "," in val and var_name not in sys_cols:
                        try:
                            val = [float(x) for x in val.split(",")]
                        except ValueError:
                            pass
                    return val
                return [extract_array(node[k], var_name, dims_left[1:]) for k in sorted(node.keys())]

            for time_val, time_node in grouped.items():
                if time_val not in merged_records: continue
                rec_vars = merged_records[time_val]["variables"]
                
                remaining_dims = shape_dims[1:] # Strip 'time' which is our base level
                
                for v_name in var_cols:
                    if len(shape_dims) == 1:
                        val = time_node.get(v_name)
                        if val == "NaN" or val == "": val = None
                        rec_vars[v_name] = {"data": val}
                    else:
                        rec_vars[v_name] = {"data": extract_array(time_node, v_name, remaining_dims)}

        formatted_results = list(merged_records.values())
        formatted_results.sort(key=lambda x: x.get("variables", {}).get("time", {}).get("data", ""))
            
        return {"results": formatted_results}
        
    async def _discover_and_fetch_all_versions(self, make: str, model: str, query_args: List[str]) -> List[dict]:
        """Dynamically discovers all versioned and shape-split datasets for a device and fetches them concurrently."""
        safe_make = make.replace("-", "_")
        safe_model = model.replace("-", "_")
        search_url = f"{self.base_url}/tabledap/allDatasets.json?datasetID&datasetID=~%22telemetry_{safe_make}_{safe_model}_v.*%22"
        dataset_ids = []
        
        try:
            # 1. Ask ERDDAP which shape datasets exist (v1_time, v1_time_diameter, etc.)
            resp = await self.http.get(search_url)
            if resp.status_code == 200:
                rows = resp.json().get("table", {}).get("rows", [])
                dataset_ids = [row[0] for row in rows]
        except Exception as e:
            self.logger.warning(f"Dataset discovery failed for {make} {model}. Reason: {e}")
            
        if not dataset_ids:
            dataset_ids = [f"telemetry_{safe_make}_{safe_model}_v1_time"]

        # 2. Fetch all discovered shape datasets concurrently
        tasks = [self._fetch_tabledap(ds_id, query_args) for ds_id in dataset_ids]
        results = await asyncio.gather(*tasks)
        
        # 3. Combine flat rows across datasets
        combined_flat_data = []
        for res in results:
            combined_flat_data.extend(res.get("results", []))
            
        return combined_flat_data
    
    # ---------------------------------------------------------
    # TELEMETRY QUERIES
    # ---------------------------------------------------------
    # async def device_data_get(self, request: DataRequest, definition: dict = None) -> dict:
    #     """Fetches historical device telemetry natively from ERDDAP and repacks it."""
    #     make = request.make
    #     model = request.model
    #     sn = request.serial_number
        
    #     if request.device_id and "::" in request.device_id:
    #         parts = request.device_id.split("::")
    #         if not make and len(parts) > 0: make = parts[0]
    #         if not model and len(parts) > 1: model = parts[1]
    #         if not sn and len(parts) > 2: sn = parts[2]

    #     self.logger.warning(f"BUILDING DATASET ID: make={make}, model={model}, sn={sn}, raw_device_id={request.device_id}")

    #     query_args = []
    #     if sn:
    #         # Removed quotes in case ERDDAP typed this column as numeric
    #         query_args.append(f'serial_number=%22{sn}%22')
            
    #     if request.start_time:  # <--- Was start_timestamp
    #         safe_start = urllib.parse.quote(request.start_time)
    #         query_args.append(f"time%3E={safe_start}")
            
    #     if request.end_time:    # <--- Was end_timestamp
    #         safe_end = urllib.parse.quote(request.end_time)
    #         query_args.append(f"time%3C={safe_end}")
            
    #     query_args.append("orderBy(%22time%22)")

    #     # Fetch and stitch all timeline versions automatically
    #     combined_flat_data = await self._discover_and_fetch_all_versions(make, model, query_args)

    #     # Repackage ERDDAP's flat data into the nested Datastore JSON format
    #     formatted_results = []
    #     for row in combined_flat_data:
    #         formatted_record = {"variables": {}}
    #         for key, val in row.items():
    #             formatted_record["variables"][key] = {"data": val}
    #         formatted_results.append(formatted_record)
            
    #     return {"results": formatted_results}

    async def device_data_get(self, request: DataRequest, definition: dict = None) -> dict:
        """Fetches historical device telemetry natively from ERDDAP and repacks it."""
        make = request.make
        model = request.model
        sn = request.serial_number
        
        if request.device_id and "::" in request.device_id:
            parts = request.device_id.split("::")
            if not make and len(parts) > 0: make = parts[0]
            if not model and len(parts) > 1: model = parts[1]
            if not sn and len(parts) > 2: sn = parts[2]

        self.logger.warning(f"BUILDING DATASET ID: make={make}, model={model}, sn={sn}, raw_device_id={request.device_id}")

        query_args = []
        if sn:
            query_args.append(f'serial_number=%22{sn}%22')
            
        if request.start_time:
            safe_start = urllib.parse.quote(request.start_time)
            query_args.append(f"time%3E={safe_start}")
            
        if request.end_time:
            safe_end = urllib.parse.quote(request.end_time)
            query_args.append(f"time%3C={safe_end}")
            
        query_args.append("orderBy(%22time%22)")

        # Fetch all shape dataset slices
        combined_flat_data = await self._discover_and_fetch_all_versions(make, model, query_args)

        # Merge rows by time timestamp so 1D and 2D variables unify into single records
        merged_records = {}
        for row in combined_flat_data:
            time_key = row.get("time")
            if not time_key: continue
            
            if time_key not in merged_records:
                merged_records[time_key] = {"variables": {}}
                
            rec_vars = merged_records[time_key]["variables"]
            for key, val in row.items():
                if val is None or val == "": continue
                if isinstance(val, str):
                    val_s = val.strip()
                    if val_s.startswith("[") and val_s.endswith("]"):
                        try:
                            val = json.loads(val_s)
                        except Exception:
                            pass
                    elif "," in val_s and key not in ["time", "make", "model", "serial_number", "timestamp"]:
                        try:
                            val = [float(x) for x in val_s.split(",")]
                        except ValueError:
                            pass
                rec_vars[key] = {"data": val}
            
        formatted_results = list(merged_records.values())
        formatted_results.sort(key=lambda x: x.get("variables", {}).get("time", {}).get("data", ""))
            
        return {"results": formatted_results}
    
    async def controller_data_get(self, request: ControllerDataRequest, definition: dict = None) -> dict:
        """Fetches historical controller telemetry natively from ERDDAP and repacks it."""
        make = request.make
        model = request.model
        sn = request.serial_number
        
        if request.controller_id and "::" in request.controller_id:
            parts = request.controller_id.split("::")
            if not make and len(parts) > 0: make = parts[0]
            if not model and len(parts) > 1: model = parts[1]
            if not sn and len(parts) > 2: sn = parts[2]

        query_args = []
        if sn:
            # Removed quotes in case ERDDAP typed this column as numeric
            query_args.append(f'serial_number=%22{sn}%22')
            
        if request.start_time:  # <--- Was start_timestamp
            safe_start = urllib.parse.quote(request.start_time)
            query_args.append(f"time%3E={safe_start}")
            
        if request.end_time:    # <--- Was end_timestamp
            safe_end = urllib.parse.quote(request.end_time)
            query_args.append(f"time%3C={safe_end}")
            
        query_args.append("orderBy(%22time%22)")

        # Fetch and stitch all timeline versions automatically
        combined_flat_data = await self._discover_and_fetch_all_versions(make, model, query_args)

        # Repackage ERDDAP's flat data into the nested Datastore JSON format
        formatted_results = []
        for row in combined_flat_data:
            formatted_record = {"variables": {}}
            for key, val in row.items():
                formatted_record["variables"][key] = {"data": val}
            formatted_results.append(formatted_record)
            
        return {"results": formatted_results}
        
    async def variableset_data_get(self, request: VariableSetDataRequest) -> dict:
        """Fetches historical curated L1 telemetry from ERDDAP."""
        dataset_id = f"telemetry_variableset_{request.variableset}".replace("-", "_")
        query_args = []
        
        if request.variableset_id:
            query_args.append(f'variableset_id="{request.variableset_id}"')
            
        if request.start_timestamp:
            query_args.append(f"time>={request.start_timestamp}")
        if request.end_timestamp:
            query_args.append(f"time<={request.end_timestamp}")
            
        query_args.append("orderBy(%22time%22)")
        return await self._fetch_tabledap(dataset_id, query_args)


    # ---------------------------------------------------------
    # HARDWARE & SYSTEM REGISTRY (Read-Through Cache)
    # ---------------------------------------------------------
    async def _fetch_hardware_registry(self, kind: str, query_id: str) -> dict:
        """Fetches device/controller hardware schemas using specific make/model columns."""
        dataset_id = "envds_hardware_registry" 
        query_args = [f'kind="{kind}"']
        
        if query_id:
            # Splitting 'make::model::version' to match the ERDDAP columns
            parts = query_id.split("::")
            if len(parts) >= 3:
                query_args.append(f'make="{parts[0]}"')
                query_args.append(f'model="{parts[1]}"')
                query_args.append(f'version="{parts[2]}"')
            
        # FIX: Group by identifiers and return the row with the max time
        query_args.append("orderByMax(%22make,model,version,time%22)")
        result = await self._fetch_tabledap(dataset_id, query_args)
        
        parsed_results = []
        for row in result.get("results", []):
            try:
                payload = json.loads(row.get("payload", "{}"))
                parsed_results.append(payload)
            except Exception:
                continue
                
        return {"results": parsed_results}

    async def _fetch_system_registry(self, kind: str, query_id: str) -> dict:
        """Fetches variablemaps/variablesets using the generic 'name' column."""
        dataset_id = "envds_system_registry" 
        query_args = [f'kind="{kind}"']
        
        if query_id:
            query_args.append(f'name="{query_id}"')
            
        # FIX: Group by name and return the row with the max time
        query_args.append("orderByMax(%22name,time%22)")
        result = await self._fetch_tabledap(dataset_id, query_args)
        
        parsed_results = []
        for row in result.get("results", []):
            try:
                payload = json.loads(row.get("payload", "{}"))
                parsed_results.append(payload)
            except Exception:
                continue
                
        return {"results": parsed_results}

    async def device_definition_registry_get(self, request: DeviceDefinitionRequest) -> dict:
        return await self._fetch_hardware_registry("device-definition", request.device_definition_id)

    async def controller_definition_registry_get(self, request: ControllerDefinitionRequest) -> dict:
        return await self._fetch_hardware_registry("controller-definition", request.controller_definition_id)

    async def variablemap_definition_registry_get(self, request: VariableMapDefinitionRequest) -> dict:
        return await self._fetch_system_registry("variablemap-definition", request.variablemap_definition_id)

    async def variableset_definition_registry_get(self, request: VariableSetDefinitionRequest) -> dict:
        return await self._fetch_system_registry("variableset-definition", request.variableset_definition_id)


    # ---------------------------------------------------------
    # OPERATIONS REGISTRY (Read-Through Cache)
    # ---------------------------------------------------------
    # async def sampling_definition_registry_get(self, resource: str, query: dict) -> dict:
    #     """Fetches historical operational definitions (conditions, modes) from ERDDAP."""
    #     dataset_id = "envds_ops_registry"
        
    #     kind_map = {
    #         "samplingcondition": "SamplingCondition",
    #         "samplingmode": "SamplingMode",
    #         "samplingstate": "SamplingState",
    #         "systemmode": "SystemMode"
    #     }
    #     kind = kind_map.get(resource, resource)
        
    #     query_args = [f'kind="{kind}"']
        
    #     if "name" in query and query["name"]:
    #         parts = query["name"].split("::")
            
    #         # Extract both namespace and name to guarantee strict node isolation
    #         if len(parts) >= 3:
    #             namespace_part = parts[0]
    #             name_part = parts[1]
    #             query_args.append(f'namespace="{namespace_part}"')
    #         else:
    #             name_part = parts[0] # Just in case a legacy call slips through
                
    #         query_args.append(f'name="{name_part}"')
            
    #     # FIX: Group by namespace and name, returning the row with the max time
    #     query_args.append("orderByMax(%22namespace,name,time%22)")
    #     result = await self._fetch_tabledap(dataset_id, query_args)
        
    #     parsed_results = []
    #     for row in result.get("results", []):
    #         try:
    #             payload = json.loads(row.get("payload", "{}"))
    #             parsed_results.append(payload)
    #         except Exception:
    #             continue
                
    #     return {"results": parsed_results}
    
    async def sampling_definition_registry_get(self, resource: str, query: dict) -> dict:
        """Fetches historical operational definitions (conditions, modes) from ERDDAP."""
        dataset_id = "envds_ops_registry"
        
        # Enforce the strict *-definition suffix
        kind = f"{resource}-definition"
        query_args = [f'kind="{kind}"']
        
        if "name" in query and query["name"]:
            parts = query["name"].split("::")
            name_part = parts[0] 
            query_args.append(f'name="{name_part}"')
            
        # Group by name, returning the row with the max time
        query_args.append("orderByMax(%22name,time%22)")
        result = await self._fetch_tabledap(dataset_id, query_args)
        
        parsed_results = []
        for row in result.get("results", []):
            try:
                payload = json.loads(row.get("payload", "{}"))
                
                # STRICT ENVELOPE STRIPPER
                if isinstance(payload, dict):
                    expected_keys = {resource, f"{resource}-definition"}
                    for w_key in expected_keys:
                        if w_key in payload:
                            payload = payload[w_key]
                            break
                            
                parsed_results.append(payload)
            except Exception:
                continue
                
        return {"results": parsed_results}