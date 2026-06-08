import httpx
import logging
import json
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
        url = f"{self.base_url}/tabledap/{dataset_id}.json?{query_string}"
        
        self.logger.debug(f"ERDDAP Query: {url}")
        try:
            resp = await self.http.get(url)
            if resp.status_code == 404:
                return {"results": []}
                
            resp.raise_for_status()
            data = resp.json()
            
            table = data.get("table", {})
            cols = table.get("columnNames", [])
            rows = table.get("rows", [])
            
            return {"results": [dict(zip(cols, row)) for row in rows]}
            
        except Exception as e:
            self.logger.error("ERDDAP fetch failed", extra={"url": url, "reason": str(e)})
            return {"results": []}

    async def _discover_and_fetch_all_versions(self, make: str, model: str, query_args: List[str]) -> List[dict]:
        """Dynamically discovers all versioned datasets for a device and fetches them concurrently."""
        search_url = f"{self.base_url}/tabledap/allDatasets.json?datasetID&datasetID=~%22telemetry_{make}_{model}_v.*_time%22"
        dataset_ids = []
        
        try:
            # 1. Ask ERDDAP which versions exist (v1, v2, v3, etc.)
            resp = await self.http.get(search_url)
            if resp.status_code == 200:
                rows = resp.json().get("table", {}).get("rows", [])
                dataset_ids = [row[0] for row in rows]
        except Exception as e:
            self.logger.warning(f"Dataset discovery failed for {make} {model}, falling back to hardcoded versions. Reason: {e}")
            
        # Fallback just in case the allDatasets query fails
        if not dataset_ids:
            dataset_ids = [f"telemetry_{make}_{model}_v1_time", f"telemetry_{make}_{model}_v2_time"]

        # 2. Fetch all discovered versions concurrently to prevent pipeline slowdowns
        tasks = [self._fetch_tabledap(ds_id, query_args) for ds_id in dataset_ids]
        results = await asyncio.gather(*tasks)
        
        # 3. Combine the flat data from all versions
        combined_flat_data = []
        for res in results:
            combined_flat_data.extend(res.get("results", []))
            
        # 4. Sort strictly by time to perfectly stitch the v1->v2 transitions together
        combined_flat_data.sort(key=lambda x: x.get("time", ""))
        return combined_flat_data

    # ---------------------------------------------------------
    # TELEMETRY QUERIES
    # ---------------------------------------------------------
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

        query_args = []
        if sn:
            # Removed quotes in case ERDDAP typed this column as numeric
            query_args.append(f'serial_number={sn}')
            
        if request.start_timestamp:
            # Strictly encode the timestamp and the > sign (%3E)
            safe_start = urllib.parse.quote(request.start_timestamp)
            query_args.append(f"time%3E={safe_start}")
            
        if request.end_timestamp:
            # Strictly encode the timestamp and the < sign (%3C)
            safe_end = urllib.parse.quote(request.end_timestamp)
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
            query_args.append(f'serial_number={sn}')
            
        if request.start_timestamp:
            # Strictly encode the timestamp and the > sign (%3E)
            safe_start = urllib.parse.quote(request.start_timestamp)
            query_args.append(f"time%3E={safe_start}")
            
        if request.end_timestamp:
            # Strictly encode the timestamp and the < sign (%3C)
            safe_end = urllib.parse.quote(request.end_timestamp)
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
            
        query_args.append("orderByLimitMax(%22-time%22)")
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
            
        query_args.append("orderByLimitMax(%22-time%22)")
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
    async def sampling_definition_registry_get(self, resource: str, query: dict) -> dict:
        """Fetches historical operational definitions (conditions, modes) from ERDDAP."""
        dataset_id = "envds_ops_registry"
        
        kind_map = {
            "samplingcondition": "SamplingCondition",
            "samplingmode": "SamplingMode",
            "samplingstate": "SamplingState",
            "systemmode": "SystemMode"
        }
        kind = kind_map.get(resource, resource)
        
        query_args = [f'kind="{kind}"']
        
        if "name" in query and query["name"]:
            name_part = query["name"].split("::")[0]
            query_args.append(f'name="{name_part}"')
            
        query_args.append("orderByLimitMax(%22-time%22)")
        result = await self._fetch_tabledap(dataset_id, query_args)
        
        parsed_results = []
        for row in result.get("results", []):
            try:
                payload = json.loads(row.get("payload", "{}"))
                parsed_results.append(payload)
            except Exception:
                continue
                
        return {"results": parsed_results}