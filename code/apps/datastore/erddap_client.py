import httpx
import logging
import json
from typing import List, Dict, Any

from datastore_requests import (
    DataRequest,
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
        # We assume the ERDDAP sidecar is reachable via this config or a known internal DNS
        self.base_url = self.config.get("erddap_http_connection", "http://erddap-sidecar:8000/erddap")
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(self.config.get("log_level", "INFO").upper())
        self.http = httpx.AsyncClient(timeout=30.0)

    async def _fetch_tabledap(self, dataset_id: str, query_args: List[str]) -> Dict[str, Any]:
        """
        Executes a query against ERDDAP's tabledap endpoint and converts the 
        columnar JSON response back into a standard list of dicts.
        """
        query_string = "&".join(query_args)
        url = f"{self.base_url}/tabledap/{dataset_id}.json?{query_string}"
        
        self.logger.debug(f"ERDDAP Query: {url}")
        try:
            resp = await self.http.get(url)
            
            # ERDDAP returns 404 if the dataset exists but no rows match the query
            if resp.status_code == 404:
                return {"results": []}
                
            resp.raise_for_status()
            data = resp.json()
            
            table = data.get("table", {})
            cols = table.get("columnNames", [])
            rows = table.get("rows", [])
            
            # Zip columns and rows into a list of dictionaries
            results = [dict(zip(cols, row)) for row in rows]
            return {"results": results}
            
        except Exception as e:
            self.logger.error("ERDDAP fetch failed", extra={"url": url, "reason": str(e)})
            return {"results": []}

    # ---------------------------------------------------------
    # TELEMETRY QUERIES
    # ---------------------------------------------------------
    async def device_data_get(self, request: DataRequest, definition: dict = None) -> dict:
        """Fetches historical device telemetry from the Sidecar Egress API as NCO-JSON."""
        version_clean = request.version.replace(".", "_") if request.version else "v1"
        dataset_id = f"telemetry_{request.make}_{request.model}_{version_clean}_time".replace("-", "_")
        
        # Adjust URL to point to the new sidecar /api/data endpoint
        url = f"{self.base_url.replace('/erddap', '')}/api/data/{dataset_id}"
        
        query_args = []
        if request.serial_number:
            query_args.append(f'serial_number="{request.serial_number}"')
        if request.start_timestamp:
            query_args.append(f"time>={request.start_timestamp}")
        if request.end_timestamp:
            query_args.append(f"time<={request.end_timestamp}")
            
        query_args.append("orderBy(%22time%22)")
        query_string = "&".join(query_args)
        
        self.logger.debug(f"Sidecar NCO-JSON Query: {url}?{query_string}")
        
        try:
            resp = await self.http.post(f"{url}?{query_string}", json=definition or {})
            
            if resp.status_code == 404:
                return {"results": []}
                
            resp.raise_for_status()
            return resp.json() 
            
        except Exception as e:
            self.logger.error("Sidecar fetch failed", extra={"url": url, "reason": str(e)})
            return {"results": []}
        
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
            # In sidecar.py, system IDs map directly to the 'name' column
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