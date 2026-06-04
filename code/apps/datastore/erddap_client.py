import httpx
import logging
from typing import List, Dict, Any
import time

from datastore_requests import (
    DataRequest,
    VariableSetDataRequest
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
        # Join query arguments with '&'
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
    async def device_data_get(self, request: DataRequest) -> dict:
        """Fetches historical raw device telemetry from ERDDAP."""
        # Note: ERDDAP creates a dataset per shape. For 1D time-series, the suffix is _time.
        # If your frontend needs to query multidimensional data, you would dynamically build this.
        version_clean = request.version.replace(".", "_") if request.version else "v1"
        dataset_id = f"telemetry_{request.make}_{request.model}_{version_clean}_time".replace("-", "_")
        
        query_args = []
        if request.serial_number:
            query_args.append(f'serial_number="{request.serial_number}"')
            
        if request.start_timestamp:
            query_args.append(f"time>={request.start_timestamp}")
        if request.end_timestamp:
            query_args.append(f"time<={request.end_timestamp}")
            
        # Sort by time ascending
        query_args.append("orderBy(%22time%22)")
        
        return await self._fetch_tabledap(dataset_id, query_args)

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
    # OPERATIONS REGISTRY (Read-Through Cache)
    # ---------------------------------------------------------
    async def sampling_definition_registry_get(self, resource: str, query: dict) -> dict:
        """Fetches historical operational definitions (conditions, modes) from ERDDAP."""
        # Using the ops_registry_dataset we built in the sidecar
        dataset_id = "envds_ops_registry"
        
        # In ERDDAP, we map the resource (e.g. 'samplingcondition') to the 'kind' column
        kind_map = {
            "samplingcondition": "SamplingCondition",
            "samplingmode": "SamplingMode",
            "samplingstate": "SamplingState",
            "systemmode": "SystemMode"
        }
        kind = kind_map.get(resource, resource)
        
        query_args = [f'kind="{kind}"']
        
        if "name" in query and query["name"]:
            # If the query name includes the time ID (e.g., cn_limit::2026-05-25T00:00:00Z)
            name_part = query["name"].split("::")[0]
            query_args.append(f'name="{name_part}"')
            
        # Get the latest revision first
        query_args.append("orderByLimitMax(%22-time%22)")
        
        result = await self._fetch_tabledap(dataset_id, query_args)
        
        # Unpack the stringified JSON payload back into dicts for the Datastore
        parsed_results = []
        import json
        for row in result.get("results", []):
            try:
                payload = json.loads(row.get("payload", "{}"))
                parsed_results.append(payload)
            except Exception:
                continue
                
        return {"results": parsed_results}