import asyncio
import json
import logging
import urllib.parse
import os
import shutil
from pathlib import Path

from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask
import httpx
import uvicorn
from pydantic import BaseSettings
from ulid import ULID
from aiomqtt import Client, MqttError
from cloudevents.http import from_json
from logfmter import Logfmter
from jinja2 import Environment, FileSystemLoader
import time

# ---------------------------------------------------------
# LOGGING & CONFIGURATION
# ---------------------------------------------------------
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger("erddap_sidecar")
L.setLevel(logging.INFO)

class ERDDAPSidecarConfig(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8000
    erddap_internal_url: str = "http://127.0.0.1:8080/erddap"
    mqtt_broker: str = "mosquitto.default"
    mqtt_port: int = 1883
    # Listen for telemetry, definitions, and operational statuses
    mqtt_subscriptions: str = "envds/+/+/+/data/#,envds/+/+/+/registry/#,envds/+/+/+/status/#"
    daq_id: str | None = None
    data_dir: str = "/erddapData"
    insert_password: str = os.environ.get("ERDDAP_INSERT_PASSWORD", "default_secret")
    author_name: str = "envds_sidecar"

    class Config:
        env_prefix = "ERDDAP_SIDECAR_"

config = ERDDAPSidecarConfig()
app = FastAPI()
http_client = httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=100))

# Limit ERDDAP ingestion to 50 concurrent HTTP requests to protect Tomcat
http_semaphore = asyncio.Semaphore(50)

# In-memory cache for sensor dimensional shapes and static coordinate values
# Format: {"make_model_v1": {"shapes": {...}, "coords": {...}}}
definition_cache = {}

# ---------------------------------------------------------
# 1. ERDDAP CONFIG COMPILER
# ---------------------------------------------------------
class ERDDAPConfigCompiler:
    def __init__(self, data_dir: str, templates_dir="/app/templates"):
        self.data_dir = Path(data_dir)
        self.templates_dir = Path(templates_dir)
        self.datasets_d = self.data_dir / "datasets.d"
        self.master_xml_path = self.data_dir / "datasets.xml"
        self.flags_dir = self.data_dir / "hardFlag"
        
        self.datasets_d.mkdir(parents=True, exist_ok=True)
        self.flags_dir.mkdir(parents=True, exist_ok=True)
        
        self.env = Environment(loader=FileSystemLoader(self.templates_dir))
        self.telemetry_template = self.env.get_template("telemetry_dataset.xml.j2")

    def initialize_static_datasets(self):
        """Seeds the Persistent Volume with Ops Registry/Status datasets on startup."""
        static_files = ["ops_registry_dataset.xml", "ops_status_dataset.xml"]
        needs_rebuild = False
        
        for static_file in static_files:
            source_path = self.templates_dir / static_file
            dest_path = self.datasets_d / static_file
            
            if not dest_path.exists() or source_path.read_text() != dest_path.read_text():
                if source_path.exists():
                    shutil.copy(source_path, dest_path)
                    L.info(f"Seeded static dataset: {static_file}")
                    needs_rebuild = True
                else:
                    L.error(f"Missing static template in image: {source_path}")

        if needs_rebuild or not self.master_xml_path.exists():
            self.rebuild_master_xml()
            (self.flags_dir / "datasets.xml").touch()
            L.info("Triggered initial ERDDAP datasets.xml load.")

    def handle_definition(self, ce: dict):
        """Parses sensor definitions, groups by shape, caches metadata, and builds XML."""
        definition = ce.get("data", {})
        attributes = definition.get("attributes", {})
        variables = definition.get("variables", {})
        
        make = attributes.get("make", {}).get("data", "unknown")
        model = attributes.get("model", {}).get("data", "unknown")
        sn = attributes.get("serial_number", {}).get("data", "unknown")
        
        version_raw = attributes.get("format_version", {}).get("data", "1")
        major_version = str(version_raw).split('.')[0]
        version = f"v{major_version}"
        
        # 1. Update Definition Cache for the unroller
        cache_key = f"{make}_{model}_{version}"
        definition_cache[cache_key] = {
            "shapes": {k: v.get("shape", ["time"]) for k, v in variables.items()},
            "coords": {
                k: v.get("data", []) 
                for k, v in variables.items() 
                if v.get("attributes", {}).get("variable_type", {}).get("data") == "coordinate"
            }
        }
        
        # 2. Group variables by shape to create datasets
        shape_groups = {}
        for var_name, var_data in variables.items():
            shape_tuple = tuple(var_data.get("shape", ["time"]))
            if shape_tuple not in shape_groups:
                shape_groups[shape_tuple] = []
            
            shape_groups[shape_tuple].append({
                "name": var_name,
                "type": var_data.get("type", "float"),
                "units": var_data.get("attributes", {}).get("units", {}).get("data", ""),
                "long_name": var_data.get("attributes", {}).get("long_name", {}).get("data", var_name),
                "variable_type": var_data.get("attributes", {}).get("variable_type", {}).get("data", "")
            })

        # 3. Generate XML Snippets
        needs_rebuild = False
        for shape, cols in shape_groups.items():
            shape_joined = "_".join(shape)
            dataset_id = f"telemetry_{make}_{model}_{version}_{shape_joined}".replace("-", "_")
            
            # Ensure coordinates (like diameter) are declared as columns if they aren't explicit data vars
            for dim in shape:
                if dim != "time" and not any(c["name"] == dim for c in cols):
                    if dim in variables:
                        dim_var = variables[dim]
                        cols.insert(1, {
                            "name": dim,
                            "type": dim_var.get("type", "float"),
                            "units": dim_var.get("attributes", {}).get("units", {}).get("data", ""),
                            "long_name": dim_var.get("attributes", {}).get("long_name", {}).get("data", dim)
                        })

            xml_content = self.telemetry_template.render(
                dataset_id=dataset_id,
                make=make,
                model=model,
                sn=sn,
                version=version,
                format_version=str(version_raw),
                shape_joined=shape_joined,
                columns=cols
            )
            
            snippet_path = self.datasets_d / f"{dataset_id}.xml"
            if not snippet_path.exists() or snippet_path.read_text() != xml_content:
                snippet_path.write_text(xml_content)
                L.info(f"Generated new ERDDAP dataset: {dataset_id}")
                needs_rebuild = True

        if needs_rebuild:
            self.rebuild_master_xml()
            (self.flags_dir / "datasets.xml").touch()

    def rebuild_master_xml(self):
        master_xml = ['<?xml version="1.0" encoding="ISO-8859-1" ?>\n<erddap>']
        for snippet_file in sorted(self.datasets_d.glob("*.xml")):
            master_xml.append(f"\n")
            master_xml.append(snippet_file.read_text())
        master_xml.append('\n</erddap>')
        self.master_xml_path.write_text("\n".join(master_xml))

compiler = ERDDAPConfigCompiler(data_dir=config.data_dir)

# ---------------------------------------------------------
# 2. INGESTION LOGIC (Telemetry & Operations)
# ---------------------------------------------------------
def _extract_val(obj, key, default=None):
    """Safely extract values from the envds payload structure."""
    val = obj.get(key)
    if val is None: return default
    if isinstance(val, dict) and "data" in val:
        return val.get("data", default)
    return val

def unroll_multidimensional_data(base_row, shape_dims, coords_dict, var_dict):
    """Recursively unrolls nested N-dimensional arrays into flat rows for ERDDAP."""
    if not shape_dims:
        row = base_row.copy()
        row.update(var_dict)
        yield row
        return
        
    def recurse(dim_index, current_indices, current_row):
        if dim_index == len(shape_dims):
            row = current_row.copy()
            for v_name, v_data in var_dict.items():
                val = v_data
                try:
                    for idx in current_indices:
                        val = val[idx]
                    row[v_name] = val
                except (IndexError, TypeError):
                    row[v_name] = None
            yield row
            return
            
        dim_name = shape_dims[dim_index]
        dim_coords = coords_dict.get(dim_name, [])
        
        for i, coord_val in enumerate(dim_coords):
            next_row = current_row.copy()
            next_row[dim_name] = coord_val
            yield from recurse(dim_index + 1, current_indices + [i], next_row)
            
    yield from recurse(0, [], base_row)

async def _send_insert(url: str):
    """Executes the HTTP GET request with a concurrency limit."""
    async with http_semaphore:
        try:
            resp = await http_client.get(url)
            resp.raise_for_status()
        except Exception as e:
            L.error("ERDDAP Insert Failed", extra={"url": url.split('?')[0], "reason": str(e)})

async def insert_telemetry_to_erddap(ce: dict):
    data = ce.get("data", {})
    attributes = data.get("attributes", {})
    variables = data.get("variables", {})
    
    make = _extract_val(attributes, "make", "unknown")
    model = _extract_val(attributes, "model", "unknown")
    sn = _extract_val(attributes, "serial_number", "unknown")
    version_raw = _extract_val(attributes, "format_version", "1.0.0")
    version = f"v{str(version_raw).split('.')[0]}"
    
    time_val = _extract_val(variables, "time")
    if not time_val: return

    # Check cache for shapes and coordinates
    def_key = f"{make}_{model}_{version}"
    cached_def = definition_cache.get(def_key, {"shapes": {}, "coords": {}})
    
    shape_groups = {}
    for var_name, var_payload in variables.items():
        if var_name == "time": continue
        shape = tuple(cached_def["shapes"].get(var_name, ["time"]))
        if shape not in shape_groups: shape_groups[shape] = {}
        shape_groups[shape][var_name] = _extract_val(variables, var_name)

    base_params = {
        "author": config.author_name,
        "password": config.insert_password,
        "make": make,
        "model": model,
        "serial_number": sn,
        "format_version": str(version_raw),
        "time": time_val
    }

    insert_tasks = []
    for shape, var_dict in shape_groups.items():
        shape_joined = "_".join(shape)
        dataset_id = f"telemetry_{make}_{model}_{version}_{shape_joined}".replace("-", "_")
        extra_dims = [dim for dim in shape if dim != "time"]
        
        coords_dict = cached_def["coords"].copy()
        for dim in extra_dims:
            payload_coord = _extract_val(variables, dim)
            if payload_coord: coords_dict[dim] = payload_coord
                
        for flat_row in unroll_multidimensional_data(base_params, extra_dims, coords_dict, var_dict):
            query_string = urllib.parse.urlencode(flat_row)
            insert_url = f"{config.erddap_internal_url}/tabledap/{dataset_id}.insert?{query_string}"
            insert_tasks.append(_send_insert(insert_url))

    if insert_tasks:
        await asyncio.gather(*insert_tasks, return_exceptions=True)

async def handle_ops_registry_insert(ce: dict):
    """Inserts definitions (e.g. SamplingConditions) into the historical Ops Registry."""
    data = ce.get("data", {})
    kind = data.get("kind")
    metadata = data.get("metadata", {})
    if not kind or not metadata: return

    params = {
        "author": config.author_name,
        "password": config.insert_password,
        "time": time.time(),
        "kind": kind,
        "name": metadata.get("name", "unknown"),
        "namespace": metadata.get("sampling_namespace", "unknown"),
        "valid_config_time": metadata.get("valid_config_time", "unknown"),
        "revision": metadata.get("revision", 1),
        "payload": json.dumps(data, separators=(',', ':'))
    }
    
    query_string = urllib.parse.urlencode(params)
    insert_url = f"{config.erddap_internal_url}/tabledap/envds_ops_registry.insert?{query_string}"
    await _send_insert(insert_url)

async def handle_ops_status_insert(ce: dict):
    """Inserts 1Hz logic evaluations into the real-time Ops Status dataset."""
    data = ce.get("data", {})
    id_block = data.get("id", {})
    timestamp = data.get("timestamp")
    if not timestamp: return

    state_block = data.get("state", {})
    requested, actual = "unknown", "unknown"
    if state_block and isinstance(state_block, dict):
        state_vals = list(state_block.values())[0]
        if isinstance(state_vals, dict):
            requested = str(state_vals.get("requested", "unknown"))
            actual = str(state_vals.get("actual", "unknown"))

    params = {
        "author": config.author_name,
        "password": config.insert_password,
        "time": timestamp,
        "app_group": id_block.get("app_group", "unknown"),
        "app_uid": id_block.get("app_uid", "unknown"),
        "namespace": id_block.get("sampling_namespace", "unknown"),
        "valid_config_time": id_block.get("valid_config_time", "unknown"),
        "requested_state": requested,
        "actual_state": actual
    }
    
    query_string = urllib.parse.urlencode(params)
    insert_url = f"{config.erddap_internal_url}/tabledap/envds_ops_status.insert?{query_string}"
    await _send_insert(insert_url)

# ---------------------------------------------------------
# 3. BACKGROUND TASKS & API ROUTING
# ---------------------------------------------------------
async def mqtt_loop():
    reconnect = 10
    while True:
        try:
            L.info(f"Connecting to MQTT Broker: {config.mqtt_broker}")
            async with Client(config.mqtt_broker, port=config.mqtt_port, identifier=str(ULID())) as client:
                for topic in config.mqtt_subscriptions.split(","):
                    if topic.strip():
                        await client.subscribe(f"$share/erddap/{topic.strip()}")

                async for message in client.messages:
                    try:
                        ce = from_json(message.payload)
                        ce_type = ce.get("type", "")
                        
                        # 1. Definitions / Registries
                        if "registry.update" in ce_type:
                            if "sampling" in ce_type or "system" in ce_type:
                                await handle_ops_registry_insert(ce)
                            else:
                                compiler.handle_definition(ce)
                                
                        # 2. Hardware Telemetry Data
                        elif "data.update" in ce_type:
                            await insert_telemetry_to_erddap(ce)
                            
                        # 3. Operational Status Tracking
                        elif "status.update" in ce_type:
                            if any(ops in ce_type for ops in ["samplingcondition", "samplingstate", "samplingmode", "systemmode"]):
                                await handle_ops_status_insert(ce)

                    except Exception as e:
                        L.error("Error processing MQTT message", extra={"reason": str(e)})
        except MqttError as e:
            L.error(f"MQTT Error: {e}. Reconnecting in {reconnect}s...")
            await asyncio.sleep(reconnect)

@app.on_event("startup")
async def startup_event():
    L.info("Initializing ERDDAP Sidecar...")
    compiler.initialize_static_datasets()
    asyncio.create_task(mqtt_loop())

@app.on_event("shutdown")
async def shutdown_event():
    await http_client.aclose()

@app.api_route("/erddap/{path_name:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy_erddap(request: Request, path_name: str):
    """Reverse Proxies traffic exactly as ERDDAP expects it."""
    target_path = path_name if path_name else "index.html"
    url = f"{config.erddap_internal_url}/{target_path}"
    
    req_headers = dict(request.headers)
    req_headers.pop("host", None)
    req_headers["X-Forwarded-Prefix"] = "/msp/data" 
    
    rp_req = http_client.build_request(
        request.method,
        url,
        headers=req_headers,
        content=await request.body(),
        params=request.query_params
    )
    
    try:
        rp_resp = await http_client.send(rp_req, stream=True)
    except httpx.ConnectError:
        return Response("ERDDAP container is not ready or reachable.", status_code=503)

    return StreamingResponse(
        rp_resp.aiter_raw(),
        status_code=rp_resp.status_code,
        headers=rp_resp.headers,
        background=BackgroundTask(rp_resp.aclose),
    )

if __name__ == "__main__":
    uvicorn.run("sidecar:app", host=config.host, port=config.port)