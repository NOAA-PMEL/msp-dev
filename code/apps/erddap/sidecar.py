import asyncio
import json
import logging
import urllib.parse
import os
import shutil
import re
from pathlib import Path

from fastapi import FastAPI, Request, Response, status
from fastapi.responses import StreamingResponse, HTMLResponse
from starlette.background import BackgroundTask
import httpx
import uvicorn
from pydantic_settings import BaseSettings
from ulid import ULID
from aiomqtt import Client, MqttError
from cloudevents.http import from_json, from_http
from logfmter import Logfmter
from jinja2 import Environment, FileSystemLoader
import time
from xml.sax.saxutils import escape

# ---------------------------------------------------------
# LOGGING & CONFIGURATION
# ---------------------------------------------------------
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger("erddap_sidecar")
L.setLevel(logging.DEBUG)

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
http_client = httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=100),timeout=60.0)

# Limit ERDDAP ingestion to 50 concurrent HTTP requests to protect Tomcat
http_semaphore = asyncio.Semaphore(50)

# In-memory cache for sensor dimensional shapes and static coordinate values
# Format: {"make_model_v1": {"shapes": {...}, "coords": {...}}}
definition_cache = {}
definition_registry_cache = {}

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
        """Seeds the Persistent Volume templates and unconditionally compiles the master XML."""
        L.info("Initializing ERDDAP datasets configuration...")
        
        # 1. Clean out old XMLs
        for old_file in self.datasets_d.glob("*.xml"):
            old_file.unlink()

        # 2. Handle file-based Registries (Ops AND Hardware)
        for reg_file in ["ops_registry_dataset.xml", "hardware_registry_dataset.xml"]:
            reg_source = self.templates_dir / reg_file
            reg_dest = self.datasets_d / reg_file
            if reg_source.exists():
                shutil.copy(reg_source, reg_dest)
                L.info(f"Copied {reg_file} to active datasets.")
            else:
                L.warning(f"Could not find {reg_file} in templates!")

        # 3. Render the HTTP-based Status and Log datasets dynamically
        http_templates = ["ops_status_dataset.xml.j2", "ops_log_dataset.xml.j2"]
        for template_name in http_templates:
            target_name = template_name.replace(".j2", "")
            dest_path = self.datasets_d / target_name
            
            try:
                template = self.env.get_template(template_name)
                xml_content = template.render(
                    author=escape(config.author_name),
                    password=escape(config.insert_password)
                )
                dest_path.write_text(xml_content)
                L.info(f"Rendered {template_name} successfully.")
            except Exception as e:
                L.error(f"Failed to render {template_name}", extra={"error": str(e)})

        # 4. UNCONDITIONALLY rebuild the master datasets.xml on every single startup
        L.info("Compiling master datasets.xml from active directory state...")
        self.rebuild_master_xml()
        
        # 5. Poke ERDDAP to ensure it reloads the newly compiled master file
        (self.flags_dir / "datasets.xml").touch()
        L.info("ERDDAP initialization sequence complete.")

    def handle_definition(self, ce: dict):
        """Parses sensor definitions, groups by shape, caches metadata, and builds XML."""
        definition = ce.data if hasattr(ce, "data") else ce.get("data", {})
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
                make=escape(make),
                model=escape(model),
                sn=escape(sn),
                version=version,
                format_version=str(version_raw),
                shape_joined=shape_joined,
                columns=cols,
                author=escape(config.author_name),        
                password=escape(config.insert_password)
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
        master_xml = ['<?xml version="1.0" encoding="ISO-8859-1" ?>\n<erddapDatasets>']
        for snippet_file in sorted(self.datasets_d.glob("*.xml")):
            content = snippet_file.read_text()
            master_xml.append(f"\n")
            master_xml.append(content)
            
            # --- AUTO-CREATE ERDDAP DATA DIRECTORIES ---
            dir_match = re.search(r'<fileDir>([^<]+)</fileDir>', content)
            if dir_match:
                data_dir = Path(dir_match.group(1))
                data_dir.mkdir(parents=True, exist_ok=True)
            # -------------------------------------------
                
        master_xml.append('\n</erddapDatasets>')
        self.master_xml_path.write_text("\n".join(master_xml))

    def get_all_datasets(self):
        """Scans datasets.d/ and returns metadata for the UI."""
        datasets = []
        for xml_file in self.datasets_d.glob("*.xml"):
            content = xml_file.read_text()
            
            # Extract basic info using regex to avoid heavy XML parsing
            id_match = re.search(r'datasetID="([^"]+)"', content)
            active_match = re.search(r'active="([^"]+)"', content)
            dir_match = re.search(r'<fileDir>([^<]+)</fileDir>', content)
            
            if id_match:
                datasets.append({
                    "id": id_match.group(1),
                    "active": active_match.group(1) == "true" if active_match else False,
                    "file_dir": dir_match.group(1) if dir_match else "N/A",
                    "xml_file": xml_file.name
                })
        # Sort alphabetically by dataset ID
        return sorted(datasets, key=lambda x: x["id"])

    def toggle_active(self, dataset_id: str):
        """Flips the active boolean in the XML and rebuilds."""
        for xml_file in self.datasets_d.glob("*.xml"):
            content = xml_file.read_text()
            if f'datasetID="{dataset_id}"' in content:
                if 'active="true"' in content:
                    content = content.replace('active="true"', 'active="false"')
                else:
                    content = content.replace('active="false"', 'active="true"')
                    
                xml_file.write_text(content)
                self.rebuild_master_xml()
                (self.flags_dir / "datasets.xml").touch()
                return True
        return False

    def delete_dataset(self, dataset_id: str):
        """Nukes the data directory, the XML snippet, and rebuilds."""
        for xml_file in self.datasets_d.glob("*.xml"):
            content = xml_file.read_text()
            if f'datasetID="{dataset_id}"' in content:
                
                # 1. Find and delete the data directory
                dir_match = re.search(r'<fileDir>([^<]+)</fileDir>', content)
                if dir_match:
                    data_dir = Path(dir_match.group(1))
                    if data_dir.exists() and data_dir.is_dir():
                        shutil.rmtree(data_dir, ignore_errors=True)
                        L.info(f"Deleted data directory: {data_dir}")

                # 2. Delete the XML snippet and rebuild
                xml_file.unlink()
                self.rebuild_master_xml()
                (self.flags_dir / "datasets.xml").touch()
                return True
        return False

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
    data = ce.data if hasattr(ce, "data") else ce.get("data", {})
    attributes = data.get("attributes", {})
    variables = data.get("variables", {})
    
    make = _extract_val(attributes, "make", "unknown")
    model = _extract_val(attributes, "model", "unknown")
    sn = _extract_val(attributes, "serial_number", "unknown")
    version_raw = _extract_val(attributes, "format_version", "1.0.0")
    version = f"v{str(version_raw).split('.')[0]}"
    
    # 1. Grab time["data"]
    time_data = _extract_val(variables, "time")
    if not time_data: return

    # Normalize to a list so we can seamlessly handle both single measurements and chunked arrays
    time_array = time_data if isinstance(time_data, list) else [time_data]

    # Check cache for shapes and coordinates
    def_key = f"{make}_{model}_{version}"
    cached_def = definition_cache.get(def_key, {"shapes": {}, "coords": {}})
    
    insert_tasks = []
    
    # 2. Iterate through the time dimension (unrolling chunked data)
    for i, current_time in enumerate(time_array):
        base_params = {
            "author": config.author_name,
            "password": config.insert_password,
            "make": make,
            "model": model,
            "serial_number": sn,
            "format_version": str(version_raw),
            "time": current_time
        }
        
        # Extract the values for this specific time slice
        slice_vars = {}
        for v_name, v_data in variables.items():
            if v_name == "time": continue
            v_val = _extract_val(variables, v_name)
            
            # If the data is chunked and matches the time array length, slice it!
            if isinstance(v_val, list) and len(v_val) == len(time_array):
                slice_vars[v_name] = v_val[i]
            else:
                slice_vars[v_name] = v_val

        # Group by shape for this specific time slice
        shape_groups = {}
        for v_name, v_val in slice_vars.items():
            shape = tuple(cached_def["shapes"].get(v_name, ["time"]))
            if shape not in shape_groups: shape_groups[shape] = {}
            shape_groups[shape][v_name] = v_val
            
        # 3. Build the insert URLs
        for shape, var_dict in shape_groups.items():
            shape_joined = "_".join(shape)
            dataset_id = f"telemetry_{make}_{model}_{version}_{shape_joined}".replace("-", "_")
            extra_dims = [dim for dim in shape if dim != "time"]
            
            coords_dict = cached_def["coords"].copy()
            for dim in extra_dims:
                payload_coord = slice_vars.get(dim)
                if payload_coord: coords_dict[dim] = payload_coord
                    
            # Unroll any nested N-dimensional arrays (like spectral bins)
            for flat_row in unroll_multidimensional_data(base_params, extra_dims, coords_dict, var_dict):
                query_string = urllib.parse.urlencode(flat_row)
                insert_url = f"{config.erddap_internal_url}/tabledap/{dataset_id}.insert?{query_string}"
                insert_tasks.append(_send_insert(insert_url))

    # 4. Fire them all into ERDDAP concurrently
    if insert_tasks:
        await asyncio.gather(*insert_tasks, return_exceptions=True)

async def handle_ops_registry_insert(ce: dict):
    """Appends Ops Definitions (Deployments, Platforms, etc.) directly to a JSONL file."""
    attrs = ce.get("attributes", ce)
    data = ce.get("data", {})
    if not data: return

    # Dynamically find the definition block
    def_key = next((k for k in data.keys() if "definition" in k), None)
    if not def_key: return
    
    def_block = data.get(def_key, {})
    metadata = def_block.get("metadata", {})
    
    # Safety check: If it has no metadata block, it does not belong in the ops registry
    if not metadata: 
        return

    kind = def_key
    namespace = metadata.get("sampling_namespace", "unknown")
    name = metadata.get("name", "unknown")
    revision = metadata.get("revision", 1)
    
    # Standardized time extraction for Ops definitions
    valid_config_time = (
        def_block.get("revision-time") or 
        metadata.get("revision-time") or 
        def_block.get("valid_config_time") or 
        metadata.get("valid_config_time") or 
        attrs.get("time", "2026-01-01T00:00:00Z")
    )

    try:
        revision = int(revision)
    except (ValueError, TypeError):
        revision = 1

    registry_dir = Path(config.data_dir) / "registry" / "system" / kind
    registry_dir.mkdir(parents=True, exist_ok=True)
    file_path = registry_dir / f"{kind}_registry.jsonl"

    record = {
        "time": time.time(),
        "kind": kind,
        "namespace": namespace,
        "name": name,
        "valid_config_time": valid_config_time,
        "revision": revision,
        "payload": json.dumps(data, separators=(',', ':'))
    }

    # Write the file, prepending ERDDAP JsonlCSV headers if it's brand new
    is_new = not file_path.exists() or file_path.stat().st_size == 0
    with open(file_path, "a") as f:
        if is_new:
            f.write('{"time":"time","kind":"kind","namespace":"namespace","name":"name","valid_config_time":"valid_config_time","revision":"revision","payload":"payload"}\n')
            f.write('{"time":"double","kind":"String","namespace":"String","name":"String","valid_config_time":"String","revision":"int","payload":"String"}\n')
        f.write(json.dumps(record) + "\n")
        
    # Trigger ERDDAP reload
    flag_dir = Path(config.data_dir) / "hardFlag"
    flag_dir.mkdir(parents=True, exist_ok=True)
    (flag_dir / "envds_system_registry").touch()

async def handle_ops_status_insert(ce: dict):
    """Inserts 1Hz logic evaluations into the real-time Ops Status dataset."""
    data = ce.data if hasattr(ce, "data") else ce.get("data", {})
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

async def handle_ops_log_insert(ce: dict):
    """Inserts discrete operational event logs into ERDDAP."""
    data = ce.data if hasattr(ce, "data") else ce.get("data", {})
    if not data: return

    # Standard CloudEvent time string (e.g., 2026-06-04T12:00:00Z)
    time_str = ce.get("time") 
    from envds.util.util import string_to_timestamp
    timestamp = string_to_timestamp(time_str) if time_str else time.time()

    params = {
        "author": config.author_name,
        "password": config.insert_password,
        "time": timestamp,
        "deployment_ref": ce.get("deploymentref", "unknown"),
        "project_ref": ce.get("projectref", "unknown"),
        "event_type": data.get("event_type", "unknown"),
        "subject": ce.get("subject", "system"),
        "description": data.get("description", "")
    }
    
    query_string = urllib.parse.urlencode(params)
    insert_url = f"{config.erddap_internal_url}/tabledap/envds_ops_log.insert?{query_string}"
    await _send_insert(insert_url)

async def handle_hardware_registry_insert(ce: dict):
    """Appends Hardware Definitions (Device, Controller) directly to a JSONL file."""
    attrs = ce.get("attributes", ce)
    data = ce.data if hasattr(ce, "data") else ce.get("data", {})
    if not data: return

    # Dynamically find the definition block
    def_key = next((k for k in data.keys() if "definition" in k), None)
    if not def_key: return
    
    def_block = data.get(def_key, {})
    def_attrs = def_block.get("attributes", {})
    if not def_attrs: return

    kind = def_key
    
    # Extract Exact Hardware Schema
    make = def_attrs.get("make", {}).get("data", "unknown")
    model = def_attrs.get("model", {}).get("data", "unknown")
    exact_version = str(def_block.get("version") or def_attrs.get("format_version", {}).get("data", "1.0.0")).strip()

    valid_config_time = (
        def_block.get("valid_time") or 
        def_attrs.get("valid_time", {}).get("data") or 
        attrs.get("time", "2026-01-01T00:00:00Z")
    )
    
    registry_dir = Path(config.data_dir) / "registry" / "hardware" / kind
    registry_dir.mkdir(parents=True, exist_ok=True)
    file_path = registry_dir / f"{kind}_registry.jsonl"

    record = {
        "time": time.time(),
        "kind": kind,
        "make": make,
        "model": model,
        "version": exact_version,
        "valid_config_time": valid_config_time,
        "payload": json.dumps(data, separators=(',', ':'))
    }

    is_new = not file_path.exists() or file_path.stat().st_size == 0
    with open(file_path, "a") as f:
        if is_new:
            f.write('{"time":"time","kind":"kind","make":"make","model":"model","version":"version","valid_config_time":"valid_config_time","payload":"payload"}\n')
            f.write('{"time":"double","kind":"String","make":"String","model":"String","version":"String","valid_config_time":"String","payload":"String"}\n')
        f.write(json.dumps(record) + "\n")
        
    # Trigger ERDDAP reload for hardware registry
    flag_dir = Path(config.data_dir) / "hardFlag"
    flag_dir.mkdir(parents=True, exist_ok=True)
    (flag_dir / "envds_hardware_registry").touch()

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
                            
                            # Always push EVERY definition to the envds_ops_registry table
                            await handle_ops_registry_insert(ce)
                            
                            # ONLY compile dataset XMLs if it's hardware
                            if any(hw in ce_type for hw in ["device", "controller"]):
                                compiler.handle_definition(ce)
                                
                        # 2. Hardware Telemetry Data
                        elif "data.update" in ce_type:
                            await insert_telemetry_to_erddap(ce)
                            
                        # 3. Operational Status Tracking
                        elif "status.update" in ce_type:
                            if any(ops in ce_type for ops in ["samplingcondition", "samplingstate", "samplingmode", "systemmode"]):
                                await handle_ops_status_insert(ce)

                        # 4. Discrete Operational Logs
                        elif "operations.log" in ce_type:
                            await handle_ops_log_insert(ce)

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

# ---------------------------------------------------------
# KNATIVE INGESTION (HTTP) - For Definitions & Registries
# ---------------------------------------------------------
@app.post("/registry/update/")
async def registry_update(request: Request):
    """Catches Knative Eventing HTTP POSTs for all definition updates."""
    try:
        body = await request.body()
        ce = from_http(request.headers, body)
        ce_type = ce.get("type", "")
        
        L.debug(f"Received Knative Registry Update", extra={"type": ce_type})
        
        # 1. HARDWARE: Route to the XML Compiler (Do NOT send to Ops Registry)
        if any(hw in ce_type for hw in ["device-definition", "controller-definition"]):
            compiler.handle_definition(ce)
            await handle_hardware_registry_insert(ce)
            
        # 2. OPERATIONS: Route to the System Registry JSONL 
        else:
            await handle_ops_registry_insert(ce)
            
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as e:
        L.error("Knative registry update failed", extra={"reason": str(e)})
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    
# ---------------------------------------------------------
# MAINTENANCE ADMIN UI
# ---------------------------------------------------------
@app.get("/admin", response_class=HTMLResponse)
async def admin_page():
    datasets = compiler.get_all_datasets()
    
    html = """
    <html>
    <head>
        <title>ENVDS ERDDAP Maintenance</title>
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 40px; background-color: #f8f9fa;}
            .container { max-width: 1200px; margin: auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); }
            h2 { color: #343a40; border-bottom: 2px solid #e9ecef; padding-bottom: 10px; }
            table { border-collapse: collapse; width: 100%; margin-top: 20px; }
            th, td { border: 1px solid #dee2e6; padding: 12px; text-align: left; }
            th { background-color: #e9ecef; color: #495057; }
            .badge { padding: 4px 8px; border-radius: 4px; font-size: 12px; font-weight: bold; color: white; }
            .bg-active { background-color: #28a745; }
            .bg-inactive { background-color: #6c757d; }
            .btn { padding: 6px 12px; text-decoration: none; border-radius: 4px; color: white; cursor: pointer; border: none; font-size: 14px; margin-right: 5px; }
            .btn-toggle { background-color: #f0ad4e; color: #fff; }
            .btn-delete { background-color: #dc3545; color: #fff; }
            .btn-delete:hover { background-color: #c82333; }
        </style>
        <script>
            async function toggle(id) {
                await fetch(`./admin/datasets/${id}/toggle`, {method: 'POST'});
                location.reload();
            }
            async function delDataset(id) {
                if(confirm(`WARNING! Are you absolutely sure you want to permanently delete all data and configurations for ${id}?`)) {
                    await fetch(`./admin/datasets/${id}`, {method: 'DELETE'});
                    location.reload();
                }
            }
        </script>
    </head>
    <body>
        <div class="container">
            <h2>ERDDAP Datasets Maintenance</h2>
            <p>Use this panel to safely disable or purge historical datasets from the ERDDAP engine.</p>
            <table>
                <tr><th>Dataset ID</th><th>Status</th><th>File Directory</th><th>Actions</th></tr>
    """
    
    for ds in datasets:
        status_text = "Active" if ds["active"] else "Inactive"
        status_class = "bg-active" if ds["active"] else "bg-inactive"
        
        html += f"""
            <tr>
                <td style="font-family: monospace;">{ds['id']}</td>
                <td><span class="badge {status_class}">{status_text}</span></td>
                <td style="font-size: 12px; color: #666;">{ds['file_dir']}</td>
                <td>
                    <button class="btn btn-toggle" onclick="toggle('{ds['id']}')">Toggle</button>
                    <button class="btn btn-delete" onclick="delDataset('{ds['id']}')">Delete</button>
                </td>
            </tr>
        """
        
    html += """
            </table>
        </div>
    </body>
    </html>
    """
    return html

@app.post("/admin/datasets/{dataset_id}/toggle")
async def toggle_dataset(dataset_id: str):
    success = compiler.toggle_active(dataset_id)
    return {"success": success}

@app.delete("/admin/datasets/{dataset_id}")
async def delete_dataset(dataset_id: str):
    success = compiler.delete_dataset(dataset_id)
    return {"success": success}

@app.api_route("/erddap/{path_name:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy_erddap(request: Request, path_name: str):
    """Reverse Proxies traffic exactly as ERDDAP expects it."""
    target_path = path_name if path_name else "index.html"
    url = f"{config.erddap_internal_url}/{target_path}"
    
    req_headers = dict(request.headers)
    req_headers["host"] = request.headers.get("x-forwarded-host", request.headers.get("host"))
    req_headers["X-Forwarded-Prefix"] = "/envds/data" 
    
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