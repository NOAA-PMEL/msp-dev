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
    mqtt_subscriptions: str = "envds/+/+/+/data/#,envds/+/+/+/registry/#,envds/+/+/+/status/#"
    daq_id: str | None = None
    data_dir: str = "/erddapData"
    insert_password: str = os.environ.get("ERDDAP_INSERT_PASSWORD", "default_secret")
    author_name: str = "envds_sidecar"
    
    # URL for the Sync Loop to poll the Datastore
    # datastore_url: str = os.environ.get("ERDDAP_SIDECAR_DATASTORE_URL", "http://filemanager:8080")

    class Config:
        env_prefix = "ERDDAP_SIDECAR_"

config = ERDDAPSidecarConfig()
app = FastAPI()
http_client = httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=100),timeout=60.0)

http_semaphore = asyncio.Semaphore(50)

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
        L.info("Initializing ERDDAP datasets configuration...")
        
        # 1. Handle file-based Registries (Ops AND Hardware)
        for reg_file in ["ops_registry_dataset.xml", "hardware_registry_dataset.xml"]:
            reg_source = self.templates_dir / reg_file
            reg_dest = self.datasets_d / reg_file
            if reg_source.exists():
                shutil.copy(reg_source, reg_dest)
                L.info(f"Copied {reg_file} to active datasets.")
            else:
                L.warning(f"Could not find {reg_file} in templates!")

        # 2. Render the HTTP-based Status and Log datasets dynamically
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

        # 3. UNCONDITIONALLY rebuild the master datasets.xml on every single startup
        L.info("Compiling master datasets.xml from active directory state...")
        self.rebuild_master_xml()
        
        # 4. Poke ERDDAP to ensure it reloads the newly compiled master file
        (self.flags_dir / "datasets.xml").touch()
        L.info("ERDDAP initialization sequence complete.")

    def handle_definition(self, ce: dict):
        """Parses sensor definitions, groups by shape, caches metadata, and builds XML."""
        data = ce.data if hasattr(ce, "data") else ce.get("data", {})
        
        # Dynamically unnest the definition block
        def_key = next((k for k in data.keys() if "definition" in k), None)
        if not def_key: 
            return
            
        def_block = data.get(def_key, {})
        attributes = def_block.get("attributes", {})
        variables = def_block.get("variables", {})
        
        make = attributes.get("make", {}).get("data", "unknown")
        model = attributes.get("model", {}).get("data", "unknown")
        sn = attributes.get("serial_number", {}).get("data", "unknown")
        
        version_raw = attributes.get("format_version", {}).get("data", "1")
        major_version = str(version_raw).replace("v", "").split('.')[0]
        version = f"v{major_version}"
        
        cache_key = f"{make}_{model}_{version}"
        definition_cache[cache_key] = {
            "shapes": {k: v.get("shape", ["time"]) for k, v in variables.items()},
            "coords": {
                k: v.get("data", []) 
                for k, v in variables.items() 
                if v.get("attributes", {}).get("variable_type", {}).get("data") == "coordinate"
            }
        }
        
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

        needs_rebuild = False
        for shape, cols in shape_groups.items():
            shape_joined = "_".join(shape)
            dataset_id = f"telemetry_{make}_{model}_{version}_{shape_joined}".replace("-", "_")
            
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
            
            dir_match = re.search(r'<fileDir>([^<]+)</fileDir>', content)
            if dir_match:
                data_dir = Path(dir_match.group(1))
                data_dir.mkdir(parents=True, exist_ok=True)
                
        master_xml.append('\n</erddapDatasets>')
        self.master_xml_path.write_text("\n".join(master_xml))

    def get_all_datasets(self):
        datasets = []
        for xml_file in self.datasets_d.glob("*.xml"):
            content = xml_file.read_text()
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
        return sorted(datasets, key=lambda x: x["id"])

    def toggle_active(self, dataset_id: str):
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
        for xml_file in self.datasets_d.glob("*.xml"):
            content = xml_file.read_text()
            if f'datasetID="{dataset_id}"' in content:
                dir_match = re.search(r'<fileDir>([^<]+)</fileDir>', content)
                if dir_match:
                    data_dir = Path(dir_match.group(1))
                    if data_dir.exists() and data_dir.is_dir():
                        shutil.rmtree(data_dir, ignore_errors=True)
                        L.info(f"Deleted data directory: {data_dir}")

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
    val = obj.get(key)
    if val is None: return default
    if isinstance(val, dict) and "data" in val:
        return val.get("data", default)
    return val

def unroll_multidimensional_data(base_row, shape_dims, coords_dict, var_dict):
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

async def _send_insert(url: str, payload: dict, retries: int = 6, delay: int = 5):
    """Executes the HTTP POST request with a concurrency limit and retries for ERDDAP reloads."""
    async with http_semaphore:
        for attempt in range(retries):
            try:
                # Use POST and pass the dictionary natively to the 'data' parameter (Form URL-Encoded)
                resp = await http_client.post(url, data=payload)
                resp.raise_for_status()
                return  # Success, exit the retry loop
                
            except httpx.HTTPStatusError as e:
                # If 404, ERDDAP might still be reloading datasets.xml. Wait and retry.
                if e.response.status_code == 404 and attempt < retries - 1:
                    L.debug(f"ERDDAP 404 on insert (reloading?). Retrying in {delay}s...", extra={"url": url})
                    await asyncio.sleep(delay)
                    continue
                
                L.error("ERDDAP Insert Failed", extra={"url": url, "reason": str(e)})
                return
                
            except Exception as e:
                L.error("ERDDAP Connection Failed", extra={"url": url, "reason": str(e)})
                return

async def insert_telemetry_to_erddap(ce: dict):
    data = ce.data if hasattr(ce, "data") else ce.get("data", {})
    attributes = data.get("attributes", {})
    variables = data.get("variables", {})
    
    make = _extract_val(attributes, "make", "unknown")
    model = _extract_val(attributes, "model", "unknown")
    sn = _extract_val(attributes, "serial_number", "unknown")
    version_raw = _extract_val(attributes, "format_version", "1.0.0")
    version = f"v{str(version_raw).split('.')[0]}"
    
    time_data = _extract_val(variables, "time")
    if not time_data: return

    time_array = time_data if isinstance(time_data, list) else [time_data]

    def_key = f"{make}_{model}_{version}"
    cached_def = definition_cache.get(def_key, {"shapes": {}, "coords": {}})
    
    insert_tasks = []
    
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
        
        slice_vars = {}
        for v_name, v_data in variables.items():
            if v_name == "time": continue
            v_val = _extract_val(variables, v_name)
            
            if isinstance(v_val, list) and len(v_val) == len(time_array):
                slice_vars[v_name] = v_val[i]
            else:
                slice_vars[v_name] = v_val

        shape_groups = {}
        for v_name, v_val in slice_vars.items():
            shape = tuple(cached_def["shapes"].get(v_name, ["time"]))
            if shape not in shape_groups: shape_groups[shape] = {}
            shape_groups[shape][v_name] = v_val
            
        for shape, var_dict in shape_groups.items():
            shape_joined = "_".join(shape)
            dataset_id = f"telemetry_{make}_{model}_{version}_{shape_joined}".replace("-", "_")
            extra_dims = [dim for dim in shape if dim != "time"]
            
            coords_dict = cached_def["coords"].copy()
            for dim in extra_dims:
                payload_coord = slice_vars.get(dim)
                if payload_coord: coords_dict[dim] = payload_coord
                    
            for flat_row in unroll_multidimensional_data(base_params, extra_dims, coords_dict, var_dict):
                # We no longer need the ?query_string!
                insert_url = f"{config.erddap_internal_url}/tabledap/{dataset_id}.insert"
                insert_tasks.append(_send_insert(insert_url, payload=flat_row))

    if insert_tasks:
        await asyncio.gather(*insert_tasks, return_exceptions=True)

async def handle_ops_registry_insert(ce: dict):
    attrs = ce.get("attributes", ce) if isinstance(ce, dict) else ce.get_attributes()
    data = ce.data if hasattr(ce, "data") else ce.get("data", {})
    if not data: return

    def_key = next((k for k in data.keys() if "definition" in k), None)
    if not def_key: return
    
    def_block = data.get(def_key, {})
    metadata = def_block.get("metadata", {})
    
    if not metadata: 
        return

    kind = def_key
    namespace = metadata.get("sampling_namespace", "unknown")
    name = metadata.get("name", "unknown")
    revision = metadata.get("revision", 1)
    
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

    record = [
        time.time(),
        kind,
        namespace,
        name,
        valid_config_time,
        revision,
        json.dumps(data, separators=(',', ':'))
    ]

    is_new = not file_path.exists() or file_path.stat().st_size == 0
    with open(file_path, "a") as f:
        if is_new:
            f.write('["time","kind","namespace","name","valid_config_time","revision","payload"]\n')
            f.write('["double","String","String","String","String","int","String"]\n')
        f.write(json.dumps(record) + "\n")
        
    flag_dir = Path(config.data_dir) / "hardFlag"
    flag_dir.mkdir(parents=True, exist_ok=True)
    (flag_dir / "envds_system_registry").touch()

async def handle_ops_status_insert(ce: dict):
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
    
    insert_url = f"{config.erddap_internal_url}/tabledap/envds_ops_status.insert"
    await _send_insert(insert_url, payload=params)

async def handle_ops_log_insert(ce: dict):
    data = ce.data if hasattr(ce, "data") else ce.get("data", {})
    if not data: return
    
    attrs = ce.get_attributes() if hasattr(ce, "get_attributes") else ce

    time_str = attrs.get("time") 
    from envds.util.util import string_to_timestamp
    timestamp = string_to_timestamp(time_str) if time_str else time.time()

    params = {
        "author": config.author_name,
        "password": config.insert_password,
        "time": timestamp,
        "deployment_ref": attrs.get("deploymentref", "unknown"),
        "project_ref": attrs.get("projectref", "unknown"),
        "event_type": data.get("event_type", "unknown"),
        "subject": attrs.get("subject", "system"),
        "description": data.get("description", "")
    }
    
    insert_url = f"{config.erddap_internal_url}/tabledap/envds_ops_log.insert"
    await _send_insert(insert_url, payload=params)
    
async def handle_hardware_registry_insert(ce: dict):
    attrs = ce.get("attributes", ce) if isinstance(ce, dict) else ce.get_attributes()
    data = ce.data if hasattr(ce, "data") else ce.get("data", {})
    if not data: return

    def_key = next((k for k in data.keys() if "definition" in k), None)
    if not def_key: return
    
    def_block = data.get(def_key, {})
    def_attrs = def_block.get("attributes", {})
    if not def_attrs: return

    kind = def_key
    
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

    record = [
        time.time(),
        kind,
        make,
        model,
        exact_version,
        valid_config_time,
        json.dumps(data, separators=(',', ':'))
    ]

    is_new = not file_path.exists() or file_path.stat().st_size == 0
    with open(file_path, "a") as f:
        if is_new:
            f.write('["time","kind","make","model","version","valid_config_time","payload"]\n')
            f.write('["double","String","String","String","String","String","String"]\n')
        f.write(json.dumps(record) + "\n")
        
    flag_dir = Path(config.data_dir) / "hardFlag"
    flag_dir.mkdir(parents=True, exist_ok=True)
    (flag_dir / "envds_hardware_registry").touch()

# ---------------------------------------------------------
# 3. BACKGROUND TASKS & API ROUTING
# ---------------------------------------------------------
async def sync_definitions_loop():
    """Periodically fetches active definitions from the Datastore to ensure ERDDAP is in sync."""
    await asyncio.sleep(10) # Give ERDDAP and Datastore time to fully boot
    
    datastore_host = f"datastore.{config.daq_id}-system.svc.cluster.local"
    datastore_url = f"http://{datastore_host}" 
    
    HARDWARE_RESOURCES = ["device", "controller"]
    
    OPS_RESOURCES = [
        "platform", "project", "deployment", "contact", 
        "systemmode", "samplingmode", "samplingstate", 
        "samplingcondition", "action",
        "variablemap", "variableset"
    ]
    
    # Initialize the memory cache
    all_resources = HARDWARE_RESOURCES + OPS_RESOURCES
    known_ids = {f"{res}-definition": set() for res in all_resources}
    
    # -----------------------------------------------------------------
    # PRE-FLIGHT DISK DISCOVERY: Seed known_ids from what ERDDAP already has
    # -----------------------------------------------------------------
    L.info("Sync Loop starting pre-flight storage discovery...")
    base_data_path = Path(config.data_dir) / "registry"
    
    # Scan Hardware Registry Directory
    hw_path = base_data_path / "hardware"
    if hw_path.exists():
        for jsonl_file in hw_path.glob("*/*_registry.jsonl"):
            try:
                with open(jsonl_file, "r") as f:
                    for line in f:
                        if line.startswith("["): # Only parse ERDDAP jsonlCSV arrays
                            row = json.loads(line)
                            if len(row) > 4 and row[0] != "time": # Skip headers
                                # Hardware ID format from row arrays: make::model::version
                                make, model, version = row[2], row[3], row[4]
                                endpoint_key = jsonl_file.parent.name # 'device-definition' or 'controller-definition'
                                known_ids[endpoint_key].add(f"{make}::{model}::{version}")
            except Exception as e:
                L.error(f"Discovery failed to parse hardware file {jsonl_file.name}", extra={"reason": str(e)})

    # Scan Operations/System Registry Directory
    sys_path = base_data_path / "system"
    if sys_path.exists():
        for jsonl_file in sys_path.glob("*/*_registry.jsonl"):
            try:
                with open(jsonl_file, "r") as f:
                    for line in f:
                        if line.startswith("["):
                            row = json.loads(line)
                            if len(row) > 3 and row[0] != "time": # Skip headers
                                # System ID format from row arrays: namespace::name::valid_config_time
                                namespace, name = row[2], row[3]
                                endpoint_key = jsonl_file.parent.name # e.g., 'platform-definition'
                                
                                # Variablesets and Variablemaps use compound IDs in Datastore
                                if endpoint_key in ["variablemap-definition", "variableset-definition"]:
                                    # For compound structures, the final array row element payload contains the true tracking ID
                                    payload = json.loads(row[6])
                                    def_id = payload.get(f"{endpoint_key.replace('-', '_')}_id")
                                    if def_id:
                                        known_ids[endpoint_key].add(def_id)
                                else:
                                    # Standard sampling definitions register by name string
                                    known_ids[endpoint_key].add(name)
            except Exception as e:
                L.error(f"Discovery failed to parse system file {jsonl_file.name}", extra={"reason": str(e)})

    L.info("Pre-flight discovery complete.", extra={k: len(v) for k, v in known_ids.items()})

    # -----------------------------------------------------------------
    # THE ACTIVE SYNC POLLING LOOP
    # -----------------------------------------------------------------
    while True:
        try:
            # SECTION 1: HARDWARE DEFINITIONS (Triggers ERDDAP XML Builds)
            for resource in HARDWARE_RESOURCES:
                endpoint = f"{resource}-definition"
                ids_url = f"{datastore_url}/{endpoint}/registry/ids/get/"
                ids_resp = await http_client.get(ids_url)
                
                if ids_resp.status_code == 200:
                    remote_ids = ids_resp.json().get("results", [])
                    missing_ids = [rid for rid in remote_ids if rid not in known_ids[endpoint]]
                    
                    if missing_ids:
                        L.info(f"Sync Loop found {len(missing_ids)} missing {endpoint}s. Fetching payloads...")
                        for missing_id in missing_ids:
                            param_name = f"{endpoint.replace('-', '_')}_id"
                            get_url = f"{datastore_url}/{endpoint}/registry/get/"
                            
                            def_resp = await http_client.get(get_url, params={param_name: missing_id})
                            if def_resp.status_code == 200:
                                definitions = def_resp.json().get("results", [])
                                
                                for def_payload in definitions:
                                    ce_mock = {"data": {endpoint: def_payload}}
                                    compiler.handle_definition(ce_mock)
                                    await handle_hardware_registry_insert(ce_mock)
                                    
                                known_ids[endpoint].add(missing_id)

            # SECTION 2: OPERATIONS & SAMPLING DEFINITIONS (JSONL Registries Only)
            for resource in OPS_RESOURCES:
                endpoint = f"{resource}-definition"
                ids_url = f"{datastore_url}/{endpoint}/registry/ids/get/"
                ids_resp = await http_client.get(ids_url)
                
                if ids_resp.status_code == 200:
                    remote_ids = ids_resp.json().get("results", [])
                    missing_ids = [rid for rid in remote_ids if rid not in known_ids[endpoint]]
                    
                    if missing_ids:
                        L.info(f"Sync Loop found {len(missing_ids)} missing {endpoint}s. Fetching payloads...")
                        for missing_id in missing_ids:
                            if resource == "variablemap":
                                params = {"variablemap_definition_id": missing_id}
                            elif resource == "variableset":
                                params = {"variableset_definition_id": missing_id}
                            else:
                                params = {"name": missing_id}
                            
                            get_url = f"{datastore_url}/{endpoint}/registry/get/"
                            
                            def_resp = await http_client.get(get_url, params=params)
                            if def_resp.status_code == 200:
                                definitions = def_resp.json().get("results", [])
                                
                                for def_payload in definitions:
                                    ce_mock = {"data": {endpoint: def_payload}}
                                    await handle_ops_registry_insert(ce_mock)
                                    
                                known_ids[endpoint].add(missing_id)

        except Exception as e:
            L.error("Failed to sync definitions from Datastore", extra={"reason": str(e)})
        
        await asyncio.sleep(60) # Sync every 60 seconds

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
                        
                        # 1. STRICT MATCH for Hardware Telemetry Data (ignores variableset pollution)
                        if ce_type in ["envds.data.update", "envds.controller.data.update"]:
                            await insert_telemetry_to_erddap(ce)
                            
                        # 2. Operational Status Tracking
                        elif "status.update" in ce_type:
                            if any(ops in ce_type for ops in ["samplingcondition", "samplingstate", "samplingmode", "systemmode"]):
                                await handle_ops_status_insert(ce)

                        # 3. Discrete Operational Logs
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
    asyncio.create_task(sync_definitions_loop())

@app.on_event("shutdown")
async def shutdown_event():
    await http_client.aclose()

# ---------------------------------------------------------
# KNATIVE INGESTION (HTTP) - For Definitions & Registries
# ---------------------------------------------------------
@app.post("/registry/update/")
async def registry_update(request: Request):
    try:
        body = await request.body()
        ce = from_http(request.headers, body)
        ce_type = ce.get("type", "")
        
        L.debug(f"Received Knative Registry Update", extra={"type": ce_type})
        
        if any(hw in ce_type for hw in ["device-definition", "controller-definition"]):
            compiler.handle_definition(ce)
            await handle_hardware_registry_insert(ce)
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