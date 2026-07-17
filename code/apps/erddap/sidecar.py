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
from pydantic import field_validator
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
    author_name: str = "envds-sidecar" # no underscores

    class Config:
        env_prefix = "ERDDAP_SIDECAR_"

    @field_validator("author_name")
    @classmethod
    def replace_underscores_with_hyphens(cls, v: str) -> str:
        """Automatically converts underscores to hyphens to protect ERDDAP parsing."""
        return v.replace("_", "-")
    
config = ERDDAPSidecarConfig()
app = FastAPI()
http_client = httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=100),timeout=60.0)

http_semaphore = asyncio.Semaphore(50)

definition_cache = {}
definition_registry_cache = {}

# ---------------------------------------------------------
# EXPLICIT GITOPS WRAPPER CONTRACT
# ---------------------------------------------------------
VALID_DEFINITION_KEYS = {
    "device-definition", "controller-definition",
    "platform-definition", "project-definition", "deployment-definition",
    "contact-definition", "projectallocation-definition",
    "variablemap-definition", "variableset-definition", "dataset-definition",
    "systemmode-definition", "samplingmode-definition", 
    "samplingstate-definition", "samplingcondition-definition", "action-definition"
}

# ---------------------------------------------------------
# TYPE MAPPING HELPER
# ---------------------------------------------------------
def map_erddap_type(raw_type: str) -> str:
    """Translates arbitrary datastore/sensor types into ERDDAP PrimitiveArray types."""
    t = str(raw_type).lower()
    if t in ["string", "str", "char", "text", "boolean", "bool"]:
        return "String"
    if t in ["int", "integer", "short"]:
        return "int"
    if t in ["long"]:
        return "long"
    if t in ["float"]:
        return "float"
    return "double"

# ---------------------------------------------------------
# DYNAMIC NCO-JSON UNFLATTENER
# ---------------------------------------------------------
def unflatten_telemetry_to_ncojson(flat_data: dict, definition: dict) -> list:
    """Dynamically reconstructs n-dimensional NCO-JSON from flat ERDDAP rows."""
    if not definition:
        return flat_data.get("table", {}).get("rows", [])
        
    cols = flat_data.get("table", {}).get("columnNames", [])
    rows = flat_data.get("table", {}).get("rows", [])
    
    if not cols or not rows:
        return []
        
    sys_cols = {"timestamp", "author", "command"}
    var_cols = [c for c in cols if c not in sys_cols]
    
    # Context-aware Shape Identification Fix
    target_var = next((v for v in var_cols if v in definition.get("variables", {})), None)

    if target_var and "shape" in definition["variables"][target_var]:
        shape_dims = [dim for dim in definition["variables"][target_var]["shape"] if dim in cols]
    else:
        def_dims = list(definition.get("dimensions", {}).keys())
        shape_dims = [dim for dim in def_dims if dim in cols]
        shape_dims = sorted(shape_dims, key=lambda d: def_dims.index(d))
    
    var_cols = [c for c in var_cols if c not in shape_dims]
    
    # Build the N-Dimensional Grouping Tree
    grouped = {}
    for row in rows:
        row_dict = dict(zip(cols, row))
        current_level = grouped
        for i, dim in enumerate(shape_dims):
            dim_val = row_dict[dim]
            if i == len(shape_dims) - 1:
                current_level[dim_val] = row_dict 
            else:
                if dim_val not in current_level:
                    current_level[dim_val] = {}
                current_level = current_level[dim_val]
    
    def _cast(val, target_type):
        if val is None or val == "NaN": return None
        t = str(target_type).lower()
        try:
            if t in ["int", "integer", "short", "long"]: return int(float(val))
            if t in ["float", "double"]: return float(val)
            if t in ["bool", "boolean"]: return str(val).lower() in ["true", "1", "t", "y", "yes"]
            return str(val)
        except (ValueError, TypeError):
            return val

    def extract_array(node, var_name, dims_left):
        if not dims_left:
            return _cast(node.get(var_name), definition["variables"].get(var_name, {}).get("type", "float"))
        return [extract_array(node[k], var_name, dims_left[1:]) for k in sorted(node.keys())]
        
    def extract_coords(node, dims_left, coords_dict):
        if not dims_left: return
        current_dim = dims_left[0]
        sorted_keys = sorted(node.keys())
        if current_dim not in coords_dict:
            coords_dict[current_dim] = [_cast(k, definition["variables"].get(current_dim, {}).get("type", "float")) for k in sorted_keys]
        if sorted_keys:
            extract_coords(node[sorted_keys[0]], dims_left[1:], coords_dict)

    nco_results = []
    for time_val, time_node in grouped.items():
        leaf = time_node
        for _ in range(len(shape_dims) - 1):
            leaf = leaf[list(leaf.keys())[0]]
            
        make = definition['attributes'].get('make', {}).get('data', 'unknown')
        model = definition['attributes'].get('model', {}).get('data', 'unknown')
        sn = leaf.get('serial_number') or definition['attributes'].get('serial_number', {}).get('data', 'unknown')
        
        record = {
            "device_id": f"{make}::{model}::{sn}",
            "timestamp": leaf.get("timestamp", 0.0),
            "attributes": definition.get("attributes", {}),
            "dimensions": {},
            "variables": {"time": {"data": time_val}}
        }
        
        if len(shape_dims) > 1:
            coords_dict = {}
            extract_coords(time_node, shape_dims[1:], coords_dict)
            for c_name, c_data in coords_dict.items():
                record["dimensions"][c_name] = {"data": c_data}
                
        for var_name in var_cols:
            if var_name == "serial_number" or var_name.endswith("_dim"): continue
            if len(shape_dims) == 1:
                record["variables"][var_name] = {"data": _cast(leaf.get(var_name), definition["variables"].get(var_name, {}).get("type", "float"))}
            else:
                record["variables"][var_name] = {"data": extract_array(time_node, var_name, shape_dims[1:])}
                
        nco_results.append(record)
        
    return nco_results

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
        
        for reg_file in ["ops_registry_dataset.xml", "hardware_registry_dataset.xml"]:
            reg_source = self.templates_dir / reg_file
            reg_dest = self.datasets_d / reg_file
            if reg_source.exists():
                shutil.copy(reg_source, reg_dest)
                L.info(f"Copied {reg_file} to active datasets.")
            else:
                L.warning(f"Could not find {reg_file} in templates!")

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
                
                dir_match = re.search(r'<fileDir>([^<]+)</fileDir>', xml_content)
                if dir_match:
                    dataset_dir = Path(dir_match.group(1))
                    dataset_dir.mkdir(parents=True, exist_ok=True)
                    
                    seed_file = dataset_dir / "seed.jsonl"
                    if "status" in template_name:
                        col_names = ["app_group", "app_uid", "namespace", "time", "valid_config_time", "requested_state", "actual_state", "timestamp", "author", "command"]
                        dummy_vals = ["seed", "seed", "seed", "1970-01-01T00:00:00.000000Z", "seed", "seed", "seed", 0.0, "seed", 0]
                    else:
                        col_names = ["deployment_ref", "project_ref", "time", "event_type", "subject", "description", "timestamp", "author", "command"]
                        dummy_vals = ["seed", "seed", "1970-01-01T00:00:00.000000Z", "seed", "seed", "seed", 0.0, "seed", 0]
                        
                    seed_content = f"{json.dumps(col_names, separators=(',', ':'))}\n{json.dumps(dummy_vals, separators=(',', ':'))}\n"
                    seed_file.write_text(seed_content)
                    L.info(f"Dropped space-free operational seed file into {dataset_dir}")
                
                L.info(f"Rendered {template_name} successfully.")
            except Exception as e:
                L.error(f"Failed to render {template_name}", extra={"error": str(e)})

        L.info("Compiling master datasets.xml from active directory state...")
        self.rebuild_master_xml()
        
        (self.flags_dir / "datasets").touch()
        L.info("ERDDAP initialization sequence complete.")

    def handle_definition(self, ce: dict):
        """Parses sensor definitions, groups by shape, caches metadata, and builds XML."""
        data = ce.data if hasattr(ce, "data") else ce.get("data", {})
        
        # STRICT MATCHING
        def_key = next((k for k in data.keys() if k in VALID_DEFINITION_KEYS), None)
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
            if var_data.get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
                continue
            
            shape_tuple = tuple(var_data.get("shape", ["time"]))
            if shape_tuple not in shape_groups:
                shape_groups[shape_tuple] = []
            
            raw_type = var_data.get("type", "float")
            shape_groups[shape_tuple].append({
                "name": var_name,
                "type": map_erddap_type(raw_type),
                "original_type": raw_type,
                "units": var_data.get("attributes", {}).get("units", {}).get("data", ""),
                "long_name": var_data.get("attributes", {}).get("long_name", {}).get("data", var_name),
                "variable_type": var_data.get("attributes", {}).get("variable_type", {}).get("data", "")
            })

        needs_rebuild = False
        for shape, cols in shape_groups.items():
            shape_joined = "_".join(shape)
            dataset_id = f"telemetry_{make}_{model}_{version}_{shape_joined}".replace("-", "_")
            
            req_vars_list = ["make", "model", "format_version", "serial_number", "time"]
            for dim in shape:
                if dim != "time":
                    req_vars_list.append(dim)
            req_vars_str = ",".join(req_vars_list)
            
            coord_idx = 0
            for dim in shape:
                if dim != "time" and not any(c["name"] == dim for c in cols):
                    if dim in variables:
                        dim_var = variables[dim]
                        raw_type = dim_var.get("type", "float")
                        
                        cols.insert(coord_idx, {
                            "name": dim,
                            "type": map_erddap_type(raw_type),
                            "original_type": raw_type,
                            "units": dim_var.get("attributes", {}).get("units", {}).get("data", ""),
                            "long_name": dim_var.get("attributes", {}).get("long_name", {}).get("data", dim)
                        })
                        coord_idx += 1
                        
                        dim_len_name = f"{dim}_dim"
                        if not any(c["name"] == dim_len_name for c in cols):
                            cols.insert(coord_idx, {
                                "name": dim_len_name,
                                "type": "int",
                                "original_type": "int",
                                "units": "count",
                                "long_name": f"{dim} Dimension Length"
                            })
                            coord_idx += 1
                    else:
                        cols.insert(coord_idx, {
                            "name": dim,
                            "type": "int",
                            "original_type": "int",
                            "units": "count",
                            "long_name": f"{dim} Index"
                        })
                        coord_idx += 1
                        
                        dim_len_name = f"{dim}_dim"
                        if not any(c["name"] == dim_len_name for c in cols):
                            cols.insert(coord_idx, {
                                "name": dim_len_name,
                                "type": "int",
                                "original_type": "int",
                                "units": "count",
                                "long_name": f"{dim} Dimension Length"
                            })
                            coord_idx += 1

            xml_content = self.telemetry_template.render(
                dataset_id=dataset_id, make=escape(make), model=escape(model),
                sn=escape(sn), version=version, format_version=str(version_raw),
                shape_joined=shape_joined, columns=cols,
                author=escape(config.author_name), password=escape(config.insert_password),
                req_vars=req_vars_str
            )
            
            snippet_path = self.datasets_d / f"{dataset_id}.xml"
            if not snippet_path.exists() or snippet_path.read_text() != xml_content:
                snippet_path.write_text(xml_content)
                L.info(f"Generated new ERDDAP dataset: {dataset_id}")
                needs_rebuild = True

            dir_match = re.search(r'<fileDir>([^<]+)</fileDir>', xml_content)
            if dir_match:
                dataset_dir = Path(dir_match.group(1))
                dataset_dir.mkdir(parents=True, exist_ok=True)
                seed_file = dataset_dir / "seed.jsonl"
                
                base_cols = ["make", "model", "format_version", "serial_number", "time"]
                dyn_cols = [c["name"] for c in cols if c["name"] != "time"]
                tail_cols = ["timestamp", "author", "command"]
                
                col_names = base_cols + dyn_cols + tail_cols
                dummy_vals = []
                for name in col_names:
                    if name in ["make", "model", "format_version", "serial_number", "author"]:
                        dummy_vals.append("seed")
                    elif name == "time":
                        dummy_vals.append("1970-01-01T00:00:00.000000Z")
                    elif name == "timestamp":
                        dummy_vals.append(0.0)
                    elif name == "command":
                        dummy_vals.append(0) 
                    else:
                        c = next((c for c in cols if c["name"] == name), None)
                        if c and c["type"] == "String": dummy_vals.append("seed")
                        elif c and c["type"] in ["int", "long"]: dummy_vals.append(0)
                        else: dummy_vals.append(0.0)
                            
                seed_content = f"{json.dumps(col_names, separators=(',', ':'))}\n{json.dumps(dummy_vals, separators=(',', ':'))}\n"
                seed_file.write_text(seed_content)
                
            (self.flags_dir / dataset_id).touch()

        if needs_rebuild:
            self.rebuild_master_xml()
            (self.flags_dir / "datasets").touch()

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
                (self.flags_dir / "datasets").touch()
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
                (self.flags_dir / "datasets").touch()
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

def format_erddap_array(data_list):
    """Helper to format Python lists into ERDDAP's expected bracketed string array."""
    if not isinstance(data_list, list):
        return data_list
    return f"[{','.join(str(x) for x in data_list)}]"

def unroll_multidimensional_data(base_row, shape_dims, coords_dict, var_dict):
    """
    Unrolls multi-dimensional arrays up to the second-to-last dimension. 
    Leaves the final dimension intact as an ERDDAP string array for native ingestion, 
    and explicitly calculates/stores the dimension length.
    """
    # CASE 1: 1D Data (Only Time)
    if not shape_dims:
        row = base_row.copy()
        row.update(var_dict)
        yield row
        return
        
    # CASE 2: 2D Data (Time + 1 Dim, e.g., Diameter)
    if len(shape_dims) == 1:
        dim_name = shape_dims[0]
        row = base_row.copy()
        
        dim_data = coords_dict.get(dim_name, [])
        if not dim_data and var_dict:
            first_var = next(iter(var_dict.values()))
            if isinstance(first_var, list):
                dim_data = list(range(len(first_var)))

        row[dim_name] = format_erddap_array(dim_data)
        
        # Save the explicit length of the dimension array
        row[f"{dim_name}_dim"] = len(dim_data) if isinstance(dim_data, list) else 1
        
        for v_name, v_data in var_dict.items():
            row[v_name] = format_erddap_array(v_data)
            
        yield row
        return
        
    # CASE 3: 3D+ Data (Time + >=2 Dims)
    last_dim = shape_dims[-1]
    unroll_dims = shape_dims[:-1]
    
    def recurse(dim_index, current_indices, current_row):
        if dim_index == len(unroll_dims):
            row = current_row.copy()
            
            dim_data = coords_dict.get(last_dim, [])
            if not dim_data and var_dict:
                first_var = next(iter(var_dict.values()))
                val = first_var
                try:
                    for idx in current_indices:
                        val = val[idx]
                    if isinstance(val, list):
                        dim_data = list(range(len(val)))
                except (IndexError, TypeError):
                    pass

            row[last_dim] = format_erddap_array(dim_data)
            
            # Save the explicit length of the dimension array
            row[f"{last_dim}_dim"] = len(dim_data) if isinstance(dim_data, list) else 1
            
            for v_name, v_data in var_dict.items():
                val = v_data
                try:
                    for idx in current_indices:
                        val = val[idx]
                    row[v_name] = format_erddap_array(val)
                except (IndexError, TypeError):
                    row[v_name] = None
                    
            yield row
            return
            
        dim_name = unroll_dims[dim_index]
        dim_coords = coords_dict.get(dim_name, [])
        
        if not dim_coords and var_dict:
            first_var = next(iter(var_dict.values()))
            val = first_var
            try:
                for idx in current_indices:
                    val = val[idx]
                if isinstance(val, list):
                    dim_coords = list(range(len(val)))
            except (IndexError, TypeError):
                pass
                
        for i, coord_val in enumerate(dim_coords):
            next_row = current_row.copy()
            next_row[dim_name] = coord_val 
            yield from recurse(dim_index + 1, current_indices + [i], next_row)
            
    yield from recurse(0, [], base_row)

async def _send_insert(url: str, payload: dict, retries: int = 1, delay: int = 5):
    async with http_semaphore:
        
        # Enforce exact ERDDAP parameter alignment based on the XML schema order
        ordered_payload = {}
        
        # 1. Base metadata fields (Matches the top of telemetry_dataset.xml.j2)
        base_keys = ["make", "model", "format_version", "serial_number", "time"]
        for k in base_keys:
            if k in payload:
                ordered_payload[k] = payload[k]
                
        # 2. Dynamic data columns (Coordinates, lengths, and variables)
        tail_keys = ["timestamp", "author", "command"]
        for k in payload.keys():
            if k not in base_keys and k not in tail_keys:
                ordered_payload[k] = payload[k]
                
        # 3. Trailing system fields (Matches the bottom of telemetry_dataset.xml.j2)
        for k in tail_keys:
            if k in payload:
                ordered_payload[k] = payload[k]

        headers = {
            "X-Forwarded-Proto": "https",
            "X-Forwarded-For": "127.0.0.1"
        }

        for attempt in range(retries):
            try:
                # Dispatch using the strictly ordered payload
                resp = await http_client.post(url, params=ordered_payload, headers=headers)
                resp.raise_for_status()
                return 
                
            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404 and attempt < retries - 1:
                    L.debug(f"ERDDAP 404 on insert (reloading?). Retrying in {delay}s...", extra={"url": url})
                    if retries > 1:
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
            "make": make,
            "model": model,
            "format_version": str(version_raw),
            "serial_number": sn,
            "time": current_time,
            "author": f"{config.author_name}_{config.insert_password}"
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
            shape_joined = "_join" if isinstance(shape, str) else "_".join(shape)
            dataset_id = f"telemetry_{make}_{model}_{version}_{shape_joined}".replace("-", "_")
            extra_dims = [dim for dim in shape if dim != "time"]
            
            coords_dict = cached_def["coords"].copy()
            for dim in extra_dims:
                payload_coord = slice_vars.get(dim)
                if payload_coord: coords_dict[dim] = payload_coord
                    
            for flat_row in unroll_multidimensional_data(base_params, extra_dims, coords_dict, var_dict):
                insert_url = f"{config.erddap_internal_url}/tabledap/{dataset_id}.insert"
                insert_tasks.append(_send_insert(insert_url, payload=flat_row))

    if insert_tasks:
        await asyncio.gather(*insert_tasks, return_exceptions=True)

async def handle_ops_registry_insert(ce: dict):
    attrs = ce.get("attributes", ce) if isinstance(ce, dict) else ce.get_attributes()
    data = ce.data if hasattr(ce, "data") else ce.get("data", {})
    if not data: return

    # STRICT MATCHING
    def_key = next((k for k in data.keys() if k in VALID_DEFINITION_KEYS), None)
    if not def_key: return
    
    def_block = data.get(def_key, {})
    metadata = def_block.get("metadata", {})
    
    if not metadata: 
        return

    kind = def_key
    namespace = metadata.get("sampling_namespace", "unknown")
    name = metadata.get("name") or def_block.get(f"{kind.replace('-', '_')}_id", "unknown")
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
        time.time(), kind, namespace, name, valid_config_time, revision,
        json.dumps(data, separators=(',', ':'))
    ]

    existing_records = []
    if file_path.exists() and file_path.stat().st_size > 0:
        with open(file_path, "r") as f:
            lines = f.readlines()
            if len(lines) >= 2: 
                for line in lines[2:]:
                    try:
                        row = json.loads(line)
                        if not (row[2] == namespace and row[3] == name):
                            existing_records.append(line)
                    except json.JSONDecodeError:
                        pass

    with open(file_path, "w") as f:
        f.write('["time","kind","namespace","name","valid_config_time","revision","payload"]\n')
        f.write('["double","String","String","String","String","int","String"]\n')
        for rec in existing_records:
            f.write(rec)
        f.write(json.dumps(record, separators=(',', ':')) + "\n")
        
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
        "time": timestamp,
        "app_group": id_block.get("app_group", "unknown"),
        "app_uid": id_block.get("app_uid", "unknown"),
        "namespace": id_block.get("sampling_namespace", "unknown"),
        "valid_config_time": id_block.get("valid_config_time", "unknown"),
        "requested_state": requested,
        "actual_state": actual,
        "author": f"{config.author_name}_{config.insert_password}"
    }
    
    insert_url = f"{config.erddap_internal_url}/tabledap/envds_ops_status.insert"
    await _send_insert(insert_url, payload=params)

async def handle_ops_log_insert(ce: dict):
    data = ce.data if hasattr(ce, "data") else ce.get("data", {})
    if not data: return
    
    attrs = ce.get_attributes() if hasattr(ce, "get_attributes") else ce

    time_str = attrs.get("time") 
    if not time_str:
        from datetime import datetime, timezone
        time_str = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    params = {
        "time": time_str,
        "deployment_ref": attrs.get("deploymentref", "unknown"),
        "project_ref": attrs.get("projectref", "unknown"),
        "event_type": data.get("event_type", "unknown"),
        "subject": attrs.get("subject", "system"),
        "description": data.get("description", ""),
        "author": f"{config.author_name}_{config.insert_password}"
    }
    
    insert_url = f"{config.erddap_internal_url}/tabledap/envds_ops_log.insert"
    await _send_insert(insert_url, payload=params)
    
async def handle_hardware_registry_insert(ce: dict):
    attrs = ce.get("attributes", ce) if isinstance(ce, dict) else ce.get_attributes()
    data = ce.data if hasattr(ce, "data") else ce.get("data", {})
    if not data: return

    # STRICT MATCHING
    def_key = next((k for k in data.keys() if k in VALID_DEFINITION_KEYS), None)
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
        time.time(), kind, make, model, exact_version, valid_config_time,
        json.dumps(data, separators=(',', ':'))
    ]

    existing_records = []
    if file_path.exists() and file_path.stat().st_size > 0:
        with open(file_path, "r") as f:
            lines = f.readlines()
            if len(lines) >= 2: 
                for line in lines[2:]:
                    try:
                        row = json.loads(line)
                        if not (row[2] == make and row[3] == model and row[4] == exact_version):
                            existing_records.append(line)
                    except json.JSONDecodeError:
                        pass

    with open(file_path, "w") as f:
        f.write('["time","kind","make","model","version","valid_config_time","payload"]\n')
        f.write('["double","String","String","String","String","String","String"]\n')
        for rec in existing_records:
            f.write(rec)
        f.write(json.dumps(record, separators=(',', ':')) + "\n")
        
    flag_dir = Path(config.data_dir) / "hardFlag"
    flag_dir.mkdir(parents=True, exist_ok=True)
    (flag_dir / "envds_hardware_registry").touch()

# ---------------------------------------------------------
# 3. BACKGROUND TASKS & API ROUTING
# ---------------------------------------------------------
async def sync_definitions_loop():
    """Periodically fetches active definitions from the Datastore to ensure ERDDAP is in sync."""
    await asyncio.sleep(10) 
    
    datastore_host = f"datastore.{config.daq_id}-system.svc.cluster.local"
    datastore_url = f"http://{datastore_host}" 
    
    HARDWARE_RESOURCES = ["device", "controller"]
    
    OPS_RESOURCES = [
        "platform", "project", "deployment", "contact", 
        "systemmode", "samplingmode", "samplingstate", 
        "samplingcondition", "action",
        "variablemap", "variableset", 
        "projectallocation"
    ]
    
    all_resources = HARDWARE_RESOURCES + OPS_RESOURCES
    known_ids = {f"{res}-definition": set() for res in all_resources}
    
    L.info("Sync Loop starting pre-flight storage discovery...")
    base_data_path = Path(config.data_dir) / "registry"
    
    hw_path = base_data_path / "hardware"
    if hw_path.exists():
        for jsonl_file in hw_path.glob("*/*_registry.jsonl"):
            try:
                with open(jsonl_file, "r") as f:
                    for line in f:
                        if line.startswith("["): 
                            row = json.loads(line)
                            if len(row) > 4 and row[0] not in ["time", "double"]: 
                                make, model, version = row[2], row[3], row[4]
                                endpoint_key = jsonl_file.parent.name 
                                known_ids[endpoint_key].add(f"{make}::{model}::{version}")
            except Exception as e:
                L.error(f"Discovery failed to parse hardware file {jsonl_file.name}", extra={"reason": str(e)})

    # sys_path = base_data_path / "system"
    # if sys_path.exists():
    #     for jsonl_file in sys_path.glob("*/*_registry.jsonl"):
    #         try:
    #             with open(jsonl_file, "r") as f:
    #                 for line in f:
    #                     if line.startswith("["):
    #                         row = json.loads(line)
    #                         if len(row) > 3 and row[0] not in ["time", "double"]: 
    #                             namespace, name = row[2], row[3]
    #                             endpoint_key = jsonl_file.parent.name 
                                
    #                             if endpoint_key in ["variablemap-definition", "variableset-definition"]:
    #                                 payload = json.loads(row[6])
    #                                 def_id = payload.get(f"{endpoint_key.replace('-', '_')}_id")
    #                                 if def_id:
    #                                     known_ids[endpoint_key].add(def_id)
    #                             else:
    #                                 known_ids[endpoint_key].add(name)
    sys_path = base_data_path / "system"
    if sys_path.exists():
        for jsonl_file in sys_path.glob("*/*_registry.jsonl"):
            try:
                with open(jsonl_file, "r") as f:
                    for line in f:
                        if line.startswith("["):
                            row = json.loads(line)
                            # FIX: Check for len > 4 to extract valid_time safely
                            if len(row) > 4 and row[0] not in ["time", "double"]: 
                                namespace, name, valid_time = row[2], row[3], row[4]
                                endpoint_key = jsonl_file.parent.name 
                                
                                if endpoint_key in ["variablemap-definition", "variableset-definition"]:
                                    payload = json.loads(row[6])
                                    def_id = payload.get(f"{endpoint_key.replace('-', '_')}_id")
                                    if def_id:
                                        known_ids[endpoint_key].add(def_id)
                                else:
                                    # ---> NEW ID FORMAT: namespace::name::time <---
                                    known_ids[endpoint_key].add(f"{namespace}::{name}::{valid_time}")
            except Exception as e:
                L.error(f"Discovery failed to parse system file {jsonl_file.name}", extra={"reason": str(e)})

    L.info("Pre-flight discovery complete.", extra={k: len(v) for k, v in known_ids.items()})

    while True:
        try:
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
        
        await asyncio.sleep(60)

async def mqtt_loop():
    reconnect = 10
    while True:
        try:
            L.info(f"Connecting to MQTT Broker: {config.mqtt_broker}")
            async with Client(config.mqtt_broker, port=config.mqtt_port, identifier=str(ULID())) as client:
                for topic in config.mqtt_subscriptions.split(","):
                    L.info("mqtt_loop", extra={"sub_topic": topic})
                    if topic.strip():
                        await client.subscribe(f"$share/erddap/{topic.strip()}")

                async for message in client.messages:
                    try:
                        ce = from_json(message.payload)
                        ce_type = ce.get("type", "")
                        
                        if ce_type in ["envds.data.update", "envds.controller.data.update"]:
                            await insert_telemetry_to_erddap(ce)
                            
                        elif "status.update" in ce_type:
                            L.info("mqtt_loop", extra={"status_update": ce.data})
                            await handle_ops_status_insert(ce)

                        elif "operations.log" in ce_type:
                            L.info("mqtt_loop", extra={"ops_log": ce.data})
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

# ---------------------------------------------------------
# INTERNAL EGRESS API (Returns Native NCO-JSON)
# ---------------------------------------------------------
@app.post("/api/data/{dataset_id}")
async def get_ncojson_data(request: Request, dataset_id: str):
    """Internal Datastore Egress Route. Expects a POST body containing the definition."""
    try:
        definition = await request.json()
        
        erddap_url = f"{config.erddap_internal_url}/tabledap/{dataset_id}.json"
        if request.url.query:
            erddap_url = f"{erddap_url}?{request.url.query}"
            
        rp_req = http_client.build_request("GET", erddap_url)
        erddap_resp = await http_client.send(rp_req)
        
        if erddap_resp.status_code == 404:
            return {"results": []}
        elif erddap_resp.status_code != 200:
            return Response(content=erddap_resp.content, status_code=erddap_resp.status_code)
            
        flat_data = erddap_resp.json()
        nco_json_payload = unflatten_telemetry_to_ncojson(flat_data, definition)
        
        return {"results": nco_json_payload}
        
    except Exception as e:
        L.error(f"Failed to rebuild NCO-JSON", extra={"reason": str(e)})
        return Response(status_code=500, content=str(e))

@app.get("/api/definition/{registry_type}/{kind}")
async def get_ncojson_definition(request: Request, registry_type: str, kind: str):
    """Retrieves original NCO-JSON definitions from the ERDDAP payload columns."""
    actual_registry_type = "system" if registry_type == "ops" else registry_type
    dataset_id = f"envds_{actual_registry_type}_registry"
    erddap_url = f"{config.erddap_internal_url}/tabledap/{dataset_id}.json"
    
    query_parts = []
    if request.url.query:
        query_parts.append(request.url.query)
    query_parts.append(f'kind="{kind}"')
    
    erddap_url = f"{erddap_url}?{'&'.join(query_parts)}"
    
    rp_req = http_client.build_request("GET", erddap_url)
    erddap_resp = await http_client.send(rp_req)
    
    if erddap_resp.status_code == 404:
        return {"results": []}
        
    data = erddap_resp.json()
    cols = data.get("table", {}).get("columnNames", [])
    rows = data.get("table", {}).get("rows", [])
    
    if "payload" not in cols:
        return {"results": []}
        
    payload_idx = cols.index("payload")
    nco_results = []
    for row in rows:
        try:
            nco_results.append(json.loads(row[payload_idx]))
        except json.JSONDecodeError:
            continue
            
    return {"results": nco_results}

@app.get("/api/status/{dataset_id}")
async def get_ncojson_status(request: Request, dataset_id: str = "envds_ops_status"):
    """Fetches operational status updates isolating explicit payload columns."""
    erddap_url = f"{config.erddap_internal_url}/tabledap/{dataset_id}.json?app_group,app_uid,namespace,time,valid_config_time,requested_state,actual_state"
    if request.url.query:
        erddap_url = f"{erddap_url}&{request.url.query}"
        
    rp_req = http_client.build_request("GET", erddap_url)
    erddap_resp = await http_client.send(rp_req)
    
    if erddap_resp.status_code == 404:
        return {"results": []}
        
    flat_data = erddap_resp.json()
    cols = flat_data.get("table", {}).get("columnNames", [])
    rows = flat_data.get("table", {}).get("rows", [])
    nco_results = [dict(zip(cols, row)) for row in rows]
    
    return {"results": nco_results}

@app.get("/api/log/{dataset_id}")
async def get_ncojson_log(request: Request, dataset_id: str = "envds_ops_log"):
    """Fetches operational logs isolating explicit payload columns."""
    erddap_url = f"{config.erddap_internal_url}/tabledap/{dataset_id}.json?deployment_ref,project_ref,time,event_type,subject,description"
    if request.url.query:
        erddap_url = f"{erddap_url}&{request.url.query}"
        
    rp_req = http_client.build_request("GET", erddap_url)
    erddap_resp = await http_client.send(rp_req)
    
    if erddap_resp.status_code == 404:
        return {"results": []}
        
    flat_data = erddap_resp.json()
    cols = flat_data.get("table", {}).get("columnNames", [])
    rows = flat_data.get("table", {}).get("rows", [])
    nco_results = [dict(zip(cols, row)) for row in rows]
    
    return {"results": nco_results}

# ---------------------------------------------------------
# STANDARD ERDDAP PROXY
# ---------------------------------------------------------
@app.api_route("/erddap/{path_name:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy_erddap(request: Request, path_name: str):
    target_path = path_name if path_name else "index.html"
    url = f"{config.erddap_internal_url}/{target_path}"
    
    if request.url.query:
        url = f"{url}?{request.url.query}"
    
    req_headers = dict(request.headers)
    req_headers["host"] = request.headers.get("x-forwarded-host", request.headers.get("host"))
    req_headers["X-Forwarded-Prefix"] = "/envds/data" 
    
    rp_req = http_client.build_request(
        request.method,
        url,
        headers=req_headers,
        content=await request.body()
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