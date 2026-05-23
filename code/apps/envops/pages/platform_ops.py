import dash
from dash import html, dcc, callback, Input, Output, State, no_update
from dash_extensions import WebSocket
import dash_bootstrap_components as dbc
import httpx
# from cachetools import cached, TTLCache
import logging
import traceback
import json
from datetime import datetime
from pydantic import BaseSettings

from utils import get_registry_data, config

# Register with dynamic routing so the URL passes the deployment_id directly!
dash.register_page(__name__, path_template='/deployment/<deployment_id>/ops', title="Group Ops", nav_bar=False)

L = logging.getLogger(__name__)

# class Settings(BaseSettings):
#     daq_id: str = "mspbase01"
#     external_hostname: str = "mspbase01.pmel.noaa.gov"
#     ws_port: str = "8080"
#     ws_use_tls: str = "false"
#     class Config:
#         env_prefix = "ENVOPS_"
#         case_sensitive = False

# config = Settings()
# datastore_url = f"datastore.{config.daq_id}-system.svc.cluster.local"

ws_protocol = "wss://" if config.ws_use_tls.lower() == "true" else "ws://"
ws_url = f"{ws_protocol}{config.external_hostname}:{config.ws_port}/envds/envops/ws/system-ops/main"
# ws_url = f"{ws_protocol}{config.external_hostname}:{config.ws_port}/ws/system-ops/main"

# registry_cache = TTLCache(maxsize=128, ttl=300)

# @cached(cache=registry_cache)
# def get_registry_data(endpoint: str):
#     """Safely fetches data using httpx, heavily cached to protect the datastore."""
#     url = f"http://{datastore_url}/{endpoint}"
#     L.debug("Cache miss! Re-fetching registry data from datastore", extra={"fetch_url": url})
    
#     try:
#         with httpx.Client() as client:
#             response = client.get(url, timeout=5.0)
            
#         if response.status_code == 200:
#             data = response.json()
#             results = data.get("results", [])
#             L.debug("Successfully parsed registry items", extra={"endpoint": endpoint, "count": len(results)})
#             return results
#         else:
#             L.error("API returned non-200 code", extra={"status": response.status_code, "body": response.text})
            
#     except httpx.RequestError as e:
#         L.error("CONNECTION ERROR during fetch", extra={"fetch_url": url, "failure_detail": str(e)})
#     except Exception as e:
#         L.error("Unexpected fetch failure", extra={"endpoint": endpoint, "failure_detail": str(e)})
        
#     return []


# -----------------------------------------------------------------------------
# Layout Generator (Runs once per page load)
# -----------------------------------------------------------------------------
def layout(deployment_id=None, **kwargs):
    if not deployment_id:
        return dbc.Alert("No Deployment ID provided.", color="danger", className="m-4")

    # 1. Fetch all deployments to build relationships
    all_deployments = get_registry_data("deployment-definition/registry/get/")
    
    # 2. Find the requested host deployment
    host_dep = next((d for d in all_deployments if d.get("metadata", {}).get("name") == deployment_id), None)
    if not host_dep:
        return dbc.Alert(f"Deployment {deployment_id} not found.", color="warning", className="m-4")
        
    host_data = host_dep.get("data", {})
    host_plat_ref = host_data.get("platform_ref", "")
    host_name = host_data.get("display_name", host_plat_ref)
    
    # 3. Gather all child payloads attached to this host
    child_deps = [d for d in all_deployments if d.get("data", {}).get("host_platform_ref") == host_plat_ref and d.get("metadata", {}).get("name") != deployment_id]
    
    # 4. Create a list of all platform IDs in this group (to filter WebSocket traffic)
    group_platforms = [host_plat_ref.split(".")[-1]] + [c.get("data", {}).get("platform_ref", "").split(".")[-1] for c in child_deps]

    # --- UI: Header ---
    header = dbc.Row([
        dbc.Col([
            html.H2([html.I(className="bi bi-hdd-network me-2"), host_name], className="fw-bold mb-0"),
            html.P(f"ID: {deployment_id} | Attached Payloads: {len(child_deps)}", className="text-muted mb-0")
        ]),
        dbc.Col([
            dbc.Badge("Group Health: Pending", id="ops-health-badge", color="secondary", className="fs-5 shadow-sm rounded-pill px-3 py-2")
        ], width="auto", className="text-end align-self-center")
    ], className="mb-4 align-items-center border-bottom pb-3")

    # --- UI: Operations Ribbon ---
    ops_ribbon = dbc.Card(dbc.CardBody(dbc.Row([
        dbc.Col([
            html.H6("System Mode", className="text-muted mb-1 small text-uppercase"),
            html.H5("STANDBY", id="ops-sys-mode", className="fw-bold mb-0")
        ], width=3, className="border-end"),
        dbc.Col([
            html.H6("Sampling State", className="text-muted mb-1 small text-uppercase"),
            html.H5("IDLE", id="ops-samp-state", className="fw-bold mb-0")
        ], width=3, className="border-end"),
        dbc.Col([
            html.H6("Active Alarms", className="text-muted mb-1 small text-uppercase"),
            html.H5("0", id="ops-alarm-count", className="text-success fw-bold mb-0")
        ], width=3),
        dbc.Col([
            dbc.Button([html.I(className="bi bi-sliders me-2"), "Command & Control"], color="dark", className="w-100 shadow-sm fw-bold h-100")
        ], width=3)
    ])), className="shadow-sm border-0 mb-4 bg-light")

    # --- UI: Telemetry Grid ---
    telemetry_grid = dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardHeader("Navigation & Attitude", className="fw-bold bg-white"),
            dbc.CardBody(html.Pre("Awaiting Nav Data...", id="ops-nav-data", className="small text-muted mb-0", style={"whiteSpace": "pre-wrap"}))
        ], className="shadow-sm border-0 h-100"), width=4),
        
        dbc.Col(dbc.Card([
            dbc.CardHeader("Meteorology", className="fw-bold bg-white"),
            dbc.CardBody(html.Pre("Awaiting Met Data...", id="ops-met-data", className="small text-muted mb-0", style={"whiteSpace": "pre-wrap"}))
        ], className="shadow-sm border-0 h-100"), width=4),
        
        dbc.Col(dbc.Card([
            dbc.CardHeader("Air Quality & Aerosols", className="fw-bold bg-white"),
            dbc.CardBody(html.Pre("Awaiting AQ Data...", id="ops-aq-data", className="small text-muted mb-0", style={"whiteSpace": "pre-wrap"}))
        ], className="shadow-sm border-0 h-100"), width=4),
    ], className="mb-4 align-items-stretch")
    
    # --- UI: Sub-system Drill Downs ---
    child_links = [
        dbc.ListGroupItem([
            html.Div([
                html.Span(c.get("data", {}).get("display_name", "Unknown Payload"), className="fw-bold"),
                dbc.Button("Device Details", size="sm", color="outline-primary", href=f"/envds/envops/platform/{c.get('data', {}).get('platform_ref')}", className="float-end")
            ], className="w-100")
        ]) for c in child_deps
    ]
    sub_systems = dbc.Card([
        dbc.CardHeader("Attached Sub-Systems (Level 3 Pathways)", className="fw-bold bg-dark text-white"),
        dbc.ListGroup(child_links, flush=True) if child_links else dbc.CardBody(html.P("No attached payloads.", className="text-muted mb-0"))
    ], className="shadow-sm border-0")

    return html.Div([
        # Hidden stores for real-time state management
        dcc.Store(id="ops-group-platforms", data=group_platforms),
        dcc.Store(id="ops-telemetry-cache", data={}),
        WebSocket(id="ws-ops-telemetry", url=ws_url),
        
        header, ops_ribbon, telemetry_grid, sub_systems
    ], className="container-fluid mt-3")


# -----------------------------------------------------------------------------
# Callbacks
# -----------------------------------------------------------------------------
@callback(
    Output("ops-telemetry-cache", "data"),
    Input("ws-ops-telemetry", "message"),
    State("ops-group-platforms", "data"), # <-- Active!
    State("ops-telemetry-cache", "data")
)
def ingest_live_telemetry(msg, group_platforms, current_cache):
    """Parses mapped variablesets and filters cleanly by TRUE Platform ID."""
    if not msg or "data" not in msg or not group_platforms: 
        return no_update

    try:
        # 1. Unpack the WebSocket wrapper
        raw_data = msg["data"]
        ws_payload = json.loads(raw_data)
        current_topic = ws_payload.get("topic", "")
        parts = current_topic.split("/")
        
        # 2. Strictly filter for Variablesets
        if len(parts) < 4 or parts[2] != "variableset":
            return no_update
            
        # 3. Unpack the CloudEvent
        cloud_event = ws_payload.get("data", {})
        payload = cloud_event.get("data", {})
        if not payload:
            return no_update

        # --- 4. THE TRICK: Extract the TRUE Platform ID ---
        attributes = payload.get("attributes", {})
        platform_id = attributes.get("platform", {}).get("data")
        
        # Fallback to the topic map name just in case
        if not platform_id:
            platform_id = parts[3].split("::")[0]
            
        # 5. THE FILTER: Safely drop data from other deployments!
        if platform_id not in group_platforms:
            return no_update
            
        # 6. Prevent Dictionary Mutation Traps
        new_cache = current_cache.copy() if current_cache else {}
        if platform_id not in new_cache:
            new_cache[platform_id] = {"variables": {}, "state": {}}
            
        # 7. Safely merge all the clean, mapped variables into the cache
        incoming_vars = payload.get("variables", {})
        if incoming_vars:
            new_cache[platform_id]["variables"].update(incoming_vars)
            
        return new_cache
        
    except Exception as e:
        L.debug("Ops Variableset parse failure", extra={"failure_detail": str(e)})
        return no_update
    
@callback(
    Output("ops-health-badge", "children"),
    Output("ops-health-badge", "color"),
    Output("ops-sys-mode", "children"),
    Output("ops-sys-mode", "className"),
    Output("ops-alarm-count", "children"),
    Input("ops-telemetry-cache", "data")
)
def update_ribbon_ui(cache):
    """Reads the group cache and updates the top operations ribbon."""
    if not cache: 
        return no_update
    
    total_alarms = 0
    sys_mode = "STANDBY"
    sys_mode_color = "fw-bold text-muted mb-0"
    
    for uid, data in cache.items():
        state = data.get("state", {})
        if "alarm" in str(state).lower() or "error" in str(state).lower():
            total_alarms += 1
            
        # Mockup system mode logic based on potential incoming state variables
        if "system_active" in state:
            sys_mode = "ACTIVE" if str(state["system_active"].get("actual", "")).lower() == "true" else "STANDBY"
            sys_mode_color = "fw-bold text-primary mb-0" if sys_mode == "ACTIVE" else "fw-bold text-muted mb-0"
            
    # Roll up Health
    if total_alarms > 0:
        health_badge = f"{total_alarms} Critical Alarms"
        health_color = "danger"
    else:
        health_badge = "Group Nominal"
        health_color = "success"

    return health_badge, health_color, sys_mode, sys_mode_color, str(total_alarms)

@callback(
    Output("ops-nav-data", "children"),
    Output("ops-met-data", "children"),
    Output("ops-aq-data", "children"),
    Input("ops-telemetry-cache", "data")
)
def update_telemetry_grid(cache):
    """Parses the variables from the group cache and populates the UI cards."""
    if not cache: 
        return no_update

    # 1. Pool all variables from all devices in the deployment group
    all_vars = {}
    for uid, data in cache.items():
        all_vars.update(data.get("variables", {}))

    # 2. Helper function to safely extract and format a value
    def get_val(key, unit=""):
        var_obj = all_vars.get(key, {})
        val = var_obj.get("data")
        
        if val is None:
            return "Waiting..."
            
        # Clean up floating point math for presentation
        if isinstance(val, float):
            val = f"{val:.2f}"
            
        return f"{val} {unit}".strip()

    # 3. Map the exact variable keys from your varmaps.json
    nav_text = (
        f"Latitude:  {get_val('latitude', '°')}\n"
        f"Longitude: {get_val('longitude', '°')}\n"
        f"Heading:   {get_val('platform_heading', '°')}\n"
        f"Speed:     {get_val('platform_speed', 'kts')}"
    )

    met_text = (
        f"Temperature: {get_val('air_temperature', '°C')}\n"
        f"Humidity:    {get_val('relative_humidity', '%')}\n"
        f"Pressure:    {get_val('air_pressure', 'hPa')}\n"
        f"Wind Speed:  {get_val('true_wind_speed', 'm/s')}"
    )

    aq_text = (
        f"PM2.5: {get_val('pm2_5_concentration', 'µg/m³')}\n"
        f"PM10:  {get_val('pm10_concentration', 'µg/m³')}\n"
        f"Ozone: {get_val('o3_concentration', 'ppb')}\n"
        f"NO2:   {get_val('no2_concentration', 'ppb')}"
    )

    return nav_text, met_text, aq_text