import dash
from dash import html, dcc, callback, Input, Output, State, no_update, MATCH, ALL
from dash_extensions import WebSocket
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from plotly.subplots import make_subplots
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

def create_empty_dual_plot(title, y1_name, y2_name, y1_color="#1f77b4", y2_color="#d62728"):
    """Generates a highly stylized, empty dual-axis plot skeleton."""
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    # Initialize empty traces so `extendData` has a target
    fig.add_trace(go.Scatter(x=[], y=[], name=y1_name, mode="lines", line=dict(color=y1_color, width=2)), secondary_y=False)
    fig.add_trace(go.Scatter(x=[], y=[], name=y2_name, mode="lines", line=dict(color=y2_color, width=2)), secondary_y=True)
    
    fig.update_layout(
        title=dict(text=title, font=dict(size=14), y=0.95),
        margin=dict(l=40, r=40, t=40, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="white",
        paper_bgcolor="white",
        uirevision="constant" # Prevents zoom/pan resets when new data arrives!
    )
    
    # Subtle gridlines
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="LightGray", secondary_y=False)
    fig.update_yaxes(showgrid=False, secondary_y=True)
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="LightGray")
    
    return fig

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

    # # --- UI: Telemetry Grid ---
    # telemetry_grid = dbc.Row([
    #     dbc.Col(dbc.Card([
    #         dbc.CardHeader("Navigation & Attitude", className="fw-bold bg-white"),
    #         dbc.CardBody(html.Pre("Awaiting Nav Data...", id="ops-nav-data", className="small text-muted mb-0", style={"whiteSpace": "pre-wrap"}))
    #     ], className="shadow-sm border-0 h-100"), width=4),
        
    #     dbc.Col(dbc.Card([
    #         dbc.CardHeader("Meteorology", className="fw-bold bg-white"),
    #         dbc.CardBody(html.Pre("Awaiting Met Data...", id="ops-met-data", className="small text-muted mb-0", style={"whiteSpace": "pre-wrap"}))
    #     ], className="shadow-sm border-0 h-100"), width=4),
        
    #     dbc.Col(dbc.Card([
    #         dbc.CardHeader("Air Quality & Aerosols", className="fw-bold bg-white"),
    #         dbc.CardBody(html.Pre("Awaiting AQ Data...", id="ops-aq-data", className="small text-muted mb-0", style={"whiteSpace": "pre-wrap"}))
    #     ], className="shadow-sm border-0 h-100"), width=4),
    # ], className="mb-4 align-items-stretch")
    
    # --- UI: Live Operational Plots ---
    plots_accordion = dbc.Accordion([
        # 1. Meteorology Group
        dbc.AccordionItem([
            dbc.Row([
                dbc.Col(dcc.Graph(id="ops-plot-wind", figure=create_empty_dual_plot("Wind", "True Speed (m/s)", "True Dir (°)", "#1f77b4", "#7f7f7f"), style={"height": "300px"}), width=4),
                dbc.Col(dcc.Graph(id="ops-plot-atm", figure=create_empty_dual_plot("Atmosphere", "Temp (°C)", "RH (%)", "#ff7f0e", "#17becf"), style={"height": "300px"}), width=4),
                dbc.Col(dcc.Graph(id="ops-plot-precip", figure=create_empty_dual_plot("Precip & Pressure", "Rain (mm/h)", "Pressure (hPa)", "#2ca02c", "#8c564b"), style={"height": "300px"}), width=4),
            ])
        ], title=html.B([html.I(className="bi bi-cloud-sun me-2"), "Meteorology"]), item_id="met"),

        # 2. Gas Phase Group
        dbc.AccordionItem([
            dbc.Row([
                dbc.Col(dcc.Graph(id="ops-plot-o3-co", figure=create_empty_dual_plot("Ozone & CO", "O3 (ppb)", "CO (ppb)", "#9467bd", "#e377c2"), style={"height": "300px"}), width=6),
                dbc.Col(dcc.Graph(id="ops-plot-no-no2", figure=create_empty_dual_plot("Nitrogen Oxides", "NO (ppb)", "NO2 (ppb)", "#1f77b4", "#ff7f0e"), style={"height": "300px"}), width=6),
            ])
        ], title=html.B([html.I(className="bi bi-wind me-2"), "Gas Phase Chemistry"]), item_id="gas"),

        # 3. Sampling Operations Group
        dbc.AccordionItem([
            dbc.Row([
                dbc.Col(dcc.Graph(id="ops-plot-rel-wind", figure=create_empty_dual_plot("Platform Relative Wind", "Rel Speed (m/s)", "Rel Dir (°)", "#bcbd22", "#7f7f7f"), style={"height": "300px"}), width=6),
                dbc.Col(dcc.Graph(id="ops-plot-flow", figure=create_empty_dual_plot("Inlet Flow & Particulates", "Inlet Flow (LPM)", "PM 2.5 (µg/m³)", "#17becf", "#d62728"), style={"height": "300px"}), width=6),
            ])
        ], title=html.B([html.I(className="bi bi-sliders me-2"), "Sampling Operations"]), item_id="ops")
    ], always_open=True, active_item=["met", "gas", "ops"], className="mb-4 shadow-sm")

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
        
        header, ops_ribbon, plots_accordion, sub_systems
    ], className="container-fluid mt-3")


# -----------------------------------------------------------------------------
# Callbacks
# -----------------------------------------------------------------------------
@callback(
    Output("ops-telemetry-cache", "data"),
    Input("ws-ops-telemetry", "message"),
    State("ops-group-platforms", "data"),
    State("ops-telemetry-cache", "data")
)
def ingest_live_telemetry(msg, group_platforms, current_cache):
    """Parses standard CloudEvents and buffers them into a sliding window cache."""
    if not msg or "data" not in msg: 
        return no_update

    try:
        # 1. msg["data"] is the raw JSON string of the CloudEvent
        ce = json.loads(msg["data"])
        
        # 2. Extract the topic directly from the CloudEvent metadata!
        current_topic = ce.get("destpath", "") or ce.get("sourcepath", "")
        parts = current_topic.split("/")
        
        # 3. Strictly filter for Variablesets
        if len(parts) < 4 or parts[2] != "variableset":
            return no_update
            
        # 4. Extract the payload (Layer 2)
        payload = ce.get("data", {})
        if not payload:
            return no_update

        # 5. Extract the TRUE Platform ID from the payload attributes
        attributes = payload.get("attributes", {})
        platform_id = attributes.get("platform", {}).get("data")
        
        if not platform_id:
            platform_id = parts[3].split("::")[0]
            
        # 6. FILTER: Drop data if it belongs to a different deployment group
        if group_platforms and platform_id not in group_platforms:
            return no_update
            
        # 7. --- SLIDING WINDOW CACHE LOGIC ---
        new_cache = current_cache.copy() if current_cache else {}
        if platform_id not in new_cache:
            new_cache[platform_id] = {"variables": {}, "state": {}}
            
        incoming_vars = payload.get("variables", {})
        
        # Determine the timestamp for this data tick
        current_time = incoming_vars.get("time", {}).get("data") or datetime.now().isoformat()
        
        for var_name, var_data in incoming_vars.items():
            val = var_data.get("data")
            if val is None:
                continue
                
            if var_name not in new_cache[platform_id]["variables"]:
                new_cache[platform_id]["variables"][var_name] = {"x": [], "y": []}
                
            # Append the new coordinates
            new_cache[platform_id]["variables"][var_name]["x"].append(current_time)
            new_cache[platform_id]["variables"][var_name]["y"].append(val)
            
            # Cap the array to a rolling window (e.g., 300 points = 5 minutes at 1Hz)
            if len(new_cache[platform_id]["variables"][var_name]["x"]) > 300:
                new_cache[platform_id]["variables"][var_name]["x"].pop(0)
                new_cache[platform_id]["variables"][var_name]["y"].pop(0)
            
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

@callback(
    Output("ops-plot-wind", "extendData"),
    Output("ops-plot-atm", "extendData"),
    Output("ops-plot-precip", "extendData"),
    Output("ops-plot-o3-co", "extendData"),
    Output("ops-plot-no-no2", "extendData"),
    Output("ops-plot-rel-wind", "extendData"),
    Output("ops-plot-flow", "extendData"),
    Input("ops-telemetry-cache", "data"),
    prevent_initial_call=True
)
def update_operational_plots(cache):
    if not cache:
        return [no_update] * 7

    # 1. Flatten all available variables across the entire deployment group
    all_vars = {}
    for uid, data in cache.items():
        all_vars.update(data.get("variables", {}))

    # Fallback to local system time if the instruments aren't providing a timestamp
    current_time = all_vars.get("time", {}).get("data")
    if not current_time:
        current_time = datetime.now().isoformat()

    def get_val(key):
        """Safely extracts a float value from the variable mapping."""
        val = all_vars.get(key, {}).get("data")
        try:
            return float(val) if val is not None else None
        except (ValueError, TypeError):
            return None

    def build_extend_payload(var1_name, var2_name, max_points=300):
        """Constructs the Plotly extendData tuple dynamically based on available data."""
        v1 = get_val(var1_name)
        v2 = get_val(var2_name)
        
        x_data, y_data, traces = [], [], []
        
        # Left Y-Axis (Trace 0)
        if v1 is not None:
            x_data.append([current_time])
            y_data.append([v1])
            traces.append(0)
            
        # Right Y-Axis (Trace 1)
        if v2 is not None:
            x_data.append([current_time])
            y_data.append([v2])
            traces.append(1)
            
        if not traces:
            return no_update
            
        return ({"x": x_data, "y": y_data}, traces, max_points)

    # 2. Map the incoming variable keys to their respective graphs
    return [
        build_extend_payload("true_wind_speed", "true_wind_direction"),
        build_extend_payload("air_temperature", "relative_humidity"),
        build_extend_payload("pressure", "rain_intensity"),
        build_extend_payload("O3", "CO"),
        build_extend_payload("NO", "NO2"),
        build_extend_payload("relative_wind_speed", "relative_wind_direction"),
        build_extend_payload("inlet_flow", "PM2_5") # Replace PM2_5 with CN concentration key when available
    ]
