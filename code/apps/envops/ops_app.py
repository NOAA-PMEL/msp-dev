import dash
from dash import html, dcc, Input, Output, State, no_update, ALL, ctx
from dash_extensions import WebSocket
import dash_bootstrap_components as dbc
import logging
import json
from datetime import datetime

# Import shared tools from our utility layer
from utils import get_registry_data, config, create_unified_shell, register_sidebar_callbacks

L = logging.getLogger(__name__)

# --- Initialize Isolated Dash App ---
app = dash.Dash(__name__, requests_pathname_prefix="/envds/envops/ops/", routes_pathname_prefix="/")
register_sidebar_callbacks(app)

# --- Core Shell and URL Router ---
app.layout = create_unified_shell(html.Div([
    dcc.Location(id="ops-url", refresh=False),
    html.Div(id="ops-page-content") 
]), active_item="ops")

@app.callback(
    Output("ops-page-content", "children"),
    Input("ops-url", "pathname")
)
def render_deployment_ops(pathname):
    if not pathname or "deployment/" not in pathname:
        return dbc.Alert("Select a deployment from the sidebar.", color="info", className="m-4")
        
    deployment_id = pathname.split("/")[-1]
    return build_ops_layout(deployment_id)

def create_metric_card(label, element_id, default_val="Awaiting..."):
    """Generates a reusable, clean readout item with an integrated status indicator."""
    return dbc.Card([
        dbc.CardBody([
            html.Div([
                html.Span(label, className="text-muted small fw-bold text-uppercase d-block mb-1"),
                html.H3(default_val, id=element_id, className="fw-bold mb-0 text-dark transition-all"),
            ], className="position-relative")
        ], className="p-3")
    ], id=f"card-{element_id}", className="border-0 shadow-sm mb-3 bg-white border-start border-4 border-secondary")

def build_ops_layout(deployment_id):
    all_deployments = get_registry_data("deployment")
    
    host_dep = next((d for d in all_deployments if d.get("metadata", {}).get("name") == deployment_id), None)
    if not host_dep:
        return dbc.Alert(f"Deployment {deployment_id} not found.", color="warning", className="m-4")
        
    host_data = host_dep.get("data", {})
    host_plat_ref = host_data.get("platform_ref", "")
    host_name = host_data.get("display_name", host_plat_ref)
    
    child_deps = [d for d in all_deployments if d.get("data", {}).get("host_platform_ref") == host_plat_ref and d.get("metadata", {}).get("name") != deployment_id]
    group_platforms = [host_plat_ref.split(".")[-1]] + [c.get("data", {}).get("platform_ref", "").split(".")[-1] for c in child_deps]

    header = dbc.Row([
        dbc.Col([
            html.H2([html.I(className="bi bi-hdd-network me-2"), host_name], className="fw-bold mb-0"),
            html.P(f"ID: {deployment_id} | Attached Payloads: {len(child_deps)}", className="text-muted mb-0")
        ]),
        dbc.Col([
            dbc.Badge("Group Health: Pending", id="ops-health-badge", color="secondary", className="fs-5 shadow-sm rounded-pill px-3 py-2")
        ], width="auto", className="text-end align-self-center")
    ], className="mb-4 align-items-center border-bottom pb-3")

    ops_ribbon = dbc.Card(dbc.CardBody(dbc.Row([
        dbc.Col([html.H6("System Mode", className="text-muted mb-1 small text-uppercase"), html.H5("STANDBY", id="ops-sys-mode", className="fw-bold mb-0")], width=3, className="border-end"),
        dbc.Col([html.H6("Sampling State", className="text-muted mb-1 small text-uppercase"), html.H5("IDLE", id="ops-samp-state", className="fw-bold mb-0")], width=3, className="border-end"),
        dbc.Col([html.H6("Active Alarms", className="text-muted mb-1 small text-uppercase"), html.H5("0", id="ops-alarm-count", className="text-success fw-bold mb-0")], width=3),
        dbc.Col([dbc.Button([html.I(className="bi bi-sliders me-2"), "Command & Control"], color="dark", className="w-100 shadow-sm fw-bold h-100")], width=3)
    ])), className="shadow-sm border-0 mb-4 bg-light")
    
    # --- Simplified Metrics Grid ---
    metrics_sections = html.Div([
        # Section 1: Navigation & Context
        html.H5("Navigation & Position", className="fw-bold mb-3 text-secondary"),
        dbc.Row([
            dbc.Col(create_metric_card("Latitude", "ops-val-latitude"), width=12, md=3),
            dbc.Col(create_metric_card("Longitude", "ops-val-longitude"), width=12, md=3),
            dbc.Col(create_metric_card("Heading", "ops-val-platform_heading"), width=12, md=3),
            dbc.Col(create_metric_card("Platform Speed", "ops-val-platform_speed"), width=12, md=3),
        ], className="mb-4"),

        # Section 2: Meteorology Readouts
        html.H5("Meteorological Suite", className="fw-bold mb-3 text-secondary"),
        dbc.Row([
            dbc.Col(create_metric_card("Air Temperature", "ops-val-air_temperature"), width=12, md=3),
            dbc.Col(create_metric_card("Relative Humidity", "ops-val-relative_humidity"), width=12, md=3),
            dbc.Col(create_metric_card("Atmospheric Pressure", "ops-val-air_pressure"), width=12, md=3),
            dbc.Col(create_metric_card("True Wind Speed", "ops-val-true_wind_speed"), width=12, md=3),
        ], className="mb-4"),

        # Section 3: Gas Phase & Environmental Chemistry
        html.H5("Gas Phase & Sampling Environment", className="fw-bold mb-3 text-secondary"),
        dbc.Row([
            dbc.Col(create_metric_card("Ozone Concentration", "ops-val-O3"), width=12, md=3),
            dbc.Col(create_metric_card("Carbon Monoxide", "ops-val-CO"), width=12, md=3),
            dbc.Col(create_metric_card("Nitric Oxide (NO)", "ops-val-NO"), width=12, md=3),
            dbc.Col(create_metric_card("Nitrogen Dioxide (NO2)", "ops-val-NO2"), width=12, md=3),
        ], className="mb-4"),

        # Section 4: Operational Fluid Dynamics & Particulates
        html.H5("Inlet Diagnostics", className="fw-bold mb-3 text-secondary"),
        dbc.Row([
            dbc.Col(create_metric_card("Inlet Volumetric Flow", "ops-val-inlet_flow"), width=12, md=4),
            dbc.Col(create_metric_card("Particulate Matter (PM2.5)", "ops-val-PM2_5"), width=12, md=4),
            dbc.Col(create_metric_card("Rain Intensity", "ops-val-rain_intensity"), width=12, md=4),
        ], className="mb-4"),
    ])

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

    ws_protocol = "wss://" if config.ws_use_tls.lower() == "true" else "ws://"
    ws_base = f"{ws_protocol}{config.external_hostname}:{config.ws_port}/envds/envops"

    ws_connections = [
        WebSocket(id="ws-ops-system", url=f"{ws_base}/ws/system-ops/main")
    ]
    
    for p_id in group_platforms:
        if p_id: 
            ws_connections.append(WebSocket(id={"type": "ws-ops-platform", "index": p_id}, url=f"{ws_base}/ws/platform/{p_id}"))

    return html.Div([
        dcc.Store(id="ops-group-platforms", data=group_platforms),
        dcc.Store(id="ops-telemetry-cache", data={}),
        html.Div(ws_connections),
        header, ops_ribbon, metrics_sections, sub_systems
    ], className="container-fluid mt-3")


# --- Callbacks ---

@app.callback(
    Output("ops-telemetry-cache", "data"),
    Input({"type": "ws-ops-platform", "index": ALL}, "message"),
    State("ops-telemetry-cache", "data")
)
def ingest_live_telemetry(messages, current_cache):
    if not ctx.triggered:
        return no_update

    msg = ctx.triggered[0].get("value")
    if not msg or "data" not in msg: 
        return no_update

    try:
        ws_wrapper = json.loads(msg["data"])
        cloud_event = ws_wrapper.get("data-update", ws_wrapper.get("data", {}))
        if not cloud_event:
            return no_update
            
        current_topic = ws_wrapper.get("topic") or cloud_event.get("destpath", "") or cloud_event.get("sourcepath", "")
        parts = current_topic.split("/")
        
        payload = cloud_event.get("data", {})
        if not payload:
            return no_update

        attributes = payload.get("attributes", {})
        platform_id = attributes.get("platform", {}).get("data")
        if not platform_id:
            platform_id = parts[3].split("::")[0] if len(parts) > 3 else "unknown"
            
        new_cache = current_cache.copy() if current_cache else {}
        if platform_id not in new_cache:
            new_cache[platform_id] = {"variables": {}, "state": {}}
            
        incoming_vars = payload.get("variables", {})
        
        # Save only the absolute freshest value for each key rather than a historical list window
        for var_name, var_data in incoming_vars.items():
            val = var_data.get("data")
            if val is not None:
                new_cache[platform_id]["variables"][var_name] = val
            
        return new_cache
    except Exception:
        return no_update
    
@app.callback(
    Output("ops-health-badge", "children"),
    Output("ops-health-badge", "color"),
    Output("ops-sys-mode", "children"),
    Output("ops-sys-mode", "className"),
    Output("ops-alarm-count", "children"),
    Input("ops-telemetry-cache", "data")
)
def update_ribbon_ui(cache):
    if not cache: 
        return no_update
    
    total_alarms = 0
    sys_mode = "STANDBY"
    sys_mode_color = "fw-bold text-muted mb-0"
    
    for uid, data in cache.items():
        state = data.get("state", {})
        if "alarm" in str(state).lower() or "error" in str(state).lower():
            total_alarms += 1
            
        if "system_active" in state:
            sys_mode = "ACTIVE" if str(state["system_active"].get("actual", "")).lower() == "true" else "STANDBY"
            sys_mode_color = "fw-bold text-primary mb-0" if sys_mode == "ACTIVE" else "fw-bold text-muted mb-0"
            
    if total_alarms > 0:
        health_badge = f"{total_alarms} Critical Alarms"
        health_color = "danger"
    else:
        health_badge = "Group Nominal"
        health_color = "success"

    return health_badge, health_color, sys_mode, sys_mode_color, str(total_alarms)


@app.callback(
    # Row 1: Nav
    Output("ops-val-latitude", "children"), Output("card-ops-val-latitude", "className"),
    Output("ops-val-longitude", "children"), Output("card-ops-val-longitude", "className"),
    Output("ops-val-platform_heading", "children"), Output("card-ops-val-platform_heading", "className"),
    Output("ops-val-platform_speed", "children"), Output("card-ops-val-platform_speed", "className"),
    # Row 2: Met
    Output("ops-val-air_temperature", "children"), Output("card-ops-val-air_temperature", "className"),
    Output("ops-val-relative_humidity", "children"), Output("card-ops-val-relative_humidity", "className"),
    Output("ops-val-air_pressure", "children"), Output("card-ops-val-air_pressure", "className"),
    Output("ops-val-true_wind_speed", "children"), Output("card-ops-val-true_wind_speed", "className"),
    # Row 3: Gas
    Output("ops-val-O3", "children"), Output("card-ops-val-O3", "className"),
    Output("ops-val-CO", "children"), Output("card-ops-val-CO", "className"),
    Output("ops-val-NO", "children"), Output("card-ops-val-NO", "className"),
    Output("ops-val-NO2", "children"), Output("card-ops-val-NO2", "className"),
    # Row 4: Diagnostics
    Output("ops-val-inlet_flow", "children"), Output("card-ops-val-inlet_flow", "className"),
    Output("ops-val-PM2_5", "children"), Output("card-ops-val-PM2_5", "className"),
    Output("ops-val-rain_intensity", "children"), Output("card-ops-val-rain_intensity", "className"),
    Input("ops-telemetry-cache", "data"),
    prevent_initial_call=True
)
def update_metrics_grid(cache):
    if not cache:
        return [no_update] * 30 # 15 readouts + 15 border card class states

    # Pool variables across all connected devices in the group
    flat_vars = {}
    for uid, data in cache.items():
        flat_vars.update(data.get("variables", {}))

    def evaluate_metric(key, unit="", low_crit=None, low_warn=None, hi_warn=None, hi_crit=None):
        """Formats the reading and computes custom CSS border colors based on health parameters."""
        raw_val = flat_vars.get(key)
        
        if raw_val is None:
            return "Waiting...", "border-0 shadow-sm mb-3 bg-white border-start border-4 border-secondary"
            
        try:
            val = float(raw_val)
            formatted_text = f"{val:.2f} {unit}".strip()
            
            # Threshold verification matrix
            if low_crit is not None and val <= low_crit:
                return formatted_text, "border-0 shadow-sm mb-3 bg-soft-danger border-start border-4 border-danger animate-pulse"
            if low_warn_val := low_warn if low_warn is not None else None:
                if val <= low_warn_val:
                    return formatted_text, "border-0 shadow-sm mb-3 bg-soft-warning border-start border-4 border-warning"
            if hi_crit is not None and val >= hi_crit:
                return formatted_text, "border-0 shadow-sm mb-3 bg-soft-danger border-start border-4 border-danger animate-pulse"
            if hi_warn is not None and val >= hi_warn:
                return formatted_text, "border-0 shadow-sm mb-3 bg-soft-warning border-start border-4 border-warning"
                
            # Nominal State
            return formatted_text, "border-0 shadow-sm mb-3 bg-white border-start border-4 border-success"
            
        except (ValueError, TypeError):
            # If value is text/string rather than float, treat it normally
            return f"{raw_val} {unit}".strip(), "border-0 shadow-sm mb-3 bg-white border-start border-4 border-success"

    # Define validation thresholds for your field sensors
    lat_txt, lat_style = evaluate_metric("latitude", "°")
    lon_txt, lon_style = evaluate_metric("longitude", "°")
    hdg_txt, hdg_style = evaluate_metric("platform_heading", "°")
    spd_txt, spd_style = evaluate_metric("platform_speed", "kts", hi_warn=25.0, hi_crit=35.0) # High velocity alerts

    temp_txt, temp_style = evaluate_metric("air_temperature", "°C", low_crit=-10.0, low_warn=0.0, hi_warn=38.0, hi_crit=45.0)
    rh_txt, rh_style = evaluate_metric("relative_humidity", "%", low_crit=5.0, hi_warn=95.0) # Condensation thresholds
    press_txt, press_style = evaluate_metric("air_pressure", "hPa", low_warn=960.0, hi_warn=1040.0)
    wind_txt, wind_style = evaluate_metric("true_wind_speed", "m/s", hi_warn=15.0, hi_crit=22.0)

    o3_txt, o3_style = evaluate_metric("O3", "ppb", hi_warn=70.0, hi_crit=100.0) # Air quality alerts
    co_txt, co_style = evaluate_metric("CO", "ppb", hi_warn=900.0, hi_crit=2000.0)
    no_txt, no_style = evaluate_metric("NO", "ppb", hi_warn=50.0)
    no2_txt, no2_style = evaluate_metric("NO2", "ppb", hi_warn=40.0)

    flow_txt, flow_style = evaluate_metric("inlet_flow", "LPM", low_crit=14.0, low_warn=15.5, hi_warn=17.5, hi_crit=19.0) # Critical inlet ranges (Target 16.7 LPM)
    pm_txt, pm_style = evaluate_metric("PM2_5", "µg/m³", hi_warn=35.0, hi_crit=55.0)
    rain_txt, rain_style = evaluate_metric("rain_intensity", "mm/h", hi_warn=5.0, hi_crit=20.0)

    return [
        lat_txt, lat_style, lon_txt, lon_style, hdg_txt, hdg_style, spd_txt, spd_style,
        temp_txt, temp_style, rh_txt, rh_style, press_txt, press_style, wind_txt, wind_style,
        o3_txt, o3_style, co_txt, co_style, no_txt, no_style, no2_txt, no2_style,
        flow_txt, flow_style, pm_txt, pm_style, rain_txt, rain_style
    ]