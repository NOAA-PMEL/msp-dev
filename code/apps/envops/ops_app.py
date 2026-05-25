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

def build_ops_layout(deployment_id):
    all_deployments = get_registry_data("deployment")
    
    host_dep = next((d for d in all_deployments if d.get("metadata", {}).get("name") == deployment_id), None)
    if not host_dep:
        return dbc.Alert(f"Deployment {deployment_id} not found.", color="warning", className="m-4")
        
    host_data = host_dep.get("data", {})
    host_plat_ref = host_data.get("platform_ref", "")
    host_name = host_data.get("display_name", host_plat_ref)
    
    child_deps = [d for d in all_deployments if d.get("data", {}).get("host_platform_ref") == host_plat_ref and d.get("metadata", {}).get("name") != deployment_id]
    
    raw_targets = [host_plat_ref] + [c.get("data", {}).get("platform_ref", "") for c in child_deps]
    short_targets = [p.split(".")[-1] for p in raw_targets if "." in p]
    group_platforms = list(set(raw_targets + short_targets))
    group_platforms = [p for p in group_platforms if p]

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
    ], className="shadow-sm border-0 mb-4")

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
        header, 
        ops_ribbon, 
        
        # --- NEW: Dynamic Metrics Container ---
        html.Div(id="dynamic-metrics-container", children=[dbc.Spinner(color="primary")]),
        
        sub_systems
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
        
        payload = ws_wrapper.get("data-update")
        if not payload:
            return no_update

        attributes = payload.get("attributes", {})
        platform_id = attributes.get("platform", {}).get("data")
        
        if not platform_id:
            current_topic = ws_wrapper.get("topic") or payload.get("destpath", "") or payload.get("sourcepath", "")
            parts = current_topic.split("/")
            platform_id = parts[3].split("::")[0] if len(parts) > 3 else "unknown"
            
        new_cache = current_cache.copy() if current_cache else {}
        if platform_id not in new_cache:
            new_cache[platform_id] = {"variables": {}, "state": {}}
            
        incoming_vars = payload.get("variables", {})
        
        for var_name, var_data in incoming_vars.items():
            val = var_data.get("data")
            if val is not None:
                # We save the entire var_data dictionary so we can extract units and metadata later if needed
                new_cache[platform_id]["variables"][var_name] = {"value": val, "unit": var_data.get("unit", "")}
            
        return new_cache
    except Exception as e:
        L.error(f"[OPS WS] Parse error: {e}")
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
    Output("dynamic-metrics-container", "children"),
    Input("ops-telemetry-cache", "data")
)
def update_dynamic_metrics(cache):
    """
    Dynamically generates UI cards for EVERY variable found in the cache, 
    grouped by the subsystem that provided them.
    """
    if not cache:
        return dbc.Alert("Awaiting telemetry...", color="info")

    # A helper dictionary containing health thresholds for known variables. 
    # If a variable isn't in here, it just defaults to "Nominal/Green".
    thresholds = {
        "platform_speed": {"hi_warn": 25.0, "hi_crit": 35.0},
        "air_temperature": {"low_crit": -10.0, "low_warn": 0.0, "hi_warn": 38.0, "hi_crit": 45.0},
        "relative_humidity": {"low_crit": 5.0, "hi_warn": 95.0},
        "air_pressure": {"low_warn": 960.0, "hi_warn": 1040.0},
        "true_wind_speed": {"hi_warn": 15.0, "hi_crit": 22.0},
        "O3": {"hi_warn": 70.0, "hi_crit": 100.0},
        "CO": {"hi_warn": 900.0, "hi_crit": 2000.0},
        "NO": {"hi_warn": 50.0},
        "NO2": {"hi_warn": 40.0},
        "inlet_flow": {"low_crit": 14.0, "low_warn": 15.5, "hi_warn": 17.5, "hi_crit": 19.0},
        "PM2_5": {"hi_warn": 35.0, "hi_crit": 55.0},
        "rain_intensity": {"hi_warn": 5.0, "hi_crit": 20.0}
    }

    def evaluate_metric(var_name, var_data):
        """Formats the reading and determines CSS styling based on thresholds."""
        raw_val = var_data.get("value")
        unit = var_data.get("unit", "")
        
        if raw_val is None:
            return "Waiting...", "border-0 shadow-sm mb-3 bg-white border-start border-4 border-secondary"
            
        try:
            val = float(raw_val)
            formatted_text = f"{val:.2f} {unit}".strip()
            
            # Fetch thresholds if this is a known variable
            bounds = thresholds.get(var_name, {})
            low_crit, low_warn = bounds.get("low_crit"), bounds.get("low_warn")
            hi_crit, hi_warn = bounds.get("hi_crit"), bounds.get("hi_warn")
            
            # Threshold verification
            if low_crit is not None and val <= low_crit:
                return formatted_text, "border-0 shadow-sm mb-3 bg-soft-danger border-start border-4 border-danger animate-pulse"
            if low_warn is not None and val <= low_warn:
                return formatted_text, "border-0 shadow-sm mb-3 bg-soft-warning border-start border-4 border-warning"
            if hi_crit is not None and val >= hi_crit:
                return formatted_text, "border-0 shadow-sm mb-3 bg-soft-danger border-start border-4 border-danger animate-pulse"
            if hi_warn is not None and val >= hi_warn:
                return formatted_text, "border-0 shadow-sm mb-3 bg-soft-warning border-start border-4 border-warning"
                
            # Nominal State
            return formatted_text, "border-0 shadow-sm mb-3 bg-white border-start border-4 border-success"
            
        except (ValueError, TypeError):
            # Non-float values (like strings or states) just display normally
            return f"{raw_val} {unit}".strip(), "border-0 shadow-sm mb-3 bg-white border-start border-4 border-success"


    # Build the dynamic layout
    sections = []
    
    # Iterate through every platform in the cache
    for platform_id, p_data in cache.items():
        variables = p_data.get("variables", {})
        if not variables:
            continue # Skip platforms that haven't sent variable data yet
            
        # Ignore timestamps as standalone cards
        if "time" in variables:
            del variables["time"]

        cards = []
        # Generate a card for every single variable this platform is sending
        for var_name, var_data in variables.items():
            formatted_text, css_class = evaluate_metric(var_name, var_data)
            
            # Clean up the variable name for display (e.g. "air_temperature" -> "Air Temperature")
            display_name = var_name.replace("_", " ").title()
            
            card = dbc.Col(
                dbc.Card([
                    dbc.CardBody([
                        html.Div([
                            html.Span(display_name, className="text-muted small fw-bold text-uppercase d-block mb-1 text-truncate"),
                            html.H3(formatted_text, className="fw-bold mb-0 text-dark transition-all"),
                        ], className="position-relative")
                    ], className="p-3")
                ], className=css_class), 
                width=12, md=4, lg=3
            )
            cards.append(card)

        # Create a visually grouped section for this platform
        section = html.Div([
            html.H5([html.I(className="bi bi-cpu me-2"), f"Source: {platform_id}"], className="fw-bold mb-3 text-secondary mt-4"),
            dbc.Row(cards, className="mb-2")
        ])
        sections.append(section)

    if not sections:
        return dbc.Alert("Listening for data streams...", color="info")

    return html.Div(sections)