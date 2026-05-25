import dash
from dash import html, dcc, Input, Output, State, no_update, ALL, MATCH, ctx
from dash_extensions import WebSocket
import dash_bootstrap_components as dbc
import logging
import json
from datetime import datetime, timezone

from utils import get_registry_data, config, create_unified_shell, register_sidebar_callbacks

L = logging.getLogger(__name__)

# --- Initialize Isolated Dash App ---
app = dash.Dash(__name__, requests_pathname_prefix="/envds/envops/ops/", routes_pathname_prefix="/", suppress_callback_exceptions=True)
register_sidebar_callbacks(app)

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
            dbc.Button([html.I(className="bi bi-graph-up me-2"), "View Analytics & Plots"], 
                       href=f"/envds/envops/plots/deployment/{deployment_id}", 
                       color="primary", className="fw-bold shadow-sm me-3"),
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

    # --- FIX: ISOLATED MEMORY CACHES ---
    ws_connections = [WebSocket(id="ws-ops-system", url=f"{ws_base}/ws/system-ops/main")]
    platform_stores = []
    
    for p_id in group_platforms:
        if p_id: 
            ws_connections.append(WebSocket(id={"type": "ws-ops-platform", "index": p_id}, url=f"{ws_base}/ws/platform/{p_id}"))
            # Each WebSocket gets its own dedicated memory block
            platform_stores.append(dcc.Store(id={"type": "platform-cache", "index": p_id}, data={"variables": {}, "state": {}}))

    return html.Div([
        html.Div(ws_connections),
        html.Div(platform_stores),
        header, ops_ribbon, 
        html.Div(id="dynamic-metrics-container", children=[dbc.Spinner(color="primary")]),
        sub_systems
    ], className="container-fluid mt-3")

# --- Callbacks ---

@app.callback(
    Output({"type": "platform-cache", "index": MATCH}, "data"),
    Input({"type": "ws-ops-platform", "index": MATCH}, "message"),
    State({"type": "platform-cache", "index": MATCH}, "data")
)
def ingest_live_telemetry(msg, current_cache):
    """Safely updates a SINGLE platform's cache, completely immune to race conditions."""
    if not msg or "data" not in msg: return no_update

    try:
        ws_wrapper = json.loads(msg["data"])
        payload = ws_wrapper.get("data-update")
        if not payload: return no_update
            
        # current_cache is specific to ONE platform (e.g., msp_enclosure_01)
        new_cache = current_cache.copy() if current_cache else {"variables": {}, "state": {}}
        incoming_vars = payload.get("variables", {})
        current_time = incoming_vars.get("time", {}).get("data") or datetime.now(timezone.utc).isoformat()
        
        # We must copy the variables dict so Dash detects the React state change
        new_vars = new_cache.get("variables", {}).copy()
        
        for var_name, var_data in incoming_vars.items():
            if var_name == "time": continue
            val = var_data.get("data")
            if val is not None:
                new_vars[var_name] = {"value": val, "unit": var_data.get("unit", ""), "timestamp": current_time}
        
        new_cache["variables"] = new_vars
        return new_cache
    except Exception:
        return no_update
    
@app.callback(
    Output("ops-health-badge", "children"), Output("ops-health-badge", "color"),
    Output("ops-sys-mode", "children"), Output("ops-sys-mode", "className"),
    Output("ops-alarm-count", "children"),
    Input({"type": "platform-cache", "index": ALL}, "data")
)
def update_ribbon_ui(caches):
    total_alarms = 0
    sys_mode, sys_mode_color = "STANDBY", "fw-bold text-muted mb-0"
    for data in caches:
        if not data: continue
        state = data.get("state", {})
        if "alarm" in str(state).lower() or "error" in str(state).lower(): total_alarms += 1
        if "system_active" in state:
            sys_mode = "ACTIVE" if str(state["system_active"].get("actual", "")).lower() == "true" else "STANDBY"
            sys_mode_color = "fw-bold text-primary mb-0" if sys_mode == "ACTIVE" else "fw-bold text-muted mb-0"
            
    if total_alarms > 0: return f"{total_alarms} Critical Alarms", "danger", sys_mode, sys_mode_color, str(total_alarms)
    return "Group Nominal", "success", sys_mode, sys_mode_color, str(total_alarms)

CATEGORY_MAP = {
    "latitude": "Navigation & Position", "longitude": "Navigation & Position", 
    "platform_heading": "Navigation & Position", "platform_speed": "Navigation & Position",
    "air_temperature": "Meteorology", "relative_humidity": "Meteorology", 
    "air_pressure": "Meteorology", "true_wind_speed": "Meteorology", 
    "true_wind_direction": "Meteorology", "rain_intensity": "Meteorology",
    "O3": "Gas Phase Chemistry", "CO": "Gas Phase Chemistry", "NO": "Gas Phase Chemistry", "NO2": "Gas Phase Chemistry",
    "inlet_flow": "Sampling & Aerosols", "PM2_5": "Sampling & Aerosols", "PM10": "Sampling & Aerosols"
}

@app.callback(
    Output("dynamic-metrics-container", "children"),
    Input({"type": "platform-cache", "index": ALL}, "data"),
    State({"type": "platform-cache", "index": ALL}, "id")
)
def update_dynamic_metrics(caches, cache_ids):
    """Aggregates all isolated platform caches downstream and builds the UI grid."""
    # Rebuild the master dictionary
    cache = {c_id["index"]: c_data for c_data, c_id in zip(caches, cache_ids) if c_data}
    
    if not cache: return dbc.Alert("Awaiting telemetry...", color="info")

    now = datetime.now(timezone.utc)
    STALE_SECONDS = 120 # Over 2 minutes = Stale

    thresholds = {
        "platform_speed": {"hi_warn": 25.0, "hi_crit": 35.0},
        "air_temperature": {"low_crit": -10.0, "low_warn": 0.0, "hi_warn": 38.0, "hi_crit": 45.0},
        "relative_humidity": {"low_crit": 5.0, "hi_warn": 95.0},
        "air_pressure": {"low_warn": 960.0, "hi_warn": 1040.0},
        "true_wind_speed": {"hi_warn": 15.0, "hi_crit": 22.0},
        "O3": {"hi_warn": 70.0, "hi_crit": 100.0},
        "inlet_flow": {"low_crit": 14.0, "low_warn": 15.5, "hi_warn": 17.5, "hi_crit": 19.0},
        "PM2_5": {"hi_warn": 35.0, "hi_crit": 55.0}
    }

    def evaluate_metric(var_name, var_dict):
        raw_val = var_dict.get("value")
        last_time_str = var_dict.get("timestamp")
        unit = var_dict.get("unit", "")
        
        is_stale = False
        try:
            last_time = datetime.fromisoformat(str(last_time_str).replace("Z", "+00:00"))
            if (now - last_time).total_seconds() > STALE_SECONDS: is_stale = True
        except Exception: pass

        if raw_val is None: return "Waiting...", "border-0 shadow-sm mb-3 bg-white border-start border-4 border-secondary", False
            
        try:
            val = float(raw_val)
            formatted_text = f"{val:.2f} {unit}".strip()
            if is_stale: return formatted_text, "border-0 shadow-sm mb-3 bg-light border-start border-4 border-secondary opacity-75", True

            bounds = thresholds.get(var_name, {})
            low_crit, low_warn = bounds.get("low_crit"), bounds.get("low_warn")
            hi_crit, hi_warn = bounds.get("hi_crit"), bounds.get("hi_warn")
            
            if low_crit is not None and val <= low_crit: return formatted_text, "border-0 shadow-sm mb-3 bg-soft-danger border-start border-4 border-danger animate-pulse", False
            if low_warn is not None and val <= low_warn: return formatted_text, "border-0 shadow-sm mb-3 bg-soft-warning border-start border-4 border-warning", False
            if hi_crit is not None and val >= hi_crit: return formatted_text, "border-0 shadow-sm mb-3 bg-soft-danger border-start border-4 border-danger animate-pulse", False
            if hi_warn is not None and val >= hi_warn: return formatted_text, "border-0 shadow-sm mb-3 bg-soft-warning border-start border-4 border-warning", False
                
            return formatted_text, "border-0 shadow-sm mb-3 bg-white border-start border-4 border-success", False
            
        except (ValueError, TypeError):
            css = "border-0 shadow-sm mb-3 bg-light border-start border-4 border-secondary opacity-75" if is_stale else "border-0 shadow-sm mb-3 bg-white border-start border-4 border-success"
            return f"{raw_val} {unit}".strip(), css, is_stale

    grouped_cards = {}
    for platform_id, p_data in cache.items():
        for var_name, var_dict in p_data.get("variables", {}).items():
            category = CATEGORY_MAP.get(var_name, "Other Variables")
            formatted_text, css_class, is_stale = evaluate_metric(var_name, var_dict)
            
            display_name = var_name.replace("_", " ").title()
            label_components = [display_name, html.Br(), html.Span(f"({platform_id})", className="text-secondary opacity-50")]
            if is_stale: label_components.append(html.Span(" STALE", className="ms-2 text-danger fw-bold"))

            card = dbc.Col(dbc.Card(dbc.CardBody([
                    html.Div([html.Span(label_components, className="text-muted small fw-bold text-uppercase d-block mb-1 text-truncate"),
                              html.H3(formatted_text, className="fw-bold mb-0 text-dark transition-all")], className="position-relative")
                ], className="p-3"), className=css_class), width=12, md=4, lg=3)
            grouped_cards.setdefault(category, []).append(card)

    sections = []
    for cat_name, cards in grouped_cards.items():
        section = html.Div([
            html.H5([html.I(className="bi bi-collection me-2"), cat_name], className="fw-bold mb-3 mt-4 border-bottom pb-2 text-secondary"),
            dbc.Row(cards, className="mb-2")
        ])
        sections.append(section)

    return html.Div(sections)