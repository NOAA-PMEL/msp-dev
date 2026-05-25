import dash
from dash import html, dcc, Input, Output, State, no_update, ALL, MATCH, ctx, Patch
from dash_extensions import WebSocket
import dash_bootstrap_components as dbc
import logging
import json
import traceback  # <--- Essential for our error visualizer
from datetime import datetime, timezone

from utils import get_registry_data, config, create_unified_shell, register_sidebar_callbacks

L = logging.getLogger(__name__)

# --- Initialize Isolated Dash App ---
app = dash.Dash(__name__, requests_pathname_prefix="/envds/envops/ops/", routes_pathname_prefix="/", suppress_callback_exceptions=True)
register_sidebar_callbacks(app)

SERVER_CACHE = {}

app.layout = create_unified_shell(html.Div([
    dcc.Location(id="ops-url", refresh=False),
    html.Div(id="ops-page-content") 
]), active_item="ops")

@app.callback(
    Output("ops-page-content", "children"),
    Input("ops-url", "pathname")
)
def render_deployment_ops(pathname):
    L.info(f"[[DEBUG OPS ROUTER]] 🚦 Callback triggered. Pathname: {pathname}")
    try:
        if not pathname or "deployment/" not in pathname:
            L.info("[[DEBUG OPS ROUTER]] 🛑 Invalid path. Returning Alert.")
            return dbc.Alert("Select a deployment from the sidebar.", color="info", className="m-4")
        
        deployment_id = pathname.split("/")[-1]
        L.info(f"[[DEBUG OPS ROUTER]] ⚙️ Extracted ID: {deployment_id}. Building layout...")
        
        layout = build_ops_layout(deployment_id)
        L.info("[[DEBUG OPS ROUTER]] ✅ Layout built successfully! Returning to browser.")
        return layout
        
    except Exception as e:
        # exc_info=True forces the full stack trace into your Docker/k3d logs
        L.error(f"[[DEBUG OPS ROUTER]] 💥 CRASH: {e}", exc_info=True) 
        # Keep the return object simple to prevent secondary Dash 500 errors
        return dbc.Alert(f"Fatal Error: {str(e)}", color="danger", className="m-4")

def build_ops_layout(deployment_id):
    L.info(f"[[DEBUG LAYOUT]] 🔍 Starting build for {deployment_id}")
    
    all_deployments = get_registry_data("deployment") or []
    L.info(f"[[DEBUG LAYOUT]] 🗄️ Datastore returned {len(all_deployments)} deployments.")
    
    host_dep = next((d for d in all_deployments if d.get("metadata", {}).get("name") == deployment_id), None)
    if not host_dep:
        L.warning("[[DEBUG LAYOUT]] ⚠️ Host deployment not found in registry.")
        return dbc.Alert(f"Deployment {deployment_id} not found.", color="warning", className="m-4")
        
    host_data = host_dep.get("data", {})
    host_plat_ref = host_data.get("platform_ref", "")
    host_name = host_data.get("display_name", host_plat_ref)
    L.info(f"[[DEBUG LAYOUT]] 🎯 Host platform ref: {host_plat_ref}, Display name: {host_name}")
    
    child_deps = [d for d in all_deployments if d.get("data", {}).get("host_platform_ref") == host_plat_ref and d.get("metadata", {}).get("name") != deployment_id]
    L.info(f"[[DEBUG LAYOUT]] 🔗 Found {len(child_deps)} child payloads attached.")

    raw_targets = [host_plat_ref] + [c.get("data", {}).get("platform_ref", "") for c in child_deps]
    short_targets = [p.split(".")[-1] for p in raw_targets if "." in p]
    group_platforms = list(set(raw_targets + short_targets))
    group_platforms = [p for p in group_platforms if p]
    L.info(f"[[DEBUG LAYOUT]] 📡 Final group platforms to listen to: {group_platforms}")

    header = dbc.Row([
        dbc.Col([
            html.H2([html.I(className="bi bi-hdd-network me-2"), host_name], className="fw-bold mb-0"),
            html.P(f"ID: {deployment_id} | Tactical Quick Look", className="text-muted mb-0")
        ]),
        dbc.Col([
            dbc.Button([html.I(className="bi bi-graph-up me-2"), "View Full Analytics"], 
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
    
    # SAFEGUARD 2: Type cast ws_use_tls to string to prevent 'bool' has no attribute 'lower' crashes
    ws_protocol = "wss://" if str(config.ws_use_tls).lower() == "true" else "ws://"
    ws_base = f"{ws_protocol}{config.external_hostname}:{config.ws_port}/envds/envops"

    ws_connections = [WebSocket(id="ws-ops-system", url=f"{ws_base}/ws/system-ops/main")]
    platform_stores = []
    
    for p_id in group_platforms:
        if p_id: 
            ws_connections.append(WebSocket(id={"type": "ws-ops-platform", "index": p_id}, url=f"{ws_base}/ws/platform/{p_id}"))
            platform_stores.append(dcc.Store(id={"type": "platform-cache", "index": p_id}, data={"variables": {}, "state": {}}))

    return html.Div([
        html.Div(ws_connections),
        html.Div(platform_stores),
        header, ops_ribbon, 
        html.Div(id="tactical-metrics-container", children=[dbc.Spinner(color="primary")], className="mt-4"),
    ], className="container-fluid mt-3")

# --- Callbacks ---

@app.callback(
    Output({"type": "platform-cache", "index": MATCH}, "data"),
    Input({"type": "ws-ops-platform", "index": MATCH}, "message") # <-- No State needed!
)
def ingest_live_telemetry(msg):
    if not msg or "data" not in msg: return no_update

    plat_id = ctx.triggered_id.get("index") if ctx.triggered_id else "Unknown"
    
    # Initialize the server memory for this platform if it doesn't exist yet
    if plat_id not in SERVER_CACHE:
        SERVER_CACHE[plat_id] = {"variables": {}, "state": {}}

    try:
        ws_wrapper = json.loads(msg["data"])
        payload = ws_wrapper.get("data-update")
        if not payload: return no_update
            
        incoming_vars = payload.get("variables", {})
        
        # --- DEBUGGING ---
        L.info(f"[[DEBUG INGEST]] 📩 WS hit for {plat_id}. Processing {len(incoming_vars)} vars...")
        
        current_time = incoming_vars.get("time", {}).get("data") or datetime.now(timezone.utc).isoformat()
        has_updates = False
        
        # Write directly to the persistent Python memory
        for var_name, var_data in incoming_vars.items():
            if var_name == "time": continue
            val = var_data.get("data")
            if val is not None:
                SERVER_CACHE[plat_id]["variables"][var_name] = {"value": val, "unit": var_data.get("unit", ""), "timestamp": current_time}
                has_updates = True
        
        if has_updates:
            # We must return a dict() copy so Dash realizes the data changed and triggers the UI!
            L.info(f"[[DEBUG INGEST]] ✅ Cache for {plat_id} now holds {len(SERVER_CACHE[plat_id]['variables'])} total merged variables.")
            return dict(SERVER_CACHE[plat_id])
            
        return no_update
    except Exception as e:
        L.error(f"[[DEBUG INGEST]] 💥 Parse error: {e}")
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


# -----------------------------------------------------------------------------
# TACTICAL QUICK LOOK RENDERER
# -----------------------------------------------------------------------------
@app.callback(
    Output("tactical-metrics-container", "children"),
    Input({"type": "platform-cache", "index": ALL}, "data"),
    State({"type": "platform-cache", "index": ALL}, "id")
)
def update_tactical_quick_look(caches, cache_ids):
    # --- DEBUGGING INJECTED HERE ---
    L.info(f"[[DEBUG RENDER]] 🎨 Triggered. Processing {len(caches)} platform caches.")
    
    flat_vars = {}
    for c_data, c_id in zip(caches, cache_ids):
        if not c_data: continue
        plat_id = c_id["index"]
        for v_name, v_data in c_data.get("variables", {}).items():
            flat_vars[v_name] = {**v_data, "platform": plat_id}
            
    L.info(f"[[DEBUG RENDER]] 📊 Flattened cache contains {len(flat_vars)} total variables.")
            
    if not flat_vars: 
        L.info("[[DEBUG RENDER]] ⏳ Cache empty. Displaying 'Awaiting telemetry...'")
        return dbc.Alert("Awaiting telemetry...", color="info")

    now = datetime.now(timezone.utc)
    STALE_SECONDS = 120

    thresholds = {
        "platform_speed": {"hi_warn": 25.0, "hi_crit": 35.0},
        "air_temperature": {"low_crit": -10.0, "low_warn": 0.0, "hi_warn": 38.0, "hi_crit": 45.0},
        "relative_humidity": {"low_crit": 5.0, "hi_warn": 95.0},
        "inlet_flow": {"low_crit": 14.0, "low_warn": 15.5, "hi_warn": 17.5, "hi_crit": 19.0},
        "rain_intensity": {"hi_warn": 5.0, "hi_crit": 20.0},
        "O3": {"hi_warn": 70.0, "hi_crit": 100.0},
        "CO": {"hi_warn": 900.0, "hi_crit": 2000.0},
        "NO": {"hi_warn": 50.0},
        "NO2": {"hi_warn": 40.0}
    }

    def get_var_status(var_name):
        v = flat_vars.get(var_name)
        if not v: return None, "Waiting...", "border-0 shadow-sm mb-3 bg-white border-start border-4 border-secondary", False, ""
        
        raw_val = v["value"]
        unit = v.get("unit", "")
        
        is_stale = False
        try:
            last_time = datetime.fromisoformat(str(v["timestamp"]).replace("Z", "+00:00"))
            if (now - last_time).total_seconds() > STALE_SECONDS: is_stale = True
        except Exception: pass

        css_class = "border-0 shadow-sm mb-3 bg-white border-start border-4 border-success"
        if is_stale:
            css_class = "border-0 shadow-sm mb-3 bg-light border-start border-4 border-secondary opacity-75"
        else:
            try:
                val_float = float(raw_val)
                bounds = thresholds.get(var_name, {})
                if bounds.get("low_crit") is not None and val_float <= bounds["low_crit"]: css_class = "border-0 shadow-sm mb-3 bg-soft-danger border-start border-4 border-danger animate-pulse"
                elif bounds.get("low_warn") is not None and val_float <= bounds["low_warn"]: css_class = "border-0 shadow-sm mb-3 bg-soft-warning border-start border-4 border-warning"
                elif bounds.get("hi_crit") is not None and val_float >= bounds["hi_crit"]: css_class = "border-0 shadow-sm mb-3 bg-soft-danger border-start border-4 border-danger animate-pulse"
                elif bounds.get("hi_warn") is not None and val_float >= bounds["hi_warn"]: css_class = "border-0 shadow-sm mb-3 bg-soft-warning border-start border-4 border-warning"
            except (ValueError, TypeError): pass

        try: formatted_val = f"{float(raw_val):.2f}"
        except: formatted_val = str(raw_val)

        return raw_val, formatted_val, css_class, is_stale, unit

    # --- CARD COMPONENT BUILDERS ---
    def standard_card(title, var_name):
        _, fmt_val, css, is_stale, unit = get_var_status(var_name)
        val_display = f"{fmt_val} {unit}".strip() if fmt_val != "Waiting..." else fmt_val
        stale_badge = html.Span(" STALE", className="text-danger fw-bold ms-2") if is_stale else ""
        
        return dbc.Col(dbc.Card(dbc.CardBody([
            html.Span([title, stale_badge], className="text-muted small fw-bold text-uppercase d-block mb-1 text-truncate"),
            html.H3(val_display, className="fw-bold mb-0 text-dark")
        ], className="p-3"), className=css), width=12, md=3)

    def wind_card(title, spd_var, dir_var):
        _, spd_fmt, spd_css, spd_stale, spd_unit = get_var_status(spd_var)
        _, dir_fmt, _, _, dir_unit = get_var_status(dir_var)
        
        stale_badge = html.Span(" STALE", className="text-danger fw-bold ms-2") if spd_stale else ""
        if spd_fmt == "Waiting...": val_display = "Waiting..."
        else: val_display = html.Div([html.Span(f"{spd_fmt} {spd_unit}"), html.Span(f" @ {dir_fmt}{dir_unit}", className="text-secondary ms-2 fs-5")])

        return dbc.Col(dbc.Card(dbc.CardBody([
            html.Span([title, stale_badge], className="text-muted small fw-bold text-uppercase d-block mb-1 text-truncate"),
            html.H3(val_display, className="fw-bold mb-0 text-dark")
        ], className="p-3"), className=spd_css), width=12, md=3)

    def flow_card(title, flow_var, sp_var):
        _, flow_fmt, flow_css, is_stale, flow_unit = get_var_status(flow_var)
        sp_raw, sp_fmt, _, _, _ = get_var_status(sp_var)
        
        stale_badge = html.Span(" STALE", className="text-danger fw-bold ms-2") if is_stale else ""
        if flow_fmt == "Waiting...": val_display = "Waiting..."
        else: 
            sp_display = f" (SP: {sp_fmt})" if sp_raw is not None else ""
            val_display = html.Div([html.Span(f"{flow_fmt} {flow_unit}"), html.Span(sp_display, className="text-secondary ms-2 fs-6")])

        return dbc.Col(dbc.Card(dbc.CardBody([
            html.Span([title, stale_badge], className="text-muted small fw-bold text-uppercase d-block mb-1 text-truncate"),
            html.H3(val_display, className="fw-bold mb-0 text-dark")
        ], className="p-3"), className=flow_css), width=12, md=3)

    def optics_card(title, b_var, g_var, r_var):
        b_raw, b_fmt, css, is_stale, b_unit = get_var_status(b_var)
        g_raw, g_fmt, _, _, g_unit = get_var_status(g_var)
        r_raw, r_fmt, _, _, r_unit = get_var_status(r_var)
        
        stale_badge = html.Span(" STALE", className="text-danger fw-bold ms-2") if is_stale else ""
        if b_raw is None and g_raw is None and r_raw is None:
            content = html.H3("Waiting...", className="fw-bold mb-0 text-dark")
        else:
            content = html.Div([
                html.Div([html.I(className="bi bi-circle-fill text-primary me-2"), html.Span(f"Blue: {b_fmt} {b_unit}" if b_raw else "Blue: N/A", className="fw-bold fs-5")]),
                html.Div([html.I(className="bi bi-circle-fill text-success me-2"), html.Span(f"Green: {g_fmt} {g_unit}" if g_raw else "Green: N/A", className="fw-bold fs-5")]),
                html.Div([html.I(className="bi bi-circle-fill text-danger me-2"), html.Span(f"Red: {r_fmt} {r_unit}" if r_raw else "Red: N/A", className="fw-bold fs-5")])
            ])

        return dbc.Col(dbc.Card(dbc.CardBody([
            html.Span([title, stale_badge], className="text-muted small fw-bold text-uppercase d-block mb-2 text-truncate"),
            content
        ], className="p-3"), className=css), width=12, md=4)

    # --- ASSEMBLE THE GROUPS ---
    
    group_nav = html.Div([
        html.H5([html.I(className="bi bi-compass me-2"), "Navigation"], className="fw-bold mb-3 text-secondary border-bottom pb-2"),
        dbc.Row([
            standard_card("Latitude", "latitude"),
            standard_card("Longitude", "longitude"),
            standard_card("Heading", "platform_heading"),
            standard_card("Speed", "platform_speed"),
        ], className="mb-4")
    ])

    group_met = html.Div([
        html.H5([html.I(className="bi bi-cloud-sun me-2"), "Meteorology & Solar"], className="fw-bold mb-3 text-secondary border-bottom pb-2"),
        dbc.Row([
            wind_card("True Wind", "true_wind_speed", "true_wind_direction"),
            standard_card("Temperature", "air_temperature"),
            standard_card("Rel. Humidity", "relative_humidity"),
            standard_card("Pressure", "pressure"), 
            standard_card("Solar Irradiance", "irradiance"),
            standard_card("Rain Rate", "rain_intensity"),
        ], className="mb-4")
    ])

    group_aerosols = html.Div([
        html.H5([html.I(className="bi bi-brightness-high me-2"), "Aerosols & Optics"], className="fw-bold mb-3 text-secondary border-bottom pb-2"),
        dbc.Row([
            standard_card("CN Concentration", "cn_concentration"),
            optics_card("Scattering (Mm⁻¹)", "scatter_blue", "scatter_green", "scatter_red"),
            optics_card("Absorption (Mm⁻¹)", "absorption_blue", "absorption_green", "absorption_red"),
        ], className="mb-4")
    ])

    group_gas = html.Div([
        html.H5([html.I(className="bi bi-wind me-2"), "Gas Phase Chemistry"], className="fw-bold mb-3 text-secondary border-bottom pb-2"),
        dbc.Row([
            standard_card("Ozone (O3)", "O3"),
            standard_card("Carbon Monoxide (CO)", "CO"),
            standard_card("Nitric Oxide (NO)", "NO"),
            standard_card("Nitrogen Dioxide (NO2)", "NO2"),
        ], className="mb-4")
    ])

    group_ops = html.Div([
        html.H5([html.I(className="bi bi-sliders me-2"), "Operational States"], className="fw-bold mb-3 text-secondary border-bottom pb-2"),
        dbc.Row([
            flow_card("Inlet Flow", "inlet_flow", "inlet_flow_sp"),
            wind_card("Relative Wind", "relative_wind_speed", "relative_wind_direction"),
        ], className="mb-4")
    ])

    try:
        final_layout = html.Div([group_nav, group_met, group_aerosols, group_gas, group_ops])
        L.info("[[DEBUG RENDER]] ✅ Layout dynamically assembled. Returning to browser.")
        return final_layout
    except Exception as e:
        L.error(f"[[DEBUG RENDER]] 💥 Layout assembly failed: {e}")
        return dbc.Alert(f"UI Build Error: {e}", color="danger")