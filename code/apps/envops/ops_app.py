import dash
from dash import html, dcc, Input, Output, State, no_update, ALL, MATCH, ctx, Patch
from dash_extensions import WebSocket
import dash_bootstrap_components as dbc
import logging
import json
import traceback
from datetime import datetime, timezone

from utils import get_registry_data, config, create_unified_shell, register_sidebar_callbacks

L = logging.getLogger(__name__)

# --- Initialize Isolated Dash App ---
app = dash.Dash(__name__, requests_pathname_prefix="/envds/envops/ops/", routes_pathname_prefix="/", suppress_callback_exceptions=True)
register_sidebar_callbacks(app)

# --- GLOBAL IN-MEMORY CACHES ---
SERVER_CACHE = {}
CONDITIONS_CACHE = {} 

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
        return layout
        
    except Exception as e:
        L.error(f"[[DEBUG OPS ROUTER]] 💥 CRASH: {e}", exc_info=True) 
        return dbc.Alert(f"Fatal Error: {str(e)}", color="danger", className="m-4")


def build_c2_panel(host_name, host_plat_ref):
    """Builds the Command & Control interface dynamically, scoped to the Host Platform."""
    
    sys_modes_data = get_registry_data("systemmode") or []
    samp_modes_data = get_registry_data("samplingmode") or []

    sys_mode_options = [
        {"label": m.get("metadata", {}).get("name", "Unknown").replace("_", " ").title(),
         "value": m.get("metadata", {}).get("name", "Unknown")}
        for m in sys_modes_data
    ]
    
    samp_mode_options = [
        {"label": m.get("metadata", {}).get("name", "Unknown").replace("_", " ").title(),
         "value": m.get("metadata", {}).get("name", "Unknown")}
        for m in samp_modes_data
    ]

    if not sys_mode_options:
        sys_mode_options = [{"label": "Normal", "value": "normal"}, {"label": "Startup", "value": "startup"}]
    if not samp_mode_options:
        samp_mode_options = [
            {"label": "Nominal Sampling", "value": "nominal_sampling"}, 
            {"label": "System Startup", "value": "system_startup"}
        ]

    return dbc.Card([
        # 🟢 CONTEXT: Add the Host Name to the Header visually
        dbc.CardHeader([html.I(className="bi bi-sliders me-2"), f"C2: {host_name}"], className="fw-bold bg-dark text-white"),
        dbc.CardBody([
            # 🟢 CONTEXT: Store the exact platform reference invisibly for the callback
            dcc.Store(id="c2-target-platform", data=host_plat_ref),
            
            html.H6("Operation Mode", className="text-muted small text-uppercase fw-bold"),
            dbc.RadioItems(
                id="c2-operation-mode",
                options=[
                    {"label": "Autonomous", "value": "auto"},
                    {"label": "Manual Override", "value": "manual"},
                ],
                value="auto",
                inline=True,
                className="mb-3"
            ),
            
            html.H6("System Mode", className="text-muted small text-uppercase fw-bold"),
            dbc.Select(
                id="c2-system-mode",
                options=sys_mode_options,
                value=sys_mode_options[0]["value"],
                className="mb-4"
            ),
            
            html.Hr(),
            html.H6("Manual Subsystem Overrides", className="text-muted small text-uppercase fw-bold mb-2"),
            
            dbc.Checklist(
                id="c2-sampling-modes-checklist",
                options=samp_mode_options,
                value=[],
                switch=True,
                className="mb-2"
            ),
            
            dbc.Button("Apply Overrides", id="c2-apply-btn", color="warning", className="w-100 fw-bold mt-3 shadow-sm")
        ])
    ], className="shadow-sm border-0 h-100")


def build_ops_layout(deployment_id):
    L.info(f"[[DEBUG LAYOUT]] 🔍 Starting build for {deployment_id}")
    all_deployments = get_registry_data("deployment") or []
    
    # 1. Resolve Host Deployment Context
    host_dep = next((d for d in all_deployments if d.get("metadata", {}).get("name") == deployment_id), None)
    if not host_dep:
        L.warning("[[DEBUG LAYOUT]] ⚠️ Host deployment not found in registry.")
        return dbc.Alert(f"Deployment {deployment_id} not found.", color="warning", className="m-4")
        
    host_data = host_dep.get("data", {})
    host_plat_ref = host_data.get("platform_ref", "")
    host_name = host_data.get("display_name", host_plat_ref)
    
    # 2. Resolve Child Sub-systems
    child_deps = [d for d in all_deployments if d.get("data", {}).get("host_platform_ref") == host_plat_ref and d.get("metadata", {}).get("name") != deployment_id]

    raw_targets = [host_plat_ref] + [c.get("data", {}).get("platform_ref", "") for c in child_deps]
    short_targets = [p.split(".")[-1] for p in raw_targets if "." in p]
    group_platforms = list(set(raw_targets + short_targets))
    group_platforms = [p for p in group_platforms if p]

    # 3. Build the Header
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

    # 4. Build the Status Ribbon
    ops_ribbon = dbc.Card(dbc.CardBody(dbc.Row([
        dbc.Col([html.H6("System Mode", className="text-muted mb-1 small text-uppercase"), html.H5("STANDBY", id="ops-sys-mode-disp", className="fw-bold mb-0")], width=3, className="border-end"),
        dbc.Col([html.H6("Sampling State", className="text-muted mb-1 small text-uppercase"), html.H5("IDLE", id="ops-samp-state", className="fw-bold mb-0")], width=3, className="border-end"),
        dbc.Col([html.H6("Active Alarms", className="text-muted mb-1 small text-uppercase"), html.H5("0", id="ops-alarm-count", className="text-success fw-bold mb-0")], width=3),
        dbc.Col([html.H6("Operations", className="text-muted mb-1 small text-uppercase"), html.H5("AUTONOMOUS", id="ops-auto-state", className="text-primary fw-bold mb-0")], width=3)
    ])), className="shadow-sm border-0 mb-4 bg-light")
    
    # 5. Initialize WebSockets & Stores
    ws_protocol = "wss://" if str(config.ws_use_tls).lower() == "true" else "ws://"
    ws_base = f"{ws_protocol}{config.external_hostname}:{config.ws_port}/envds/envops"

    ws_connections = [
        WebSocket(id="ws-ops-system", url=f"{ws_base}/ws/system-ops/main"),
        WebSocket(id="ws-ops-conditions", url=f"{ws_base}/ws/conditions/main") 
    ]
    platform_stores = []
    
    for p_id in group_platforms:
        if p_id: 
            ws_connections.append(WebSocket(id={"type": "ws-ops-platform", "index": p_id}, url=f"{ws_base}/ws/platform/{p_id}"))
            platform_stores.append(dcc.Store(id={"type": "platform-cache", "index": p_id}, data={"variables": {}, "state": {}}))

    # 6. Build the Contextual C2 Panel
    c2_panel = build_c2_panel(host_name, host_plat_ref)

    # 7. Assemble and Return the Layout
    return html.Div([
        html.Div(ws_connections),
        html.Div(platform_stores),
        header, ops_ribbon, 
        dbc.Row([
            # 🟢 The cards are dynamically injected into this container by the callback!
            dbc.Col(html.Div(id="tactical-metrics-container", children=[dbc.Spinner(color="primary")]), width=9),
            dbc.Col(c2_panel, width=3) 
        ], className="mt-4"),
    ], className="container-fluid mt-3")


# --- CALLBACKS ---

@app.callback(
    Output({"type": "platform-cache", "index": MATCH}, "data"),
    Input({"type": "ws-ops-platform", "index": MATCH}, "message")
)
def ingest_live_telemetry(msg):
    if not msg or "data" not in msg: return no_update
    plat_id = ctx.triggered_id.get("index") if ctx.triggered_id else "Unknown"
    
    if plat_id not in SERVER_CACHE:
        SERVER_CACHE[plat_id] = {"variables": {}, "state": {}}

    try:
        ws_wrapper = json.loads(msg["data"])
        payload = ws_wrapper.get("data-update")
        if not payload: return no_update
            
        incoming_vars = payload.get("variables", {})
        current_time = incoming_vars.get("time", {}).get("data") or datetime.now(timezone.utc).isoformat()
        has_updates = False
        
        for var_name, var_data in incoming_vars.items():
            if var_name == "time": continue
            val = var_data.get("data")
            if val is not None:
                SERVER_CACHE[plat_id]["variables"][var_name] = {"value": val, "unit": var_data.get("unit", ""), "timestamp": current_time}
                has_updates = True
        
        if has_updates:
            return dict(SERVER_CACHE[plat_id])
            
        return no_update
    except Exception as e:
        L.error(f"[[DEBUG INGEST]] 💥 Parse error: {e}")
        return no_update


@app.callback(
    Output("ops-auto-state", "children"), 
    Input("ws-ops-conditions", "message")
)
def ingest_backend_conditions(msg):
    """Listens to manager.py state evaluations and updates the global dictionary."""
    if not msg or "data" not in msg: return no_update
    
    try:
        payload = json.loads(msg["data"])
        ce_type = payload.get("type", "")
        data = payload.get("data", {})
        
        id_block = data.get("id", {})
        state_block = data.get("state", {})
        app_uid = id_block.get("app_uid")
        
        if not app_uid: return no_update
        
        L.debug(f"[[DEBUG C2 CACHE]] Intercepted envdsStatus for uid: {app_uid}, type: {ce_type}")
        
        if "samplingmode" in ce_type: actual = state_block.get("mode_active", {}).get("actual", "false")
        elif "samplingstate" in ce_type: actual = state_block.get("state_active", {}).get("actual", "false")
        elif "samplingcondition" in ce_type: actual = state_block.get("condition_met", {}).get("actual", "false")
        else: actual = "false"

        CONDITIONS_CACHE[app_uid] = {
            "type": ce_type,
            "actual": str(actual).lower() == "true",
            "timestamp": data.get("timestamp")
        }
        
        L.info(f"[[DEBUG C2 CACHE]] Updated {app_uid} cache -> actual: {CONDITIONS_CACHE[app_uid]['actual']}")
        return no_update
    except Exception as e:
        L.error(f"[[DEBUG C2 CACHE]] Parse error: {e}", exc_info=True)
        return no_update


@app.callback(
    Output("ws-ops-conditions", "send"),
    Input("c2-apply-btn", "n_clicks"),
    State("c2-operation-mode", "value"),
    State("c2-system-mode", "value"), 
    State("c2-sampling-modes-checklist", "value"),
    State("c2-sampling-modes-checklist", "options"), 
    State("c2-target-platform", "data"), 
    prevent_initial_call=True
)
def send_c2_command(n_clicks, op_mode, sys_mode, active_samp_modes, all_samp_options, target_platform):
    """Sends Manual Overrides down to the backend manager, strictly scoped to the Host Platform."""
    samp_mode_states = {}
    if all_samp_options:
        for opt in all_samp_options:
            val = opt["value"]
            samp_mode_states[val] = (val in (active_samp_modes or []))

    payload = {
        "type": "c2_command",
        "payload": {
            "target_platform": target_platform, 
            "operation_mode": op_mode,
            "system_mode": sys_mode, 
            "sampling_modes": samp_mode_states 
        }
    }
    L.info(f"[[DEBUG C2 COMMAND]] Dispatching payload to {target_platform}: {json.dumps(payload, indent=2)}")
    return json.dumps(payload)


@app.callback(
    Output("ops-health-badge", "children"), Output("ops-health-badge", "color"),
    Output("ops-sys-mode-disp", "children"), Output("ops-sys-mode-disp", "className"),
    Output("ops-samp-state", "children"), Output("ops-samp-state", "className"), 
    Output("ops-alarm-count", "children"),
    Input({"type": "platform-cache", "index": ALL}, "data")
)
def update_ribbon_ui(caches):
    total_alarms = 0
    sys_mode, sys_mode_color = "STANDBY", "fw-bold text-muted mb-0"
    samp_state, samp_state_color = "IDLE", "fw-bold text-muted mb-0"
    
    for data in caches:
        if not data: continue
        vars_dict = data.get("variables", {})
        for v_name, v_data in vars_dict.items():
            val_str = str(v_data.get("value", "")).strip().lower()
            if val_str in ["error", "alarm", "fault"]: total_alarms += 1
            if "power_state" in v_name or "system_active" in v_name:
                if val_str in ["1", "true", "active", "on"]:
                    sys_mode = "ACTIVE"
                    sys_mode_color = "fw-bold text-primary mb-0"
            if "sampling_state" in v_name:
                if val_str:
                    samp_state = val_str.upper()
                    if samp_state == "SAMPLING": samp_state_color = "fw-bold text-success mb-0"
                    elif samp_state in ["ERROR", "MAINTENANCE"]: samp_state_color = "fw-bold text-danger mb-0"
                    else: samp_state_color = "fw-bold text-warning mb-0"

    if total_alarms > 0: 
        return f"{total_alarms} Critical Alarms", "danger", sys_mode, sys_mode_color, samp_state, samp_state_color, str(total_alarms)
    return "Group Nominal", "success", sys_mode, sys_mode_color, samp_state, samp_state_color, str(total_alarms)


@app.callback(
    Output("tactical-metrics-container", "children"),
    Input({"type": "platform-cache", "index": ALL}, "data"),
    State({"type": "platform-cache", "index": ALL}, "id")
)
def update_tactical_quick_look(caches, cache_ids):
    # 🟢 THIS CALLBACK IS WHAT RENDERS ALL THE CARDS!
    flat_vars = {}
    for c_data, c_id in zip(caches, cache_ids):
        if not c_data: continue
        plat_id = c_id["index"]
        for v_name, v_data in c_data.get("variables", {}).items():
            flat_vars[v_name] = {**v_data, "platform": plat_id}
            
    if not flat_vars: 
        L.debug("[[DEBUG RENDER]] flat_vars empty, returning awaiting telemetry alert.")
        return dbc.Alert("Awaiting telemetry...", color="info")

    now = datetime.now(timezone.utc)
    STALE_SECONDS = 120

    def get_var_status(var_name, related_ids=None):
        v = flat_vars.get(var_name)
        if not v: 
            return None, "Waiting...", "border-0 shadow-sm mb-3 bg-white border-start border-4 border-secondary", False, "", []
        
        raw_val = v["value"]
        unit = v.get("unit", "")
        
        is_stale = False
        try:
            last_time = datetime.fromisoformat(str(v["timestamp"]).replace("Z", "+00:00"))
            if (now - last_time).total_seconds() > STALE_SECONDS: is_stale = True
        except Exception: pass

        css_class = "border-0 shadow-sm mb-3 bg-white border-start border-4 border-success"
        active_triggers = []

        if is_stale:
            css_class = "border-0 shadow-sm mb-3 bg-light border-start border-4 border-secondary opacity-75"
        else:
            if related_ids:
                if isinstance(related_ids, str): related_ids = [related_ids]
                for uid in related_ids:
                    if uid in CONDITIONS_CACHE:
                        L.debug(f"[[DEBUG RENDER]] Checking condition {uid} for {var_name}. Actual: {CONDITIONS_CACHE[uid]['actual']}")
                        if CONDITIONS_CACHE[uid]["actual"]:
                            active_triggers.append(uid)
                    else:
                        L.debug(f"[[DEBUG RENDER]] Condition {uid} not found in CONDITIONS_CACHE yet.")
                
                if active_triggers:
                    css_class = "border-0 shadow-sm mb-3 bg-soft-warning border-start border-4 border-warning"

        try: formatted_val = f"{float(raw_val):.2f}"
        except: formatted_val = str(raw_val)

        return raw_val, formatted_val, css_class, is_stale, unit, active_triggers

    # --- CARD COMPONENT BUILDERS ---
    def standard_card(title, var_name, related_ids=None):
        _, fmt_val, css, is_stale, unit, active_triggers = get_var_status(var_name, related_ids)
        val_display = f"{fmt_val} {unit}".strip() if fmt_val != "Waiting..." else fmt_val
        stale_badge = html.Span(" STALE", className="text-danger fw-bold ms-2") if is_stale else ""
        
        trigger_badges = [dbc.Badge(t, color="warning", className="ms-1 shadow-sm") for t in active_triggers]
        
        return dbc.Col(dbc.Card(dbc.CardBody([
            html.Div([
                html.Span([title, stale_badge], className="text-muted small fw-bold text-uppercase d-block text-truncate"),
                html.Div(trigger_badges, className="mt-1")
            ], className="mb-2"),
            html.H3(val_display, className="fw-bold mb-0 text-dark")
        ], className="p-3"), className=css), width=12, md=3)
        
    def wind_card(title, spd_var, dir_var, related_ids=None):
        _, spd_fmt, spd_css, spd_stale, spd_unit, active_triggers = get_var_status(spd_var, related_ids)
        _, dir_fmt, _, _, dir_unit, _ = get_var_status(dir_var)
        
        stale_badge = html.Span(" STALE", className="text-danger fw-bold ms-2") if spd_stale else ""
        if spd_fmt == "Waiting...": val_display = "Waiting..."
        else: val_display = html.Div([html.Span(f"{spd_fmt} {spd_unit}"), html.Span(f" @ {dir_fmt}{dir_unit}", className="text-secondary ms-2 fs-5")])
        
        trigger_badges = [dbc.Badge(t, color="warning", className="ms-1 shadow-sm") for t in active_triggers]

        return dbc.Col(dbc.Card(dbc.CardBody([
            html.Div([
                html.Span([title, stale_badge], className="text-muted small fw-bold text-uppercase d-block text-truncate"),
                html.Div(trigger_badges, className="mt-1")
            ], className="mb-2"),
            html.H3(val_display, className="fw-bold mb-0 text-dark")
        ], className="p-3"), className=spd_css), width=12, md=3)

    def flow_card(title, flow_var, sp_var, related_ids=None):
        _, flow_fmt, flow_css, is_stale, flow_unit, active_triggers = get_var_status(flow_var, related_ids)
        sp_raw, sp_fmt, _, _, _, _ = get_var_status(sp_var)
        
        stale_badge = html.Span(" STALE", className="text-danger fw-bold ms-2") if is_stale else ""
        if flow_fmt == "Waiting...": val_display = "Waiting..."
        else: 
            sp_display = f" (SP: {sp_fmt})" if sp_raw is not None else ""
            val_display = html.Div([html.Span(f"{flow_fmt} {flow_unit}"), html.Span(sp_display, className="text-secondary ms-2 fs-6")])

        trigger_badges = [dbc.Badge(t, color="warning", className="ms-1 shadow-sm") for t in active_triggers]

        return dbc.Col(dbc.Card(dbc.CardBody([
            html.Div([
                html.Span([title, stale_badge], className="text-muted small fw-bold text-uppercase d-block text-truncate"),
                html.Div(trigger_badges, className="mt-1")
            ], className="mb-2"),
            html.H3(val_display, className="fw-bold mb-0 text-dark")
        ], className="p-3"), className=flow_css), width=12, md=3)

    def optics_card(title, b_var, g_var, r_var, related_ids=None):
        b_raw, b_fmt, css, is_stale, b_unit, active_triggers = get_var_status(b_var, related_ids)
        g_raw, g_fmt, _, _, g_unit, _ = get_var_status(g_var)
        r_raw, r_fmt, _, _, r_unit, _ = get_var_status(r_var)
        
        stale_badge = html.Span(" STALE", className="text-danger fw-bold ms-2") if is_stale else ""
        if b_raw is None and g_raw is None and r_raw is None:
            content = html.H3("Waiting...", className="fw-bold mb-0 text-dark")
        else:
            content = html.Div([
                html.Div([html.I(className="bi bi-circle-fill text-primary me-2"), html.Span(f"Blue: {b_fmt} {b_unit}" if b_raw else "Blue: N/A", className="fw-bold fs-5")]),
                html.Div([html.I(className="bi bi-circle-fill text-success me-2"), html.Span(f"Green: {g_fmt} {g_unit}" if g_raw else "Green: N/A", className="fw-bold fs-5")]),
                html.Div([html.I(className="bi bi-circle-fill text-danger me-2"), html.Span(f"Red: {r_fmt} {r_unit}" if r_raw else "Red: N/A", className="fw-bold fs-5")])
            ])
            
        trigger_badges = [dbc.Badge(t, color="warning", className="ms-1 shadow-sm") for t in active_triggers]

        return dbc.Col(dbc.Card(dbc.CardBody([
            html.Div([
                html.Span([title, stale_badge], className="text-muted small fw-bold text-uppercase d-block text-truncate"),
                html.Div(trigger_badges, className="mt-1")
            ], className="mb-2"),
            content
        ], className="p-3"), className=css), width=12, md=4)

    # --- ASSEMBLE GROUPS (Tied directly to JSON configurations) ---
    group_nav = html.Div([
        html.H5([html.I(className="bi bi-compass me-2"), "Navigation"], className="fw-bold mb-3 text-secondary border-bottom pb-2"),
        dbc.Row([
            standard_card("Latitude", "latitude"),
            standard_card("Longitude", "longitude"),
            standard_card("Heading", "platform_heading"),
            standard_card("Speed", "platform_speed", related_ids=["at_pmel"]),
        ], className="mb-4")
    ])

    group_met = html.Div([
        html.H5([html.I(className="bi bi-cloud-sun me-2"), "Meteorology & Solar"], className="fw-bold mb-3 text-secondary border-bottom pb-2"),
        dbc.Row([
            wind_card("True Wind", "true_wind_speed", "true_wind_direction", related_ids=["sampling_wind_speed_limit", "sampling_wind_direction_limit", "in_sector"]),
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
            standard_card("CN Concentration", "cn_concentration", related_ids=["cn_limit", "nominal_concentrations"]),
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
        L.debug("[[DEBUG RENDER]] ✅ Layout dynamically assembled. Returning to browser.")
        return final_layout
    except Exception as e:
        L.error(f"[[DEBUG RENDER]] 💥 Layout assembly failed: {e}", exc_info=True)
        return dbc.Alert(f"UI Build Error: {e}", color="danger")