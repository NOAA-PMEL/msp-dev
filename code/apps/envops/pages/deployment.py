import dash
import json
import time
import logging
import httpx
from dash import html, dcc, callback, Input, Output, State, MATCH, ALL, ctx
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from dash_extensions import WebSocket
from pydantic import BaseSettings
from ulid import ULID

L = logging.getLogger(__name__)

dash.register_page(
    __name__,
    path_template="/deployment/<deployment_id>",
    title="Deployment Command & Control",
    nav_bar=False 
)

# --- CONFIG ---
class Settings(BaseSettings):
    daq_id: str = "default"
    ws_port: int = 80
    external_hostname: str = "localhost"
    class Config:
        env_prefix = "ENVOPS_"
        case_sensitive = False

config = Settings()
ws_url_base = f"ws://{config.external_hostname}:{config.ws_port}"
datastore_url = f"datastore.{config.daq_id}-system.svc.cluster.local"

# --- HELPER: REST FETCH ---
def fetch_registry_data(resource_type: str):
    url = f"http://{datastore_url}/{resource_type}-definition/registry/ids/get/"
    docs = []
    try:
        timeout = httpx.Timeout(10.0)
        id_response = httpx.get(url, timeout=timeout)
        if id_response.status_code == 200:
            ids = id_response.json().get("results", [])
            for doc_id in ids:
                if doc_id:
                    doc_url = f"http://{datastore_url}/{resource_type}-definition/registry/get/"
                    doc_response = httpx.get(doc_url, params={"name": doc_id}, timeout=timeout) 
                    if doc_response.status_code == 200:
                        doc_results = doc_response.json().get("results", [])
                        if doc_results: docs.append(doc_results[0])
    except Exception as e:
        L.error(f"Failed to fetch {resource_type} definitions: {e}")
    return docs

def get_deployment_bundle(host_id):
    deployments = fetch_registry_data("deployment")
    varsets = fetch_registry_data("variableset")
    
    host_dep = None
    subs = []
    platforms = set()
    
    for dep in deployments:
        if dep.get("metadata", {}).get("name") == host_id:
            host_dep = dep
            platforms.add(dep.get("data", {}).get("platform_ref"))
            break
            
    if host_dep:
        host_platform_ref = host_dep.get("data", {}).get("platform_ref")
        for dep in deployments:
            if dep.get("data", {}).get("host_platform_ref") == host_platform_ref:
                subs.append(dep)
                platforms.add(dep.get("data", {}).get("platform_ref"))
                
    required_varsets = set()
    for vs in varsets:
        attributes = vs.get("attributes", {})
        
        p_obj = attributes.get("platform")
        vs_platform = p_obj.get("data") if isinstance(p_obj, dict) else p_obj
        
        if vs_platform in platforms:
            vmap_obj = attributes.get("variablemap") or attributes.get("variablemap_id")
            vmap = vmap_obj.get("data") if isinstance(vmap_obj, dict) else vmap_obj
            
            vs_name = vs.get("variableset")
            
            if vs_name:
                routing_key = f"{vmap}::{vs_name}" if vmap else vs_name
                required_varsets.add(routing_key)

    return host_dep, subs, list(required_varsets)

# --- UI HELPERS ---
def make_kpi_col(label, id_str):
    return dbc.Col([
        html.Div(label, className="text-muted small fw-bold text-uppercase", style={"fontSize": "0.7rem"}),
        html.Div("--", id=id_str, className="fs-6 fw-semibold")
    ], width=6, className="mb-2")


# --- LAYOUT ---
def layout(deployment_id=None):
    if not deployment_id:
        return html.Div("No Deployment ID provided.", className="p-4 text-danger")

    host_dep, subs, varsets = get_deployment_bundle(deployment_id)
    
    display_name = host_dep.get("data", {}).get("display_name", deployment_id) if host_dep else deployment_id
    bundle_ids = [deployment_id] + [s.get("metadata", {}).get("name") for s in subs]

    # --- DYNAMIC WEBSOCKET GENERATION ---
    websockets = []
    
    # Status Sockets for the Host and ALL Subs
    for b_id in bundle_ids:
        websockets.append(WebSocket(
            id={"type": "ws-dep-status", "index": b_id}, 
            url=f"{ws_url_base}/envds/envops/ws/deployment/{b_id}/c2"
        ))
        
    # Telemetry Sockets for all discovered Variablesets
    for vs in varsets:
        websockets.append(WebSocket(
            id={"type": "ws-varset", "index": vs}, 
            url=f"{ws_url_base}/envds/envops/ws/variableset/{vs}"
        ))

    return html.Div([
        dbc.Row([
            dbc.Col([
                html.H2(f"C2: {display_name}", className="text-primary mb-0"),
                html.P(f"Host Deployment ID: {deployment_id}", className="text-muted small")
            ]),
            dbc.Col(html.Div(id="live-system-mode-badge", className="float-end mt-2"))
        ], className="mb-4 mt-3"),

        dbc.Row([
            # --- LEFT COLUMN: C2 & Bundled Health ---
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H5("Command & Control", className="mb-0")),
                    dbc.CardBody([
                        html.P("Set the overarching operational mode for this bundle.", className="text-muted small"),
                        dbc.ButtonGroup([
                            dbc.Button("AUTO", id="btn-mode-auto", color="success", outline=True, className="fw-bold"),
                            dbc.Button("MANUAL", id="btn-mode-manual", color="warning", outline=True, className="fw-bold"),
                        ], className="w-100 mb-3"),
                        html.Hr(),
                        dbc.Button("Trigger Calibration", id="btn-trigger-cal", color="secondary", size="sm", className="w-100 mb-2", disabled=True),
                        dbc.Button("Initiate Flow Check", id="btn-trigger-flow", color="secondary", size="sm", className="w-100", disabled=True),
                    ])
                ], className="shadow-sm mb-3 border-dark"),

                dbc.Card([
                    dbc.CardHeader(html.H5("Bundled Operations Health", className="mb-0")),
                    dbc.CardBody([
                        html.Div(id="ops-health-container", children=html.P("Waiting for status events...", className="text-muted text-center"))
                    ])
                ], className="shadow-sm mb-3 border-dark"),

                dbc.Card([
                    dbc.CardHeader(html.H5("Data & Telemetry Links", className="mb-0")),
                    dbc.CardBody([
                        dbc.ListGroup([
                            dbc.ListGroupItem(
                                "View Variableset Plots", 
                                # CHANGE THIS LINE
                                href=dash.get_relative_path(f"/variablesets/{deployment_id}"), 
                                action=True, color="info", className="fw-bold"
                            ),
                            dbc.ListGroupItem("View Raw Asset Telemetry", href=dash.get_relative_path("/assets"), action=True, className="fw-bold")
                        ])
                    ])
                ], className="shadow-sm border-dark")
            ], width=4),

            # --- RIGHT COLUMN: Expanded Quick Looks ---
            dbc.Col([
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader("Navigation", className="p-2 bg-light fw-bold"),
                            dbc.CardBody(dbc.Row([
                                make_kpi_col("Lat / Lon", "kpi-nav-latlon"), make_kpi_col("Speed / Hdg", "kpi-nav-spdhdg"), make_kpi_col("Pitch / Roll", "kpi-nav-pitchroll"),
                            ], className="g-2"), className="p-2")
                        ], className="mb-3 shadow-sm"),
                        
                        dbc.Card([
                            dbc.CardHeader("Aerosols", className="p-2 bg-light fw-bold"),
                            dbc.CardBody(dbc.Row([
                                make_kpi_col("CN", "kpi-aero-cn"), make_kpi_col("Scat (B/G/R)", "kpi-aero-scat"), make_kpi_col("Abs (B/G/R)", "kpi-aero-abs"),
                            ], className="g-2"), className="p-2")
                        ], className="mb-3 shadow-sm"),

                        dbc.Card([
                            dbc.CardHeader("Gas Phase", className="p-2 bg-light fw-bold"),
                            dbc.CardBody(dbc.Row([
                                make_kpi_col("O3", "kpi-gas-o3"), make_kpi_col("CO", "kpi-gas-co"), make_kpi_col("NO / NO2", "kpi-gas-nox"),
                            ], className="g-2"), className="p-2")
                        ], className="mb-3 shadow-sm")
                    ], width=6),
                    
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader("Meteorology", className="p-2 bg-light fw-bold"),
                            dbc.CardBody(dbc.Row([
                                make_kpi_col("True WS/WDIR", "kpi-met-wind"), make_kpi_col("Temp / RH", "kpi-met-temprh"), make_kpi_col("Pressure", "kpi-met-press"),
                                make_kpi_col("Rain Rate", "kpi-met-rain"), make_kpi_col("Irradiance", "kpi-met-irrad"),
                            ], className="g-2"), className="p-2")
                        ], className="mb-3 shadow-sm"),

                        dbc.Card([
                            dbc.CardHeader("Operational", className="p-2 bg-light fw-bold"),
                            dbc.CardBody(dbc.Row([
                                make_kpi_col("Rel WS/WDIR", "kpi-ops-relwind"), make_kpi_col("Inlet Flow", "kpi-ops-flow"), make_kpi_col("Inlet SP", "kpi-ops-flowsp"),
                            ], className="g-2"), className="p-2")
                        ], className="mb-3 shadow-sm")
                    ], width=6)
                ])
            ], width=8)
        ]),

        # --- DYNAMIC WEBSOCKETS & CENTRAL CACHES ---
        html.Div(websockets),
        WebSocket(id="ws-c2-sender", url=f"{ws_url_base}/envds/envops/ws/deployment/{deployment_id}/c2"),
        html.Div(id="ws-c2-send-buffer", style={"display": "none"}),
        
        dcc.Interval(id="kpi-staleness-interval", interval=5 * 1000, n_intervals=0),
        
        dcc.Store(id="store-deployment-id", data=deployment_id),
        dcc.Store(id="c2-health-store", data={}),
        dcc.Store(id="unified-telemetry-store", data={})
    ])

# --- CALLBACKS ---

# 1. SEND COMMANDS
@callback(
    Output("ws-c2-send-buffer", "children"),
    Input("btn-mode-auto", "n_clicks"),
    Input("btn-mode-manual", "n_clicks"),
    State("store-deployment-id", "data"),
    prevent_initial_call=True
)
def handle_c2_mode_switch(auto_clicks, manual_clicks, deployment_id):
    if not ctx.triggered: raise PreventUpdate
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    req_mode = "auto" if button_id == "btn-mode-auto" else "manual"
    event = {
        "type": "envds.control.request", "source": f"envds.{config.daq_id}.dashboard",
        "id": str(ULID()), "datacontenttype": "application/json",
        "data": {"system_mode": {"requested": req_mode}},
        "destpath": f"envds/{config.daq_id}/system/control/request", "deploymentref": deployment_id
    }
    return json.dumps(event)

@callback(Output("ws-c2-sender", "send"), Input("ws-c2-send-buffer", "children"))
def send_c2_request(payload):
    if payload: return payload
    raise PreventUpdate

# 2. AGGREGATE OPERATIONS HEALTH
@callback(
    Output("c2-health-store", "data"),
    Input({"type": "ws-dep-status", "index": ALL}, "message"),
    State("c2-health-store", "data"),
    prevent_initial_call=True
)
def aggregate_health(messages, current_store):
    if current_store is None: current_store = {}
    updated = False
    
    # Process only the websockets that actually fired this tick
    for t in ctx.triggered:
        if not t["value"] or "data" not in t["value"]: continue
        try:
            status_data = json.loads(t["value"]["data"])
            app_uid = status_data.get("id", {}).get("app_uid", "")
            if app_uid:
                current_store[app_uid] = status_data
                updated = True
        except Exception as e:
            L.error(f"Health Parse Error: {e}")
            
    if not updated: raise PreventUpdate
    return current_store

# 3. RENDER OPERATIONS HEALTH
@callback(
    Output("live-system-mode-badge", "children"),
    Output("ops-health-container", "children"),
    Input("c2-health-store", "data"),
    State("store-deployment-id", "data"),
    prevent_initial_call=True
)
# 3. RENDER OPERATIONS HEALTH & UPDATE C2 BUTTON STATES
@callback(
    Output("ops-health-container", "children"),
    Output("btn-mode-auto", "outline"),
    Output("btn-mode-manual", "outline"),
    Output("c2-manual-container", "style"),
    Input("c2-health-store", "data"),
    State("store-deployment-id", "data"),
    prevent_initial_call=True
)
def render_bundle_health(health_store, host_id):
    if not health_store: raise PreventUpdate
        
    def get_badge(val):
        # Extract actual if it's a dict
        if isinstance(val, dict):
            val = val.get("actual", val.get("requested", "UNKNOWN"))

        v_str = str(val).lower()
        if v_str in ["auto", "normal", "nominal_sampling", "nominal"]: 
            color = "success"
        elif v_str in ["manual", "startup", "system_startup", "standby"]: 
            color = "warning"
        elif v_str in ["true", "active"]:
            color = "success"
        elif v_str in ["false", "inactive"]:
            color = "secondary"
        else: 
            color = "primary"
            
        display_text = str(val).upper().replace("_", " ")
        return dbc.Badge(display_text, color=color, className="ms-2")

    def render_active_only(data_dict):
        """Filters a dict to ONLY show keys where actual=true/active."""
        if not data_dict or not isinstance(data_dict, dict): 
            return html.Div(html.Span("None currently active.", className="text-muted small ms-3"))
        
        items = []
        for k, v in data_dict.items():
            actual_val = str(v.get("actual", v) if isinstance(v, dict) else v).lower()
            
            # Only render if it's active! (Removes clutter of false states)
            if actual_val in ["true", "active", "1", "yes"]:
                items.append(html.Li([
                    html.Span("● ", className="text-success"),
                    html.Span(k.replace("_", " ").title(), className="font-monospace text-dark fw-bold")
                ], className="mb-1"))
                
        if not items:
            return html.Div(html.Span("None currently active.", className="text-muted small ms-3"))
            
        return html.Ul(items, className="list-unstyled ms-3 mb-0")

    # --- DETERMINE HOST C2 STATE FOR BUTTONS ---
    host_state = health_store.get(host_id, {}).get("state", {})
    
    # System Mode is usually a string or dict {"actual": "..."}
    raw_host_mode = host_state.get("system_mode", "unknown")
    if isinstance(raw_host_mode, dict):
        actual_host_mode = str(raw_host_mode.get("actual", "unknown")).lower()
    else:
        actual_host_mode = str(raw_host_mode).lower()
        
    is_auto = actual_host_mode in ["auto", "normal"]
    auto_outline = not is_auto
    manual_outline = is_auto
    manual_style = {"display": "none"} if is_auto else {"display": "block"}
    
    # --- BUILD HEALTH ACCORDIONS ---
    accordions = []
    for dep_id, s_data in health_store.items():
        state_dict = s_data.get("state", {})
        
        # 1. System Mode
        raw_sys_mode = state_dict.get("system_mode", "UNKNOWN")
        sys_mode_ui = html.Div([
            html.Span("System Mode:", className="fw-bold me-2"),
            get_badge(raw_sys_mode)
        ], className="mb-3")
        
        # 2. Sampling Modes (Filtered to active)
        sm_ui = html.Div([
            html.Div("Active Sampling Modes", className="fw-bold text-info border-bottom mb-1"),
            render_active_only(state_dict.get("sampling_mode", {}))
        ], className="mb-3")
        
        # 3. Sampling States (Filtered to active)
        ss_ui = html.Div([
            html.Div("Active Sampling States", className="fw-bold text-success border-bottom mb-1"),
            render_active_only(state_dict.get("sampling_state", {}))
        ], className="mb-2")
        
        content = html.Div([sys_mode_ui, sm_ui, ss_ui], style={"fontSize": "0.85rem"})
        
        dep_name = dep_id.split('.')[-1]
        actual_sys = str(raw_sys_mode.get("actual", raw_sys_mode) if isinstance(raw_sys_mode, dict) else raw_sys_mode).lower()
        title_color = "text-success" if actual_sys in ["auto", "normal"] else "text-warning"
        
        title = html.Span([f"{dep_name} ", html.Span("●", className=title_color)])
        accordions.append(dbc.AccordionItem(content, title=title))
        
    accordion_ui = dbc.Accordion(accordions, start_collapsed=False, flush=True)
    
    return accordion_ui, auto_outline, manual_outline, manual_style

# 4. AGGREGATE TELEMETRY
@callback(
    Output("unified-telemetry-store", "data"),
    Input({"type": "ws-varset", "index": ALL}, "message"),
    State("unified-telemetry-store", "data"),
    prevent_initial_call=True
)
def aggregate_telemetry(messages, current_store):
    if current_store is None: current_store = {}
    updated = False
    now = time.time()
    
    for t in ctx.triggered:
        if not t["value"] or "data" not in t["value"]: continue
        try:
            payload = json.loads(t["value"]["data"])
            variables = payload.get("variables", {})
            for var_name, v_data in variables.items():
                if var_name == "time": continue
                current_store[var_name] = {"val": v_data.get("data"), "ts": now}
                updated = True
        except Exception as e:
            L.error(f"Telemetry Parse Error: {e}")
            
    if not updated: raise PreventUpdate
    return current_store

# 5. RENDER TELEMETRY
@callback(
    Output("kpi-nav-latlon", "children"), Output("kpi-nav-spdhdg", "children"), Output("kpi-nav-pitchroll", "children"),
    Output("kpi-met-wind", "children"), Output("kpi-met-temprh", "children"), Output("kpi-met-press", "children"), Output("kpi-met-rain", "children"), Output("kpi-met-irrad", "children"),
    Output("kpi-aero-cn", "children"), Output("kpi-aero-scat", "children"), Output("kpi-aero-abs", "children"),
    Output("kpi-gas-o3", "children"), Output("kpi-gas-co", "children"), Output("kpi-gas-nox", "children"),
    Output("kpi-ops-relwind", "children"), Output("kpi-ops-flow", "children"), Output("kpi-ops-flowsp", "children"),
    Input("unified-telemetry-store", "data"),
    Input("kpi-staleness-interval", "n_intervals"), 
    prevent_initial_call=True
)
def update_quick_looks(telemetry_store, n_intervals):
    if not telemetry_store: raise PreventUpdate
    now = time.time()

    def get_val(keys):
        """Checks list of synonyms. Returns formatted span if found, else '--'."""
        for k in keys:
            if k in telemetry_store:
                val = telemetry_store[k]["val"]
                ts = telemetry_store[k]["ts"]
                fmt_val = f"{val:.2f}" if isinstance(val, float) else str(val)
                # Stale check (120 seconds)
                if now - ts > 120:
                    return html.Span(fmt_val, className="text-danger fw-bold", title=f"Stale: {(now-ts)/60:.1f}m ago")
                return fmt_val
        return "--"

    return (
        [get_val(["latitude", "lat"]), " / ", get_val(["longitude", "lon"])],
        [get_val(["sog", "speed"]), " / ", get_val(["cog", "heading"])],
        [get_val(["pitch"]), " / ", get_val(["roll"])],
        [get_val(["true_wind_speed", "tws"]), " / ", get_val(["true_wind_dir", "twdir"])],
        [get_val(["temperature", "air_temp"]), " / ", get_val(["rh", "relative_humidity"])],
        get_val(["pressure", "baro"]),
        get_val(["rain_rate", "precip"]),
        get_val(["irradiance", "solar"]),
        get_val(["cn_concentration", "cn"]),
        [get_val(["scatter_blue", "scat_blue"]), " / ", get_val(["scatter_green", "scat_green"]), " / ", get_val(["scatter_red", "scat_red"])],
        [get_val(["absorption_blue", "abs_blue"]), " / ", get_val(["absorption_green", "abs_green"]), " / ", get_val(["absorption_red", "abs_red"])],
        get_val(["o3", "ozone"]),
        get_val(["co", "carbon_monoxide"]),
        [get_val(["no", "nitric_oxide"]), " / ", get_val(["no2", "nitrogen_dioxide"])],
        [get_val(["relative_wind_speed", "rel_wind_speed", "rws"]), " / ", get_val(["relative_wind_dir", "rel_wind_dir", "rwdir"])],
        get_val(["inlet_flow", "flow"]),
        get_val(["inlet_flow_sp", "flow_setpoint"])
    )