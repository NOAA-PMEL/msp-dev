import dash
import json
import time
import logging
import httpx
from dash import html, dcc, callback, Input, Output, State, MATCH, ALL, ctx, Patch
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
                
    url = f"http://{datastore_url}/variableset-definition/registry/ids/get/"
    try:
        timeout = httpx.Timeout(10.0)
        response = httpx.get(url, timeout=timeout)
        all_vs_ids = response.json().get("results", []) if response.status_code == 200 else []
    except Exception as e:
        L.error(f"Failed to fetch variableset IDs: {e}")
        all_vs_ids = []

    required_varsets = set()
    for full_id in all_vs_ids:
        if not full_id: continue
        parts = full_id.split("::")
        if len(parts) >= 4:
            vs_platform = parts[0]
            if vs_platform in platforms:
                routing_key = f"{parts[1]}::{parts[3]}"
                required_varsets.add(routing_key)

    return host_dep, subs, list(required_varsets)

def make_kpi_col(title, id_val, icon=None):
    icon_el = html.I(className=f"{icon} me-1 opacity-75") if icon else ""
    return dbc.Col([
        html.Div([
            html.Div(
                [icon_el, title], 
                className="text-muted fw-bold text-uppercase mb-1 text-nowrap", 
                style={"fontSize": "0.65rem", "letterSpacing": "0.5px"}
            ),
            html.Div(
                id=id_val, 
                # Flex added here so side-by-side binary badges space out nicely
                className="fw-bold text-dark text-nowrap d-flex align-items-center justify-content-center gap-2", 
                style={"fontSize": "1.1rem"} 
            )
        ], className="bg-white border rounded shadow-sm p-2 h-100 d-flex flex-column justify-content-center align-items-center")
    ], style={"flex": "1 1 auto", "minWidth": "140px"})

def layout(deployment_id=None):
    if not deployment_id:
        return html.Div("No Deployment ID provided.", className="p-4 text-danger")

    host_dep, subs, varsets = get_deployment_bundle(deployment_id)
    display_name = host_dep.get("data", {}).get("display_name", deployment_id) if host_dep else deployment_id
    
    systemmodes = fetch_registry_data("systemmode")
    actions = fetch_registry_data("action")
    
    sm_options = [{"label": sm.get("metadata", {}).get("name", "Unknown").upper(), "value": sm.get("metadata", {}).get("name", "Unknown")} for sm in systemmodes if sm.get("metadata", {}).get("name")]
    act_options = [{"label": act.get("metadata", {}).get("name", "Unknown").replace("_", " ").title(), "value": act.get("metadata", {}).get("name", "Unknown")} for act in actions if act.get("metadata", {}).get("name")]

    websockets = [WebSocket(id={"type": "ws-varset", "index": vs}, url=f"{ws_url_base}/envds/envops/ws/variableset/{vs}") for vs in varsets]

    return html.Div([
        dbc.Row([
            dbc.Col([
                html.H2(f"C2: {display_name}", className="text-primary mb-0"),
                html.P(f"Host Deployment ID: {deployment_id}", className="text-muted small")
            ])
        ], className="mb-4 mt-3"),

        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H5("Command & Control", className="mb-0")),
                    dbc.CardBody([
                        html.P("Set the overarching operational mode for this bundle.", className="text-muted small mb-2"),
                        dbc.ButtonGroup([
                            dbc.Button("AUTO", id="btn-mode-auto", color="success", outline=True, className="fw-bold w-50"),
                            dbc.Button("MANUAL", id="btn-mode-manual", color="warning", outline=True, className="fw-bold w-50"),
                        ], className="w-100 mb-3"),
                        
                        html.Div([
                            html.P("Manual Mode Override:", className="text-muted small mb-1"),
                            dbc.InputGroup([
                                dbc.Select(id="c2-mode-select", options=sm_options, placeholder="Select Mode..."),
                                dbc.Button("Apply", id="btn-apply-mode", color="primary", className="fw-bold")
                            ])
                        ], id="c2-manual-container", style={"display": "none"}), 
                        
                        html.Hr(),
                        html.P("Trigger System Action:", className="text-muted small mb-1"),
                        dbc.InputGroup([
                            dbc.Select(id="c2-action-select", options=act_options, placeholder="Select Action..."),
                            dbc.Button("Execute", id="btn-execute-action", color="danger", className="fw-bold")
                        ])
                    ])
                ], className="shadow-sm mb-3 border-dark"),

                dbc.Card([
                    dbc.CardHeader(html.H5("Bundled Operations Health", className="mb-0")),
                    dbc.CardBody(id="ops-health-container", className="p-2 bg-light")
                ], className="shadow-sm mb-3 border-dark"),

                dbc.Card([
                    dbc.CardHeader(html.H5("Data & Telemetry Links", className="mb-0")),
                    dbc.CardBody([
                        dbc.ListGroup([
                            dbc.ListGroupItem("View Variableset Plots", href=dash.get_relative_path(f"/variablesets/{deployment_id}"), action=True, color="info", className="fw-bold"),
                            dbc.ListGroupItem("View Raw Asset Telemetry", href=dash.get_relative_path("/assets"), action=True, className="fw-bold")
                        ])
                    ])
                ], className="shadow-sm border-dark")
            ], width=5),

            dbc.Col([
                dbc.Row([
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader(html.H6(html.B("Navigation"), className="mb-0 text-primary"), className="p-2 bg-white border-bottom-0"),
                            dbc.CardBody(dbc.Row([
                                make_kpi_col("Lat / Lon", "kpi-nav-latlon", "bi bi-geo-alt"), 
                                make_kpi_col("Speed / Hdg", "kpi-nav-spdhdg", "bi bi-compass"), 
                                make_kpi_col("Pitch / Roll", "kpi-nav-pitchroll", "bi bi-arrows-move")
                            ], className="g-2"), className="p-2 bg-light")
                        ], className="mb-3 shadow-sm border-0"),
                        
                        dbc.Card([
                            dbc.CardHeader(html.H6(html.B("Aerosol Optics"), className="mb-0 text-primary"), className="p-2 bg-white border-bottom-0"),
                            dbc.CardBody(dbc.Row([
                                make_kpi_col("CN", "kpi-aero-cn", "bi bi-moisture"), 
                                make_kpi_col("Scat (B/G/R)", "kpi-aero-scat", "bi bi-activity"), 
                                make_kpi_col("Abs (B/G/R)", "kpi-aero-abs", "bi bi-bullseye")
                            ], className="g-2"), className="p-2 bg-light")
                        ], className="mb-3 shadow-sm border-0"),

                        dbc.Card([
                            dbc.CardHeader(html.H6(html.B("Gas Phase"), className="mb-0 text-primary"), className="p-2 bg-white border-bottom-0"),
                            dbc.CardBody(dbc.Row([
                                make_kpi_col("O3", "kpi-gas-o3", "bi bi-cloud-haze"), 
                                make_kpi_col("CO", "kpi-gas-co", "bi bi-cloud-slash"), 
                                make_kpi_col("NO / NO2", "kpi-gas-nox", "bi bi-clouds")
                            ], className="g-2"), className="p-2 bg-light")
                        ], className="mb-3 shadow-sm border-0")
                    ], width=6),
                    
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader(html.H6(html.B("Meteorology"), className="mb-0 text-primary"), className="p-2 bg-white border-bottom-0"),
                            dbc.CardBody(dbc.Row([
                                make_kpi_col("Wind (True)", "kpi-met-wind", "bi bi-wind"), 
                                make_kpi_col("Temp / RH", "kpi-met-temprh", "bi bi-thermometer-half"), 
                                make_kpi_col("Pressure", "kpi-met-press", "bi bi-speedometer2"), 
                                make_kpi_col("Rain Rate", "kpi-met-rain", "bi bi-cloud-rain"), 
                                make_kpi_col("Irradiance", "kpi-met-irrad", "bi bi-brightness-high")
                            ], className="g-2"), className="p-2 bg-light")
                        ], className="mb-3 shadow-sm border-0"),

                        dbc.Card([
                            dbc.CardHeader(html.H6(html.B("Operational"), className="mb-0 text-primary"), className="p-2 bg-white border-bottom-0"),
                            dbc.CardBody(dbc.Row([
                                make_kpi_col("Wind (Rel)", "kpi-ops-relwind", "bi bi-flag"), 
                                make_kpi_col("Inlet Flow", "kpi-ops-flow", "bi bi-fan"), 
                                make_kpi_col("Inlet SP", "kpi-ops-flowsp", "bi bi-sliders")
                            ], className="g-2"), className="p-2 bg-light")
                        ], className="mb-3 shadow-sm border-0"),

                        dbc.Card([
                            dbc.CardHeader(html.H6(html.B("Power Settings"), className="mb-0 text-primary"), className="p-2 bg-white border-bottom-0"),
                            dbc.CardBody(dbc.Row([
                                make_kpi_col("OPC / SMPS", "kpi-power-opcsmps", "bi bi-lightning"), 
                                make_kpi_col("CPC / APS", "kpi-power-cpcaps", "bi bi-lightning-charge")
                            ], className="g-2"), className="p-2 bg-light")
                        ], className="mb-3 shadow-sm border-0")
                    ], width=6)
                ])
            ], width=7)
        ]),

        html.Div(websockets),
        WebSocket(id="ws-system-ops", url=f"{ws_url_base}/envds/envops/ws/deployment/{deployment_id}/c2"),
        html.Div(id="ws-c2-send-buffer", style={"display": "none"}),
        
        dcc.Interval(id="kpi-staleness-interval", interval=1000, n_intervals=0),
        dcc.Store(id="store-deployment-id", data=deployment_id),
        dcc.Store(id="c2-health-store", data={}),
        dcc.Store(id="unified-telemetry-store", data={})
    ])

@callback(
    Output("ws-c2-send-buffer", "children"),
    Input("btn-mode-auto", "n_clicks"),
    Input("btn-mode-manual", "n_clicks"),
    Input("btn-apply-mode", "n_clicks"),
    Input("btn-execute-action", "n_clicks"),
    State("c2-mode-select", "value"),
    State("c2-action-select", "value"),
    State("store-deployment-id", "data"),
    prevent_initial_call=True
)
def handle_c2_commands(auto_clicks, manual_clicks, apply_clicks, exec_clicks, mode_val, action_val, deployment_id):
    if not ctx.triggered: raise PreventUpdate
    trigger_id = ctx.triggered[0]["prop_id"].split(".")[0]
    
    event = {
        "type": "envds.control.request", "source": f"envds.{config.daq_id}.dashboard",
        "id": str(ULID()), "datacontenttype": "application/json",
        "destpath": f"envds/{config.daq_id}/system/control/request", "deploymentref": deployment_id,
        "data": {}
    }

    if trigger_id == "btn-mode-auto": event["data"] = {"system_mode": {"requested": "auto"}}
    elif trigger_id == "btn-mode-manual": event["data"] = {"system_mode": {"requested": "manual"}}
    elif trigger_id == "btn-apply-mode" and mode_val: event["data"] = {"system_mode": {"requested": mode_val}}
    elif trigger_id == "btn-execute-action" and action_val: event["data"] = {"action": {"requested": action_val}}
    else: raise PreventUpdate

    return json.dumps(event)

@callback(Output("ws-system-ops", "send"), Input("ws-c2-send-buffer", "children"))
def send_c2_request(payload):
    if payload: return payload
    raise PreventUpdate

@callback(
    Output("c2-health-store", "data"),
    Input("ws-system-ops", "message"),
    State("c2-health-store", "data"),
    prevent_initial_call=True
)
def aggregate_health(message, current_store):
    if current_store is None: current_store = {}
    if not message or "data" not in message: raise PreventUpdate
    
    try:
        payload = json.loads(message["data"])
        status_data = payload.get("data", {})
        dep_ref = payload.get("deploymentref", "unknown")
        app_uid = status_data.get("id", {}).get("app_uid", "")
        
        if dep_ref and app_uid:
            if dep_ref not in current_store: current_store[dep_ref] = {}
            current_store[dep_ref][app_uid] = status_data
            return current_store
    except Exception as e:
        L.error(f"Health Parse Error: {e}")
            
    raise PreventUpdate

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
    if not health_store: return html.P("Waiting for telemetry...", className="text-muted text-center m-3"), True, False, {"display": "none"}

    host_sys_mode = "unknown"
    node_cards = []

    sorted_deps = sorted(health_store.keys(), key=lambda x: 0 if x == host_id else 1)

    for dep_ref in sorted_deps:
        statuses = health_store[dep_ref]
        sys_modes, samp_modes, samp_states = [], [], []

        for uid, status in statuses.items():
            app_group = status.get("id", {}).get("app_group", "")
            state_block = status.get("state", {})
            
            is_active = False
            for k, v in state_block.items():
                actual = str(v.get("actual", "") if isinstance(v, dict) else v).lower()
                if actual in ["true", "active", "1", "yes"]:
                    is_active = True
                    break
            
            if is_active:
                clean_name = uid.replace("_", " ").title()
                if app_group == "system": sys_modes.append(clean_name)
                elif app_group == "mode": samp_modes.append(clean_name)
                elif app_group == "state": samp_states.append(clean_name)

        if dep_ref == host_id and sys_modes:
            host_sys_mode = sys_modes[0]

        def build_badge_group(items, color):
            if not items: return html.Span("None", className="text-muted small fst-italic")
            return html.Div([dbc.Badge(m, color=color, className="me-1 mb-1") for m in items], className="d-flex flex-wrap")

        is_host = (dep_ref == host_id)
        card_header_color = "bg-primary text-white" if is_host else "bg-secondary text-white"
        node_label = "HOST NODE" if is_host else "SUB-NODE"

        node_card = dbc.Card([
            dbc.CardHeader([
                html.Span(node_label, className="small fw-bold me-2"),
                html.Span(f"| {dep_ref}", className="small font-monospace")
            ], className=f"p-1 px-2 {card_header_color}"),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col(html.Span("Sys Mode:", className="small fw-bold text-muted"), width=4),
                    dbc.Col(build_badge_group(sys_modes, "dark"), width=8)
                ], className="mb-2 border-bottom pb-1"),
                dbc.Row([
                    dbc.Col(html.Span("Active Logic:", className="small fw-bold text-muted"), width=4),
                    dbc.Col(build_badge_group(samp_modes, "info"), width=8)
                ], className="mb-2 border-bottom pb-1"),
                dbc.Row([
                    dbc.Col(html.Span("Stabilized:", className="small fw-bold text-muted"), width=4),
                    dbc.Col(build_badge_group(samp_states, "success"), width=8)
                ])
            ], className="p-2")
        ], className="mb-2 shadow-sm border-0")
        
        node_cards.append(node_card)

    is_auto = host_sys_mode.lower() in ["auto", "normal", "nominal", "nominal sampling"]
    auto_outline = not is_auto
    manual_outline = is_auto
    manual_style = {"display": "none"} if is_auto else {"display": "block"}
    
    return html.Div(node_cards), auto_outline, manual_outline, manual_style

# --- 1. THE DATA PIPELINE: Parses WebSockets 100% in Browser Memory ---
dash.clientside_callback(
    """
    function(messages, current_store) {
        if (!dash_clientside.callback_context.triggered) {
            return window.dash_clientside.no_update;
        }
        
        let store = current_store ? Object.assign({}, current_store) : {};
        let updated = false;
        let now = Date.now() / 1000.0;
        
        let triggered = dash_clientside.callback_context.triggered;
        for (let i = 0; i < triggered.length; i++) {
            let t = triggered[i];
            if (!t.value || !t.value.data) continue;
            try {
                let payload = JSON.parse(t.value.data);
                let variables = payload.variables || {};
                for (let key in variables) {
                    if (key === "time") continue;
                    
                    let varObj = variables[key];
                    let val = varObj.data;
                    
                    // --- NESTED DICT FIX FOR CONTROLLERS ---
                    if (val !== null && typeof val === 'object') {
                        if (val.data && typeof val.data === 'object' && val.data.actual !== undefined) {
                            val = val.data.actual;
                        } else if (val.actual !== undefined) {
                            val = val.actual;
                        }
                    }
                    // ---------------------------------------

                    // --- DYNAMIC UNIT EXTRACTION ---
                    let unit = "";
                    if (varObj.attributes && varObj.attributes.units && varObj.attributes.units.data) {
                        unit = varObj.attributes.units.data;
                    }
                    // -------------------------------

                    store[key.toLowerCase()] = {
                        val: val,
                        unit: unit,
                        ts: now
                    };
                    updated = true;
                }
            } catch(e) {
                console.error("Telemetry Parse Error:", e);
            }
        }
        
        if (!updated) {
            return window.dash_clientside.no_update;
        }
        return store;
    }
    """,
    Output("unified-telemetry-store", "data"),
    Input({"type": "ws-varset", "index": ALL}, "message"),
    State("unified-telemetry-store", "data"),
    prevent_initial_call=True
)

# --- 2. THE RENDER PIPELINE: Evaluates exactly once per second ---
@callback(
    Output("kpi-nav-latlon", "children"), Output("kpi-nav-spdhdg", "children"), Output("kpi-nav-pitchroll", "children"),
    Output("kpi-met-wind", "children"), Output("kpi-met-temprh", "children"), Output("kpi-met-press", "children"), Output("kpi-met-rain", "children"), Output("kpi-met-irrad", "children"),
    Output("kpi-aero-cn", "children"), Output("kpi-aero-scat", "children"), Output("kpi-aero-abs", "children"),
    Output("kpi-gas-o3", "children"), Output("kpi-gas-co", "children"), Output("kpi-gas-nox", "children"),
    Output("kpi-ops-relwind", "children"), Output("kpi-ops-flow", "children"), Output("kpi-ops-flowsp", "children"),
    Output("kpi-power-opcsmps", "children"), Output("kpi-power-cpcaps", "children"),
    
    Input("kpi-staleness-interval", "n_intervals"),  
    State("unified-telemetry-store", "data"),        
    prevent_initial_call=True
)
def update_quick_looks(n_intervals, telemetry_store):
    if not telemetry_store or not isinstance(telemetry_store, dict): 
        raise PreventUpdate
        
    now = time.time()

    def get_val(keys, is_binary=False):
        for k in keys:
            if k in telemetry_store:
                item = telemetry_store.get(k, {})
                val = item.get("val")
                raw_unit = item.get("unit", "")
                ts = item.get("ts", now) 
                
                if val is None:
                    continue
                
                # --- Binary State Badges ---
                if is_binary:
                    if int(val) > 0:
                        badge = html.Span("ON", className="badge bg-success px-2 py-1 shadow-sm")
                    else:
                        badge = html.Span("OFF", className="badge bg-secondary px-2 py-1 shadow-sm opacity-75")
                    
                    if now - ts > 120:
                        return html.Span([badge], className="border border-danger rounded", title="Stale Data")
                    return badge
                # ---------------------------
                
                if isinstance(val, list):
                    val = val[-1] if len(val) > 0 else None
                    if val is None: continue

                unit_str = ""
                if raw_unit:
                    ru = str(raw_unit).strip()
                    if ru in ["degrees", "degrees_north", "degrees_east"]: unit_str = "°"
                    elif ru == "degrees_C": unit_str = " °C"
                    elif ru == "%": unit_str = "%"
                    elif ru in ["1", "count"]: unit_str = "" 
                    elif ru == "ug/m3": unit_str = " µg/m³"
                    elif ru == "Mm-1": unit_str = " Mm⁻¹"
                    elif ru == "cm-3": unit_str = " cm⁻³"
                    else: unit_str = f" {ru}"
                    
                if isinstance(val, float):
                    fmt_val = f"{val:.2f}{unit_str}"
                else:
                    fmt_val = f"{val}{unit_str}"
                
                if now - ts > 120:
                    return html.Span(fmt_val, className="text-danger fw-bold", title=f"Stale: {(now-ts)/60:.1f}m ago")
                return fmt_val
                
        return "--"

    return (
        [get_val(["latitude", "lat"]), " / ", get_val(["longitude", "lon"])],
        [get_val(["platform_speed", "sog", "speed"]), " / ", get_val(["platform_heading", "cog", "heading"])],
        [get_val(["platform_pitch", "pitch"]), " / ", get_val(["platform_roll", "roll"])],
        [get_val(["true_wind_speed", "tws"]), " / ", get_val(["true_wind_direction", "true_wind_dir", "twdir"])],
        [get_val(["air_temperature", "temperature", "air_temp"]), " / ", get_val(["relative_humidity", "rh"])],
        get_val(["pressure", "baro"]),
        get_val(["rain_intensity", "rain_rate", "precip"]),
        get_val(["irradiance", "solar"]),
        get_val(["cn_concentration", "cn"]),
        [get_val(["scatter_blue", "scat_blue"]), " / ", get_val(["scatter_green", "scat_green"]), " / ", get_val(["scatter_red", "scat_red"])],
        [get_val(["absorption_blue", "abs_blue"]), " / ", get_val(["absorption_green", "abs_green"]), " / ", get_val(["absorption_red", "abs_red"])],
        get_val(["o3", "ozone"]),
        get_val(["co", "carbon_monoxide"]),
        [get_val(["no", "nitric_oxide"]), " / ", get_val(["no2", "nitrogen_dioxide"])],
        [get_val(["relative_wind_speed", "rel_wind_speed", "rws"]), " / ", get_val(["relative_wind_direction", "rel_wind_dir", "rwdir"])],
        get_val(["inlet_flow", "flow"]),
        get_val(["inlet_flow_sp", "flow_setpoint"]),
        [get_val(["power_opc"], is_binary=True), get_val(["power_smps"], is_binary=True)],
        [get_val(["power_cpc"], is_binary=True), get_val(["power_aps"], is_binary=True)]
    )