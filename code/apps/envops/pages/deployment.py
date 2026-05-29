import dash
import json
import time
import logging
import httpx
from dash import html, dcc, callback, Input, Output, State, ctx
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
    """Finds the Host deployment and any Sub-deployments hitched to its platform."""
    deployments = fetch_registry_data("deployment")
    
    host_dep = None
    subs = []
    
    for dep in deployments:
        if dep.get("metadata", {}).get("name") == host_id:
            host_dep = dep
            break
            
    if not host_dep:
        return None, []
        
    host_platform_ref = host_dep.get("data", {}).get("platform_ref")
    
    for dep in deployments:
        if dep.get("data", {}).get("host_platform_ref") == host_platform_ref:
            subs.append(dep)
            
    return host_dep, subs


# --- UI HELPERS ---
def make_kpi_col(label, id_str):
    """Helper to generate dense metric blocks."""
    return dbc.Col([
        html.Div(label, className="text-muted small fw-bold text-uppercase", style={"fontSize": "0.7rem"}),
        html.Div("--", id=id_str, className="fs-6 fw-semibold")
    ], width=6, className="mb-2")


# --- LAYOUT ---
def layout(deployment_id=None):
    if not deployment_id:
        return html.Div("No Deployment ID provided.", className="p-4 text-danger")

    host_dep, subs = get_deployment_bundle(deployment_id)
    
    if not host_dep:
        display_name = deployment_id
        bundle_ids = [deployment_id]
    else:
        display_name = host_dep.get("data", {}).get("display_name", deployment_id)
        bundle_ids = [deployment_id] + [s.get("metadata", {}).get("name") for s in subs]

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
                # Command & Control Panel
                dbc.Card([
                    dbc.CardHeader(html.H5("Command & Control", className="mb-0")),
                    dbc.CardBody([
                        html.P("Set the overarching operational mode for this bundle. This command will be processed by the Host and cascaded to subsystems.", className="text-muted small"),
                        dbc.ButtonGroup([
                            dbc.Button("AUTO", id="btn-mode-auto", color="success", outline=True, className="fw-bold"),
                            dbc.Button("MANUAL", id="btn-mode-manual", color="warning", outline=True, className="fw-bold"),
                        ], className="w-100 mb-3"),
                        html.Hr(),
                        html.P("Manual Overrides", className="text-muted small"),
                        dbc.Button("Trigger Calibration", id="btn-trigger-cal", color="secondary", size="sm", className="w-100 mb-2", disabled=True),
                        dbc.Button("Initiate Flow Check", id="btn-trigger-flow", color="secondary", size="sm", className="w-100", disabled=True),
                    ])
                ], className="shadow-sm mb-3 border-dark"),

                # Operations Health
                dbc.Card([
                    dbc.CardHeader(html.H5("Bundled Operations Health", className="mb-0")),
                    dbc.CardBody([
                        html.Div(id="ops-health-container", children=html.P("Waiting for status events...", className="text-muted text-center"))
                    ])
                ], className="shadow-sm mb-3 border-dark"),

                # Navigation Links
                dbc.Card([
                    dbc.CardHeader(html.H5("Data & Telemetry Links", className="mb-0")),
                    dbc.CardBody([
                        dbc.ListGroup([
                            dbc.ListGroupItem(
                                "View Variableset Plots", 
                                href=dash.get_relative_path(f"/deployment/{deployment_id}/variablesets"), 
                                action=True, color="info", className="fw-bold"
                            ),
                            dbc.ListGroupItem(
                                "View Raw Asset Telemetry", 
                                href=dash.get_relative_path("/assets"), 
                                action=True, className="fw-bold"
                            )
                        ])
                    ])
                ], className="shadow-sm border-dark")
            ], width=4),

            # --- RIGHT COLUMN: Expanded Quick Looks ---
            dbc.Col([
                dbc.Row([
                    # Sub-Column 1 (Nav, Aero, Gas)
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader("Navigation", className="p-2 bg-light fw-bold"),
                            dbc.CardBody(dbc.Row([
                                make_kpi_col("Lat / Lon", "kpi-nav-latlon"),
                                make_kpi_col("Speed / Hdg", "kpi-nav-spdhdg"),
                                make_kpi_col("Pitch / Roll", "kpi-nav-pitchroll"),
                            ], className="g-2"), className="p-2")
                        ], className="mb-3 shadow-sm"),
                        
                        dbc.Card([
                            dbc.CardHeader("Aerosols", className="p-2 bg-light fw-bold"),
                            dbc.CardBody(dbc.Row([
                                make_kpi_col("CN", "kpi-aero-cn"),
                                make_kpi_col("Scat (B/G/R)", "kpi-aero-scat"),
                                make_kpi_col("Abs (B/G/R)", "kpi-aero-abs"),
                            ], className="g-2"), className="p-2")
                        ], className="mb-3 shadow-sm"),

                        dbc.Card([
                            dbc.CardHeader("Gas Phase", className="p-2 bg-light fw-bold"),
                            dbc.CardBody(dbc.Row([
                                make_kpi_col("O3", "kpi-gas-o3"),
                                make_kpi_col("CO", "kpi-gas-co"),
                                make_kpi_col("NO / NO2", "kpi-gas-nox"),
                            ], className="g-2"), className="p-2")
                        ], className="mb-3 shadow-sm")
                    ], width=6),
                    
                    # Sub-Column 2 (Met, Ops)
                    dbc.Col([
                        dbc.Card([
                            dbc.CardHeader("Meteorology", className="p-2 bg-light fw-bold"),
                            dbc.CardBody(dbc.Row([
                                make_kpi_col("True WS/WDIR", "kpi-met-wind"),
                                make_kpi_col("Temp / RH", "kpi-met-temprh"),
                                make_kpi_col("Pressure", "kpi-met-press"),
                                make_kpi_col("Rain Rate", "kpi-met-rain"),
                                make_kpi_col("Irradiance", "kpi-met-irrad"),
                            ], className="g-2"), className="p-2")
                        ], className="mb-3 shadow-sm"),

                        dbc.Card([
                            dbc.CardHeader("Operational", className="p-2 bg-light fw-bold"),
                            dbc.CardBody(dbc.Row([
                                make_kpi_col("Rel WS/WDIR", "kpi-ops-relwind"),
                                make_kpi_col("Inlet Flow", "kpi-ops-flow"),
                                make_kpi_col("Inlet SP", "kpi-ops-flowsp"),
                            ], className="g-2"), className="p-2")
                        ], className="mb-3 shadow-sm")
                    ], width=6)
                ])
            ], width=8)
        ]),

        # --- WEBSOCKETS & STATE ---
        WebSocket(id="ws-deployment-c2", url=f"{ws_url_base}/envds/envops/ws/deployment/{deployment_id}/c2"),
        WebSocket(id="ws-deployment-telemetry", url=f"{ws_url_base}/envds/envops/ws/deployment/{deployment_id}/telemetry"),
        html.Div(id="ws-c2-send-buffer", style={"display": "none"}),
        dcc.Store(id="store-deployment-id", data=deployment_id),
        dcc.Store(id="store-bundle-ids", data=bundle_ids),
        dcc.Store(id="c2-health-store", data={}),
        
        # --- STALENESS HEARTBEAT (Ticks every 10 seconds) ---
        dcc.Interval(id="kpi-staleness-interval", interval=10 * 1000, n_intervals=0),

        # --- TELEMETRY CACHES (Now tracking timestamps) ---
        dcc.Store(id="kpi-nav-cache", data={k: {"val": "--", "ts": 0} for k in ["lat", "lon", "spd", "hdg", "pitch", "roll"]}),
        dcc.Store(id="kpi-met-cache", data={k: {"val": "--", "ts": 0} for k in ["tws", "twdir", "temp", "rh", "press", "rain", "irrad"]}),
        dcc.Store(id="kpi-ops-cache", data={k: {"val": "--", "ts": 0} for k in ["rws", "rwdir", "flow", "flowsp"]}),
        dcc.Store(id="kpi-aero-cache", data={k: {"val": "--", "ts": 0} for k in ["cn", "scat_b", "scat_g", "scat_r", "abs_b", "abs_g", "abs_r"]}),
        dcc.Store(id="kpi-gas-cache", data={k: {"val": "--", "ts": 0} for k in ["o3", "co", "no", "no2"]})
    ])

# --- CALLBACKS ---

@callback(
    Output("ws-c2-send-buffer", "children"),
    Input("btn-mode-auto", "n_clicks"),
    Input("btn-mode-manual", "n_clicks"),
    State("store-deployment-id", "data"),
    prevent_initial_call=True
)
def handle_c2_mode_switch(auto_clicks, manual_clicks, deployment_id):
    if not ctx.triggered:
        raise PreventUpdate
        
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    requested_mode = "auto" if button_id == "btn-mode-auto" else "manual"

    event = {
        "type": "envds.control.request",
        "source": f"envds.{config.daq_id}.dashboard",
        "id": str(ULID()),
        "datacontenttype": "application/json; charset=utf-8",
        "data": {
            "system_mode": {
                "requested": requested_mode
            }
        },
        "destpath": f"envds/{config.daq_id}/system/control/request",
        "deploymentref": deployment_id
    }
    
    L.info(f"Issuing C2 Request to Host {deployment_id}: Switch to {requested_mode.upper()} mode.")
    return json.dumps(event)

@callback(
    Output("ws-deployment-c2", "send"), 
    Input("ws-c2-send-buffer", "children")
)
def send_c2_request(payload):
    if payload:
        return payload
    raise PreventUpdate

@callback(
    Output("c2-health-store", "data"),
    Input("ws-deployment-c2", "message"),
    State("store-bundle-ids", "data"),
    State("c2-health-store", "data"),
    prevent_initial_call=True
)
def accumulate_bundle_health(message, bundle_ids, current_store):
    if not message or "data" not in message:
        raise PreventUpdate
        
    try:
        status_data = json.loads(message["data"])
        app_uid = status_data.get("id", {}).get("app_uid", "")
        
        if app_uid in bundle_ids:
            if current_store is None:
                current_store = {}
            current_store[app_uid] = status_data
            return current_store
            
    except Exception as e:
        L.error(f"Error parsing C2 status update: {e}")
        
    raise PreventUpdate

@callback(
    Output("live-system-mode-badge", "children"),
    Output("ops-health-container", "children"),
    Input("c2-health-store", "data"),
    State("store-deployment-id", "data"),
    prevent_initial_call=True
)
def render_bundle_health(health_store, host_id):
    if not health_store:
        raise PreventUpdate
        
    # Helpers for building the state machine tree
    def get_badge(val):
        """Converts booleans or common mode strings into color-coded badges."""
        if isinstance(val, bool):
            color = "success" if val else "secondary"
            text = "TRUE" if val else "FALSE"
        elif isinstance(val, str):
            val_lower = val.lower()
            if val_lower in ["auto", "normal", "nominal_sampling"]:
                color = "success"
            elif val_lower in ["manual", "startup", "system_startup"]:
                color = "warning"
            else:
                color = "secondary"
            text = val.upper()
        else:
            color = "light"
            text = str(val)
        return dbc.Badge(text, color=color, className="ms-2")

    def render_list(title, title_color, data_dict):
        """Renders a subsection (like Sampling Modes or States) as a clean unordered list."""
        if not data_dict:
            return html.Div()
            
        items = []
        for k, v in data_dict.items():
            # Handle if value is a dict ({"actual": bool}) or just a raw boolean
            actual_val = v.get("actual") if isinstance(v, dict) else v
            items.append(html.Li([
                html.Span(k, className="font-monospace text-dark"), 
                get_badge(actual_val)
            ], className="mb-1"))
            
        return html.Div([
            html.Div(title, className=f"fw-bold mt-3 mb-2 border-bottom {title_color}"),
            html.Ul(items, className="list-unstyled ms-3 mb-0")
        ])

    # 1. Evaluate Host Status for the Top Badge
    host_data = health_store.get(host_id, {})
    host_state = host_data.get("state", {})
    
    current_mode = host_state.get("system_mode", {}).get("actual", "UNKNOWN")
    badge_color = "success" if current_mode.lower() in ["auto", "normal"] else "warning"
    top_badge = dbc.Badge(f"HOST MODE: {current_mode.upper()}", color=badge_color, className="p-2 fs-6")
    
    # 2. Render Accordions for Host and Subs
    accordions = []
    for dep_id, s_data in health_store.items():
        state_dict = s_data.get("state", {})
        title_prefix = "HOST: " if dep_id == host_id else "SUB: "
        
        # Calculate Title Color
        actual_sys_mode = state_dict.get("system_mode", {}).get("actual", "unknown").lower()
        text_color = "text-success" if actual_sys_mode in ["auto", "normal"] else "text-warning"
        
        # Build the hierarchical tree content
        sys_mode_ui = html.Div([
            html.Div("System Mode", className="fw-bold mb-2 text-primary border-bottom"),
            html.Span("Current Active Mode:", className="ms-3 text-muted me-2"),
            get_badge(state_dict.get("system_mode", {}).get("actual", "UNKNOWN"))
        ])
        
        sm_ui = render_list("Sampling Modes", "text-info", state_dict.get("sampling_mode", {}))
        ss_ui = render_list("Sampling States", "text-success", state_dict.get("sampling_state", {}))
        sc_ui = render_list("Sampling Conditions", "text-secondary", state_dict.get("sampling_condition", {}))
        
        content = html.Div([sys_mode_ui, sm_ui, ss_ui, sc_ui], style={"fontSize": "0.85rem", "maxHeight": "400px", "overflowY": "auto"})
        
        # Assemble the accordion item
        title = html.Span([f"{title_prefix}{dep_id.split('.')[-1]} ", html.Span("●", className=text_color)])
        accordions.append(dbc.AccordionItem(content, title=title))
        
    health_ui = dbc.Accordion(accordions, start_collapsed=False, flush=True) if accordions else dash.no_update
    
    return top_badge, health_ui

@callback(
    Output("kpi-nav-latlon", "children"),
    Output("kpi-nav-spdhdg", "children"),
    Output("kpi-nav-pitchroll", "children"),
    Output("kpi-met-wind", "children"),
    Output("kpi-met-temprh", "children"),
    Output("kpi-met-press", "children"),
    Output("kpi-met-rain", "children"),
    Output("kpi-met-irrad", "children"),
    Output("kpi-aero-cn", "children"),
    Output("kpi-aero-scat", "children"),
    Output("kpi-aero-abs", "children"),
    Output("kpi-gas-o3", "children"),
    Output("kpi-gas-co", "children"),
    Output("kpi-gas-nox", "children"),
    Output("kpi-ops-relwind", "children"),
    Output("kpi-ops-flow", "children"),
    Output("kpi-ops-flowsp", "children"),
    Input("ws-deployment-telemetry", "message"),
    Input("kpi-staleness-interval", "n_intervals"), 
    State("kpi-nav-cache", "data"),
    State("kpi-met-cache", "data"),
    State("kpi-ops-cache", "data"),
    State("kpi-aero-cache", "data"),
    State("kpi-gas-cache", "data"),
    prevent_initial_call=True
)
def update_quick_looks(message, n_intervals, n_cache, m_cache, o_cache, a_cache, g_cache):
    trigger = ctx.triggered_id
    now = time.time()
    stale_threshold = 120  # Seconds until data is considered "Stale"

    # --- DEBUGGING: Track the trigger ---
    if trigger == "kpi-staleness-interval":
        # print(f"DEBUG: Heartbeat tick at {now}")
        pass
    elif trigger == "ws-deployment-telemetry":
        print(f"\n--- DEBUG: WEBSOCKET EVENT RECEIVED ---")

    # 1. Update caches ONLY if triggered by new WebSocket data
    if trigger == "ws-deployment-telemetry" and message and "data" in message:
        try:
            payload = json.loads(message["data"])
            
            print(f"DEBUG Payload Keys: {list(payload.keys())}")
            
            variables = payload.get("variables", {})
            print(f"DEBUG Received Variables: {list(variables.keys())}")
            
            def get_val(var_name):
                if var_name in variables:
                    val = variables[var_name].get("data")
                    return f"{val:.2f}" if isinstance(val, float) else str(val)
                return None

            # NAV
            for key, v_names in [("lat", ["latitude", "lat"]), ("lon", ["longitude", "lon"]), 
                                 ("spd", ["sog", "speed"]), ("hdg", ["cog", "heading"]),
                                 ("pitch", ["pitch"]), ("roll", ["roll"])]:
                for vn in v_names:
                    if val := get_val(vn): 
                        n_cache[key] = {"val": val, "ts": now}
                        print(f"DEBUG: Matched NAV {key} -> {val}")

            # MET
            for key, v_names in [("tws", ["true_wind_speed", "tws"]), ("twdir", ["true_wind_dir", "twdir"]),
                                 ("temp", ["temperature", "air_temp"]), ("rh", ["rh", "relative_humidity"]),
                                 ("press", ["pressure", "baro"]), ("rain", ["rain_rate", "precip"]),
                                 ("irrad", ["irradiance", "solar"])]:
                for vn in v_names:
                    if val := get_val(vn): 
                        m_cache[key] = {"val": val, "ts": now}
                        print(f"DEBUG: Matched MET {key} -> {val}")
                        
            # AERO
            for key, v_names in [("cn", ["cn_concentration", "cn"])]:
                for vn in v_names:
                    if val := get_val(vn): a_cache[key] = {"val": val, "ts": now}
            for key, v_names in [("scat_b", ["scatter_blue", "scat_blue"]), ("scat_g", ["scatter_green", "scat_green"]), ("scat_r", ["scatter_red", "scat_red"]),
                                 ("abs_b", ["absorption_blue", "abs_blue"]), ("abs_g", ["absorption_green", "abs_green"]), ("abs_r", ["absorption_red", "abs_red"])]:
                for vn in v_names:
                    if val := get_val(vn): a_cache[key] = {"val": val, "ts": now}

            # GAS
            for key, v_names in [("o3", ["o3", "ozone"]), ("co", ["co", "carbon_monoxide"]), 
                                 ("no", ["no", "nitric_oxide"]), ("no2", ["no2", "nitrogen_dioxide"])]:
                for vn in v_names:
                    if val := get_val(vn): g_cache[key] = {"val": val, "ts": now}

            # OPS
            for key, v_names in [("rws", ["relative_wind_speed", "rel_wind_speed", "rws"]), ("rwdir", ["relative_wind_dir", "rel_wind_dir", "rwdir"]),
                                 ("flow", ["inlet_flow", "flow"]), ("flowsp", ["inlet_flow_sp", "flow_setpoint"])]:
                for vn in v_names:
                    if val := get_val(vn): o_cache[key] = {"val": val, "ts": now}
                    
        except Exception as e:
            L.error(f"KPI Parsing Error: {e}")
            print(f"DEBUG Parsing Error: {e}")
            raise PreventUpdate

    # 2. Rendering Logic (Applies to both WS events and Heartbeat ticks)
    def fmt(c_dict):
        val = c_dict["val"]
        ts = c_dict["ts"]
        if val == "--": return val
        
        # If older than threshold, turn the text red
        if now - ts > stale_threshold:
            return html.Span(str(val), className="text-danger fw-bold", title=f"Stale: Last updated {(now-ts)/60:.1f}m ago")
        return str(val)

    return (
        [fmt(n_cache['lat']), " / ", fmt(n_cache['lon'])],
        [fmt(n_cache['spd']), " / ", fmt(n_cache['hdg'])],
        [fmt(n_cache['pitch']), " / ", fmt(n_cache['roll'])],
        [fmt(m_cache['tws']), " / ", fmt(m_cache['twdir'])],
        [fmt(m_cache['temp']), " / ", fmt(m_cache['rh'])],
        fmt(m_cache['press']),
        fmt(m_cache['rain']),
        fmt(m_cache['irrad']),
        fmt(a_cache['cn']),
        [fmt(a_cache['scat_b']), " / ", fmt(a_cache['scat_g']), " / ", fmt(a_cache['scat_r'])],
        [fmt(a_cache['abs_b']), " / ", fmt(a_cache['abs_g']), " / ", fmt(a_cache['abs_r'])],
        fmt(g_cache['o3']),
        fmt(g_cache['co']),
        [fmt(g_cache['no']), " / ", fmt(g_cache['no2'])],
        [fmt(o_cache['rws']), " / ", fmt(o_cache['rwdir'])],
        fmt(o_cache['flow']),
        fmt(o_cache['flowsp'])
    )