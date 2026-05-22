import dash
from dash import html, dcc, callback, Input, Output, State, no_update
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from dash_extensions import WebSocket
import dash_ag_grid as dag
import json
import logging
from logfmter import Logfmter
import traceback
from pydantic import BaseSettings

# Configure structured logging
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.INFO)

# Register the page with a dynamic path variable matching home.py's routing
dash.register_page(
    __name__,
    path_template="/deployment/<deployment_id>/ops",
    title="Platform Operations",
    nav_bar=False # Hidden from sidebar since it requires a deployment_id context
)

class Settings(BaseSettings):
    external_hostname: str = "localhost" # Fallback, recommend setting via env vars
    ws_use_tls: bool = False
    ws_port: int = 8080
    wss_port: int = 443

    class Config:
        env_prefix = "ENVOPS_"
        case_sensitive = False

config = Settings()

# Standardized WebSocket URL construction
ws_url_base = f"ws://{config.external_hostname}:{config.ws_port}"
if config.ws_use_tls:
    ws_url_base = f"wss://{config.external_hostname}:{config.wss_port}"

# -----------------------------------------------------------------------------
# Layout Definition
# -----------------------------------------------------------------------------
def layout(deployment_id=None, **kwargs):
    """Dynamic layout that accepts the deployment_id from the URL."""
    
    return html.Div([
        # 1. Header Section
        dbc.Row([
            dbc.Col([
                html.H2(f"Operations: {deployment_id}", className="fw-bold mb-1"),
                html.P("Real-time telemetry, mode management, and state evaluation.", className="text-muted")
            ]),
            dbc.Col([
                dbc.Button(
                    [html.I(className="bi bi-arrow-left me-2"), "Back to Fleet"], 
                    color="outline-secondary", 
                    href="/envds/envops/",
                    className="float-end shadow-sm"
                )
            ], width="auto", align="center")
        ], className="mb-4"),

        # 2. Overall System Health / Mode
        dbc.Card([
            dbc.CardBody(id="platform-ops-health-display", children=[
                dbc.Alert("Awaiting System Mode Status...", color="secondary", className="mb-0 text-center fw-bold shadow-sm")
            ])
        ], className="shadow-sm mb-4 border-0 bg-transparent"),

        # 3. Main Modes and States Grid
        dbc.Row([
            # System Modes
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("System Modes", className="fw-bold bg-dark text-white"),
                    dbc.CardBody(id="platform-ops-system-modes", children=[
                        html.P("Waiting for telemetry...", className="text-muted small fst-italic mb-0")
                    ], className="p-0")
                ], className="shadow-sm h-100 border-0")
            ], width=12, md=4, className="mb-4"),
            
            # Sampling Modes
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Sampling Modes", className="fw-bold bg-primary text-white"),
                    dbc.CardBody(id="platform-ops-sampling-modes", children=[
                        html.P("Waiting for telemetry...", className="text-muted small fst-italic mb-0")
                    ], className="p-0")
                ], className="shadow-sm h-100 border-0")
            ], width=12, md=4, className="mb-4"),

            # Sampling States
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Sampling States", className="fw-bold bg-info text-white"),
                    dbc.CardBody(id="platform-ops-sampling-states", children=[
                        html.P("Waiting for telemetry...", className="text-muted small fst-italic mb-0")
                    ], className="p-0")
                ], className="shadow-sm h-100 border-0")
            ], width=12, md=4, className="mb-4"),
        ]),

        # 4. Comprehensive Conditions Table
        dbc.Card([
            dbc.CardHeader("Active Conditions Tracker", className="fw-bold bg-secondary text-white"),
            dbc.CardBody([
                dag.AgGrid(
                    id="platform-ops-conditions-grid",
                    columnDefs=[
                        {"field": "name", "headerName": "Condition Node", "flex": 2},
                        {"field": "status", "headerName": "Evaluation Status", "flex": 1, "cellRenderer": "markdown"},
                        {"field": "time", "headerName": "Last Updated", "flex": 1},
                    ],
                    rowData=[],
                    defaultColDef={"sortable": True, "filter": True},
                    columnSizeOptions="autoSize",
                    dashGridOptions={"domLayout": "autoHeight", "rowSelection": "single", "animateRows": True},
                    className="ag-theme-alpine"
                )
            ], className="p-2")
        ], className="shadow-sm border-0"),

        # 5. Behind-the-Scenes Components
        WebSocket(
            id="ws-platform-ops",
            url=f"{ws_url_base}/envds/envops/ws/system-ops/main" 
        ),
        
        # Centralized State Store mapped from MQTT via WebSocket
        dcc.Store(id="platform-ops-state-store", data={
            "SystemMode": {},
            "SamplingMode": {},
            "SamplingState": {},
            "SamplingCondition": {}
        })
        
    ], className="container-fluid mt-2")

# -----------------------------------------------------------------------------
# Callbacks
# -----------------------------------------------------------------------------
@callback(
    Output("platform-ops-state-store", "data"),
    Input("ws-platform-ops", "message"),
    State("platform-ops-state-store", "data")
)
def update_state_store(msg, current_state):
    """Processes incoming WebSocket telemetry and maps it to the component store."""
    if not msg or "data" not in msg:
        raise PreventUpdate

    try:
        # 1. Unwrap the WebSocket envelope we built in main.py
        ws_payload = json.loads(msg["data"])
        
        # 2. Extract the CloudEvent from the WebSocket payload
        cloud_event = ws_payload.get("data", {})
        
        # 3. Extract your actual business payload from inside the CloudEvent!
        status_data = cloud_event.get("data", {})
        
        # 4. NOW we can safely grab your custom id and state dictionaries
        id_block = status_data.get("id", {})
        state_block = status_data.get("state", {})
        
        # Use isinstance to be completely bulletproof against malformed data
        if not isinstance(id_block, dict):
            return no_update
            
        app_group = id_block.get("app_group", "")
        name = id_block.get("app_uid")
        
        # Map the ontology back to the UI's store keys
        kind_map = {
            "condition": "SamplingCondition",
            "state": "SamplingState",
            "mode": "SamplingMode",
            "system": "SystemMode"
        }
        kind = kind_map.get(app_group)
        
        if kind and name and kind in current_state:
            state_key = {
                "condition": "condition_met",
                "state": "state_active",
                "mode": "mode_active",
                "system": "system_active"
            }.get(app_group, "")
            
            # Extract boolean actual status (defaults to false)
            actual_str = state_block.get(state_key, {}).get("actual", "false")
            is_active = (str(actual_str).lower() == "true")
            
            current_state[kind][name] = {
                "status": is_active,
                "time": status_data.get("timestamp", "N/A")
            }
            return current_state
            
    except Exception as e:
        L.error(f"Store update error: {e}")
        L.error(traceback.format_exc())
        
    return no_update

@callback(
    Output("platform-ops-health-display", "children"),
    Output("platform-ops-system-modes", "children"),
    Output("platform-ops-sampling-modes", "children"),
    Output("platform-ops-sampling-states", "children"),
    Output("platform-ops-conditions-grid", "rowData"),
    Input("platform-ops-state-store", "data")
)
def render_ui(state):
    """Renders the UI components based on the centralized state store."""
    
    # 1. System Health / Top Banner
    sys_modes = state.get("SystemMode", {})
    active_sys_modes = [name for name, d in sys_modes.items() if d["status"]]
    
    if active_sys_modes:
        sys_status = active_sys_modes[0]
        health_ui = dbc.Alert(
            [html.I(className="bi bi-activity me-2"), f"Active System Mode: {sys_status.upper()}"], 
            color="success", className="mb-0 text-center fw-bold fs-5 shadow-sm"
        )
    else:
        health_ui = dbc.Alert("No Active System Mode detected.", color="warning", className="mb-0 text-center fw-bold fs-5 shadow-sm")

    # 2. System Modes List
    sys_list = [
        dbc.ListGroupItem([
            html.Span(name, className="fw-bold"),
            dbc.Badge("ACTIVE", color="success", className="float-end") if data["status"] 
            else dbc.Badge("STANDBY", color="light", text_color="dark", className="float-end")
        ], className="border-0 border-bottom") for name, data in state.get("SystemMode", {}).items()
    ]
    sys_ui = dbc.ListGroup(sys_list, flush=True) if sys_list else html.P("No System Modes detected.", className="text-muted p-3 mb-0")

    # 3. Sampling Modes List
    samp_modes = state.get("SamplingMode", {})
    samp_mode_list = [
        dbc.ListGroupItem([
            html.Span(name, className="fw-bold"),
            dbc.Badge("ENGAGED", color="primary", className="float-end") if data["status"] 
            else dbc.Badge("INACTIVE", color="light", text_color="dark", className="float-end")
        ], className="border-0 border-bottom") for name, data in samp_modes.items()
    ]
    samp_modes_ui = dbc.ListGroup(samp_mode_list, flush=True) if samp_mode_list else html.P("No Sampling Modes detected.", className="text-muted p-3 mb-0")

    # 4. Sampling States List
    samp_states = state.get("SamplingState", {})
    samp_state_list = [
        dbc.ListGroupItem([
            html.Span(name, className="fw-bold text-dark small"),
            html.I(className="bi bi-check-circle-fill text-success float-end fs-5") if data["status"] 
            else html.I(className="bi bi-dash-circle text-muted float-end fs-5")
        ], className="border-0 border-bottom d-flex justify-content-between align-items-center") for name, data in samp_states.items()
    ]
    samp_states_ui = dbc.ListGroup(samp_state_list, flush=True) if samp_state_list else html.P("No Sampling States detected.", className="text-muted p-3 mb-0")

    # 5. Conditions AG Grid
    samp_conditions = state.get("SamplingCondition", {})
    grid_data = [
        {
            "name": k, 
            "status": "🟢 **MET**" if v["status"] else "🔴 **UNMET**", 
            "time": v["time"]
        }
        for k, v in samp_conditions.items()
    ]

    return (
        health_ui,
        sys_ui,
        samp_modes_ui,
        samp_states_ui,
        grid_data
    )