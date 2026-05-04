import dash
from dash import html, callback, dcc, Input, Output, State, no_update
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from dash_extensions import WebSocket
import dash_ag_grid as dag
import json
import logging
from logfmter import Logfmter
import traceback
from pydantic import BaseSettings

# Configure logging
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.DEBUG)

dash.register_page(
    __name__,
    path="/sampling-ops-demo2",
    title="Sampling Operations Dashboard",
    order=6
)

class Settings(BaseSettings):
    daq_id: str = "default"
    external_hostname: str = "localhost"
    ws_use_tls: bool = False
    ws_port: int = 8080
    wss_port: int = 443

    class Config:
        env_prefix = "DASHBOARD_"
        case_sensitive = False

config = Settings()

# Standardized WebSocket URL construction matching the rest of the application
ws_url_base = f"ws://{config.external_hostname}:{config.ws_port}"
if config.ws_use_tls:
    ws_url_base = f"wss://{config.external_hostname}:{config.wss_port}"

def get_layout():
    return html.Div([
        html.H1("Sampling Operations Architecture", className="mb-4"),
        
        # 1. Overall System Health / Mode
        dbc.Card([
            dbc.CardBody(id="system-health-display", children=[
                dbc.Alert("Awaiting System Mode Status...", color="secondary", className="mb-0 text-center fw-bold")
            ])
        ], className="shadow-sm mb-4 border-0"),

        # 2. Main Columns for Modes and States
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Sampling Modes", className="fw-bold bg-primary text-white"),
                    dbc.CardBody(id="sampling-modes-display", children=[
                        html.P("Waiting for telemetry...", className="text-muted small italic")
                    ])
                ], className="shadow-sm h-100")
            ], width=6, md=6),
            
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Sampling States", className="fw-bold bg-info text-white"),
                    dbc.CardBody(id="sampling-states-display", children=[
                        html.P("Waiting for telemetry...", className="text-muted small italic")
                    ])
                ], className="shadow-sm h-100")
            ], width=6, md=6),
        ], className="mb-4"),

        # 3. Comprehensive Conditions Table
        dbc.Card([
            dbc.CardHeader("Sampling Conditions Tracker", className="fw-bold bg-dark text-white"),
            dbc.CardBody([
                dag.AgGrid(
                    id="sampling-conditions-grid",
                    columnDefs=[
                        {"field": "name", "headerName": "Condition Node", "flex": 2},
                        {"field": "status", "headerName": "Evaluation Status", "flex": 1, "cellRenderer": "markdown"},
                        {"field": "time", "headerName": "Last Updated", "flex": 1},
                    ],
                    rowData=[],
                    columnSizeOptions="autoSize",
                    dashGridOptions={"domLayout": "autoHeight", "rowSelection": "single"}
                )
            ])
        ], className="shadow-sm"),

        # Connection and State Management
        WebSocket(
            id="ws-sampling-ops2",
            url=f"{ws_url_base}/msp/dashboardtest/ws/system-ops"
        ),
        
        dcc.Store(id="ops-state-store2", data={
            "SystemMode": {},
            "SamplingMode": {},
            "SamplingState": {},
            "SamplingCondition": {}
        })
    ], className="p-4")

layout = get_layout()

@callback(
    Output("ops-state-store2", "data"),
    Input("ws-sampling-ops2", "message"),
    State("ops-state-store2", "data")
)
def update_state_store(msg, current_state):
    if not msg or "data" not in msg:
        raise PreventUpdate

    try:
        payload = json.loads(msg["data"])
        
        # Extract underlying data payload sent by handle_mqtt_buffer in main.py
        event_data = payload.get("data", {})
        status_obj = event_data.get("status", {})
        
        kind = status_obj.get("kind")
        name = status_obj.get("name")
        
        if kind and name and kind in current_state:
            current_state[kind][name] = {
                "status": status_obj.get("status", False),
                "time": status_obj.get("time", "N/A")
            }
            return current_state
            
    except Exception as e:
        L.error(f"Sampling Ops Demo 2 - Store update error: {e}")
        L.error(traceback.format_exc())
        
    return no_update

@callback(
    Output("system-health-display", "children"),
    Output("sampling-modes-display", "children"),
    Output("sampling-states-display", "children"),
    Output("sampling-conditions-grid", "rowData"),
    Input("ops-state-store2", "data")
)
def render_ui(state):
    # 1. Evaluate Overall System Health
    sys_modes = state.get("SystemMode", {})
    active_sys_modes = [name for name, d in sys_modes.items() if d["status"]]
    
    if active_sys_modes:
        sys_status = active_sys_modes[0] # Assuming one active main mode
        health_ui = dbc.Alert(
            [html.I(className="bi bi-activity me-2"), f"Active System Mode: {sys_status.upper()}"], 
            color="success", className="mb-0 text-center fw-bold fs-5"
        )
    else:
        health_ui = dbc.Alert("No Active System Mode detected.", color="warning", className="mb-0 text-center fw-bold fs-5")

    # 2. Render Sampling Modes
    samp_modes = state.get("SamplingMode", {})
    samp_mode_list = [
        dbc.ListGroupItem([
            html.Span(name, className="fw-bold"),
            dbc.Badge("ACTIVE", color="primary", className="float-end") if data["status"] 
            else dbc.Badge("INACTIVE", color="light", text_color="dark", className="float-end")
        ], active=data["status"]) for name, data in samp_modes.items()
    ]
    samp_modes_ui = dbc.ListGroup(samp_mode_list) if samp_mode_list else html.P("No Sampling Modes detected.", className="text-muted")

    # 3. Render Sampling States
    samp_states = state.get("SamplingState", {})
    samp_state_list = [
        dbc.ListGroupItem([
            html.Span(name, className="fw-bold text-dark"),
            dbc.Badge("ENGAGED", color="success", className="float-end") if data["status"] 
            else dbc.Badge("STANDBY", color="secondary", className="float-end")
        ]) for name, data in samp_states.items()
    ]
    samp_states_ui = dbc.ListGroup(samp_state_list) if samp_state_list else html.P("No Sampling States detected.", className="text-muted")

    # 4. Render Sampling Conditions to AG Grid
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
        samp_modes_ui,
        samp_states_ui,
        grid_data
    )