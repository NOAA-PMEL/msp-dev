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
from ulid import ULID

# Configure logging
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.DEBUG)

dash.register_page(
    __name__,
    path="/system-ops-demo",
    title="System Operations",
    order=5
)

class Settings(BaseSettings):
    daq_id: str = "raz1"
    external_hostname: str = "mspbase01.pmel.noaa.gov"
    ws_use_tls: bool = False
    ws_port: int = 8080  # Updated to match your deployment
    wss_port: int = 443

    class Config:
        env_prefix = "DASHBOARD_"
        case_sensitive = False

config = Settings()

# Standardized WebSocket URL construction matching sensor.py
ws_url_base = f"ws://{config.external_hostname}:{config.ws_port}"
if config.ws_use_tls:
    ws_url_base = f"wss://{config.external_hostname}:{config.wss_port}"

ws_send_buffer = html.Div(id="ws-send-ops-buffer", style={"display": "none"})

def get_layout():
    # Use 'main' as the ID to match the server-side global broadcast logic
    client_id = "main" 
    
    return html.Div([
        html.H1("System Operations & Sampling Status", className="mb-4"),
        
        dbc.Row([
            # Column 1: System Modes
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("System Mode", className="fw-bold bg-primary text-white"),
                    dbc.CardBody(id="system-mode-container", children=[
                        html.P("Waiting for update...", className="text-muted small italic")
                    ]) 
                ], className="shadow-sm mb-4 h-100")
            ], width=4),
            
            # Column 2: Sampling Modes
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Sampling Modes", className="fw-bold bg-info text-white"),
                    dbc.CardBody(id="sampling-mode-container", children=[
                        html.P("Waiting for update...", className="text-muted small italic")
                    ]) 
                ], className="shadow-sm mb-4 h-100")
            ], width=4),

            # Column 3: Sampling States
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Sampling States", className="fw-bold bg-secondary text-white"),
                    dbc.CardBody(id="sampling-state-container", children=[
                        html.P("Waiting for update...", className="text-muted small italic")
                    ]) 
                ], className="shadow-sm mb-4 h-100")
            ], width=4),
        ], className="mb-4"),

        # Bottom Row: Detailed Conditions Table
        dbc.Card([
            dbc.CardHeader("Active Conditions Status", className="fw-bold"),
            dbc.CardBody([
                dag.AgGrid(
                    id="conditions-table",
                    columnDefs=[
                        {"field": "name", "headerName": "Condition", "flex": 2},
                        {"field": "status", "headerName": "Status", "flex": 1},
                        {"field": "time", "headerName": "Last Update", "flex": 1},
                    ],
                    rowData=[],
                    columnSizeOptions="autoSize",
                    dashGridOptions={"domLayout": "autoHeight"}
                )
            ])
        ], className="shadow-sm"),

        # Standardized WebSocket with msp/dashboardtest prefix
        # WebSocket(
        #     id="ws-system-ops",
        #     url=f"{ws_url_base}/msp/dashboardtest/ws/system-ops"
        # ),
        WebSocket(
            id="ws-system-ops",
            url=f"{ws_url_base}/msp/dashboardtest/ws/system-ops/main" # <--- Added /main
        ),
        ws_send_buffer,
        
        dcc.Store(id="system-ops-state", data={
            "SystemMode": {},
            "SamplingMode": {},
            "SamplingState": {},
            "SamplingCondition": {}
        })
    ], className="p-4")

layout = get_layout()

@callback(
    Output("system-ops-state", "data"),
    Input("ws-system-ops", "message"),
    State("system-ops-state", "data")
)
@callback(
    Output("system-ops-state", "data"),
    Input("ws-system-ops", "message"),
    State("system-ops-state", "data")
)
def update_state_store(msg, current_state):
    if not msg or "data" not in msg:
        raise PreventUpdate

    try:
        payload = json.loads(msg["data"])
        
        # Extract underlying data payload sent by handle_mqtt_buffer
        event_data = payload.get("data", {})
        
        # Parse the NEW envds-compliant status format
        id_block = event_data.get("id", {})
        state_block = event_data.get("state", {})
        
        app_group = id_block.get("app_group", "")
        name = id_block.get("app_uid")
        
        # Map the new app_group ontology back to the UI's store keys
        kind_map = {
            "condition": "SamplingCondition",
            "state": "SamplingState",
            "mode": "SamplingMode",
            "system": "SystemMode"
        }
        kind = kind_map.get(app_group)
        
        if kind and name and kind in current_state:
            # Map the app_group to the specific state parameter name
            state_key = {
                "condition": "condition_met",
                "state": "state_active",
                "mode": "mode_active",
                "system": "system_active"
            }.get(app_group, "")
            
            # Extract boolean actual status (defaults to false if missing)
            actual_str = state_block.get(state_key, {}).get("actual", "false")
            is_active = (str(actual_str).lower() == "true")
            
            current_state[kind][name] = {
                "status": is_active,
                "time": event_data.get("timestamp", "N/A")
            }
            return current_state
            
    except Exception as e:
        L.error(f"Store update error: {e}")
        L.error(traceback.format_exc())
        
    return no_update

@callback(
    Output("system-mode-container", "children"),
    Output("sampling-mode-container", "children"),
    Output("sampling-state-container", "children"),
    Output("conditions-table", "rowData"),
    Input("system-ops-state", "data")
)
def render_ui(state):
    # 1. System Modes
    sys_list = [
        dbc.ListGroupItem([
            html.Span(name, className="fw-bold"),
            dbc.Badge("ACTIVE", color="success", className="float-end") if data["status"] 
            else dbc.Badge("OFF", color="light", className="float-end text-dark")
        ], active=data["status"]) for name, data in state["SystemMode"].items()
    ]

    # 2. Sampling Modes
    samp_mode_list = [
        dbc.ListGroupItem([
            html.Span(name),
            dbc.Badge("TRUE", color="info", className="float-end") if data["status"] 
            else dbc.Badge("FALSE", color="danger", className="float-end")
        ]) for name, data in state["SamplingMode"].items()
    ]

    # 3. Sampling States
    state_list = [
        dbc.ListGroupItem([
            html.Span(name, className="small"),
            html.I(className="bi bi-check-circle-fill text-success float-end") if data["status"] 
            else html.I(className="bi bi-x-circle text-muted float-end")
        ]) for name, data in state["SamplingState"].items()
    ]

    # 4. Conditions
    grid_data = [
        {"name": k, "status": "🟢 True" if v["status"] else "🔴 False", "time": v["time"]}
        for k, v in state["SamplingCondition"].items()
    ]

    return (
        dbc.ListGroup(sys_list) if sys_list else no_update,
        dbc.ListGroup(samp_mode_list) if samp_mode_list else no_update,
        dbc.ListGroup(state_list) if state_list else no_update,
        grid_data
    )