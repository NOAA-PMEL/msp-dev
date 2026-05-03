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
    daq_id: str = "default"
    external_hostname: str = "localhost"
    http_use_tls: bool = False
    http_port: int = 80
    https_port: int = 443
    ws_use_tls: bool = False
    ws_port: int = 80
    wss_port: int = 443

    class Config:
        env_prefix = "DASHBOARD_"
        case_sensitive = False

config = Settings()

# Setup URLs
ws_url_base = f"ws://{config.external_hostname}:{config.ws_port}"
if config.ws_use_tls:
    ws_url_base = f"wss://{config.external_hostname}:{config.wss_port}"

def get_layout():
    return html.Div([
        html.H1("System Operations & Sampling Status", className="mb-4"),
        
        dbc.Row([
            # Column 1: System Mode & Available Sampling Modes
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("System Mode & Available Sampling Modes", className="fw-bold"),
                    dbc.CardBody([
                        html.Div(id="system-mode-container", children=[
                            html.P("Waiting for system mode update...", className="text-muted italic")
                        ]) 
                    ])
                ], className="shadow-sm mb-4 h-100")
            ], width=6),
            
            # Column 2: Sampling Mode & Available States
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Current Sampling Mode & Available States", className="fw-bold"),
                    dbc.CardBody([
                        html.Div(id="sampling-mode-container", children=[
                            html.P("Waiting for sampling mode update...", className="text-muted italic")
                        ]) 
                    ])
                ], className="shadow-sm mb-4 h-100")
            ], width=6),
        ], className="mb-4"),

        # Bottom Row: Conditions Table
        dbc.Card([
            dbc.CardHeader("Active Conditions Status", className="fw-bold"),
            dbc.CardBody([
                dag.AgGrid(
                    id="conditions-table",
                    columnDefs=[
                        {"field": "condition_name", "headerName": "Condition", "flex": 2},
                        {"field": "status", "headerName": "Status", "flex": 1},
                    ],
                    rowData=[],
                    columnSizeOptions="autoSize",
                    dashGridOptions={"domLayout": "autoHeight"}
                )
            ])
        ], className="shadow-sm"),

        # WebSocket & State Buffers
        WebSocket(
            id="ws-system-ops",
            url=f"{ws_url_base}/msp/dashboardtest/ws/system-ops"
        ),
        dcc.Store(id="conditions-store", data={})
    ], className="p-4")

layout = get_layout()

@callback(
    Output("system-mode-container", "children"),
    Output("sampling-mode-container", "children"),
    Output("conditions-store", "data"),
    Input("ws-system-ops", "message"),
    State("conditions-store", "data")
)
def update_system_ops(event, current_conditions):
    if not event or "data" not in event:
        raise PreventUpdate

    try:
        payload = json.loads(event["data"])
        ce_type = payload.get("type", "")
        event_data = payload.get("data", {})
        
        sys_ui = no_update
        samp_ui = no_update
        cond_data = no_update

        # --- 1. SYSTEM MODE & NESTED SAMPLING MODES ---
        if "systemmode" in ce_type:
            active_mode = event_data.get("mode", "UNKNOWN")
            # Uses available_modes from the event data to build the hierarchy list
            available = event_data.get("available_modes", [active_mode])
            
            sys_ui = html.ListGroup([
                dbc.ListGroupItem([
                    html.Div([
                        html.Span(m, className="fw-bold"),
                        html.Badge("ACTIVE", color="success", className="ms-2") if m == active_mode else html.Small("Inactive", className="text-muted")
                    ], className="d-flex justify-content-between align-items-center")
                ], active=(m == active_mode)) for m in available
            ])

        # --- 2. SAMPLING MODE & NESTED STATES ---
        elif "samplingmode" in ce_type or "samplingstate" in ce_type:
            # Handles both mode changes and specific state transitions (e.g., Idle -> Running)
            active_state = event_data.get("state") or event_data.get("mode", "UNKNOWN")
            available = event_data.get("available_states", [active_state])
            
            samp_ui = html.ListGroup([
                dbc.ListGroupItem([
                    html.Div([
                        html.Span(s, className="fw-bold"),
                        html.Badge("RUNNING", color="info", className="ms-2") if s == active_state else html.Small("Idle", className="text-muted")
                    ], className="d-flex justify-content-between align-items-center")
                ], active=(s == active_state)) for s in available
            ])

        # --- 3. CONDITIONS ---
        elif "samplingcondition" in ce_type:
            name = event_data.get("condition_name")
            status = event_data.get("status")
            if name:
                current_conditions[name] = status
                cond_data = current_conditions

        return sys_ui, samp_ui, cond_data

    except Exception as e:
        L.error(f"Error updating status UI: {e}")
        L.error(traceback.format_exc())
        return no_update, no_update, no_update

@callback(
    Output("conditions-table", "rowData"),
    Input("conditions-store", "data")
)
def update_conditions_table(conditions_dict):
    if not conditions_dict:
        raise PreventUpdate
    
    row_data = []
    for name, status in conditions_dict.items():
        status_text = "🟢 Active (True)" if status else "🔴 Inactive (False)"
        row_data.append({
            "condition_name": name,
            "status": status_text
        })
        
    return row_data