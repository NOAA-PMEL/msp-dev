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
    layout = html.Div([
        html.H1("System Operations & Sampling Status", className="mb-4"),
        
        # Top Row: Modes & States
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("System Mode", className="fw-bold text-center"),
                    dbc.CardBody(
                        html.H3(id="system-mode-display", children="UNKNOWN", className="text-center text-secondary")
                    )
                ], className="shadow-sm h-100")
            ], width=4),
            
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Sampling Mode", className="fw-bold text-center"),
                    dbc.CardBody(
                        html.H3(id="sampling-mode-display", children="UNKNOWN", className="text-center text-primary")
                    )
                ], className="shadow-sm h-100")
            ], width=4),
            
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Sampling State", className="fw-bold text-center"),
                    dbc.CardBody(
                        html.H3(id="sampling-state-display", children="UNKNOWN", className="text-center text-info")
                    )
                ], className="shadow-sm h-100")
            ], width=4),
        ], className="mb-4"),

        # Bottom Row: Conditions Table
        dbc.Card([
            dbc.CardHeader("Active Conditions Status", className="fw-bold"),
            dbc.CardBody([
                dag.AgGrid(
                    id="conditions-table",
                    columnDefs=[
                        {"field": "condition_name", "headerName": "Condition", "flex": 2},
                        {
                            "field": "status", 
                            "headerName": "Status", 
                            "flex": 1,
                            "cellRenderer": "markdown" # Allows rendering emojis/colors if desired
                        },
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
    
    return layout

layout = get_layout()

@callback(
    Output("system-mode-display", "children"),
    Output("sampling-mode-display", "children"),
    Output("sampling-state-display", "children"),
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
        
        # Default returns to preserve existing UI state
        sys_mode = dash.no_update
        samp_mode = dash.no_update
        samp_state = dash.no_update
        cond_data = dash.no_update

        if "systemmode" in ce_type:
            # Assuming payload contains {"mode": "AUTO"}
            val = event_data.get("mode", "UNKNOWN")
            sys_mode = html.Span(val, className="text-success" if val.upper() == "AUTO" else "text-warning")

        elif "samplingmode" in ce_type:
            val = event_data.get("mode", "UNKNOWN")
            samp_mode = val

        elif "samplingstate" in ce_type:
            val = event_data.get("state", "UNKNOWN")
            samp_state = val

        elif "samplingcondition" in ce_type:
            # Assuming payload is {"condition_name": "temperature_ok", "status": True}
            name = event_data.get("condition_name")
            status = event_data.get("status")
            if name:
                current_conditions[name] = status
                cond_data = current_conditions

        return sys_mode, samp_mode, samp_state, cond_data

    except Exception as e:
        L.error(f"Error parsing system ops event: {e}")
        L.error(traceback.format_exc())
        raise PreventUpdate

@callback(
    Output("conditions-table", "rowData"),
    Input("conditions-store", "data")
)
def update_conditions_table(conditions_dict):
    if not conditions_dict:
        raise PreventUpdate
    
    row_data = []
    for name, status in conditions_dict.items():
        # Formatting the status to be user friendly
        status_text = "🟢 Active (True)" if status else "🔴 Inactive (False)"
        row_data.append({
            "condition_name": name,
            "status": status_text
        })
        
    return row_data
