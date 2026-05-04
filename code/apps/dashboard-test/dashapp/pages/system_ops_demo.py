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

dash.register_page(__name__, path="/system-ops-demo", title="System Operations", order=5)

class Settings(BaseSettings):
    daq_id: str = "raz1"
    external_hostname: str = "localhost"
    ws_port: int = 8787 # Match main.py container port

    class Config:
        env_prefix = "DASHBOARD_"
        case_sensitive = False

config = Settings()
ws_url = f"ws://{config.external_hostname}:{config.ws_port}/ws/system-ops"

def get_layout():
    return html.Div([
        html.H1("System Operations & Sampling Status", className="mb-4"),
        
        dbc.Row([
            # Column 1: System Modes
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("System Mode (Orchestrator)", className="fw-bold bg-primary text-white"),
                    dbc.CardBody(id="system-mode-container", children=[
                        html.P("Waiting for update...", className="text-muted small")
                    ]) 
                ], className="shadow-sm mb-4")
            ], width=4),
            
            # Column 2: Sampling Modes
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Sampling Modes (Logic)", className="fw-bold bg-info text-white"),
                    dbc.CardBody(id="sampling-mode-container", children=[
                        html.P("Waiting for update...", className="text-muted small")
                    ]) 
                ], className="shadow-sm mb-4")
            ], width=4),

            # Column 3: Sampling States
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader("Sampling States (Validation)", className="fw-bold bg-secondary text-white"),
                    dbc.CardBody(id="sampling-state-container", children=[
                        html.P("Waiting for update...", className="text-muted small")
                    ]) 
                ], className="shadow-sm mb-4")
            ], width=4),
        ]),

        # Bottom: Conditions
        dbc.Card([
            dbc.CardHeader("Active Conditions (Raw Criteria)", className="fw-bold"),
            dbc.CardBody([
                dag.AgGrid(
                    id="conditions-table",
                    columnDefs=[
                        {"field": "name", "headerName": "Condition", "flex": 2},
                        {"field": "status", "headerName": "Status", "flex": 1},
                        {"field": "time", "headerName": "Last Update", "flex": 1},
                    ],
                    rowData=[],
                )
            ])
        ], className="shadow-sm"),

        WebSocket(id="ws-system-ops", url=ws_url),
        # Central store to keep track of the cumulative state of the cluster
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
    Input("ws-system-ops", "message") ,
    State("system-ops-state", "data")
)
def update_state_store(msg, current_state):
    if not msg or "data" not in msg:
        raise PreventUpdate

    try:
        payload = json.loads(msg["data"])
        # Manager broadcasts carry type and ce.data
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
        L.error(f"Store update error: {e}")
    return no_update

@callback(
    Output("system-mode-container", "children"),
    Output("sampling-mode-container", "children"),
    Output("sampling-state-container", "children"),
    Output("conditions-table", "rowData"),
    Input("system-ops-state", "data")
)
def render_ui(state):
    # --- 1. Render System Modes ---
    sys_list = [
        dbc.ListGroupItem([
            html.Span(name, className="fw-bold"),
            dbc.Badge("ACTIVE", color="success", className="float-end") if data["status"] else dbc.Badge("OFF", color="light", className="float-end text-dark")
        ], active=data["status"]) for name, data in state["SystemMode"].items()
    ]

    # --- 2. Render Sampling Modes ---
    samp_mode_list = [
        dbc.ListGroupItem([
            html.Span(name),
            dbc.Badge("TRUE", color="info", className="float-end") if data["status"] else dbc.Badge("FALSE", color="danger", className="float-end")
        ]) for name, data in state["SamplingMode"].items()
    ]

    # --- 3. Render Sampling States ---
    state_list = [
        dbc.ListGroupItem([
            html.Span(name, className="small"),
            html.I(className="bi bi-check-circle-fill text-success float-end") if data["status"] else html.I(className="bi bi-x-circle text-muted float-end")
        ]) for name, data in state["SamplingState"].items()
    ]

    # --- 4. Conditions Table ---
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