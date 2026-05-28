import dash
import json
import logging
from dash import html, dcc, callback, Input, Output, State, ctx
import dash_bootstrap_components as dbc
from dash_extensions import WebSocket
from pydantic import BaseSettings
from ulid import ULID

L = logging.getLogger(__name__)

dash.register_page(
    __name__,
    path_template="/deployment/<deployment_id>",
    title="Deployment Command & Control",
    nav_bar=False # Hidden from sidebar; accessed via Home page
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

# --- LAYOUT ---
def layout(deployment_id=None):
    if not deployment_id:
        return html.Div("No Deployment ID provided.", className="p-4 text-danger")

    return html.Div([
        dbc.Row([
            dbc.Col(html.H2(f"Deployment: {deployment_id}", className="text-primary")),
            dbc.Col(html.Div(id="live-system-mode-badge", className="float-end mt-2"))
        ], className="mb-4 mt-3"),

        dbc.Row([
            # --- LEFT COLUMN: C2 & Sampling Operations Health ---
            dbc.Col([
                # Command & Control Panel
                dbc.Card([
                    dbc.CardHeader(html.H5("Command & Control", className="mb-0")),
                    dbc.CardBody([
                        html.P("Set the overarching operational mode for this deployment.", className="text-muted small"),
                        dbc.ButtonGroup([
                            dbc.Button("AUTO", id="btn-mode-auto", color="success", outline=True, className="fw-bold"),
                            dbc.Button("MANUAL", id="btn-mode-manual", color="warning", outline=True, className="fw-bold"),
                        ], className="w-100 mb-3"),
                        html.Hr(),
                        html.P("Manual Overrides", className="text-muted small"),
                        dbc.Button("Trigger Calibration", id="btn-trigger-cal", color="secondary", size="sm", className="w-100 mb-2", disabled=True),
                        dbc.Button("Initiate Flow Check", id="btn-trigger-flow", color="secondary", size="sm", className="w-100", disabled=True),
                    ])
                ], className="shadow-sm mb-4 border-dark"),

                # Sampling Operations Health
                dbc.Card([
                    dbc.CardHeader(html.H5("Operations Health", className="mb-0")),
                    dbc.CardBody([
                        html.Div(id="ops-health-container", children=html.P("Waiting for status events...", className="text-muted text-center"))
                    ])
                ], className="shadow-sm border-dark")
            ], width=4),

            # --- RIGHT COLUMN: Key Metrics & Telemetry ---
            dbc.Col([
                # KPI Banner (Met & Nav)
                dbc.Row([
                    dbc.Col(dbc.Card(dbc.CardBody([html.H6("Latitude", className="text-muted"), html.H3(id="kpi-lat", children="--")])), width=4),
                    dbc.Col(dbc.Card(dbc.CardBody([html.H6("Longitude", className="text-muted"), html.H3(id="kpi-lon", children="--")])), width=4),
                    dbc.Col(dbc.Card(dbc.CardBody([html.H6("True Wind", className="text-muted"), html.H3(id="kpi-wind", children="--")])), width=4),
                ], className="mb-4"),

                # Navigation Links to lower-level plots
                dbc.Card([
                    dbc.CardHeader(html.H5("Data & Telemetry Links", className="mb-0")),
                    dbc.CardBody([
                        dbc.ListGroup([
                            dbc.ListGroupItem(
                                "View Variableset Plots", 
                                # Let Dash handle the prefix automatically
                                href=dash.get_relative_path(f"/deployment/{deployment_id}/variablesets"), 
                                action=True,
                                color="info",
                                className="fw-bold"
                            ),
                            dbc.ListGroupItem(
                                "View Raw Asset Telemetry", 
                                href=dash.get_relative_path("/assets"), 
                                action=True,
                                className="fw-bold"
                            )
                        ])
                    ])
                ], className="shadow-sm border-dark")
            ], width=8)
        ]),

        # --- WEBSOCKETS & STATE ---
        # Note: Keep the backend path mapping (/msp/dashboardtest) exactly as your FastAPI server expects it
        WebSocket(id="ws-deployment-c2", url=f"{ws_url_base}/envds/envops/ws/deployment/{deployment_id}/c2"),
        WebSocket(id="ws-deployment-telemetry", url=f"{ws_url_base}/envds/envops/ws/deployment/{deployment_id}/telemetry"),
        html.Div(id="ws-c2-send-buffer", style={"display": "none"}),
        dcc.Store(id="store-deployment-id", data=deployment_id)
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
    """Generates the envds.control.request event for the Auto/Manual toggle."""
    if not ctx.triggered:
        return dash.no_update
        
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    requested_mode = "auto" if button_id == "btn-mode-auto" else "manual"

    # Construct the CloudEvent payload based on envds event schemas
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
    
    L.info(f"Issuing C2 Request: Switch to {requested_mode.upper()} mode.")
    return json.dumps(event)

@callback(
    Output("ws-deployment-c2", "send"), 
    Input("ws-c2-send-buffer", "children")
)
def send_c2_request(payload):
    """Pushes the requested CloudEvent down the WebSocket to the backend ConnectionManager."""
    if payload:
        return payload
    return dash.no_update

@callback(
    Output("live-system-mode-badge", "children"),
    Output("ops-health-container", "children"),
    Input("ws-deployment-c2", "message"),
    prevent_initial_call=True
)
def update_operations_health(message):
    """Listens to status updates to render the state machine health."""
    if not message or "data" not in message:
        return dash.no_update, dash.no_update
        
    try:
        # This is now ce.data (envdsStatus format), NOT the full CloudEvent
        status_data = json.loads(message["data"])
        
        app_group = status_data.get("id", {}).get("app_group", "")
        state_dict = status_data.get("state", {})
        
        # 1. Update Header Badge if this is a System Mode update
        badge = dash.no_update
        if app_group == "systemmode" or "system_mode" in state_dict:
            current_mode = state_dict.get("system_mode", {}).get("actual", "UNKNOWN")
            badge_color = "success" if current_mode.lower() == "auto" else "warning"
            badge = dbc.Badge(f"SYSTEM MODE: {current_mode.upper()}", color=badge_color, className="p-2 fs-6")
            
        # 2. Format the Operations Health (States, Conditions)
        # You can expand this into a more robust UI tree component later
        health_ui = html.Pre(json.dumps(status_data, indent=2), style={"fontSize": "12px"})
        
        return badge, health_ui
            
    except Exception as e:
        L.error(f"Error parsing status update: {e}")
        
    return dash.no_update, dash.no_update