import dash
from dash import html, dcc, callback, Input, Output, State
import dash_bootstrap_components as dbc
from dash_extensions import WebSocket
import json
import logging

dash.register_page(__name__, path='/', title="EnvOps | Fleet Overview", order=0)

L = logging.getLogger(__name__)

# --- MOCK DATA --- 
# In production, fetch this via httpx from your datastore (like in your old code)
projects_data = [
    {"id": "project.pmel.beacons", "name": "BEACONS", "description": "Broad Evaluation of Aerosol-Cloud interactions"},
    {"id": "project.pmel.aeromarine", "name": "AEROMARINE", "description": "Marine aerosol baseline study"},
]

platforms_data = [
    {"id": "platform.pmel.payload_01", "name": "MSP Payload 01", "type": "MSP_Payload", "deployed": True, "project_id": "project.pmel.beacons", "health": "Healthy"},
    {"id": "platform.pmel.payload_02", "name": "MSP Payload 02", "type": "MSP_Payload", "deployed": False, "project_id": None, "health": "Offline"},
    {"id": "platform.pmel.payload_03", "name": "MSP Payload 03", "type": "MSP_Payload", "deployed": True, "project_id": "project.pmel.beacons", "health": "Warning"},
    {"id": "platform.pmel.enclosure_01", "name": "Ship Enclosure A", "type": "MSP_Enclosure", "deployed": True, "project_id": "project.pmel.aeromarine", "health": "Healthy"},
]

def create_platform_card(platform):
    """Generates a summary card for a single platform."""
    health_colors = {"Healthy": "success", "Warning": "warning", "Critical": "danger", "Offline": "secondary"}
    color = health_colors.get(platform["health"], "light")
    icon = "bi bi-check-circle-fill" if platform["health"] == "Healthy" else "bi bi-exclamation-triangle-fill"
    
    return dbc.Card(
        [
            dbc.CardHeader([
                html.H6(platform["name"], className="mb-0 fw-bold"),
            ]),
            dbc.CardBody([
                html.P([html.I(className="bi bi-box me-2"), f"{platform['type']}"], className="card-text text-muted mb-2 small"),
                html.H5([
                    html.I(className=f"{icon} text-{color} me-2"), 
                    platform['health']
                ], className="mb-3", id=f"health-text-{platform['id']}"),
                
                dbc.Button("Monitor Details", color="primary", outline=True, size="sm", href=f"/system-ops?platform={platform['id']}", className="w-100") 
                if platform['deployed'] else 
                dbc.Button("Standby", color="secondary", outline=True, size="sm", disabled=True, className="w-100")
            ])
        ],
        className=f"shadow-sm h-100 border-{color}" if platform['deployed'] else "shadow-sm h-100 border-0 bg-light"
    )

def layout():
    project_items = []
    for proj in projects_data:
        proj_platforms = [p for p in platforms_data if p["project_id"] == proj["id"]]
        platform_cards = dbc.Row(
            [dbc.Col(create_platform_card(p), width=12, md=6, lg=4, xl=3, className="mb-3") for p in proj_platforms],
        ) if proj_platforms else html.P("No platforms currently deployed.", className="text-muted")
        
        project_items.append(
            dbc.AccordionItem(
                [
                    html.P(proj["description"], className="text-muted mb-3"),
                    platform_cards
                ],
                title=f"🚀 Project: {proj['name']}  ({len(proj_platforms)} Platforms Active)",
                item_id=proj["id"]
            )
        )

    standby_platforms = [p for p in platforms_data if not p["deployed"]]
    standby_cards = dbc.Row([dbc.Col(create_platform_card(p), width=12, md=6, lg=4, xl=3, className="mb-3") for p in standby_platforms])

    return html.Div([
        html.H2("Fleet Overview", className="mb-4 fw-bold"),
        
        dbc.Card([
            dbc.CardBody([
                html.H5("Active Deployments", className="card-title text-primary fw-bold"),
                html.Hr(),
                dbc.Accordion(project_items, always_open=True, active_item=[p["id"] for p in projects_data])
            ])
        ], className="shadow-sm mb-5 border-0"),
        
        dbc.Card([
            dbc.CardBody([
                html.H5("Platform Inventory (Standby)", className="card-title text-secondary fw-bold"),
                html.Hr(),
                standby_cards if standby_platforms else html.P("No standby platforms.", className="text-muted")
            ])
        ], className="shadow-sm border-0"),
        
        # WebSocket listening to the FastAPI backend
        WebSocket(id="ws-envops-telemetry", url="ws://localhost:8080/ws/telemetry"),
        dcc.Store(id="fleet-health-store", data={})
    ], className="p-2")

@callback(
    Output("fleet-health-store", "data"),
    Input("ws-envops-telemetry", "message"),
    prevent_initial_call=True
)
def process_telemetry(msg):
    if not msg or "data" not in msg:
        raise dash.exceptions.PreventUpdate
    try:
        # Example: {"platform.pmel.payload_03": {"health": "Critical"}}
        return json.loads(msg["data"])
    except:
        raise dash.exceptions.PreventUpdate