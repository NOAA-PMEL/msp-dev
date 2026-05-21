import dash
from dash import html, dcc
import dash_bootstrap_components as dbc
import pandas as pd

dash.register_page(__name__, path='/', title="EnvOps - Fleet Overview", order=0)

# -----------------------------------------------------------------------------
# 1. Mock Data Structure (To be replaced by FastAPI/Datastore calls later)
# -----------------------------------------------------------------------------
mock_platforms = [
    {
        "id": "raz1",
        "project": "ATOMIC",
        "name": "Platform Alpha",
        "health": "Healthy",
        "system_mode": "Auto Sampling",
        "active_alarms": 0,
        "last_comms": "Just now",
        "location": "Tropical Atlantic"
    },
    {
        "id": "mspbase01",
        "project": "SAIL",
        "name": "Platform Beta",
        "health": "Warning",
        "system_mode": "Manual Control",
        "active_alarms": 2,
        "last_comms": "2 mins ago",
        "location": "Barbados"
    },
    {
        "id": "test-rig-01",
        "project": "Engineering",
        "name": "Lab Bench 1",
        "health": "Offline",
        "system_mode": "Standby",
        "active_alarms": 1,
        "last_comms": "4 hours ago",
        "location": "Seattle Lab"
    }
]

# -----------------------------------------------------------------------------
# 2. Helper Functions for UI Components
# -----------------------------------------------------------------------------
def get_health_badge(status):
    if status == "Healthy":
        return dbc.Badge("Healthy", color="success", className="ms-2")
    elif status == "Warning":
        return dbc.Badge("Warning", color="warning", text_color="dark", className="ms-2")
    return dbc.Badge("Offline", color="danger", className="ms-2")

def create_platform_card(platform):
    return dbc.Card([
        dbc.CardHeader([
            html.H5(platform["name"], className="mb-0 d-inline-block"),
            get_health_badge(platform["health"])
        ], className="d-flex justify-content-between align-items-center bg-dark text-white"),
        
        dbc.CardBody([
            html.H6(f"Project: {platform['project']}", className="card-subtitle text-muted mb-3"),
            
            dbc.Row([
                dbc.Col(html.B("System Mode:"), width=5),
                dbc.Col(platform["system_mode"])
            ], className="mb-1"),
            
            dbc.Row([
                dbc.Col(html.B("Active Alarms:"), width=5),
                dbc.Col(
                    dbc.Badge(platform["active_alarms"], color="danger" if platform["active_alarms"] > 0 else "success", pill=True)
                )
            ], className="mb-1"),
            
            dbc.Row([
                dbc.Col(html.B("Last Comms:"), width=5),
                dbc.Col(platform["last_comms"], className="small")
            ], className="mb-3"),
            
            # This button will eventually link to something like: href=f"/platform/{platform['id']}/ops"
            dbc.Button(
                "View Operations", 
                color="primary", 
                className="w-100 mt-auto",
                href=f"#" 
            )
        ])
    ], className="shadow-sm h-100 border-0")

# -----------------------------------------------------------------------------
# 3. Main Layout
# -----------------------------------------------------------------------------
layout = html.Div([
    # Page Header
    dbc.Row([
        dbc.Col([
            html.H2("Fleet Overview", className="fw-bold"),
            html.P("Real-time status of all deployed platforms and projects.", className="text-muted")
        ])
    ], className="mb-4"),
    
    # Top Level Metrics (Aggregated)
    dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([html.H4("Total Platforms"), html.H2(str(len(mock_platforms)))]), className="shadow-sm border-0 bg-light text-center"), width=4),
        dbc.Col(dbc.Card(dbc.CardBody([html.H4("Healthy"), html.H2("1", className="text-success")]), className="shadow-sm border-0 bg-light text-center"), width=4),
        dbc.Col(dbc.Card(dbc.CardBody([html.H4("Critical Alerts"), html.H2("3", className="text-danger")]), className="shadow-sm border-0 bg-light text-center"), width=4),
    ], className="mb-5"),

    # Platform Grid
    dbc.Row([
        dbc.Col(create_platform_card(p), width=12, md=6, lg=4, className="mb-4")
        for p in mock_platforms
    ])
], className="mt-2")