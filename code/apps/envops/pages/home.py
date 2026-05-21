import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import httpx
import logging
from pydantic import BaseSettings

dash.register_page(__name__, path='/', title="EnvOps - Fleet Overview", order=0)

L = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# 1. Configuration & API Setup
# -----------------------------------------------------------------------------
class Settings(BaseSettings):
    daq_id: str = "default"
    
    class Config:
        env_prefix = "ENVOPS_"
        case_sensitive = False

config = Settings()
# Build the internal Kubernetes DNS path to the datastore service
datastore_url = f"datastore.{config.daq_id}-system.svc.cluster.local"

def get_registry_data(endpoint: str):
    """Generic helper to fetch data from the datastore registry."""
    url = f"http://{datastore_url}/{endpoint}"
    try:
        timeout = httpx.Timeout(10.0)
        response = httpx.get(url, timeout=timeout)
        if response.status_code == 200:
            data = response.json()
            if "results" in data and data["results"]:
                return data["results"]
    except Exception as e:
        L.error(f"Failed to fetch {endpoint} from datastore: {e}")
    return []

# -----------------------------------------------------------------------------
# 2. Helper Functions for UI Components
# -----------------------------------------------------------------------------
def get_health_badge(status):
    if status == "Healthy":
        return dbc.Badge("Healthy", color="success", className="ms-2")
    elif status == "Warning":
        return dbc.Badge("Warning", color="warning", text_color="dark", className="ms-2")
    elif status == "Offline":
        return dbc.Badge("Offline", color="danger", className="ms-2")
    return dbc.Badge("Unknown", color="secondary", className="ms-2")

def create_platform_card(platform_def, project_name="Unknown Project"):
    # Extract data safely based on the platform_defs.json schema
    meta_name = platform_def.get("metadata", {}).get("name", "unknown_id")
    data = platform_def.get("data", {})
    display_name = data.get("display_name", meta_name)
    platform_type = data.get("platform_type", "Unknown Type")
    
    # Placeholder operational states (to be wired to MQTT later)
    health = "Unknown"
    system_mode = "Awaiting Telemetry..."
    active_alarms = 0
    last_comms = "--"

    return dbc.Card([
        dbc.CardHeader([
            html.H5(display_name, className="mb-0 d-inline-block text-truncate", style={"maxWidth": "70%"}),
            get_health_badge(health)
        ], className="d-flex justify-content-between align-items-center bg-dark text-white"),
        
        dbc.CardBody([
            html.H6(f"Project: {project_name}", className="card-subtitle text-muted mb-3 text-truncate"),
            
            dbc.Row([
                dbc.Col(html.B("Type:"), width=5),
                dbc.Col(platform_type)
            ], className="mb-1"),

            dbc.Row([
                dbc.Col(html.B("System Mode:"), width=5),
                dbc.Col(system_mode, className="fst-italic text-muted")
            ], className="mb-1"),
            
            dbc.Row([
                dbc.Col(html.B("Active Alarms:"), width=5),
                dbc.Col(
                    dbc.Badge(active_alarms, color="secondary", pill=True)
                )
            ], className="mb-1"),
            
            dbc.Row([
                dbc.Col(html.B("Last Comms:"), width=5),
                dbc.Col(last_comms, className="small text-muted")
            ], className="mb-3"),
            
            # Button for drilling down into the platform operations
            dbc.Button(
                "View Operations", 
                color="primary", 
                className="w-100 mt-auto shadow-sm",
                href=f"/msp/envops/platform/{meta_name}/ops" 
            )
        ])
    ], className="shadow-sm h-100 border-0")

# -----------------------------------------------------------------------------
# 3. Main Layout Shell
# -----------------------------------------------------------------------------
layout = html.Div([
    # Page Header
    dbc.Row([
        dbc.Col([
            html.H2("Fleet Overview", className="fw-bold"),
            html.P("Real-time status of all deployed platforms and projects.", className="text-muted")
        ])
    ], className="mb-4"),
    
    # Dynamic Containers
    html.Div(id="fleet-metrics-container"),
    html.Div(id="fleet-grid-container"),

    # Interval timer to refresh the page data automatically
    dcc.Interval(id="fleet-refresh-interval", interval=30000, n_intervals=0)
], className="mt-2")


# -----------------------------------------------------------------------------
# 4. Callbacks
# -----------------------------------------------------------------------------
@callback(
    Output("fleet-metrics-container", "children"),
    Output("fleet-grid-container", "children"),
    Input("fleet-refresh-interval", "n_intervals")
)
def update_fleet_dashboard(n):
    # Fetch actual data from Datastore
    # Adjust these endpoints if your datastore exposes them differently!
    platforms = get_registry_data("platform-definition/registry/get/")
    projects = get_registry_data("project/registry/get/")

    # If the datastore is unreachable or empty, show a warning
    if not platforms:
        metrics = dbc.Alert("No platforms found or Datastore unreachable.", color="warning")
        return metrics, html.Div()

    # Build a quick lookup for project names
    project_map = {}
    for p in projects:
        p_name = p.get("metadata", {}).get("name")
        p_display = p.get("data", {}).get("display_name", p_name)
        if p_name:
            project_map[p_name] = p_display

    # Calculate Top Level Metrics
    total_platforms = len(platforms)
    
    metrics_row = dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([html.H4("Total Platforms"), html.H2(str(total_platforms))]), className="shadow-sm border-0 bg-light text-center"), width=4),
        dbc.Col(dbc.Card(dbc.CardBody([html.H4("Healthy"), html.H2("--", className="text-secondary")]), className="shadow-sm border-0 bg-light text-center"), width=4),
        dbc.Col(dbc.Card(dbc.CardBody([html.H4("Critical Alerts"), html.H2("--", className="text-secondary")]), className="shadow-sm border-0 bg-light text-center"), width=4),
    ], className="mb-5")

    # Render Platform Grid
    # For now, we will assign the first project to the platforms if a hard link isn't established in the schema yet.
    default_project = projects[0].get("data", {}).get("display_name", "Unknown Project") if projects else "Unknown Project"

    grid_row = dbc.Row([
        dbc.Col(create_platform_card(p, project_name=default_project), width=12, md=6, lg=4, className="mb-4")
        for p in platforms
    ])

    return metrics_row, grid_row