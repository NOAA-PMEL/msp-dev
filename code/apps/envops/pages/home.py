import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import httpx
import logging
from pydantic import BaseSettings

dash.register_page(__name__, path='/', title="EnvOps - Active Deployments", order=0)

L = logging.getLogger(__name__)

class Settings(BaseSettings):
    daq_id: str = "default"
    class Config:
        env_prefix = "ENVOPS_"
        case_sensitive = False

config = Settings()
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
# UI Component Generators
# -----------------------------------------------------------------------------
def get_status_badge(status):
    status = str(status).lower()
    if status == "active":
        return dbc.Badge("Active", color="success", className="ms-2")
    elif status == "planned":
        return dbc.Badge("Planned", color="info", className="ms-2")
    elif status == "completed":
        return dbc.Badge("Completed", color="secondary", className="ms-2")
    return dbc.Badge(status.capitalize(), color="warning", text_color="dark", className="ms-2")

def create_deployment_card(deployment, project_info, platform_info):
    dep_meta = deployment.get("metadata", {})
    dep_data = deployment.get("data", {})
    
    dep_id = dep_meta.get("name", "Unknown ID")
    status = dep_data.get("deployment_status", "unknown")
    start_time = dep_data.get("start_time", "TBD")[:10]  # Just grab the YYYY-MM-DD
    
    # Extract friendly names from the mapped dictionaries
    project_name = project_info.get("data", {}).get("display_name", dep_data.get("project_id", "Unknown Project"))
    platform_name = platform_info.get("data", {}).get("display_name", dep_data.get("platform_id", "Unknown Platform"))
    
    return dbc.Card([
        dbc.CardHeader([
            html.H5(platform_name, className="mb-0 d-inline-block text-truncate", style={"maxWidth": "75%"}),
            get_status_badge(status)
        ], className="d-flex justify-content-between align-items-center bg-dark text-white"),
        
        dbc.CardBody([
            html.H6(project_name, className="card-subtitle text-primary fw-bold mb-3 text-truncate"),
            
            html.P(dep_data.get("description", "No description available."), className="small text-muted mb-4", style={"height": "40px", "overflow": "hidden"}),
            
            dbc.Row([
                dbc.Col(html.B("Deployment ID:"), width=5),
                dbc.Col(html.Span(dep_id, className="text-muted small"), className="text-truncate")
            ], className="mb-1"),

            dbc.Row([
                dbc.Col(html.B("Start Date:"), width=5),
                dbc.Col(start_time, className="text-muted small")
            ], className="mb-3"),
            
            # Action Buttons
            dbc.Row([
                dbc.Col(
                    dbc.Button(
                        "Ops Dashboard", 
                        color="primary", 
                        className="w-100 shadow-sm",
                        href=f"/envds/envops/deployment/{dep_id}/ops" 
                    ), width=6, className="pe-1"
                ),
                dbc.Col(
                    dbc.Button(
                        "Platform Details", 
                        color="outline-secondary", 
                        className="w-100 shadow-sm",
                        href=f"/envds/envops/platform/{dep_data.get('platform_id')}" 
                    ), width=6, className="ps-1"
                )
            ])
        ])
    ], className="shadow-sm h-100 border-0")

# -----------------------------------------------------------------------------
# Main Layout Shell
# -----------------------------------------------------------------------------
layout = html.Div([
    dbc.Row([
        dbc.Col([
            html.H2("Active Deployments", className="fw-bold"),
            html.P("Overview of current missions, projects, and active field operations.", className="text-muted")
        ])
    ], className="mb-4"),
    
    html.Div(id="home-metrics-container"),
    html.Div(id="home-grid-container"),
    
    dcc.Interval(id="home-refresh-interval", interval=30000, n_intervals=0)
], className="mt-2")


# -----------------------------------------------------------------------------
# Callbacks
# -----------------------------------------------------------------------------
@callback(
    Output("home-metrics-container", "children"),
    Output("home-grid-container", "children"),
    Input("home-refresh-interval", "n_intervals")
)
@callback(
    Output("home-metrics-container", "children"),
    Output("home-grid-container", "children"),
    Input("home-refresh-interval", "n_intervals")
)
def update_home_dashboard(n):
    # 1. Fetch the necessary data using the STRICT dynamically generated endpoints
    deployments = get_registry_data("deployment-definition/registry/get/")
    projects = get_registry_data("project-definition/registry/get/")
    platforms = get_registry_data("platform-definition/registry/get/")

    if not deployments:
        return dbc.Alert("No deployments found or Datastore unreachable.", color="warning"), html.Div()

    # 2. Build quick-lookup dictionaries for cross-referencing IDs to friendly names
    project_map = {p.get("metadata", {}).get("name"): p for p in projects}
    platform_map = {p.get("metadata", {}).get("name"): p for p in platforms}

    # 3. Calculate Metrics
    total_deps = len(deployments)
    active_deps = sum(1 for d in deployments if str(d.get("data", {}).get("deployment_status", d.get("data", {}).get("status", ""))).lower() == "active")
    planned_deps = sum(1 for d in deployments if str(d.get("data", {}).get("deployment_status", d.get("data", {}).get("status", ""))).lower() == "planned")

    metrics_row = dbc.Row([
        dbc.Col(dbc.Card(dbc.CardBody([html.H4("Total Deployments"), html.H2(str(total_deps))]), className="shadow-sm border-0 bg-light text-center"), width=4),
        dbc.Col(dbc.Card(dbc.CardBody([html.H4("Active"), html.H2(str(active_deps), className="text-success")]), className="shadow-sm border-0 bg-light text-center"), width=4),
        dbc.Col(dbc.Card(dbc.CardBody([html.H4("Planned"), html.H2(str(planned_deps), className="text-info")]), className="shadow-sm border-0 bg-light text-center"), width=4),
    ], className="mb-5")

    # 4. Build the Deployment Grid
    grid_row = dbc.Row([
        dbc.Col(
            create_deployment_card(
                deployment=dep,
                project_info=project_map.get(dep.get("data", {}).get("project_ref"), {}),
                platform_info=platform_map.get(dep.get("data", {}).get("platform_ref"), {})
            ), 
            width=12, md=6, lg=4, className="mb-4"
        )
        for dep in deployments
    ])

    return metrics_row, grid_row