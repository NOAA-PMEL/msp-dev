import dash
from dash import html, dcc, Input, Output, State
import dash_bootstrap_components as dbc
import httpx
import logging
from pydantic import BaseSettings

L = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Configuration & Helpers
# -----------------------------------------------------------------------------
class Settings(BaseSettings):
    daq_id: str = "mspbase01"
    class Config:
        env_prefix = "ENVOPS_"
        case_sensitive = False

config = Settings()
datastore_url = f"datastore.{config.daq_id}-system.svc.cluster.local"

def get_registry_data(endpoint: str):
    """Fetches data from the datastore for the dynamic sidebar."""
    url = f"http://{datastore_url}/{endpoint}"
    try:
        timeout = httpx.Timeout(5.0)
        response = httpx.get(url, timeout=timeout)
        if response.status_code == 200:
            data = response.json()
            if "results" in data and data["results"]:
                return data["results"]
    except Exception as e:
        L.error(f"Sidebar fetch failed for {endpoint}: {e}")
    return []

app = dash.Dash(
    __name__,
    use_pages=True,
    requests_pathname_prefix="/envds/envops/", 
)

# -----------------------------------------------------------------------------
# Sidebar Components
# -----------------------------------------------------------------------------
sidebar_header = dbc.Row([
    dbc.Col(html.H4("EnvOps", className="display-6 fw-bold")),
    dbc.Col(
        html.Button(
            html.Span(className="navbar-toggler-icon"),
            className="navbar-toggler",
            id="sidebar-toggle",
        ),
        width="auto",
        align="center",
    ),
], className="mb-4")

sidebar = html.Div([
    sidebar_header,
    dbc.Collapse([
        # 1. Static Pages (Home, Settings, etc.)
        dbc.Nav(
            [
                dbc.NavLink(
                    [html.I(className="bi bi-grid-1x2-fill me-2"), page["title"]],
                    href=page["relative_path"],
                    active="exact",
                    className="mb-2 rounded shadow-sm"
                )
                for page in dash.page_registry.values()
                if page.get("nav_bar", True)
            ],
            vertical=True,
            pills=True,
            className="mb-3"
        ),
        
        html.Hr(className="text-secondary"),
        html.H6("Active Missions", className="text-muted small text-uppercase fw-bold px-2 mb-3"),
        
        # 2. Dynamic Hierarchy Container
        html.Div(id="sidebar-mission-hierarchy"),
        
        # 3. Polling interval for sidebar updates (every 60s)
        dcc.Interval(id="sidebar-refresh-interval", interval=60000, n_intervals=0)
        
    ], id="sidebar-collapse"),
], id="sidebar")

# -----------------------------------------------------------------------------
# Main Application Shell
# -----------------------------------------------------------------------------
app.layout = html.Div([
    dcc.Location(id="url"),
    sidebar,
    html.Div([
        dash.page_container
    ], id="page-content")
])

# -----------------------------------------------------------------------------
# Callbacks
# -----------------------------------------------------------------------------
@app.callback(
    Output("sidebar-collapse", "is_open"),
    Input("sidebar-toggle", "n_clicks"),
    State("sidebar-collapse", "is_open"),
)
def toggle_collapse(n, is_open):
    if n:
        return not is_open
    return is_open

@app.callback(
    Output("sidebar-mission-hierarchy", "children"),
    Input("sidebar-refresh-interval", "n_intervals")
)
def update_sidebar_hierarchy(n):
    """Builds the dynamic project/deployment hierarchy for the sidebar."""
    deployments = get_registry_data("deployment-definition/registry/get/")
    projects = get_registry_data("project-definition/registry/get/")
    
    if not deployments or not projects:
        return html.P("No active missions found.", className="text-muted small px-2")
        
    project_map = {p.get("metadata", {}).get("name"): p for p in projects}
    
    # Group deployments by project
    projects_grouped = {}
    for dep in deployments:
        proj_ref = dep.get("data", {}).get("project_ref", "unassigned")
        if proj_ref not in projects_grouped:
            projects_grouped[proj_ref] = []
        projects_grouped[proj_ref].append(dep)
        
    accordion_items = []
    for proj_ref, deps in projects_grouped.items():
        # Get friendly project name
        proj_name = project_map.get(proj_ref, {}).get("data", {}).get("display_name", proj_ref)
        
        # Create NavLinks for each deployment under this project
        nav_links = []
        for d in deps:
            dep_id = d.get("metadata", {}).get("name")
            dep_name = d.get("data", {}).get("display_name", dep_id)
            
            nav_links.append(
                dbc.NavLink(
                    [html.I(className="bi bi-hdd-network me-2"), dep_name],
                    href=f"/envds/envops/deployment/{dep_id}/ops",
                    active="exact",
                    className="small py-1 text-truncate rounded"
                )
            )
            
        accordion_items.append(
            dbc.AccordionItem(
                dbc.Nav(nav_links, vertical=True, pills=True),
                title=proj_name,
                class_name="bg-transparent border-0 px-0",
            )
        )
        
    return dbc.Accordion(accordion_items, flush=True, start_collapsed=False, className="sidebar-accordion")

if __name__ == "__main__":
    app.run_server(debug=True)