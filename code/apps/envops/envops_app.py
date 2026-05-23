import dash
from dash import html, dcc, Input, Output, State
import dash_bootstrap_components as dbc
import httpx
# from cachetools import cached, TTLCache
import logging
import traceback
from pydantic import BaseSettings

from utils import get_registry_data, config

L = logging.getLogger(__name__)

# class Settings(BaseSettings):
#     daq_id: str = "mspbase01"
#     class Config:
#         env_prefix = "ENVOPS_"
#         case_sensitive = False

# config = Settings()
# datastore_url = f"datastore.{config.daq_id}-system.svc.cluster.local"

# registry_cache = TTLCache(maxsize=128, ttl=300)

# @cached(cache=registry_cache)
# def get_registry_data(endpoint: str):
#     """Safely fetches data using httpx, heavily cached to protect the datastore."""
#     url = f"http://{datastore_url}/{endpoint}"
#     L.debug("Cache miss! Re-fetching registry data from datastore", extra={"fetch_url": url})
    
#     try:
#         with httpx.Client() as client:
#             response = client.get(url, timeout=5.0)
            
#         if response.status_code == 200:
#             data = response.json()
#             results = data.get("results", [])
#             L.debug("Successfully parsed registry items", extra={"endpoint": endpoint, "count": len(results)})
#             return results
#         else:
#             L.error("API returned non-200 code", extra={"status": response.status_code, "body": response.text})
            
#     except httpx.RequestError as e:
#         L.error("CONNECTION ERROR during fetch", extra={"fetch_url": url, "failure_detail": str(e)})
#     except Exception as e:
#         L.error("Unexpected fetch failure", extra={"endpoint": endpoint, "failure_detail": str(e)})
        
#     return []

app = dash.Dash(
    __name__,
    use_pages=True,
    requests_pathname_prefix="/envds/envops/", 
)

# -----------------------------------------------------------------------------
# Layout Generator
# -----------------------------------------------------------------------------
def serve_layout():
    registered_pages = list(dash.page_registry.keys())
    L.info(f"[DEBUG SIDEBAR] Building layout. Registered pages found: {registered_pages}")
    
    sidebar_header = dbc.Row([
        dbc.Col(html.H4("EnvOps", className="display-6 fw-bold mb-0")),
        dbc.Col(
            dbc.Button(
                html.I(className="bi bi-list fs-3"), 
                color="link", 
                className="p-0 text-dark",
                id="sidebar-toggle",
            ),
            width="auto", align="center",
        ),
    ], className="mb-4 align-items-center")

    sidebar = html.Div([
        sidebar_header,
        dbc.Collapse([
            dbc.Nav(
                [
                    dbc.NavLink(
                        [html.I(className="bi bi-grid-1x2-fill me-2"), page["title"]],
                        href=page["relative_path"], active="exact", className="mb-2 rounded shadow-sm"
                    )
                    for page in dash.page_registry.values() if page.get("nav_bar", True)
                ],
                vertical=True, pills=True, className="mb-3"
            ),
            html.Hr(className="text-secondary"),
            html.H6("Active Missions", className="text-muted small text-uppercase fw-bold px-2 mb-3"),
            html.Div(id="sidebar-mission-hierarchy"),
            dcc.Interval(id="sidebar-refresh-interval", interval=60000, n_intervals=0)
        ], id="sidebar-collapse", is_open=True), # <-- FIX: Forces menu open on load!
    ], id="sidebar")

    return html.Div([
        dcc.Location(id="url"),
        sidebar,
        html.Div([dash.page_container], id="page-content")
    ])

app.layout = serve_layout

# -----------------------------------------------------------------------------
# Callbacks
# -----------------------------------------------------------------------------
@app.callback(
    Output("sidebar-collapse", "is_open"),
    Input("sidebar-toggle", "n_clicks"),
    State("sidebar-collapse", "is_open"),
)
def toggle_collapse(n, is_open):
    if n: return not is_open
    return is_open

@app.callback(
    Output("sidebar-mission-hierarchy", "children"),
    Input("sidebar-refresh-interval", "n_intervals")
)
def update_sidebar_hierarchy(n):
    L.info(f"[DEBUG SIDEBAR] Callback triggered for Active Missions (n_intervals={n})")
    try:
        deployments = get_registry_data("deployment-definition/registry/get/")
        projects = get_registry_data("project-definition/registry/get/")
        
        if not deployments or not projects:
            L.warning("[DEBUG SIDEBAR] Missing deployments or projects. Rendering 'No active missions'.")
            return html.P("No active missions found.", className="text-muted small px-2")
            
        L.info(f"[DEBUG SIDEBAR] Grouping {len(deployments)} deployments into {len(projects)} projects...")
        project_map = {p.get("metadata", {}).get("name"): p for p in projects}
        
        projects_grouped = {}
        for dep in deployments:
            proj_ref = dep.get("data", {}).get("project_ref", "unassigned")
            if proj_ref not in projects_grouped:
                projects_grouped[proj_ref] = []
            projects_grouped[proj_ref].append(dep)
            
        accordion_items = []
        for proj_ref, deps in projects_grouped.items():
            p_data = project_map.get(proj_ref, {}).get("data") or {}
            proj_name = p_data.get("display_name", proj_ref)
            
            nav_links = []
            for d in deps:
                dep_id = d.get("metadata", {}).get("name")
                dep_name = d.get("data", {}).get("display_name", dep_id)
                
                nav_links.append(
                    dbc.NavLink(
                        [html.I(className="bi bi-hdd-network me-2"), dep_name],
                        href=f"/envds/envops/deployment/{dep_id}/ops",
                        active="exact", className="small py-1 text-truncate rounded"
                    )
                )
                
            accordion_items.append(
                dbc.AccordionItem(
                    dbc.Nav(nav_links, vertical=True, pills=True),
                    title=proj_name, class_name="bg-transparent border-0 px-0",
                )
            )
        
        L.info("[DEBUG SIDEBAR] Successfully built Accordion items.")    
        return dbc.Accordion(accordion_items, flush=True, start_collapsed=False, className="sidebar-accordion")
        
    except Exception as e:
        L.error(f"[DEBUG SIDEBAR] Sidebar crash: {traceback.format_exc()}")
        return html.P("Sidebar Error", className="text-danger small px-2")

if __name__ == "__main__":
    app.run_server(debug=True)