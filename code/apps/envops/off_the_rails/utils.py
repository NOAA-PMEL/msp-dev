import httpx
import logging
import time
import traceback
import dash
from dash import html, dcc, Input, Output
import dash_bootstrap_components as dbc
from cachetools import cached, TTLCache
from pydantic import BaseSettings

L = logging.getLogger(__name__)

# --- Configuration ---
class Settings(BaseSettings):
    daq_id: str = "mspbase01"
    external_hostname: str = "mspbase01.pmel.noaa.gov"
    ws_port: str = "8080"
    ws_use_tls: str = "false"
    class Config:
        env_prefix = "ENVOPS_"
        case_sensitive = False

config = Settings()

# Base Datastore URL (Relies on standard K8s internal port 80 routing to 8080)
datastore_url = f"datastore.{config.daq_id}-system.svc.cluster.local"

# --- Network & Data Layer ---
# Shared memory cache: up to 128 unique endpoints, stored for 5 minutes (300s)
registry_cache = TTLCache(maxsize=128, ttl=300)

@cached(cache=registry_cache)
def get_registry_data(endpoint_or_resource: str):
    """
    Safely fetches data using httpx. Automatically translates shorthand 
    resource strings to unified registry API paths.
    """
    # Smart URL Builder translation
    if "/" not in endpoint_or_resource:
        url = f"http://{datastore_url}/{endpoint_or_resource}-definition/registry/get/"
    else:
        url = f"http://{datastore_url}/{endpoint_or_resource}"
    
    L.info(f"[TIMING] Cache miss! Initiating fetch to {url}")
    start_time = time.time()
    
    try:
        # 5 second timeout is plenty for internal cluster traffic
        with httpx.Client(timeout=5.0) as client:
            L.info(f"[TIMING] HTTPX Client opened, sending GET request...")
            response = client.get(url)
            
            elapsed = time.time() - start_time
            L.info(f"[TIMING] Response received in {elapsed:.3f} seconds. Status: {response.status_code}")
            
        if response.status_code == 200:
            data = response.json()
            results = data.get("results", [])
            L.info(f"[TIMING] Successfully parsed {len(results)} registry items.")
            return results
        else:
            L.warning(f"[TIMING] API returned non-200 code: {response.status_code}. Body: {response.text}")
            
    except httpx.ReadTimeout:
        elapsed = time.time() - start_time
        L.error(f"[TIMING] TIMEOUT! Datastore failed to respond after {elapsed:.3f} seconds.")
    except httpx.RequestError as e:
        elapsed = time.time() - start_time
        L.error(f"[TIMING] CONNECTION ERROR after {elapsed:.3f} seconds. Detail: {str(e)}")
    except Exception as e:
        elapsed = time.time() - start_time
        L.error(f"[TIMING] Unexpected fetch failure after {elapsed:.3f} seconds. Detail: {str(e)}")
        
    return []


# --- Shared UI Layout Shell ---
def create_unified_shell(page_content, active_item="home"):
    """Wraps any isolated Dash app layout with the unified global sidebar."""
    sidebar_header = dbc.Row([
        dbc.Col(html.H4("EnvOps", className="display-6 fw-bold mb-0")),
    ], className="mb-4 align-items-center")

    sidebar = html.Div([
        sidebar_header,
        dbc.Nav([
            # Standard hrefs navigate between completely separate FastAPI mounted apps
            dbc.NavLink(
                [html.I(className="bi bi-grid-1x2-fill me-2"), "Fleet Map"], 
                href="/envds/envops/", 
                active=(active_item=="home"), 
                className="mb-2 rounded shadow-sm"
            ),
        ], vertical=True, pills=True, className="mb-3"),
        
        html.Hr(className="text-secondary"),
        html.H6("Active Missions", className="text-muted small text-uppercase fw-bold px-2 mb-3"),
        
        # This container gets populated dynamically by register_sidebar_callbacks()
        html.Div(id="sidebar-mission-hierarchy"),
        dcc.Interval(id="sidebar-refresh-interval", interval=60000, n_intervals=0)
    ], id="sidebar")

    return html.Div([
        sidebar,
        html.Div(page_content, id="page-content")
    ])

def register_sidebar_callbacks(app: dash.Dash):
    """Registers the dynamic deployment sidebar logic onto an isolated Dash app."""
    @app.callback(
        Output("sidebar-mission-hierarchy", "children"),
        Input("sidebar-refresh-interval", "n_intervals")
    )
    def update_sidebar(n):
        try:
            # --- GLOBAL NAV LINKS ---
            global_links = html.Div([
                dbc.NavLink([html.I(className="bi bi-cpu me-2"), "Fleet Device Diagnostics"], 
                            href="/envds/envops/devices/", 
                            active="exact", className="fw-bold py-2 rounded mb-3 bg-light text-dark shadow-sm")
            ])

            deployments = get_registry_data("deployment") or []
            projects = get_registry_data("project") or []
            
            if not deployments or not projects:
                return html.Div([global_links, html.P("No active missions found.", className="text-muted small px-2")])
                
            project_map = {p.get("metadata", {}).get("name"): p for p in projects}
            
            # Root Deployment Filter
            platform_to_dep = {d.get("data", {}).get("platform_ref"): d for d in deployments}
            root_deps = [d for d in deployments if not d.get("data", {}).get("host_platform_ref") or d.get("data", {}).get("host_platform_ref") == d.get("data", {}).get("platform_ref") or d.get("data", {}).get("host_platform_ref") not in platform_to_dep]

            projects_grouped = {}
            for dep in root_deps:
                proj_ref = dep.get("data", {}).get("project_ref", "unassigned")
                projects_grouped.setdefault(proj_ref, []).append(dep)
                
            accordion_items = []
            for proj_ref, deps in projects_grouped.items():
                proj_name = project_map.get(proj_ref, {}).get("data", {}).get("display_name", proj_ref)
                nav_links = []
                
                for d in deps:
                    dep_id = d.get("metadata", {}).get("name")
                    dep_name = d.get("data", {}).get("display_name", dep_id)
                    
                    nav_links.append(html.Div([
                        html.Span(dep_name, className="fw-bold small d-block mb-1 text-dark"),
                        dbc.NavLink([html.I(className="bi bi-sliders me-2"), "Operations"], href=f"/envds/envops/ops/deployment/{dep_id}", active="exact", className="small py-1 text-truncate rounded ps-3"),
                        dbc.NavLink([html.I(className="bi bi-graph-up me-2"), "Analytics & Plots"], href=f"/envds/envops/plots/deployment/{dep_id}", active="exact", className="small py-1 text-truncate rounded ps-3 mb-3"),
                    ]))
                    
                accordion_items.append(dbc.AccordionItem(dbc.Nav(nav_links, vertical=True, pills=True), title=proj_name, class_name="bg-transparent border-0 px-0"))
            
            return html.Div([global_links, dbc.Accordion(accordion_items, flush=True, start_collapsed=False, className="sidebar-accordion")])
        except Exception as e:
            return html.P("Sidebar Error", className="text-danger small px-2")