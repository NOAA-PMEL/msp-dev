import dash
import logging
from dash import html
import dash_bootstrap_components as dbc
import httpx
from pydantic import BaseSettings

L = logging.getLogger(__name__)

dash.register_page(
    __name__,
    path_template="/variablesets/<deployment_id>",
    title="Deployment Variablesets",
)

class Settings(BaseSettings):
    daq_id: str = "default"
    external_hostname: str = "localhost"
    ws_port: int = 80
    class Config:
        env_prefix = "ENVOPS_"
        case_sensitive = False

config = Settings()
datastore_url = f"datastore.{config.daq_id}-system.svc.cluster.local"

# def fetch_registry_data(resource_type: str):
#     url = f"http://{datastore_url}/{resource_type}-definition/registry/ids/get/"
#     docs = []
#     try:
#         timeout = httpx.Timeout(10.0)
#         id_response = httpx.get(url, timeout=timeout)
#         if id_response.status_code == 200:
#             ids = id_response.json().get("results", [])
#             for doc_id in ids:
#                 if doc_id:
#                     doc_url = f"http://{datastore_url}/{resource_type}-definition/registry/get/"
#                     doc_response = httpx.get(doc_url, params={"name": doc_id}, timeout=timeout) 
#                     if doc_response.status_code == 200:
#                         doc_results = doc_response.json().get("results", [])
#                         if doc_results: 
#                             # Safe for overlapping names across namespaces!
#                             docs.extend(doc_results)
#     except Exception as e:
#         L.error(f"Failed to fetch {resource_type} definitions: {e}")
#     return docs

def fetch_registry_data(resource_type: str, query_params: dict = None):
    """Fetches registry documents dynamically without N+1 looping."""
    if query_params is None: 
        query_params = {}
        
    url = f"http://{datastore_url}/{resource_type}-definition/registry/get/"
    try:
        timeout = httpx.Timeout(10.0)
        # RediSearch will return all matching documents directly based on the query!
        response = httpx.get(url, params=query_params, timeout=timeout) 
        if response.status_code == 200:
            return response.json().get("results", [])
    except Exception as e:
        L.error(f"Registry fetch failed for {resource_type}: {e}")
    return []

def get_bundle_varsets(host_id):
    deployments = fetch_registry_data("deployment")
    platforms = set()
    for dep in deployments:
        if dep.get("metadata", {}).get("name") == host_id:
            host_platform = dep.get("data", {}).get("platform_ref")
            if host_platform:
                platforms.add(host_platform)
                for sub in deployments:
                    if sub.get("data", {}).get("host_platform_ref") == host_platform:
                        platforms.add(sub.get("data", {}).get("platform_ref"))
            break

    url = f"http://{datastore_url}/variableset-definition/registry/ids/get/"
    try:
        response = httpx.get(url, timeout=10.0)
        all_vs_ids = response.json().get("results", []) if response.status_code == 200 else []
    except Exception:
        all_vs_ids = []

    active_varsets = {}
    for full_id in all_vs_ids:
        if not full_id: continue
        parts = full_id.split("::")
        if len(parts) >= 4:
            vs_platform = parts[0]
            if vs_platform in platforms:
                short_id = f"{parts[1]}::{parts[3]}"
                active_varsets[short_id] = full_id
    return active_varsets

def layout(deployment_id=None):
    if not deployment_id: 
        return html.Div("No Deployment ID provided.", className="p-4 text-danger")

    active_varsets = get_bundle_varsets(deployment_id)
    
    cards = []
    for short_id, full_id in active_varsets.items():
        var_names = []
        try:
            def_url = f"http://{datastore_url}/variableset-definition/registry/get/"
            resp = httpx.get(def_url, params={"variableset_definition_id": full_id}, timeout=10.0)
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                if results:
                    var_names = [name for name in results[0].get("variables", {}).keys() if name != "time"]
        except Exception:
            pass

        # Updated to rounded-pill, light background badges for a clean instrument look
        badges = [dbc.Badge(v, color="light", text_color="dark", className="me-1 mb-1 rounded-pill border shadow-sm", style={"fontSize": "0.7rem"}) for v in var_names] if var_names else [html.Span("No variables found", className="text-muted small fst-italic")]
        
        # Extract the platform prefix safely to use as a subtitle
        platform_prefix = full_id.split("::")[0] if "::" in full_id else "Unknown Platform"

        cards.append(
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader([
                        html.H5([html.I(className="bi bi-activity me-2 text-primary"), short_id], className="mb-0 fw-bold text-dark text-truncate"),
                        html.Span(platform_prefix, className="font-monospace small text-muted text-truncate d-block mt-1")
                    ], className="bg-light p-2 border-bottom"),
                    dbc.CardBody([
                        html.Div([html.I(className="bi bi-tags me-1 opacity-75"), "Tracked Variables"], className="text-muted fw-bold text-uppercase mb-2 text-nowrap", style={"fontSize": "0.65rem", "letterSpacing": "0.5px"}),
                        html.Div(badges, className="mb-4 d-flex flex-wrap"),
                        dbc.Button(
                            [html.I(className="bi bi-display me-2"), "Open Telemetry View"], 
                            href=dash.get_relative_path(f"/variableset/{deployment_id}/{short_id}"),
                            color="primary", className="w-100 fw-bold shadow-sm mt-auto"
                        )
                    ], className="p-3 d-flex flex-column h-100")
                ], className="shadow-sm border-0 h-100"),
                width=12, md=6, lg=4, className="mb-4"
            )
        )

    return html.Div([
        # --- HEADER STRIP ---
        dbc.Row([
            dbc.Col([
                html.H2([html.I(className="bi bi-collection-play me-2 text-primary"), "Telemetry Streams"], className="text-dark fw-bold mb-0"),
                html.P(f"Host Deployment ID: {deployment_id}", className="text-muted small font-monospace mt-1 mb-0")
            ], width=8),
            dbc.Col(
                dbc.Button(
                    [html.I(className="bi bi-arrow-left me-2"), "Back to Flight Deck"], 
                    href=dash.get_relative_path(f"/deployment/{deployment_id}"), 
                    color="secondary", outline=True, className="float-end fw-bold shadow-sm"
                ), 
                width=4, className="text-end align-self-center"
            )
        ], className="mb-4 mt-3 border-bottom pb-3"),
        
        # --- CARD GRID ---
        dbc.Row(cards if cards else dbc.Col(html.P("No active telemetry streams found for this deployment.", className="text-muted fst-italic px-2")))
    ])