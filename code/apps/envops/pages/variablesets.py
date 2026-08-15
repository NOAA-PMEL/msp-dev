import dash
import logging
from dash import html, dcc, callback, Input, Output, State, ctx
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
    
    # Pre-process all variable metadata for fast client-side sorting
    varset_data = []
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
            
        parts = full_id.split("::")
        platform = parts[0] if len(parts) > 0 else "Unknown Platform"
        vmap = parts[1] if len(parts) > 1 else "Unknown VariableMap"
        vtype = parts[3] if len(parts) > 3 else "Unknown Type"
        
        varset_data.append({
            "short_id": short_id,
            "full_id": full_id,
            "platform": platform,
            "vmap": vmap,
            "type": vtype,
            "var_names": var_names,
            "deployment_id": deployment_id
        })

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
        
        # --- MAIN CONTENT GRID ---
        dbc.Row([
            # CARDS GRID (Stretched full width)
            dbc.Col([
                html.Div(id="varset-cards-container")
            ], width=12)
        ]),
        
        # --- STORES ---
        dcc.Store(id="varset-data-store", data=varset_data)
    ])

@callback(
    Output("varset-cards-container", "children"),
    Input("varset-active-group", "data"),
    State("varset-data-store", "data")
)
def render_varset_cards(group_by, varset_data):
    if not varset_data:
        return html.P("No active telemetry streams found for this deployment.", className="text-muted fst-italic px-2")
        
    grouped = {}
    for item in varset_data:
        key = item.get(group_by, "Unknown")
        if not key: key = "Unknown"
        if key not in grouped: grouped[key] = []
        grouped[key].append(item)
        
    sections = []
    for key, items in sorted(grouped.items()):
        cards = []
        for item in items:
            short_id = item["short_id"]
            var_names = item["var_names"]
            platform_prefix = item["platform"]
            deployment_id = item["deployment_id"]
            
            badges = [dbc.Badge(v, color="light", text_color="dark", className="me-1 mb-1 rounded-pill border shadow-sm", style={"fontSize": "0.7rem"}) for v in var_names] if var_names else [html.Span("No variables found", className="text-muted small fst-italic")]
            
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
                    width=12, md=12, lg=6, xl=4, className="mb-4"
                )
            )
            
        icon_map = {
            "type": "bi-diagram-3",
            "vmap": "bi-map",
            "platform": "bi-hdd-network"
        }
        icon = icon_map.get(group_by, "bi-folder2-open")
        
        header_text = str(key)
        if group_by == "type":
            header_text = f"Type: {header_text.title()}"
        elif group_by == "vmap":
            header_text = f"Map: {header_text}"
        elif group_by == "platform":
            header_text = f"Platform: {header_text}"

        sections.append(html.Div([
            html.H4([html.I(className=f"bi {icon} me-2 text-secondary"), header_text], className="fw-bold mb-3 border-bottom pb-2 text-dark"),
            dbc.Row(cards)
        ], className="mb-5"))
        
    return sections