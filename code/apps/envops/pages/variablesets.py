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

def fetch_registry_data(resource_type: str):
    url = f"http://{datastore_url}/{resource_type}-definition/registry/ids/get/"
    docs = []
    try:
        timeout = httpx.Timeout(10.0)
        id_response = httpx.get(url, timeout=timeout)
        if id_response.status_code == 200:
            ids = id_response.json().get("results", [])
            for doc_id in ids:
                if doc_id:
                    doc_url = f"http://{datastore_url}/{resource_type}-definition/registry/get/"
                    doc_response = httpx.get(doc_url, params={"name": doc_id}, timeout=timeout) 
                    if doc_response.status_code == 200:
                        doc_results = doc_response.json().get("results", [])
                        if doc_results: docs.append(doc_results[0])
    except Exception as e:
        L.error(f"Failed to fetch {resource_type}: {e}")
    return docs

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
        # Fetch the definition to show the user what's inside
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

        badges = [dbc.Badge(v, color="info", className="me-1 mb-1") for v in var_names] if var_names else [html.Span("No variables found", className="text-muted small")]

        cards.append(
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader(html.H5(short_id, className="mb-0 fw-bold text-dark")),
                    dbc.CardBody([
                        html.P("Contains:", className="small text-muted mb-2 fw-bold text-uppercase"),
                        html.Div(badges, className="mb-4"),
                        dbc.Button(
                            "Open Telemetry View ↗", 
                            href=dash.get_relative_path(f"/variableset/{deployment_id}/{short_id}"),
                            color="primary", className="w-100 fw-bold shadow-sm"
                        )
                    ])
                ], className="shadow-sm border-0 h-100"),
                width=12, md=6, lg=4, className="mb-4"
            )
        )

    return html.Div([
        dbc.Row([
            dbc.Col([
                html.H2(f"Variablesets: {deployment_id}", className="text-primary mb-0"),
                html.P("Select a variableset to monitor its live telemetry.", className="text-muted small")
            ]),
            dbc.Col(
                dbc.Button("⭠ Back to C2", href=dash.get_relative_path(f"/deployment/{deployment_id}"), color="secondary", outline=True, className="float-end fw-bold shadow-sm"), 
                width="auto"
            )
        ], className="mb-4 mt-3"),
        
        dbc.Row(cards if cards else dbc.Col(html.P("No active variablesets found.", className="text-muted fst-italic")))
    ])