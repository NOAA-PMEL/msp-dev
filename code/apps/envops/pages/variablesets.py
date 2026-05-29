import dash
import json
import logging
from dash import html, dcc, callback, Input, Output, State, MATCH, ALL, ctx
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from dash_extensions import WebSocket
import dash_ag_grid as dag
import plotly.graph_objs as go
import httpx
from pydantic import BaseSettings

L = logging.getLogger(__name__)

dash.register_page(
    __name__,
    path_template="/variablesets/<deployment_id>", # <-- Flattened route here
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
ws_url_base = f"ws://{config.external_hostname}:{config.ws_port}"

# --- HELPER: REST FETCH ---
def fetch_registry_data(resource_type: str):
    """Directly fetch definitions from datastore so we don't rely on cross-page caches."""
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
        L.error(f"Failed to fetch {resource_type} definitions: {e}")
    return docs


def layout(deployment_id=None):
    if not deployment_id:
        return html.Div("No Deployment ID provided.")

    return html.Div([
        dcc.Store(id="store-current-deployment", data=deployment_id),
        dcc.Store(id="store-active-varsets", data={}),
        
        dbc.Row([
            dbc.Col(html.H3(f"Variablesets: {deployment_id}", className="text-primary")),
            dbc.Col(dbc.Button(
                "Back to C2", 
                href=dash.get_relative_path(f"/deployment/{deployment_id}"), 
                color="secondary", outline=True, className="float-end"
            ))
        ], className="mb-4 mt-3"),

        # Dynamic container for plots and tables
        dcc.Loading(html.Div(id="variablesets-container", children=html.P("Fetching mission variablesets..."))),
        
        # We will inject WebSockets dynamically based on the fetched variablesets
        html.Div(id="dynamic-websockets-container")
    ])

@callback(
    Output("store-active-varsets", "data"),
    Output("variablesets-container", "children"),
    Output("dynamic-websockets-container", "children"),
    Input("store-current-deployment", "data"),
    # REMOVED: State("store-deployments", "data")
)
def fetch_deployment_variablesets(deployment_id):
    """Cross-references the deployment to find the platform, then fetches its variablesets."""
    if not deployment_id:
        raise PreventUpdate

    # 1. Fetch deployments directly from datastore
    deployments = fetch_registry_data("deployment")

    # 2. Find the platform_ref for this deployment
    platform_ref = None
    for dep in deployments:
        if dep.get("metadata", {}).get("name") == deployment_id:
            platform_ref = dep.get("data", {}).get("platform_ref")
            break

    if not platform_ref:
        return dash.no_update, html.P("Deployment not found in active cache.", className="text-danger"), []

    # 3. Query Datastore for Variablesets matching this platform
    active_varsets = {}
    try:
        url = f"http://{datastore_url}/variableset-definition/registry/ids/get/"
        timeout = httpx.Timeout(10.0)
        response = httpx.get(url, timeout=timeout)
        
        if response.status_code == 200:
            all_ids = response.json().get("results", [])
            for full_id in all_ids:
                # If the ID contains our platform_ref, fetch the definition
                if platform_ref in full_id:
                    def_url = f"http://{datastore_url}/variableset-definition/registry/get/"
                    def_resp = httpx.get(def_url, params={"name": full_id}, timeout=timeout)
                    if def_resp.status_code == 200:
                        vs_def = def_resp.json().get("results", [{}])[0]
                        vs_name = vs_def.get("metadata", {}).get("name")
                        if vs_name:
                            active_varsets[vs_name] = full_id
    except Exception as e:
        L.error(f"Failed to fetch variablesets for platform {platform_ref}: {e}")

    if not active_varsets:
        return active_varsets, html.P(f"No active variablesets found for platform: {platform_ref}"), []

    # 4. Build UI and WebSockets
    ui_elements = []
    websockets = []
    
    for vs_name, full_id in active_varsets.items():
        # Inject the WebSocket listener
        websockets.append(WebSocket(
            id={"type": "ws-varset", "index": vs_name}, 
            url=f"{ws_url_base}/envds/envops/ws/variableset/{vs_name}" 
        ))
        
        # Build the Table & Plot Layout
        ui_elements.append(dbc.Card([
            dbc.CardHeader(html.H5(vs_name.capitalize())),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dag.AgGrid(
                            id={"type": "varset-table", "index": vs_name},
                            rowData=[],
                            columnDefs=[{"field": "time", "headerName": "Time"}], 
                            columnSizeOptions="autoSize",
                            dashGridOptions={"domLayout": "autoHeight"}
                        )
                    ], width=5),
                    dbc.Col([
                        dcc.Graph(
                            id={"type": "varset-plot", "index": vs_name},
                            figure=go.Figure(layout={"margin": {"t": 10, "b": 10, "l": 10, "r": 10}}),
                            style={"height": "300px"}
                        )
                    ], width=7)
                ])
            ])
        ], className="mb-4 shadow-sm"))

    return active_varsets, ui_elements, websockets

# Pattern-Matching Callbacks to handle the incoming WebSockets
@callback(
    Output({"type": "varset-plot", "index": MATCH}, "extendData"),
    Output({"type": "varset-table", "index": MATCH}, "rowTransaction"),
    Output({"type": "varset-table", "index": MATCH}, "columnDefs"),
    Input({"type": "ws-varset", "index": MATCH}, "message"),
    State({"type": "varset-table", "index": MATCH}, "columnDefs"),
    prevent_initial_call=True
)
def stream_variableset_data(message, current_cols):
    """Processes incoming variableset data to update the specific plot and table."""
    if not message or "data" not in message:
        raise PreventUpdate

    try:
        event = json.loads(message["data"])
        variables = event.get("variables", {})
        
        time_val = variables.get("time", {}).get("data")
        if not time_val:
            raise PreventUpdate
            
        plot_var = None
        plot_val = None
        
        table_row = {"time": time_val}
        new_cols = [{"field": "time", "headerName": "Time"}]
        
        for v_name, v_data in variables.items():
            if v_name == "time": continue
            
            val = v_data.get("data")
            table_row[v_name] = val
            new_cols.append({"field": v_name, "headerName": v_name.replace("_", " ").title()})
            
            if plot_var is None and isinstance(val, (int, float)):
                plot_var = v_name
                plot_val = val

        cols_to_return = new_cols if len(new_cols) > len(current_cols) else dash.no_update
        plot_update = ({"x": [[time_val]], "y": [[plot_val]]}, [0], 1000) if plot_val is not None else dash.no_update
        
        return plot_update, {"add": [table_row], "addIndex": 0}, cols_to_return

    except Exception as e:
        L.error(f"Variableset Stream Error: {e}")
        raise PreventUpdate