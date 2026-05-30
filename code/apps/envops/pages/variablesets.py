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
ws_url_base = f"ws://{config.external_hostname}:{config.ws_port}"

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
        L.error(f"Failed to fetch {resource_type} definitions: {e}")
    return docs

def get_bundle_variablesets(host_id):
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
                
    # Fetch IDs directly and split to avoid the Pydantic param mismatch
    url = f"http://{datastore_url}/variableset-definition/registry/ids/get/"
    try:
        timeout = httpx.Timeout(10.0)
        response = httpx.get(url, timeout=timeout)
        all_vs_ids = response.json().get("results", []) if response.status_code == 200 else []
    except Exception as e:
        L.error(f"Failed to fetch variableset IDs: {e}")
        all_vs_ids = []

    active_varsets = {}
    for full_id in all_vs_ids:
        if not full_id: continue
        parts = full_id.split("::")
        if len(parts) >= 4:
            vs_platform = parts[0]
            if vs_platform in platforms:
                routing_key = f"{parts[1]}::{parts[3]}"
                active_varsets[routing_key] = full_id 

    return platforms, active_varsets

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

        dcc.Loading(html.Div(id="variablesets-container", children=html.P("Fetching mission variablesets..."))),
        html.Div(id="dynamic-websockets-container")
    ])

@callback(
    Output("store-active-varsets", "data"),
    Output("variablesets-container", "children"),
    Output("dynamic-websockets-container", "children"),
    Input("store-current-deployment", "data"),
)
def fetch_deployment_variablesets(deployment_id):
    if not deployment_id:
        raise PreventUpdate

    platforms, active_varsets = get_bundle_variablesets(deployment_id)

    if not active_varsets:
        plat_str = ", ".join(platforms) if platforms else "None found"
        return active_varsets, html.P(f"No active variablesets found for platforms: {plat_str}", className="text-danger"), []

    ui_elements = []
    websockets = []
    
    for routing_key, full_id in active_varsets.items():
        websockets.append(WebSocket(
            id={"type": "ws-varset", "index": routing_key}, 
            url=f"{ws_url_base}/envds/envops/ws/variableset/{routing_key}" 
        ))
        
        ui_elements.append(dbc.Card([
            dbc.CardHeader(html.H5(f"Variableset: {routing_key}")),
            dbc.CardBody([
                dbc.Row([
                    dbc.Col([
                        dag.AgGrid(
                            id={"type": "varset-table", "index": routing_key},
                            rowData=[],
                            columnDefs=[{"field": "time", "headerName": "Time"}], 
                            columnSizeOptions="autoSize",
                            dashGridOptions={"domLayout": "autoHeight"}
                        )
                    ], width=5),
                    dbc.Col([
                        dcc.Graph(
                            id={"type": "varset-plot", "index": routing_key},
                            figure=go.Figure(layout={"margin": {"t": 10, "b": 10, "l": 10, "r": 10}}),
                            style={"height": "300px"}
                        )
                    ], width=7)
                ])
            ])
        ], className="mb-4 shadow-sm"))

    return active_varsets, ui_elements, websockets

@callback(
    Output({"type": "varset-plot", "index": MATCH}, "extendData"),
    Output({"type": "varset-table", "index": MATCH}, "rowTransaction"),
    Output({"type": "varset-table", "index": MATCH}, "columnDefs"),
    Input({"type": "ws-varset", "index": MATCH}, "message"),
    State({"type": "varset-table", "index": MATCH}, "columnDefs"),
    prevent_initial_call=True
)
def stream_variableset_data(message, current_cols):
    if not message or "data" not in message:
        raise PreventUpdate

    try:
        payload = json.loads(message["data"])
        variables = payload.get("variables", {})
        
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