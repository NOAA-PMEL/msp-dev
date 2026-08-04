from datetime import datetime, timezone
import json
import logging
import dash
import plotly.express as px
import plotly.graph_objs as go
from dash import (
    html,
    callback,
    dcc,
    Input,
    Output,
    dash_table,
    State,
    MATCH,
    ALL,
    Patch,
    ctx
)
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from dash_extensions import WebSocket
from pydantic import BaseSettings
import dash_ag_grid as dag
from logfmter import Logfmter
import httpx
import traceback

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.DEBUG)

dash.register_page(
    __name__,
    path_template="/system_data",
    title="System Data",
    order=1
)

class Settings(BaseSettings):
    daq_id: str = "default"
    external_hostname: str = "localhost"
    http_use_tls: bool = False
    http_port: int = 80
    https_port: int = 443
    ws_use_tls: bool = False
    ws_port: int = 80
    wss_port: int = 443
    knative_broker: str = "http://kafka-broker-ingress.knative-eventing.svc.cluster.local/default/default"
    dry_run: bool = False

    class Config:
        env_prefix = "DASHBOARD_"
        case_sensitive = False

config = Settings()

datastore_url = f"datastore.{config.daq_id}-system.svc.cluster.local"

http_url_base = f"http://{config.external_hostname}:{config.http_port}"
if config.http_use_tls:
    http_url_base = f"https://{config.external_hostname}:{config.https_port}"
ws_url_base = f"ws://{config.external_hostname}:{config.ws_port}"
if config.ws_use_tls:
    ws_url_base = f"wss://{config.external_hostname}:{config.wss_port}"

def get_short_id(full_id: str) -> str:
    """Extracts vmap_name::vset_name from a fully qualified ID."""
    parts = full_id.split("::")
    if len(parts) >= 4:
        return f"{parts[1]}::{parts[3]}"
    return full_id

def build_tables(table_columns_dict):
    table_list = []
    for varset_id, columns in table_columns_dict.items():
        table_list.append(
            dbc.AccordionItem(
                [
                    dag.AgGrid(
                        id={"type": "system-data-table-1d", "index": varset_id}, 
                        rowData=[],
                        columnDefs=columns,
                        columnSizeOptions="autoSize", 
                    )
                ],
                title=f"Data 1-D ({varset_id})",
            )
        )
    return table_list

def build_graph_1d(dropdown_list, xaxis="time", yaxis=""):
    graph = dbc.Card(
        children=[
            dbc.CardHeader(
                children=[
                    dcc.Dropdown(
                        id={"type": "system-graph-1d-dropdown", "index": xaxis},
                        options=dropdown_list,
                        value="",
                    )
                ]
            ),
            dcc.Graph(
                id={"type": "system-graph-1d", "index": xaxis},
                figure=go.Figure(
                    data=go.Scatter(x=[], y=[], type="scatter")
                ),
                style={"height": 600},
            ),
        ]
    )
    return graph

def build_graphs(shared_dropdown_list, unique_varsets):
    return [
        dbc.AccordionItem(
            [
                dbc.Row(
                    children=[
                        dcc.Checklist(
                            id={"type": "graph-varset-filter", "index": "shared"}, 
                            options=[{"label": f" {v}", "value": v} for v in unique_varsets],
                            value=unique_varsets, # All checked by default
                            inline=True,
                            inputStyle={"margin-right": "5px", "margin-left": "15px"},
                            style={"margin-bottom": "15px", "font-weight": "bold"}
                        )
                    ]
                ),
                dbc.Row(
                    children=[
                        build_graph_1d(
                            dropdown_list=shared_dropdown_list,
                            xaxis="shared" 
                        )
                    ]
                )
            ],
            title="Plots 1-D (Combined)",
        )
    ]

def get_variableset_data(short_id: str):
    """Fetches historical data using the short ID."""
    query = {"variableset_id": short_id}
    url = f"http://{datastore_url}/variableset/data/get/"
    
    try:
        timeout = httpx.Timeout(30.0, read=None)
        response = httpx.get(url, params=query, timeout=timeout)
        if response.status_code == 200:
            results = response.json()
            if "results" in results and results["results"]:
                return results["results"]
    except Exception as e:
        L.error("get_variableset_data error", extra={"reason": e})
    return []

def layout(platform=None):
    L.debug("Starting initial API fetch for layout generation...")

    all_variablesets = {}
    current_sets_dict = {}

    # 1. Fetch initial data and build dictionary mappings
    try:
        url = f"http://{datastore_url}/variableset-definition/registry/ids/get/"
        timeout = httpx.Timeout(30.0, read=None)
        response = httpx.get(url, timeout=timeout)
        results = response.json()
        
        if "results" in results and results["results"]:
            for full_id in results["results"]:
                if full_id is not None:
                    # Convert to our clean UI identifier
                    short_id = get_short_id(full_id)
                    current_sets_dict[short_id] = full_id
                    
                    # Fetch definitions using the datastore's fully qualified ID
                    def_url = f"http://{datastore_url}/variableset-definition/registry/get/"
                    query_params = {"variableset_definition_id": full_id}
                    def_response = httpx.get(def_url, params=query_params, timeout=timeout)
                    
                    if def_response.status_code == 200:
                        new_varset_def = def_response.json().get("results", {})[0]
                        # Store locally using the short_id
                        all_variablesets[short_id] = new_varset_def
                        
    except Exception as e:
        L.error(f"Initial variableset fetch error: {e}")

    table_columns_1d = {}
    shared_graph_dropdown = []

    # 2. Build UI mappings utilizing the short_ids
    for short_id, varset_definition in all_variablesets.items():
        if not varset_definition:
            continue
            
        table_columns_1d[short_id] = [{
            "field": "time", "headerName": "Time", "filter": False, "cellDataType": "text"
        }]

        try:
            for name, var in varset_definition.get("variables", {}).items():
                
                long_name = name
                ln = var.get("attributes", {}).get("long_name", None)
                if ln:
                    long_name = ln.get("data", name)
                
                unit_val = var.get("attributes", {}).get("units", {}).get("data")
                if unit_val:
                    long_name = f"{long_name} ({unit_val})"

                dtype = var.get("type", "unknown")
                data_type = "text"
                if dtype in ["float", "double", "int"]: data_type = "number"
                elif dtype in ["str", "string", "char"]: data_type = "text"
                elif dtype in ["bool"]: data_type = "boolean"

                cd = {"field": name, "headerName": long_name, "filter": False, "cellDataType": data_type}
                table_columns_1d[short_id].append(cd)

                if data_type == "number":
                    shared_graph_dropdown.append(
                        {"label": f"{long_name} - {short_id}", "value": f"{short_id}::{name}"}
                    )

        except KeyError as e:
            L.error(f"build column_defs error for {short_id}: {e}")
    
    unique_varsets = list(all_variablesets.keys())

    # 3. Render layout
    layout_div = html.Div(
        [
            html.H1("System Data"),
            dbc.Accordion(build_tables(table_columns_1d), id="system-data-accordion"),
            dbc.Accordion(
                build_graphs(shared_graph_dropdown, unique_varsets), 
                id="sensor-plot-accordion", style={"margin-top": "30px"}
            ),

            dcc.Store(id="master-dropdown-options", data=shared_graph_dropdown),
            dcc.Store(id="system-graph-axes", data={}),

            # Bind WebSocket directly to the short_id broadcast
            html.Div([
                WebSocket(
                    id={"type": "ws-variableset-instance", "index": v_id}, 
                    url=f"{ws_url_base}/msp/dashboardtest/ws/variableset/{v_id}"
                ) 
                for v_id in unique_varsets
            ]),
            
            dcc.Store(id='variableset-store', data=current_sets_dict),
            dcc.Store(id='variableset-defs-store', data=all_variablesets),            
            
            html.Div([
                dcc.Store(id={"type": "variableset-data-buffer", "index": v_id}, data={})
                for v_id in unique_varsets
            ]),
            dcc.Interval(id='variableset-interval', interval=5*1000, n_intervals=0)
        ]
    )

    return layout_div


@callback(
    Output("variableset-store", "data"),
    Input("variableset-interval", "n_intervals"),
    State("variableset-store", "data")
)
def update_variableset_list(count, current_sets_dict):
    if current_sets_dict is None:
        current_sets_dict = {}
        
    try:
        url = f"http://{datastore_url}/variableset-definition/registry/ids/get/"
        timeout = httpx.Timeout(30.0, read=None)
        response = httpx.get(url, timeout=timeout)
        results = response.json()
        
        new_sets_dict = {}
        if "results" in results and results["results"]:
            for full_id in results["results"]:
                if full_id:
                    short_id = get_short_id(full_id)
                    new_sets_dict[short_id] = full_id
        
        if new_sets_dict != current_sets_dict:
            return new_sets_dict
            
        return dash.no_update
    except Exception as e:
        L.error(f"update_variableset_list error: {e}")
        return dash.no_update


@callback(
    Output("variableset-defs-store", "data"),
    Input("variableset-store", "data"),
    State("variableset-defs-store", "data")
)
def update_variableset_defs(varset_dict, current_defs):
    if varset_dict is None:
        raise PreventUpdate
    
    current_defs = current_defs or {}
    updated = False

    for short_id, full_id in varset_dict.items():
        if short_id not in current_defs:
            try:
                def_url = f"http://{datastore_url}/variableset-definition/registry/get/"
                query_params = {"variableset_definition_id": full_id}
                timeout = httpx.Timeout(30.0, read=None)
                response = httpx.get(def_url, params=query_params, timeout=timeout)
                
                if response.status_code == 200:
                    results = response.json().get("results", {})
                    if results:
                        current_defs[short_id] = results[0]
                        updated = True
            except Exception as e:
                L.error(f"Error fetching definition for {full_id}: {e}")
    
    keys_to_remove = [k for k in current_defs.keys() if k not in varset_dict]
    for key in keys_to_remove:
        del current_defs[key]
        updated = True

    if updated:
        return current_defs
    return dash.no_update


@callback(
    Output({"type": "variableset-data-buffer", "index": MATCH}, "data"),
    Input({"type": "ws-variableset-instance", "index": MATCH}, "message"),
    prevent_initial_call=True
)
def update_variableset_buffers(events): 
    if not ctx.triggered:
        return dash.no_update
        
    event = ctx.triggered[0].get("value")
    if event is not None and "data" in event:
        try:
            event_data = json.loads(event["data"])
            return [event_data]
        except Exception as e:
            L.error(f"data buffer JSON parse error: {e}")
            return dash.no_update
    return dash.no_update


@callback(
    Output({"type": "system-graph-1d-dropdown", "index": MATCH}, "options"),
    Input({"type": "graph-varset-filter", "index": MATCH}, "value"),
    State("master-dropdown-options", "data"),
    prevent_initial_call=False
)
def filter_graph_dropdown(selected_varsets, master_options):
    if not master_options: return dash.no_update
    if not selected_varsets: return [] 
        
    filtered_options = []
    for opt in master_options:
        try: varset_id, variable_name = opt["value"].rsplit("::", 1)
        except ValueError: continue
            
        if varset_id in selected_varsets:
            filtered_options.append(opt)
            
    return filtered_options


@callback(
    Output({"type": "system-graph-1d", "index": MATCH}, "figure"),
    Input({"type": "system-graph-1d-dropdown", "index": MATCH}, "value"),
    [
        State("system-graph-axes", "data"),
        State("variableset-defs-store", "data"),
        State({"type": "system-graph-1d-dropdown", "index": MATCH}, "id"),
    ],
)
def select_graph_1d(selected_value, graph_axes, variableset_defs, graph_id):
    default_fig = go.Figure(
        data=go.Scatter(x=[], y=[], type="scatter", mode="lines+markers"),
        layout={"xaxis": {"title": "Time"}, "yaxis": {"title": "Value"}}
    )
    
    if not selected_value:
        return default_fig
    
    try:
        short_id, y_axis = selected_value.rsplit("::", 1)

        if graph_axes is None: graph_axes = {}
        if "graph-1d" not in graph_axes: graph_axes["graph-1d"] = dict()
        graph_axes["graph-1d"][graph_id["index"]] = {"x-axis": "time", "y-axis": y_axis}

        x, y = [], []
        
        # Pull history utilizing the short ID
        results = get_variableset_data(short_id=short_id)
        
        if results and len(results) > 0:
            for doc in results:
                try:
                    variables = doc.get("variables", {})
                    if "time" in variables and y_axis in variables:
                        x.append(variables["time"]["data"])
                        y.append(variables[y_axis]["data"])
                except (KeyError, TypeError):
                    continue

        units = ""
        try:
            unit_data = variableset_defs.get(short_id, {}).get("variables", {}).get(y_axis, {}).get("attributes", {}).get("units", {}).get("data")
            if unit_data: units = f'({unit_data})'
        except Exception:
            pass

        fig = go.Figure(
                data=go.Scatter(x=x, y=y, type="scatter", mode="lines+markers"),
                layout={
                    "xaxis": {"title": "Time"},
                    "yaxis": {"title": f"{y_axis} {units}".strip()},
                },
            )

        return fig 

    except Exception as e:
        L.error(f"select_graph_1d error: {e}")
        return default_fig


@callback(
    Output({"type": "system-graph-1d", "index": ALL}, "extendData"),
    Input({"type": "variableset-data-buffer", "index": ALL}, "data"),
    State({"type": "system-graph-1d-dropdown", "index": ALL}, "value"),
    prevent_initial_call=True
)
def update_graph_1d(buffers_data, selected_values):
    if not ctx.triggered: raise PreventUpdate

    # Leverage ctx.triggered_id to instantly know which data stream fired
    triggered_id = ctx.triggered_id
    if not triggered_id or not isinstance(triggered_id, dict): raise PreventUpdate
        
    incoming_short_id = str(triggered_id.get("index"))

    triggered_val = ctx.triggered[0].get("value")
    if not triggered_val: raise PreventUpdate

    try:
        event_data = triggered_val[0].get("data-update", {})
        figs_to_update = []

        if incoming_short_id:
            for selected_value in selected_values:
                if not selected_value:
                    figs_to_update.append(dash.no_update)
                    continue

                try:
                    varset_id, y_axis = selected_value.rsplit("::", 1)
                except ValueError:
                    figs_to_update.append(dash.no_update)
                    continue

                if incoming_short_id != str(varset_id):
                    figs_to_update.append(dash.no_update)
                    continue

                variables = event_data.get("variables", {})

                x_val = variables.get("time", {}).get("data")
                if not x_val: x_val = event_data.get("time", {}).get("data")
                y_val = variables.get(y_axis, {}).get("data")

                if not x_val or y_val is None:
                    figs_to_update.append(dash.no_update)
                    continue

                if isinstance(x_val, list) and len(x_val) > 0: x_val = x_val[0]
                if isinstance(y_val, list) and len(y_val) > 0: y_val = y_val[0]

                figs_to_update.append(
                    ( {"x": [[x_val]], "y": [[y_val]]}, [0], 1000 )
                )

            if not any(f != dash.no_update for f in figs_to_update):
                raise PreventUpdate

            return figs_to_update
        
        else:
            raise PreventUpdate

    except Exception as e:
        L.error(f"data update error graph: {e}")
        raise PreventUpdate


@callback(
    Output({"type": "system-data-table-1d", "index": MATCH}, "rowTransaction"),
    Input({"type": "variableset-data-buffer", "index": MATCH}, "data"),
    State({"type": "system-data-table-1d", "index": MATCH}, "columnDefs"),
    prevent_initial_call=True
)
def update_table_1d(buffer_data, col_defs):
    if not buffer_data: raise PreventUpdate
    
    try:
        # Utilizing MATCH, we implicitly know this data belongs to this specific table!
        event_data = buffer_data[0].get("data-update", {})
        variables = event_data.get("variables", {})
        
        data = {}
        for col in col_defs:
            name = col["field"]

            if name == "time":
                t_val = variables.get("time", {}).get("data")
                if not t_val: t_val = event_data.get("time", {}).get("data", "")
                data[name] = t_val
                continue

            if name in variables:
                data[name] = variables[name].get("data", "")
            else:
                data[name] = ""

        return {"add": [data], "addIndex": 0}
        
    except Exception as e:
        L.error(f"data update error table: {e}")
        raise PreventUpdate