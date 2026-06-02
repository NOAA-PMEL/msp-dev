import dash
import json
import logging
from dash import html, dcc, callback, Input, Output, State, MATCH, ALL, ctx, Patch
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
    path_template="/variableset/<deployment_id>/<variableset_id>",
    title="Variableset Telemetry",
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

def get_variableset_definition(short_id: str):
    url = f"http://{datastore_url}/variableset-definition/registry/ids/get/"
    try:
        response = httpx.get(url, timeout=10.0)
        all_vs_ids = response.json().get("results", []) if response.status_code == 200 else []
        for full_id in all_vs_ids:
            if full_id and short_id in full_id:
                def_url = f"http://{datastore_url}/variableset-definition/registry/get/"
                resp = httpx.get(def_url, params={"variableset_definition_id": full_id}, timeout=10.0)
                if resp.status_code == 200:
                    results = resp.json().get("results", [])
                    if results: return results[0]
    except Exception as e:
        L.error(f"get_variableset_definition error: {e}")
    return {}

def get_variableset_data(short_id: str):
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
        L.error(f"get_variableset_data error: {e}")
    return []

# --- UI BUILDERS ---
def build_graph_1d(dropdown_list, xaxis="time"):
    return dbc.Card([
        dbc.CardHeader([
            html.Span("Select Y-Axis Variable:", className="small fw-bold text-muted me-2"),
            dcc.Dropdown(
                id={"type": "vs-graph-1d-dropdown", "index": xaxis},
                options=dropdown_list, value="", className="mt-1"
            )
        ], className="bg-light"),
        dbc.CardBody([
            dcc.Graph(
                id={"type": "vs-graph-1d", "index": xaxis},
                figure=go.Figure(data=go.Scatter(x=[], y=[], type="scatter")),
                style={"height": 450}
            )
        ], className="p-0")
    ], className="border-0 shadow-sm mb-3")

def build_graph_2d(dropdown_list, dim_key):
    return dbc.Card([
        dbc.CardHeader([
            html.Span("Select Z-Axis Variable:", className="small fw-bold text-muted me-2"),
            dcc.Dropdown(id={"type": "vs-graph-2d-dropdown", "index": dim_key}, options=dropdown_list, value="", className="mt-1")
        ], className="bg-light"),
        dbc.CardBody([
            dcc.Graph(id={"type": "vs-graph-2d-heatmap", "index": dim_key}, style={"height": 450})
        ], className="p-0")
    ], className="border-0 shadow-sm mb-3")

def build_graphs(layout_options):
    graph_list = []
    if "layout-1d" in layout_options and "time" in layout_options["layout-1d"]:
        opts = layout_options["layout-1d"]["time"]
        graph_list.append(dbc.AccordionItem([build_graph_1d(opts["variable-list"])], title="1-Dimensional Telemetry"))

    if "layout-2d" in layout_options:
        for dim_key, opts in layout_options["layout-2d"].items():
            graph_list.append(dbc.AccordionItem([build_graph_2d(opts["variable-list"], dim_key)], title=f"2-Dimensional Telemetry (time vs {dim_key})"))
            
    return graph_list

# --- LAYOUT ---
def layout(deployment_id=None, variableset_id=None):
    if not deployment_id or not variableset_id: 
        return html.Div("Invalid Routing.", className="p-4 text-danger")

    varset_def = get_variableset_definition(variableset_id)
    
    layout_options = {"layout-1d": {"time": {"variable-list": []}}, "layout-2d": {}}
    table_columns = [{"field": "time", "headerName": "Time"}]

    if varset_def:
        for name, var in varset_def.get("variables", {}).items():
            if name == "time": continue
            dtype = var.get("type", "unknown")
            if dtype not in ["float", "double", "int", "number"]: continue

            long_name = var.get("attributes", {}).get("long_name", {}).get("data", name)
            unit_val = var.get("attributes", {}).get("units", {}).get("data")
            if unit_val: long_name = f"{long_name} ({unit_val})"

            table_columns.append({"field": name, "headerName": long_name, "cellDataType": "number"})

            shape = var.get("shape", ["time"])
            if "time" not in shape: continue

            if len(shape) == 1:
                layout_options["layout-1d"]["time"]["variable-list"].append({"label": long_name, "value": name})
            elif len(shape) == 2:
                dim_2d = [d for d in shape if d != "time"][0]
                if dim_2d not in layout_options["layout-2d"]:
                    layout_options["layout-2d"][dim_2d] = {"variable-list": []}
                layout_options["layout-2d"][dim_2d]["variable-list"].append({"label": long_name, "value": name})

    return html.Div([
        # --- HEADER ---
        dbc.Row([
            dbc.Col([
                html.H2(f"{variableset_id}", className="text-primary mb-0"),
                html.P(f"Deployment: {deployment_id}", className="text-muted small fw-bold text-uppercase")
            ]),
            dbc.Col(dbc.Button(
                "⭠ Back to List", href=dash.get_relative_path(f"/variablesets/{deployment_id}"), 
                color="secondary", outline=True, className="float-end fw-bold shadow-sm"
            ))
        ], className="mb-4 mt-3"),

        # --- DYNAMIC PLOTS CARD ---
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader(html.H5("Live Telemetry Plots", className="mb-0")),
                    dbc.CardBody(dbc.Accordion(build_graphs(layout_options), always_open=True, flush=True))
                ], className="shadow-sm border-dark mb-4"), width=12
            )
        ]),

        # --- DYNAMIC TABLE CARD ---
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader(html.H5("Live Data Stream", className="mb-0")),
                    dbc.CardBody(
                        dag.AgGrid(
                            id="vs-data-table", 
                            rowData=[], columnDefs=table_columns, columnSizeOptions="autoSize",
                            dashGridOptions={"domLayout": "autoHeight"},
                            style={"height": None, "maxHeight": "400px", "overflow": "auto"},
                            className="ag-theme-alpine"
                        )
                    )
                ], className="shadow-sm border-dark mb-4"), width=12
            )
        ]),

        # --- HIDDEN STORES & WEBSOCKETS ---
        dcc.Store(id="vs-meta", data={"variableset_id": variableset_id}),
        dcc.Store(id="vs-def-store", data=varset_def),
        WebSocket(id="ws-vs-instance", url=f"{ws_url_base}/envds/envops/ws/variableset/{variableset_id}"),
        dcc.Store(id="vs-data-buffer", data={})
    ])

# --- CALLBACKS ---

@callback(
    Output("vs-data-buffer", "data"),
    Input("ws-vs-instance", "message"),
    prevent_initial_call=True
)
def update_variableset_buffer(message): 
    if not message or "data" not in message: raise PreventUpdate
    try: return json.loads(message["data"])
    except: raise PreventUpdate

@callback(
    Output("vs-data-table", "rowTransaction"),
    Input("vs-data-buffer", "data"),
    State("vs-data-table", "columnDefs"),
    prevent_initial_call=True
)
def update_table_1d(buffer_data, col_defs):
    if not buffer_data: raise PreventUpdate
    try:
        variables = buffer_data.get("variables", {})
        data = {}
        for col in col_defs:
            name = col["field"]
            data[name] = variables.get(name, {}).get("data", "")
        return {"add": [data], "addIndex": 0}
    except Exception:
        raise PreventUpdate

@callback(
    Output({"type": "vs-graph-1d", "index": MATCH}, "figure"),
    Input({"type": "vs-graph-1d-dropdown", "index": MATCH}, "value"),
    [
        State("vs-meta", "data"),
        State("vs-def-store", "data"),
    ],
)
def select_graph_1d(y_axis, vs_meta, varset_def):
    default_fig = go.Figure(data=go.Scatter(x=[], y=[], type="scatter", mode="lines+markers"), layout={"xaxis": {"title": "Time"}, "yaxis": {"title": "Value"}})
    if not y_axis: return default_fig
    
    try:
        x, y = [], []
        results = get_variableset_data(vs_meta["variableset_id"])
        if results:
            for doc in results:
                try:
                    variables = doc.get("variables", {})
                    if "time" in variables and y_axis in variables:
                        x.append(variables["time"]["data"])
                        y.append(variables[y_axis]["data"])
                except Exception: continue

        units = ""
        try:
            unit_data = varset_def.get("variables", {}).get(y_axis, {}).get("attributes", {}).get("units", {}).get("data")
            if unit_data: units = f'({unit_data})'
        except Exception: pass

        return go.Figure(
                data=go.Scatter(x=x, y=y, type="scatter", mode="lines+markers"),
                layout={"xaxis": {"title": "Time"}, "yaxis": {"title": f"{y_axis} {units}".strip()}},
            )
    except Exception:
        return default_fig

@callback(
    Output({"type": "vs-graph-1d", "index": ALL}, "extendData"),
    Input("vs-data-buffer", "data"),
    State({"type": "vs-graph-1d-dropdown", "index": ALL}, "value"),
    prevent_initial_call=True
)
def update_graph_1d(buffer_data, selected_values):
    if not buffer_data: raise PreventUpdate

    try:
        variables = buffer_data.get("variables", {})
        figs_to_update = []

        for y_axis in selected_values:
            if not y_axis or "time" not in variables or y_axis not in variables:
                figs_to_update.append(dash.no_update)
                continue

            x_val = variables["time"].get("data")
            y_val = variables[y_axis].get("data")

            if x_val is None or y_val is None:
                figs_to_update.append(dash.no_update)
                continue

            if isinstance(x_val, list) and len(x_val) > 0: x_val = x_val[-1]
            if isinstance(y_val, list) and len(y_val) > 0: y_val = y_val[-1]

            figs_to_update.append(( {"x": [[x_val]], "y": [[y_val]]}, [0], 1000 ))

        if not any(f != dash.no_update for f in figs_to_update): raise PreventUpdate
        return figs_to_update
    except Exception:
        raise PreventUpdate

@callback(
    Output({"type": "vs-graph-2d-heatmap", "index": MATCH}, "figure", allow_duplicate=True),
    Input({"type": "vs-graph-2d-dropdown", "index": MATCH}, "value"),
    [
        State("vs-meta", "data"),
        State("vs-def-store", "data"),
        State({"type": "vs-graph-2d-dropdown", "index": MATCH}, "id"),
    ],
    prevent_initial_call=True,
)
def select_graph_2d(z_axis, vs_meta, varset_def, graph_id):
    if not z_axis: raise PreventUpdate

    try:
        y_axis = graph_id["index"]
        x, y, orig_z = [], [], []
        
        y_is_coord = False
        if y_axis in varset_def.get("variables", {}) and varset_def["variables"][y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
            y_is_coord = True
            y = varset_def["variables"][y_axis].get("data", [])

        results = get_variableset_data(vs_meta["variableset_id"])
        if not results: raise PreventUpdate

        for doc in results:
            try:
                x.append(doc["variables"]["time"]["data"])
                if not y_is_coord:
                    y.append(doc["variables"][y_axis]["data"])
                orig_z.append(doc["variables"][z_axis]["data"])
            except KeyError: continue

        if len(y) > 0 and isinstance(y[-1], list): y = y[-1]

        z = []
        for yi in range(len(y)):
            new_z = []
            for xi in range(len(x)):
                try: new_z.append(orig_z[xi][yi])
                except IndexError: new_z.append(None)
            z.append(new_z)

        y_units = varset_def.get("variables", {}).get(y_axis, {}).get("attributes", {}).get("units", {}).get("data", "")
        if y_units: y_units = f"({y_units})"

        heatmap = go.Figure(
            data=go.Heatmap(x=x, y=y, z=z, type="heatmap", colorscale="Rainbow"),
            layout={"xaxis": {"title": "Time"}, "yaxis": {"title": f"{y_axis} {y_units}".strip()}},
        )
        if y_axis == "diameter": heatmap.update_yaxes(type="log")
        return heatmap
    except Exception as e:
        L.error(f"select_graph_2d error: {e}")
        raise PreventUpdate

@callback(
    Output({"type": "vs-graph-2d-heatmap", "index": ALL}, "figure", allow_duplicate=True),
    Input("vs-data-buffer", "data"),
    [
        State({"type": "vs-graph-2d-dropdown", "index": ALL}, "value"),
        State("vs-def-store", "data"),
        State({"type": "vs-graph-2d-heatmap", "index": ALL}, "figure"),
        State({"type": "vs-graph-2d-heatmap", "index": ALL}, "id"),
    ],
    prevent_initial_call=True,
)
def update_graph_2d_heatmap(buffer_data, z_axis_list, varset_def, current_figs, graph_ids):
    if not buffer_data: raise PreventUpdate

    heatmaps = []
    for z_axis, graph_id, current_fig in zip(z_axis_list, graph_ids, current_figs):
        if not current_fig or not z_axis:
            heatmaps.append(dash.no_update)
            continue

        y_axis = graph_id["index"]
        variables = buffer_data.get("variables", {})
        
        if "time" not in variables or z_axis not in variables:
            heatmaps.append(dash.no_update)
            continue

        x = variables["time"]["data"]
        if not isinstance(x, list): x = [x]

        if len(current_fig["data"]) > 0 and x[0] in current_fig["data"][0].get("x", []):
            heatmaps.append(dash.no_update)
            continue

        heatmap_patch = Patch()
        for nx in x: heatmap_patch["data"][0]["x"].append(nx)

        y_is_coord = False
        if y_axis in varset_def.get("variables", {}) and varset_def["variables"][y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
            y_is_coord = True

        y = current_fig["data"][0].get("y", [])
        if len(y) == 0:
            if y_is_coord: y = varset_def["variables"][y_axis].get("data", [])
            else: y = variables[y_axis]["data"]

        orig_z = variables[z_axis]["data"]
        if not isinstance(orig_z, list): orig_z = [orig_z] 

        for yi, yval in enumerate(y):
            try: heatmap_patch["data"][0]["z"][yi].append(orig_z[yi])
            except IndexError: pass

        heatmaps.append(heatmap_patch)

    if all(h == dash.no_update for h in heatmaps): raise PreventUpdate
    return heatmaps