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
    return platforms, active_varsets

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
def build_tables(table_columns_dict):
    table_list = []
    for varset_id, columns in table_columns_dict.items():
        table_list.append(
            dbc.AccordionItem([
                dag.AgGrid(
                    id={"type": "system-data-table-1d", "index": varset_id}, 
                    rowData=[], columnDefs=columns, columnSizeOptions="autoSize",
                    dashGridOptions={"domLayout": "autoHeight"},
                    style={"height": None, "maxHeight": "400px", "overflow": "auto"}
                )
            ], title=f"Data Table ({varset_id})")
        )
    return table_list

def build_graph_1d(dropdown_list, xaxis="time"):
    return dbc.Card([
        dbc.CardHeader([
            html.Span("Select Y-Axis Variable:", className="small fw-bold text-muted me-2"),
            dcc.Dropdown(
                id={"type": "system-graph-1d-dropdown", "index": xaxis},
                options=dropdown_list, value="",
                className="mt-1"
            )
        ], className="bg-light"),
        dbc.CardBody([
            dcc.Graph(
                id={"type": "system-graph-1d", "index": xaxis},
                figure=go.Figure(data=go.Scatter(x=[], y=[], type="scatter")),
                style={"height": 450}
            )
        ], className="p-0")
    ], className="border-0 shadow-sm mb-3")

def build_graph_2d(dropdown_list, xaxis="time", yaxis=""):
    idx = f"{xaxis}::{yaxis}"
    return dbc.Card([
        dbc.CardHeader([
            html.Span("Select Z-Axis Variable:", className="small fw-bold text-muted me-2"),
            dcc.Dropdown(id={"type": "graph-2d-dropdown", "index": idx}, options=dropdown_list, value="", className="mt-1")
        ], className="bg-light"),
        dbc.CardBody([
            dcc.Graph(id={"type": "graph-2d-heatmap", "index": idx}, style={"height": 450})
        ], className="p-0")
    ], className="border-0 shadow-sm mb-3")

def build_graphs(layout_options, unique_varsets):
    graph_list = []
    
    if "layout-1d" in layout_options and "shared" in layout_options["layout-1d"]:
        opts = layout_options["layout-1d"]["shared"]
        graph_list.append(
            dbc.AccordionItem([
                html.Div([
                    html.Span("Filter Variablesets:", className="small fw-bold text-muted d-block mb-2"),
                    dbc.Checklist(
                        id={"type": "graph-varset-filter", "index": "shared"},
                        options=[{"label": f" {v}", "value": v} for v in unique_varsets],
                        value=unique_varsets, inline=True,
                        labelClassName="me-3 fw-bold text-primary",
                        inputClassName="me-1"
                    )
                ], className="p-3 bg-light border rounded mb-3"),
                build_graph_1d(opts["variable-list"], xaxis="shared")
            ], title="1-Dimensional Telemetry (Combined)")
        )

    if "layout-2d" in layout_options:
        for dim_key, opts in layout_options["layout-2d"].items():
            varset_id, dim_name = dim_key.split("::", 1)
            title = f"2-Dimensional Telemetry ({varset_id}: time vs {dim_name})"
            graph_list.append(
                dbc.AccordionItem(
                    [build_graph_2d(opts["variable-list"], xaxis="time", yaxis=dim_key)],
                    title=title
                )
            )
            
    return graph_list

# --- LAYOUT ---
def layout(deployment_id=None):
    if not deployment_id: return html.Div("No Deployment ID provided.", className="p-4 text-danger")

    platforms, active_varsets = get_bundle_varsets(deployment_id)
    unique_varsets = list(active_varsets.keys())
    
    all_defs = {}
    for short_id, full_id in active_varsets.items():
        try:
            def_url = f"http://{datastore_url}/variableset-definition/registry/get/"
            resp = httpx.get(def_url, params={"variableset_definition_id": full_id}, timeout=10.0)
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                if results: all_defs[short_id] = results[0]
        except Exception as e:
            pass

    layout_options = {"layout-1d": {"shared": {"variable-list": []}}, "layout-2d": {}}
    table_columns_1d = {}

    for short_id, varset_def in all_defs.items():
        table_columns_1d[short_id] = [{"field": "time", "headerName": "Time"}]

        for name, var in varset_def.get("variables", {}).items():
            if name == "time": continue
            dtype = var.get("type", "unknown")
            if dtype not in ["float", "double", "int", "number"]: continue

            long_name = var.get("attributes", {}).get("long_name", {}).get("data", name)
            unit_val = var.get("attributes", {}).get("units", {}).get("data")
            if unit_val: long_name = f"{long_name} ({unit_val})"

            table_columns_1d[short_id].append({"field": name, "headerName": long_name, "cellDataType": "number"})

            shape = var.get("shape", ["time"])
            if "time" not in shape: continue

            if len(shape) == 1:
                layout_options["layout-1d"]["shared"]["variable-list"].append(
                    {"label": f"{long_name} - {short_id}", "value": f"{short_id}::{name}"}
                )
            elif len(shape) == 2:
                dim_2d = [d for d in shape if d != "time"][0]
                dim_key = f"{short_id}::{dim_2d}"
                if dim_key not in layout_options["layout-2d"]:
                    layout_options["layout-2d"][dim_key] = {"variable-list": []}
                layout_options["layout-2d"][dim_key]["variable-list"].append(
                    {"label": f"{long_name} - {short_id}", "value": f"{short_id}::{name}"}
                )

    shared_graph_dropdown = layout_options["layout-1d"]["shared"]["variable-list"]

    return html.Div([
        # --- HEADER ---
        dbc.Row([
            dbc.Col([
                html.H2(f"Telemetry Plots: {deployment_id}", className="text-primary mb-0"),
                html.P("Live variableset data visualization", className="text-muted small")
            ]),
            dbc.Col(dbc.Button(
                "⭠ Back to C2", href=dash.get_relative_path(f"/deployment/{deployment_id}"), 
                color="secondary", outline=True, className="float-end fw-bold shadow-sm"
            ))
        ], className="mb-4 mt-3"),

        # --- DYNAMIC PLOTS CARD ---
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H5("Live Telemetry Plots", className="mb-0")),
                    dbc.CardBody([
                        dbc.Accordion(
                            build_graphs(layout_options, unique_varsets), 
                            id="sensor-plot-accordion", 
                            always_open=True, 
                            flush=True
                        )
                    ])
                ], className="shadow-sm border-dark mb-4")
            ], width=12)
        ]),

        # --- DYNAMIC TABLES CARD ---
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H5("Live Data Tables", className="mb-0")),
                    dbc.CardBody([
                        dbc.Accordion(
                            build_tables(table_columns_1d), 
                            id="system-data-accordion", 
                            flush=True
                        )
                    ])
                ], className="shadow-sm border-dark mb-4")
            ], width=12)
        ]),

        # --- HIDDEN STORES & WEBSOCKETS ---
        dcc.Store(id="master-dropdown-options", data=shared_graph_dropdown),
        dcc.Store(id="system-graph-axes", data={}),
        dcc.Store(id="variableset-defs-store", data=all_defs),

        html.Div([
            WebSocket(id={"type": "ws-variableset-instance", "index": v_id}, url=f"{ws_url_base}/envds/envops/ws/variableset/{v_id}")
            for v_id in unique_varsets
        ]),
        html.Div([
            dcc.Store(id={"type": "variableset-data-buffer", "index": v_id}, data={})
            for v_id in unique_varsets
        ])
    ])

# --- CALLBACKS ---

@callback(
    Output({"type": "variableset-data-buffer", "index": MATCH}, "data"),
    Input({"type": "ws-variableset-instance", "index": MATCH}, "message"),
    prevent_initial_call=True
)
def update_variableset_buffers(message): 
    if not message or "data" not in message: raise PreventUpdate
    try:
        return [json.loads(message["data"])]
    except Exception:
        raise PreventUpdate

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
        try: varset_id, _ = opt["value"].rsplit("::", 1)
        except ValueError: continue
        if varset_id in selected_varsets: filtered_options.append(opt)
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
    default_fig = go.Figure(data=go.Scatter(x=[], y=[], type="scatter", mode="lines+markers"), layout={"xaxis": {"title": "Time"}, "yaxis": {"title": "Value"}})
    if not selected_value: return default_fig
    
    try:
        short_id, y_axis = selected_value.rsplit("::", 1)
        x, y = [], []
        
        results = get_variableset_data(short_id=short_id)
        if results and len(results) > 0:
            for doc in results:
                try:
                    variables = doc.get("variables", {})
                    if "time" in variables and y_axis in variables:
                        x.append(variables["time"]["data"])
                        y.append(variables[y_axis]["data"])
                except Exception: continue

        units = ""
        try:
            unit_data = variableset_defs.get(short_id, {}).get("variables", {}).get(y_axis, {}).get("attributes", {}).get("units", {}).get("data")
            if unit_data: units = f'({unit_data})'
        except Exception: pass

        return go.Figure(
                data=go.Scatter(x=x, y=y, type="scatter", mode="lines+markers"),
                layout={"xaxis": {"title": "Time"}, "yaxis": {"title": f"{y_axis} {units}".strip()}},
            )
    except Exception:
        return default_fig

# NOTE: For 1D graphs, `extendData` is naturally optimal and functions perfectly as a patch.
@callback(
    Output({"type": "system-graph-1d", "index": ALL}, "extendData"),
    Input({"type": "variableset-data-buffer", "index": ALL}, "data"),
    State({"type": "system-graph-1d-dropdown", "index": ALL}, "value"),
    prevent_initial_call=True
)
def update_graph_1d(buffers_data, selected_values):
    if not ctx.triggered: raise PreventUpdate
    triggered_id = ctx.triggered_id
    if not triggered_id or not isinstance(triggered_id, dict): raise PreventUpdate
        
    incoming_short_id = str(triggered_id.get("index"))
    triggered_val = ctx.triggered[0].get("value")
    if not triggered_val: raise PreventUpdate

    try:
        event_data = triggered_val[0]
        variables = event_data.get("variables", {})
        figs_to_update = []

        for selected_value in selected_values:
            if not selected_value:
                figs_to_update.append(dash.no_update)
                continue

            try: varset_id, y_axis = selected_value.rsplit("::", 1)
            except ValueError:
                figs_to_update.append(dash.no_update)
                continue

            if incoming_short_id != str(varset_id):
                figs_to_update.append(dash.no_update)
                continue

            x_val = variables.get("time", {}).get("data")
            y_val = variables.get(y_axis, {}).get("data")

            if not x_val or y_val is None:
                figs_to_update.append(dash.no_update)
                continue

            if isinstance(x_val, list) and len(x_val) > 0: x_val = x_val[-1]
            if isinstance(y_val, list) and len(y_val) > 0: y_val = y_val[-1]

            figs_to_update.append(( {"x": [[x_val]], "y": [[y_val]]}, [0], 1000 ))

        if not any(f != dash.no_update for f in figs_to_update): raise PreventUpdate
        return figs_to_update
    except Exception:
        raise PreventUpdate

# NOTE: Uses rowTransaction, perfectly optimal. Does not cause flashing. 
@callback(
    Output({"type": "system-data-table-1d", "index": MATCH}, "rowTransaction"),
    Input({"type": "variableset-data-buffer", "index": MATCH}, "data"),
    State({"type": "system-data-table-1d", "index": MATCH}, "columnDefs"),
    prevent_initial_call=True
)
def update_table_1d(buffer_data, col_defs):
    if not buffer_data: raise PreventUpdate
    try:
        variables = buffer_data[0].get("variables", {})
        data = {}
        for col in col_defs:
            name = col["field"]
            data[name] = variables.get(name, {}).get("data", "")
        return {"add": [data], "addIndex": 0}
    except Exception:
        raise PreventUpdate

@callback(
    Output({"type": "graph-2d-heatmap", "index": MATCH}, "figure", allow_duplicate=True),
    Input({"type": "graph-2d-dropdown", "index": MATCH}, "value"),
    [
        State("variableset-defs-store", "data"),
        State({"type": "graph-2d-dropdown", "index": MATCH}, "id"),
    ],
    prevent_initial_call=True,
)
def select_graph_2d(z_axis_val, varset_defs, graph_id):
    if not z_axis_val: raise PreventUpdate

    try:
        short_id, z_axis = z_axis_val.rsplit("::", 1)
        _, y_axis_val = graph_id["index"].split("::", 1) 
        _, y_axis = y_axis_val.rsplit("::", 1)

        x, y, orig_z = [], [], []
        y_is_coord = False
        
        def_vars = varset_defs.get(short_id, {}).get("variables", {})
        if y_axis in def_vars and def_vars[y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
            y_is_coord = True
            y = def_vars[y_axis].get("data", [])

        results = get_variableset_data(short_id)
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

        y_units = def_vars.get(y_axis, {}).get("attributes", {}).get("units", {}).get("data", "")
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

# --- REWRITTEN TO USE SURGICAL PATCH INSTEAD OF FULL FIGURE RENDER ---
@callback(
    Output({"type": "graph-2d-heatmap", "index": ALL}, "figure", allow_duplicate=True),
    Input({"type": "variableset-data-buffer", "index": ALL}, "data"),
    [
        State({"type": "graph-2d-dropdown", "index": ALL}, "value"),
        State("variableset-defs-store", "data"),
        State({"type": "graph-2d-heatmap", "index": ALL}, "figure"),
        State({"type": "graph-2d-heatmap", "index": ALL}, "id"),
    ],
    prevent_initial_call=True,
)
def update_graph_2d_heatmap(buffers_data, z_axis_list, varset_defs, current_figs, graph_ids):
    if not ctx.triggered: raise PreventUpdate
    incoming_short_id = str(ctx.triggered_id.get("index"))
    triggered_val = ctx.triggered[0].get("value")
    if not triggered_val: raise PreventUpdate

    heatmaps = []
    for z_axis_val, graph_id, current_fig in zip(z_axis_list, graph_ids, current_figs):
        if not current_fig or not z_axis_val:
            heatmaps.append(dash.no_update)
            continue

        short_id, z_axis = z_axis_val.rsplit("::", 1)
        if incoming_short_id != short_id:
            heatmaps.append(dash.no_update)
            continue

        _, y_axis_val = graph_id["index"].split("::", 1)
        _, y_axis = y_axis_val.rsplit("::", 1)

        variables = triggered_val[0].get("variables", {})
        if "time" not in variables or z_axis not in variables:
            heatmaps.append(dash.no_update)
            continue

        x = variables["time"]["data"]
        if not isinstance(x, list): x = [x]

        if x[0] in current_fig["data"][0].get("x", []):
            heatmaps.append(dash.no_update)
            continue

        # Initialize the Magic Patch!
        heatmap_patch = Patch()

        # Update the X-axis Time Column
        for nx in x: 
            heatmap_patch["data"][0]["x"].append(nx)

        y_is_coord = False
        def_vars = varset_defs.get(short_id, {}).get("variables", {})
        if y_axis in def_vars and def_vars[y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
            y_is_coord = True

        y = current_fig["data"][0].get("y", [])
        if len(y) == 0:
            if y_is_coord: y = def_vars[y_axis].get("data", [])
            else: y = variables[y_axis]["data"]

        orig_z = variables[z_axis]["data"]
        if not isinstance(orig_z, list): orig_z = [orig_z] 

        # Surgically append the new Z data values to each respective Y row
        for yi, yval in enumerate(y):
            try: 
                heatmap_patch["data"][0]["z"][yi].append(orig_z[yi])
            except IndexError: 
                pass

        heatmaps.append(heatmap_patch)

    if all(h == dash.no_update for h in heatmaps): raise PreventUpdate
    return heatmaps