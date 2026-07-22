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
from ulid import ULID

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
                        if doc_results: 
                            # Safe for overlapping names across namespaces!
                            docs.extend(doc_results)
    except Exception as e:
        L.error(f"Failed to fetch {resource_type} definitions: {e}")
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
            html.Span([html.I(className="bi bi-funnel me-2"), "Y-Axis Variable:"], className="small fw-bold text-muted me-2 text-uppercase"),
            dcc.Dropdown(
                id={"type": "vs-graph-1d-dropdown", "index": xaxis},
                options=dropdown_list, value="", className="mt-1 shadow-sm"
            )
        ], className="bg-light border-bottom"),
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
            html.Span([html.I(className="bi bi-funnel me-2"), "Z-Axis Variable:"], className="small fw-bold text-muted me-2 text-uppercase"),
            dcc.Dropdown(id={"type": "vs-graph-2d-dropdown", "index": dim_key}, options=dropdown_list, value="", className="mt-1 shadow-sm")
        ], className="bg-light border-bottom"),
        dbc.CardBody([
            dcc.Graph(id={"type": "vs-graph-2d-heatmap", "index": dim_key}, style={"height": 450})
        ], className="p-0")
    ], className="border-0 shadow-sm mb-3")

def build_graphs(layout_options):
    graph_list = []
    if "layout-1d" in layout_options and "time" in layout_options["layout-1d"]:
        opts = layout_options["layout-1d"]["time"]
        graph_list.append(dbc.AccordionItem([build_graph_1d(opts["variable-list"])], title="1-Dimensional Time Series"))

    if "layout-2d" in layout_options:
        for dim_key, opts in layout_options["layout-2d"].items():
            graph_list.append(dbc.AccordionItem([build_graph_2d(opts["variable-list"], dim_key)], title=f"2-Dimensional Heatmap (time vs {dim_key})"))
            
    return graph_list

# --- LAYOUT ---
def layout(deployment_id=None, variableset_id=None):
    if not deployment_id or not variableset_id: 
        return html.Div("Invalid Routing.", className="p-4 text-danger")

    active_varsets = get_bundle_varsets(deployment_id)
    full_id = active_varsets.get(variableset_id)

    varset_def = {}
    if full_id:
        def_url = f"http://{datastore_url}/variableset-definition/registry/get/"
        try:
            resp = httpx.get(def_url, params={"variableset_definition_id": full_id}, timeout=10.0)
            if resp.status_code == 200:
                results = resp.json().get("results", [])
                if results: varset_def = results[0]
        except Exception as e:
            L.error(f"Failed to fetch Variableset definition: {e}")
    
    layout_options = {
        "layout-settings": {"time": {"row-data-skeletons": []}},
        "layout-1d": {"time": {"variable-list": []}}, 
        "layout-2d": {}
    }
    table_columns = [{"field": "time", "headerName": "Time"}]

    if varset_def:
        for name, var in varset_def.get("variables", {}).items():
            if name == "time": continue
            dtype = var.get("type", "unknown")
            if dtype not in ["float", "double", "int", "number"]: continue

            long_name = var.get("attributes", {}).get("long_name", {}).get("data", name)
            unit_val = var.get("attributes", {}).get("units", {}).get("data")
            if unit_val: long_name = f"{long_name} ({unit_val})"

            var_type = var.get("variable_type") or var.get("attributes", {}).get("variable_type", {}).get("data", "main")
            
            if var_type == "setting":
                layout_options["layout-settings"]["time"]["row-data-skeletons"].append({
                    "parameter": name,
                    "description": var.get("attributes", {}).get("description", {}).get("data", long_name),
                    "actual_value": "--",
                    "requested_value": ""
                })
                continue

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
        # --- HEADER STRIP ---
        dbc.Row([
            dbc.Col([
                html.H2([html.I(className="bi bi-broadcast me-3 text-primary"), f"{variableset_id}"], className="text-dark fw-bold mb-0"),
                html.P(f"Host Deployment ID: {deployment_id}", className="text-muted small font-monospace mt-1 mb-0")
            ], width=8),
            dbc.Col(
                dbc.Button(
                    [html.I(className="bi bi-arrow-left me-2"), "Back to Streams"], 
                    href=dash.get_relative_path(f"/variablesets/{deployment_id}"), 
                    color="secondary", outline=True, className="float-end fw-bold shadow-sm"
                ), 
                width=4, className="text-end align-self-center"
            )
        ], className="mb-4 mt-3 border-bottom pb-3"),

        # --- CONFIGURATION & CONTROLS ---
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader(html.H6([html.I(className="bi bi-sliders me-2"), "Hardware Configuration & Controls"], className="mb-0 text-primary fw-bold"), className="p-2 bg-white border-bottom-0"),
                    dbc.CardBody([
                        dag.AgGrid(
                            id="vs-settings-table",
                            rowData=layout_options["layout-settings"]["time"]["row-data-skeletons"],
                            columnDefs=[
                                {"field": "parameter", "headerName": "Control Parameter", "editable": False, "width": 250, "pinned": "left", "checkboxSelection": True},
                                {"field": "description", "headerName": "Description", "editable": False, "flex": 1},
                                {"field": "actual_value", "headerName": "Current State", "editable": False, "width": 150},
                                {
                                    "field": "requested_value", 
                                    "headerName": "Target Value (Click to Edit)", 
                                    "editable": True, 
                                    "width": 220,
                                    "cellStyle": {"backgroundColor": "#f8f9fa", "border": "1px dashed #0d6efd", "cursor": "text"}
                                }
                            ],
                            columnSize="autoSize",
                            defaultColDef={"resizable": True, "minWidth": 120},
                            dashGridOptions={
                                "singleClickEdit": True, 
                                "rowSelection": {"mode": "singleRow"},
                                "suppressRowClickSelection": False, 
                                "stopEditingWhenCellsLoseFocus": True,
                                "autoSizeStrategy": {"type": "fitCellContents"}
                            },
                            style={"height": "400px", "width": "100%"},
                            className="ag-theme-alpine mb-3 shadow-sm border"
                        ),
                        dbc.Button([html.I(className="bi bi-send-check me-2"), "Transmit Selected Command"], id="vs-submit-setting-btn", color="primary", className="fw-bold shadow-sm")
                    ], className="bg-light p-3")
                ], className="shadow-sm border-0 mb-4"), width=12
            )
        ]),

        # --- DYNAMIC PLOTS CARD ---
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader(html.H6([html.I(className="bi bi-graph-up me-2"), "Live Telemetry Plots"], className="mb-0 text-primary fw-bold"), className="p-2 bg-white border-bottom-0"),
                    dbc.CardBody(dbc.Accordion(build_graphs(layout_options), always_open=True, flush=True, className="border-top"), className="p-0")
                ], className="shadow-sm border-0 mb-4"), width=12
            )
        ]),

        # --- DYNAMIC TABLE CARD ---
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader(html.H6([html.I(className="bi bi-table me-2"), "Live Data Stream"], className="mb-0 text-primary fw-bold"), className="p-2 bg-white border-bottom-0"),
                    dbc.CardBody(
                        dag.AgGrid(
                            id="vs-data-table", 
                            rowData=[], columnDefs=table_columns, columnSize="autoSize",
                            defaultColDef={"resizable": True, "minWidth": 120},
                            dashGridOptions={
                                "autoSizeStrategy": {"type": "fitCellContents"}
                            },
                            style={"height": "400px", "width": "100%"},
                            className="ag-theme-alpine shadow-sm border"
                        ), className="bg-light p-3"
                    )
                ], className="shadow-sm border-0 mb-4"), width=12
            )
        ]),

        # --- HIDDEN STORES & WEBSOCKETS ---
        dcc.Store(id="vs-meta", data={"variableset_id": variableset_id}),
        dcc.Store(id="vs-def-store", data=varset_def),
        WebSocket(id="ws-vs-instance", url=f"{ws_url_base}/envds/envops/ws/variableset/{variableset_id}"),
        html.Div(id="ws-send-vs-buffer", children="", style={"display": "none"}),
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
            raw_val = variables.get(name, {}).get("data", "")
            
            # THE FIX: If the value is a list (2D data), convert it to a string 
            # so the datatable can render it without crashing
            if isinstance(raw_val, list):
                data[name] = json.dumps(raw_val)
            else:
                data[name] = raw_val
                
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
        dim_y = graph_id["index"]
        real_y_axis = dim_y
        
        # Resolve the true alias for the coordinate axis (e.g. diameter -> smps_diameter)
        if varset_def:
            for v_name, v_def in varset_def.get("variables", {}).items():
                if v_def.get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
                    if dim_y in v_def.get("shape", []) or v_name == dim_y:
                        real_y_axis = v_name
                        break

        use_log = ("diameter" in real_y_axis.lower() or "dp" in real_y_axis.lower())
        x, y, orig_z = [], [], []
        
        results = get_variableset_data(vs_meta["variableset_id"])
        if not results: raise PreventUpdate
        
        for doc in results:
            try:
                x.append(doc["variables"]["time"]["data"])
                y.append(doc["variables"][real_y_axis]["data"])
                orig_z.append(doc["variables"][z_axis]["data"])
            except KeyError: continue
            
        if len(y) > 0 and isinstance(y[-1], list): 
            y = y[-1]
            
        z = []
        for yi in range(len(y)):
            new_z = []
            for xi in range(len(x)):
                try: new_z.append(orig_z[xi][yi])
                except IndexError: new_z.append(None)
            z.append(new_z)
            
        y_units = varset_def.get("variables", {}).get(real_y_axis, {}).get("attributes", {}).get("units", {}).get("data", "")
        if y_units: y_units = f"({y_units})"
        
        heatmap = go.Figure(
            data=go.Heatmap(x=x, y=y, z=z, type="heatmap", colorscale="Rainbow"),
            layout={"xaxis": {"title": "Time"}, "yaxis": {"title": f"{real_y_axis} {y_units}".strip()}},
        )
        if use_log: 
            heatmap.update_yaxes(type="log")
            heatmap.update_layout(coloraxis=dict(cmax=None, cmin=None))
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
            
        dim_y = graph_id["index"]
        real_y_axis = dim_y
        
        # Resolve the true alias for the coordinate axis
        if varset_def:
            for v_name, v_def in varset_def.get("variables", {}).items():
                if v_def.get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
                    if dim_y in v_def.get("shape", []) or v_name == dim_y:
                        real_y_axis = v_name
                        break
                        
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
        
        y = current_fig["data"][0].get("y", [])
        if len(y) == 0:
            y = variables.get(real_y_axis, {}).get("data", [])
            
        # The telemetry matrix is a flat 1D list representing the Z values for this time bucket
        orig_z = variables.get(z_axis, {}).get("data", [])
        
        # Plotly heatmaps extending over the x-axis (time) expect a 2D array [time_buckets][y_bins]
        # But when patching, Plotly expects Z to be updated per Y-bin: z[yi].append(val)
        
        # Ensure the patch structure exists
        if "z" not in current_fig["data"][0] or not current_fig["data"][0]["z"]:
             heatmap_patch["data"][0]["z"] = [[] for _ in range(len(y))]
             
        for yi in range(len(y)):
            try:
                # Safely extract the bin value from the 1D telemetry payload
                val = orig_z[yi] if isinstance(orig_z, list) and yi < len(orig_z) else None
                heatmap_patch["data"][0]["z"][yi].append(val)
            except Exception as e:
                L.error(f"Error patching heatmap bin {yi}: {e}")
                heatmap_patch["data"][0]["z"][yi].append(None)
            
        heatmaps.append(heatmap_patch)
        
    if all(h == dash.no_update for h in heatmaps): raise PreventUpdate
    return heatmaps

@callback(
    Output("ws-vs-instance", "send"), 
    Input("vs-submit-setting-btn", "n_clicks"),
    State("vs-settings-table", "selectedRows"),
    State("vs-def-store", "data"),
    State("vs-data-buffer", "data"), 
    prevent_initial_call=True
)
def handle_vs_setting_submission(n_clicks, selected_rows, varset_def, live_data):
    print(f"\n--- VARIABLESET APPLY BUTTON CLICKED ---")
    if not n_clicks or not selected_rows: 
        raise PreventUpdate
    
    selected_row = selected_rows[0]
    param_name = selected_row["parameter"]
    raw_val = selected_row.get("requested_value")
    
    if raw_val is None or raw_val == "": 
        raise PreventUpdate
    
    if not live_data:
        raise PreventUpdate
        
    live_var = live_data.get("variables", {}).get(param_name, {})
    attrs = live_var.get("attributes", {})
    
    t_id = attrs.get("source_id", {}).get("data", "unknown")
    t_type = attrs.get("source_type", {}).get("data", "sensor").lower()
    src_var = attrs.get("source_variable", {}).get("data", param_name)

    if t_id == "unknown":
        raise PreventUpdate

    dest_topic = "envds/controller/settings/request" if t_type == "controller" else "envds/sensor/settings/request"
    
    if str(raw_val).lower() in ["true", "on", "1"]:
        requested_val = 1 if selected_row.get("type") == "int" else True
    elif str(raw_val).lower() in ["false", "off", "0"]:
        requested_val = 0 if selected_row.get("type") == "int" else False
    else:
        try: requested_val = int(raw_val) if selected_row.get("type") == "int" else float(raw_val)
        except (ValueError, TypeError): requested_val = raw_val

    # --- THE FIX: Just send a lightweight raw dictionary with routing hints ---
    payload = {
        "source": f"envds.{config.daq_id}.dashboard",
        "data": {
            "settings": {
                src_var: {"requested": requested_val}
            }
        },
        "destpath": dest_topic,
        "target_type": t_type, 
        "target_id": t_id
    }

    print(f"SUCCESS! Transmitting lightweight dict: {payload}")
    return json.dumps(payload)

@callback(
    Output("vs-settings-table", "rowData"),
    Input("vs-data-buffer", "data"),
    State("vs-settings-table", "rowData"),
    prevent_initial_call=True
)
def update_vs_settings_table(buffer_data, row_data):
    if not buffer_data or not row_data: raise PreventUpdate
    
    updated = False
    variables = buffer_data.get("variables", {})
    new_rows = []
    
    for row in row_data:
        # THE FIX: Create a fresh copy to force React DOM update
        new_row = row.copy()
        param = new_row["parameter"]
        
        if param in variables:
            var_data = variables[param].get("data")
            
            actual_val = ""
            if isinstance(var_data, dict):
                if "data" in var_data and isinstance(var_data["data"], dict):
                    actual_val = var_data["data"].get("actual", "")
                else:
                    actual_val = var_data.get("actual", "")
            else:
                actual_val = var_data
            
            if str(new_row.get("actual_value")) != str(actual_val):
                new_row["actual_value"] = actual_val
                updated = True
                
        new_rows.append(new_row)
                
    if not updated:
        raise PreventUpdate
    return new_rows