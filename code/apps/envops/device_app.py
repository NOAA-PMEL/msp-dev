import dash
from dash import html, dcc, Input, Output, State, no_update, ALL, MATCH, ctx
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from dash_extensions import WebSocket
import dash_ag_grid as dag
import plotly.graph_objects as go
import logging
import json
import httpx
import traceback

from utils import get_registry_data, config, create_unified_shell, register_sidebar_callbacks

L = logging.getLogger(__name__)

app = dash.Dash(__name__, requests_pathname_prefix="/envds/envops/devices/", routes_pathname_prefix="/", suppress_callback_exceptions=True)
register_sidebar_callbacks(app)

datastore_url = f"datastore.{config.daq_id}-system.svc.cluster.local"
ws_protocol = "wss://" if str(config.ws_use_tls).lower() == "true" else "ws://"
ws_base = f"{ws_protocol}{config.external_hostname}:{config.ws_port}/msp/dashboardtest"


def get_all_devices():
    devices = []
    for d_type in ["sensor", "operational", "controller"]:
        path = "controller-instance" if d_type == "controller" else "device-instance"
        url = f"http://{datastore_url}/{path}/registry/get/"
        try:
            response = httpx.get(url, params={"device_type": d_type} if d_type in ["sensor", "operational"] else {}, timeout=10.0)
            items = response.json().get("results", [])
            for item in items:
                item["_device_type"] = d_type 
                devices.append(item)
        except Exception: pass
    return devices

def get_device_data(device_id: str, device_type: str="sensor"):
    path = "controller" if device_type == "controller" else "device"
    query = {"controller_id": device_id} if device_type == "controller" else {"device_type": device_type, "device_id": device_id}
    url = f"http://{datastore_url}/{path}/data/get/"
    try:
        response = httpx.get(url, params=query, timeout=30.0)
        results = response.json()
        if "results" in results and results["results"]: return results["results"]
    except Exception: pass
    return []

def get_device_definition(device_id: str, device_type: str="sensor"):
    path = "controller-instance" if device_type == "controller" else "device-instance"
    query = {"controller_id": device_id} if device_type == "controller" else {"device_type": device_type, "device_id": device_id}
    try:
        device = httpx.get(f"http://{datastore_url}/{path}/registry/get/", params=query, timeout=10.0).json().get("results", [{}])[0]
        if device and device.get("version"):
            device_definition_id = f"{device['make']}::{device['model']}::{device['version']}"
            def_path = "controller-definition" if device_type == "controller" else "device-definition"
            def_query = {"controller_definition_id": device_definition_id} if device_type == "controller" else {"device_type": device_type, "device_definition_id": device_definition_id}
            return httpx.get(f"http://{datastore_url}/{def_path}/registry/get/", params=def_query, timeout=10.0).json().get("results", [{}])[0]
    except Exception: pass
    return {}


# --- Dynamic Builders (Matching sensor.py) ---
def build_tables(layout_options):
    table_list = []
    for ltype, dims in layout_options.items():
        for dim, options in dims.items():
            if ltype == "layout-settings":
                col_defs = [
                    {"field": "parameter", "headerName": "Setting Parameter", "editable": False, "pinned": "left"},
                    {"field": "description", "headerName": "Description", "editable": False},
                    {"field": "actual_value", "headerName": "Actual Value", "editable": False},
                    {"field": "requested_value", "headerName": "Requested Value", "editable": True, "cellEditorSelector": {"function": "determineSettingEditor(params)"}}
                ]
                table_list.append(dbc.AccordionItem([
                    dag.AgGrid(id={"type": "settings-table", "index": dim}, rowData=options.get("row-data-skeletons", []), columnDefs=col_defs, columnSizeOptions="autoSize", dashGridOptions={"domLayout": "autoHeight", "singleClickEdit": True, "rowSelection": "single"}),
                    dbc.Button("Submit Selected Setting", id={"type": "submit-setting-btn", "index": dim}, color="primary", className="mt-3")
                ], title="Device Settings"))
            elif ltype == "layout-1d":
                table_list.append(dbc.AccordionItem([dag.AgGrid(id={"type": "data-table-1d", "index": dim}, rowData=[], columnDefs=options["table-column-defs"], columnSizeOptions="autoSize")], title=f"Data 1-D ({dim})"))
            elif ltype == "layout-2d":
                table_list.append(dbc.AccordionItem([dag.AgGrid(id={"type": "data-table-2d", "index": f"time::{dim}"}, rowData=[], columnDefs=options["table-column-defs"], columnSizeOptions="autoSize")], title=f"Data 2-D (time, {dim})"))
    return table_list

def build_graph_1d(dropdown_list, xaxis="time"):
    return dbc.Card([
        dbc.CardHeader([dcc.Dropdown(id={"type": "sensor-graph-1d-dropdown", "index": xaxis}, options=dropdown_list, value="")]),
        dcc.Graph(id={"type": "sensor-graph-1d", "index": xaxis}, figure=go.Figure(data=[go.Scatter(x=[], y=[], mode="lines+markers")]), style={"height": 300}),
    ])

def build_graph_2d(dropdown_list, xaxis="time", yaxis="", zaxis=""):
    content = dbc.Row([
        dbc.Button("Submit", {"type": "graph-2d-z-axis-submit", "index": f"{xaxis}::{yaxis}"}),
        dbc.Label("z-axis min:"), dbc.Col(dbc.Input(type="number", id={"type": "graph-2d-z-axis-min", "index": f"{xaxis}::{yaxis}"})),
        dbc.Label("z-axis max:"), dbc.Col(dbc.Input(type="number", id={"type": "graph-2d-z-axis-max", "index": f"{xaxis}::{yaxis}"})),
    ])
    return dbc.Card([
        dbc.CardHeader([dcc.Dropdown(id={"type": "graph-2d-dropdown", "index": f"{xaxis}::{yaxis}"}, options=dropdown_list, value="")]),
        dbc.Row([
            dbc.Accordion([dbc.AccordionItem([dbc.Card(children=[content])], title="Axes Settings")], start_collapsed=True),
            dbc.Col(dcc.Graph(id={"type": "graph-2d-heatmap", "index": f"{xaxis}::{yaxis}"}, figure=go.Figure(data=[go.Heatmap(x=[], y=[], z=[], type="heatmap")]), style={"height": 500})),
            dbc.Col(dcc.Graph(id={"type": "graph-2d-line", "index": f"{xaxis}::{yaxis}"}, figure=go.Figure(data=[go.Scatter(x=[], y=[], mode="lines")]), style={"height": 500})),
        ])
    ])

def build_graph_3d(dropdown_list, xaxis="", yaxis="", zaxis=""):
    content = dbc.Row([dbc.Button("Submit", {"type": "graph-3d-z-axis-submit", "index": f"{xaxis}::{yaxis}"}), dbc.Label("z-axis min:")])
    return dbc.Card([
        dbc.CardHeader([dcc.Dropdown(id={"type": "graph-3d-dropdown", "index": f"{xaxis}::{yaxis}"}, options=dropdown_list, value="")]),
        dbc.Row([
            dbc.Accordion([dbc.AccordionItem([dbc.Card(children=[content])], title="Axes Settings")], start_collapsed=True),
            dbc.Col(dcc.Graph(id={"type": "graph-3d-line", "index": f"{xaxis}::{yaxis}"}, figure=go.Figure(data=[go.Surface(x=[], y=[], z=[])]), style={"height": 500})),
            dbc.Col(dcc.Graph(id={"type": "graph-3d-heatmap", "index": f"{xaxis}::{yaxis}"}, figure=go.Figure(data=[go.Heatmap(x=[], y=[], z=[], type="heatmap")]), style={"height": 500})),
        ])
    ])

def build_graphs(layout_options):
    graph_list = []
    for ltype, dims in layout_options.items():
        for dim, options in dims.items():
            if ltype == "layout-1d": graph_list.append(dbc.AccordionItem([dbc.Row([build_graph_1d(options["variable-list"], xaxis=dim)])], title=f"Plots 1-D ({dim})"))
            elif ltype == "layout-2d": graph_list.append(dbc.AccordionItem([dbc.Row([build_graph_2d(options["variable-list"], xaxis="time", yaxis=dim)])], title=f"Plots 2-D (time, {dim})"))
            elif ltype == "layout-3d":
                axes = dim.split("::")
                graph_list.append(dbc.AccordionItem([dbc.Row([build_graph_3d(options["variable-list"], xaxis=axes[0], yaxis=axes[1])])], title=f"Plots 3-D ({axes[0]}, {axes[1]})"))
    return graph_list


# --- Core View Renderers ---
def render_registry_view():
    row_data = []
    for d in get_all_devices():
        dtype = d.get("_device_type", "sensor")
        make = d.get("make", "unknown")
        model = d.get("model", "unknown")
        sn = d.get("serial_number", d.get("serial_id", "unknown"))
        dev_id = f"{dtype}::{make}::{model}::{sn}"
        link = f"[{make} {model} ({sn})](/envds/envops/devices/view/{dev_id})"
        row_data.append({"device": link, "type": dtype.capitalize(), "make": make, "model": model, "serial_number": sn})

    columns = [
        {"field": "device", "headerName": "Device Link", "cellRenderer": "markdown", "flex": 2},
        {"field": "type", "headerName": "Type", "flex": 1},
        {"field": "make", "headerName": "Make", "flex": 1},
        {"field": "model", "headerName": "Model", "flex": 1},
        {"field": "serial_number", "headerName": "Serial Number", "flex": 1},
    ]
    return html.Div([
        html.H2([html.I(className="bi bi-cpu me-2"), "Fleet Device Registry"], className="fw-bold mb-1"),
        dag.AgGrid(rowData=row_data, columnDefs=columns, columnSizeOptions="responsiveSizeToFit", style={"height": "75vh"})
    ], className="container-fluid mt-3")


def render_device_detail(device_uri):
    L.info(f"🚨 ROUTER: Rendering detailed UI for {device_uri}")
    parts = device_uri.split("::")
    device_type = parts[0]
    device_id = f"{parts[1]}::{parts[2]}::{parts[3]}"
    device_meta = {"device_id": device_id, "device_type": device_type, "make": parts[1], "model": parts[2], "serial_number": parts[3]}
    device_definition = get_device_definition(device_id=device_id, device_type=device_type)

    layout_options = {"layout-settings": {"time": {"table-column-defs": [], "variable-list": [], "row-data-skeletons": []}}, "layout-calibration": {"time": {"table-column-defs": [], "variable-list": []}}, "layout-1d": {"time": {"table-column-defs": [], "variable-list": []}}}
    calibration_vars = []

    if device_definition:
        dimensions = device_definition.get("dimensions", {})
        multi_dim = len(dimensions.keys()) > 1

        for name, var in device_definition.get("variables", {}).items():
            var_type = var.get("attributes", {}).get("variable_type", {}).get("data", "")
            dtype = str(var.get("type", "unknown")).lower()
            
            if var_type == "setting":
                layout_options["layout-settings"]["time"]["row-data-skeletons"].append({"parameter": name, "description": var.get("attributes", {}).get("long_name", {}).get("data", name), "actual_value": "", "requested_value": "", "type": dtype})
            elif var_type == "calibration": calibration_vars.append(name)
            elif var_type == "main" or "shape" in var:
                if "shape" not in var or "time" not in var["shape"]: continue
                
                # 🟢 THE FIX: Broad matching to allow float32, float64, int32, etc.
                is_numeric = any(x in dtype for x in ["float", "double", "int", "number"])
                data_type = "number" if is_numeric else "boolean" if "bool" in dtype else "text"
                
                L.info(f"🚨 DEBUG PARSER: Extracted '{name}' | Dtype: '{dtype}' | Numeric: {is_numeric}")

                cd = {"field": name, "headerName": var.get("attributes", {}).get("long_name", {}).get("data", name), "filter": False, "cellDataType": data_type}

                if multi_dim and len(var["shape"]) == 2:
                    dim_2d = [d for d in var["shape"] if d != "time"][0]
                    if "layout-2d" not in layout_options: layout_options["layout-2d"] = {}
                    if dim_2d not in layout_options["layout-2d"]:
                        layout_options["layout-2d"][dim_2d] = {"table-column-defs": [{"field": dim_2d, "headerName": dim_2d, "filter": False, "cellDataType": "text", "pinned": "left"}], "variable-list": []}
                    layout_options["layout-2d"][dim_2d]["table-column-defs"].append(cd)
                elif multi_dim and len(var["shape"]) == 3:
                    dims_3d = [d for d in var["shape"] if d != "time"]
                    dim_3d_key = f"{dims_3d[0]}::{dims_3d[1]}"
                    if "layout-3d" not in layout_options: layout_options["layout-3d"] = {}
                    if dim_3d_key not in layout_options["layout-3d"]: layout_options["layout-3d"][dim_3d_key] = {"table-column-defs": [], "variable-list": []}
                    layout_options["layout-3d"][dim_3d_key]["table-column-defs"].append(cd)
                else: layout_options["layout-1d"]["time"]["table-column-defs"].append(cd)

        for ltype, dims in layout_options.items():
            for dim, options in dims.items():
                if "table-column-defs" in options:
                    for cd in options["table-column-defs"]:
                        if cd["field"] in dimensions or cd.get("cellDataType") != "number": continue
                        layout_options[ltype][dim]["variable-list"].append({"label": cd["field"], "value": cd["field"]})

    topic_type = "sensor" if device_type == "operational" else device_type
    id_field = "controllerid" if device_type == "controller" else "deviceid"

    return html.Div([
        dbc.Row([
            dbc.Col(html.H2(f"Diagnostics: {device_id}", className="fw-bold mb-0")),
            dbc.Col(dbc.Button([html.I(className="bi bi-arrow-left me-2"), "Back to Registry"], href="/envds/envops/devices/", color="dark", outline=True), width="auto")
        ], className="mb-4 align-items-center border-bottom pb-3"),

        dbc.Accordion(build_tables(layout_options), id="device-data-accordion", className="mb-4", start_collapsed=True),
        dbc.Accordion(build_graphs(layout_options), id="device-plot-accordion", className="mb-4"),
        dbc.Accordion([dbc.AccordionItem(html.Pre(id="calibration-display", children="Waiting for data..."), title="Calibration Values")], id="device-calibration-accordion", start_collapsed=True),
        
        WebSocket(id="ws-device-instance", url=f"{ws_base}/ws/{topic_type}/{device_id}"),
        html.Div(id="ws-send-instance-buffer", children=json.dumps({"source": f"envds.{config.daq_id}.dashboard", "data": {}, "destpath": f"envds/{topic_type}/settings/request", id_field: device_id}), style={"display": "none"}),
        
        dcc.Store(id="calibration-vars", data=calibration_vars),
        dcc.Store(id="device-definition", data=device_definition),
        dcc.Store(id="device-meta", data=device_meta),
        dcc.Store(id="graph-axes", data={}),
        dcc.Store(id="device-data-buffer", data={}),
        dcc.Store(id="device-settings-buffer", data={})
    ], className="container-fluid mt-3")


# --- Main App Router (SPA) ---
app.layout = create_unified_shell(html.Div([
    dcc.Location(id="device-url", refresh=False),
    html.Div(id="device-page-content") 
]), active_item="devices")

@app.callback(Output("device-page-content", "children"), Input("device-url", "pathname"))
def display_page(pathname):
    if not pathname or "/view/" not in pathname: return render_registry_view()
    try:
        device_uri = pathname.split("/view/")[-1]
        return render_device_detail(device_uri)
    except Exception as e: return dbc.Alert(f"Fatal Layout Error: {traceback.format_exc()}", color="danger", className="m-4")


# --- Sub-Callbacks ---
@app.callback(
    Output("device-data-buffer", "data"), Output("device-settings-buffer", "data"),
    Input("ws-device-instance", "message")
)
def update_device_buffers(event):
    if event and "data" in event:
        try:
            event_data = json.loads(event["data"])
            data_out, settings_out = no_update, no_update
            if "data-update" in event_data and event_data["data-update"]: 
                data_out = event_data["data-update"]
                L.info(f"🚨 DEBUG WS: Data Update Pushed to Buffer. Variables found: {list(data_out.get('variables', {}).keys())}")
            if "settings-update" in event_data and event_data["settings-update"]: 
                settings_out = event_data["settings-update"]
            return [data_out, settings_out]
        except Exception: pass
    return [no_update, no_update]


@app.callback(
    Output({"type": "sensor-graph-1d", "index": MATCH}, "figure"),
    Input({"type": "sensor-graph-1d-dropdown", "index": MATCH}, "value"),
    [State("device-meta", "data"), State("device-definition", "data"), State({"type": "sensor-graph-1d-dropdown", "index": MATCH}, "id")]
)
def select_graph_1d(y_axis, device_meta, device_definition, graph_id):
    default_fig = go.Figure(data=[go.Scatter(x=[], y=[])], layout={"xaxis": {"title": "Time"}, "yaxis": {"title": "Value"}, "template": "simple_white"})
    if not y_axis or not device_meta: return default_fig
    try:
        x, y = [], []
        for doc in get_device_data(device_meta.get("device_id"), device_meta.get("device_type", "sensor")):
            try:
                x.append(doc["variables"]["time"]["data"])
                y.append(doc["variables"][y_axis]["data"])
            except KeyError: pass
        return go.Figure(data=go.Scatter(x=x, y=y, mode="lines+markers"), layout={"xaxis": {"title": "Time"}, "yaxis": {"title": y_axis}, "template": "simple_white"})
    except Exception: return default_fig


@app.callback(
    Output({"type": "sensor-graph-1d", "index": ALL}, "extendData"),
    Input("device-data-buffer", "data"),
    State({"type": "sensor-graph-1d-dropdown", "index": ALL}, "value"),
    prevent_initial_call=True
)
def update_graph_1d(device_data, y_axis_list):
    if not device_data: raise PreventUpdate
    figs_to_update = []
    for y_axis in y_axis_list:
        if not y_axis or "time" not in device_data.get("variables", {}) or y_axis not in device_data.get("variables", {}):
            figs_to_update.append(no_update)
            continue
        x_val, y_val = device_data["variables"]["time"].get("data"), device_data["variables"][y_axis].get("data")
        if x_val is None or y_val is None:
            figs_to_update.append(no_update)
            continue
        if isinstance(x_val, list) and len(x_val) > 0: x_val = x_val[-1]
        if isinstance(y_val, list) and len(y_val) > 0: y_val = y_val[-1]
        figs_to_update.append(({"x": [[x_val]], "y": [[y_val]]}, [0], 1000))
    if all(f == no_update for f in figs_to_update): raise PreventUpdate
    return figs_to_update


@app.callback(
    [Output({"type": "graph-2d-heatmap", "index": MATCH}, "figure", allow_duplicate=True), Output({"type": "graph-2d-line", "index": MATCH}, "figure", allow_duplicate=True)],
    Input({"type": "graph-2d-dropdown", "index": MATCH}, "value"),
    [State("device-meta", "data"), State("device-definition", "data"), State({"type": "graph-2d-dropdown", "index": MATCH}, "id")],
    prevent_initial_call=True,
)
def select_graph_2d(z_axis, device_meta, device_definition, graph_id):
    y_axis = graph_id["index"].split("::")[1]
    default_heatmap = go.Figure(data=[go.Heatmap(x=[], y=[], z=[])], layout={"template": "simple_white", "xaxis": {"title": "Time"}, "yaxis": {"title": y_axis}})
    default_scatter = go.Figure(data=[go.Scatter(x=[], y=[])], layout={"template": "simple_white", "xaxis": {"title": y_axis}, "yaxis": {"title": "Value"}})
    if not z_axis or not device_meta: return [default_heatmap, default_scatter]
    
    use_log = (y_axis == "diameter")
    x, y, orig_z = [], [], []
    y_is_coord = False
    
    if device_definition and y_axis in device_definition.get("variables", {}):
        if device_definition["variables"][y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
            y_is_coord = True
            y = device_definition["variables"][y_axis].get("data", [])

    for doc in get_device_data(device_meta.get("device_id"), device_meta.get("device_type", "sensor")):
        try:
            t_data, z_data = doc["variables"]["time"]["data"], doc["variables"][z_axis]["data"]
            if isinstance(t_data, list):
                x.extend(t_data)
                orig_z.extend(z_data)
                if not y_is_coord: y.extend(doc["variables"][y_axis]["data"])
            else:
                x.append(t_data)
                orig_z.append(z_data)
                if not y_is_coord: y.append(doc["variables"][y_axis]["data"])
        except KeyError: continue

    if len(y) > 0 and isinstance(y[-1], list): y = y[-1]
    z = [[orig_z[xi][yi] if xi < len(orig_z) and yi < len(orig_z[xi]) else None for xi in range(len(x))] for yi in range(len(y))]

    heatmap = go.Figure(data=go.Heatmap(x=x, y=y, z=z, type="heatmap", colorscale="Rainbow"), layout={"template": "simple_white", "xaxis": {"title": "Time"}, "yaxis": {"title": y_axis}})
    scatter = go.Figure(data=[go.Scatter(x=y, y=orig_z[-1] if len(orig_z) > 0 else [])], layout={"template": "simple_white", "xaxis": {"title": y_axis}, "yaxis": {"title": z_axis}})

    if use_log:
        heatmap.update_yaxes(type="log")
        heatmap.update_layout(coloraxis=dict(cmax=None, cmin=None))
        scatter.update_xaxes(type="log")
    return [heatmap, scatter]


@app.callback(
    Output({"type": "graph-2d-heatmap", "index": ALL}, "figure", allow_duplicate=True),
    Input("device-data-buffer", "data"),
    [State({"type": "graph-2d-dropdown", "index": ALL}, "value"), State("device-definition", "data"), State({"type": "graph-2d-heatmap", "index": ALL}, "figure"), State({"type": "graph-2d-heatmap", "index": ALL}, "id")],
    prevent_initial_call=True,
)
def update_graph_2d_heatmap(device_data, z_axis_list, device_definition, current_figs, graph_ids):
    if not device_data: raise PreventUpdate
    heatmaps = []
    for z_axis, graph_id, current_fig in zip(z_axis_list, graph_ids, current_figs):
        if not current_fig or not current_fig.get("data") or not z_axis:
            heatmaps.append(no_update)
            continue
        y_axis = graph_id["index"].split("::")[1]
        y_is_coord = False
        if device_definition and y_axis in device_definition.get("variables", {}) and device_definition["variables"][y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate": y_is_coord = True

        if "time" not in device_data.get("variables", {}) or (not y_is_coord and y_axis not in device_data.get("variables", {})) or z_axis not in device_data.get("variables", {}):
            heatmaps.append(no_update)
            continue

        x = device_data["variables"]["time"]["data"]
        if x in current_fig["data"][0].get("x", []): heatmaps.append(no_update); continue

        if not isinstance(x, list): x = [x]
        for nx in x: current_fig["data"][0]["x"].append(nx)
        
        y = current_fig["data"][0].get("y", [])
        if len(y) == 0: y = device_definition["variables"][y_axis].get("data", []) if y_is_coord else device_data["variables"][y_axis]["data"]
        
        orig_z = device_data["variables"][z_axis]["data"]
        if not isinstance(orig_z, list): orig_z = [orig_z]

        if len(x) > 1:
            z = [[orig_z[xi][yi] if xi < len(orig_z) and yi < len(orig_z[xi]) else None for xi in range(len(x))] for yi in range(len(y))]
            current_fig["data"][0]["z"] = z
        else:
            for yi, yval in enumerate(y):
                try: current_fig["data"][0]["z"][yi].append(orig_z[yi])
                except IndexError: pass
        heatmaps.append(current_fig)
    if all(h == no_update for h in heatmaps): raise PreventUpdate
    return heatmaps


@app.callback(
    Output({"type": "graph-2d-line", "index": ALL}, "figure"),
    Input("device-data-buffer", "data"),
    [State({"type": "graph-2d-dropdown", "index": ALL}, "value"), State("device-definition", "data"), State({"type": "graph-2d-line", "index": ALL}, "figure"), State({"type": "graph-2d-line", "index": ALL}, "id")],
    prevent_initial_call=True,
)
def update_graph_2d_scatter(device_data, z_axis_list, device_definition, current_figs, graph_ids):
    if not device_data: raise PreventUpdate
    scatters = []
    for z_axis, graph_id, current_fig in zip(z_axis_list, graph_ids, current_figs):
        if not current_fig or not current_fig.get("data") or not z_axis:
            scatters.append(no_update)
            continue
        y_axis = graph_id["index"].split("::")[1]
        y_is_coord = False
        if device_definition and y_axis in device_definition.get("variables", {}) and device_definition["variables"][y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate": y_is_coord = True

        if "time" not in device_data.get("variables", {}) or (not y_is_coord and y_axis not in device_data.get("variables", {})) or z_axis not in device_data.get("variables", {}):
            scatters.append(no_update)
            continue

        x = device_data["variables"]["time"]["data"]
        y = device_definition["variables"][y_axis].get("data", []) if y_is_coord else device_data["variables"][y_axis]["data"]
        z = device_data["variables"][z_axis]["data"]

        current_fig["data"][0]["x"] = y
        current_fig["data"][0]["y"] = z
        if isinstance(x, list) and len(x) > 0: x = x[-1]
        current_fig["layout"]["title"] = str(x)
        scatters.append(current_fig)
    if all(s == no_update for s in scatters): raise PreventUpdate
    return scatters

@app.callback(
    Output({"type": "data-table-1d", "index": ALL}, "rowTransaction"),
    Input("device-data-buffer", "data"),
    State({"type": "data-table-1d", "index": ALL}, "columnDefs"),
    prevent_initial_call=True
)
def update_table_1d(device_data, col_defs_list):
    if not device_data: raise PreventUpdate
    transactions = []
    for col_defs in col_defs_list:
        data = {}
        for col in col_defs:
            name = col["field"]
            if name in device_data.get("variables", {}):
                val = device_data["variables"][name].get("data", "")
                if val == "": val = None
                data[name] = val
            else: data[name] = None
        transactions.append({"add": [data], "addIndex": 0})
    if not transactions: raise PreventUpdate
    return transactions

@app.callback(
    Output({"type": "data-table-2d", "index": ALL}, "rowData"), 
    Input("device-data-buffer", "data"),
    [State({"type": "data-table-2d", "index": ALL}, "rowData"), State({"type": "data-table-2d", "index": ALL}, "columnDefs"), State("device-definition", "data")],
    prevent_initial_call=True
)
def update_table_2d(device_data, row_data_list, col_defs_list, device_definition):
    if not device_data: raise PreventUpdate
    new_row_data_list = []
    for col_defs in col_defs_list:
        if not col_defs: new_row_data_list.append(no_update); continue
        dim_2d = col_defs[0]["field"]
        dim_2d_is_coord = device_definition and dim_2d in device_definition.get("variables", {}) and device_definition["variables"][dim_2d].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate"
        
        if dim_2d_is_coord: dim_data = device_definition["variables"][dim_2d].get("data", [])
        else:
            if dim_2d not in device_data.get("variables", {}): new_row_data_list.append(no_update); continue
            dim_data = device_data["variables"][dim_2d].get("data")
            if not dim_data: new_row_data_list.append(no_update); continue

        row_data = []
        for index in range(0, len(dim_data)):
            data = {dim_2d: dim_data[index]}
            for col in col_defs[1:]:
                try: data[col["field"]] = device_data["variables"][col["field"]]["data"][index]
                except (KeyError, IndexError, TypeError): data[col["field"]] = None
            row_data.append(data)
        new_row_data_list.append(row_data)
    if all(r == no_update for r in new_row_data_list): raise PreventUpdate
    return new_row_data_list

@app.callback(
    Output({"type": "settings-table", "index": ALL}, "rowData"), 
    Input("device-settings-buffer", "data"),
    State({"type": "settings-table", "index": ALL}, "rowData"),
    prevent_initial_call=True
)
def update_settings_table(device_settings, row_data_list):
    if not device_settings or not row_data_list: raise PreventUpdate
    updated_row_lists, has_updates = [], False
    for rows in row_data_list:
        if not rows: updated_row_lists.append(no_update); continue
        grid_patched = False
        for row in rows:
            param_name = row["parameter"]
            if param_name in device_settings.get("settings", {}):
                param_data = device_settings["settings"][param_name]
                if isinstance(param_data, dict) and "data" in param_data:
                    actual_val, req_val = param_data["data"].get("actual", ""), param_data["data"].get("requested", "")
                elif isinstance(param_data, dict):
                    actual_val, req_val = param_data.get("actual", ""), param_data.get("requested", "")
                else: continue

                if str(row.get("actual_value")) != str(actual_val): row["actual_value"] = actual_val; grid_patched = True
                if row.get("requested_value") in ["", None]: row["requested_value"] = req_val; grid_patched = True
        if grid_patched: updated_row_lists.append(rows); has_updates = True
        else: updated_row_lists.append(no_update)
    if not has_updates: raise PreventUpdate
    return updated_row_lists

@app.callback(Output("calibration-display", "children"), Input("device-data-buffer", "data"), [State("calibration-display", "children"), State("calibration-vars", "data")], prevent_initial_call=True)
def update_calibration_display(device_data, current_display, cal_vars):
    if not device_data or not cal_vars: raise PreventUpdate
    try: cal_data = json.loads(current_display)
    except: cal_data = {}
    has_updates = False
    for name in cal_vars:
        if name in device_data.get("variables", {}):
            new_val = device_data["variables"][name].get("data")
            if cal_data.get(name) != new_val: cal_data[name] = new_val; has_updates = True
    if not has_updates and current_display != "Waiting for data...": raise PreventUpdate
    if not cal_data: return "Waiting for data..."
    return json.dumps(cal_data, indent=2)

@app.callback(
    Output("ws-send-instance-buffer", "children", allow_duplicate=True),
    Input({"type": "submit-setting-btn", "index": ALL}, "n_clicks"),
    State({"type": "settings-table", "index": ALL}, "selectedRows"),
    State("device-meta", "data"),
    prevent_initial_call=True
)
def submit_setting_change(n_clicks_list, selected_rows_list, device_meta):
    if not any(n for n in n_clicks_list if n): raise PreventUpdate
    selected_row = next((rows[0] for rows in selected_rows_list if rows), None)
    if not selected_row: raise PreventUpdate
    raw_val = selected_row.get("requested_value")
    if raw_val is None or raw_val == "": raise PreventUpdate
    try:
        if selected_row.get("type") == "int": requested_val = int(raw_val)
        elif selected_row.get("type") == "float": requested_val = float(raw_val)
        elif raw_val in ["True", "False"]: requested_val = raw_val == "True"
        else: requested_val = str(raw_val)
    except: requested_val = raw_val
    dtype = device_meta.get("device_type", "sensor")
    topic_type = "sensor" if dtype == "operational" else dtype
    id_field = "controllerid" if dtype == "controller" else "deviceid"
    return json.dumps({"source": f"envds.{config.daq_id}.dashboard", "data": {"settings": {selected_row["parameter"]: {"requested": requested_val}}}, "destpath": f"envds/{topic_type}/settings/request", id_field: device_meta["device_id"]})

@app.callback(Output({"type": "graph-2d-heatmap", "index": MATCH}, "figure", allow_duplicate=True), [Input({"type": "graph-2d-z-axis-submit", "index": MATCH}, "n_clicks")], [State({"type": "graph-2d-z-axis-min", "index": MATCH}, "value"), State({"type": "graph-2d-z-axis-max", "index": MATCH}, "value"), State({"type": "graph-2d-heatmap", "index": MATCH}, "figure")], prevent_initial_call=True)
def set_2d_z_axis_range(n, axis_min, axis_max, heatmap): return go.Figure(heatmap).update_layout(coloraxis=dict(cauto=False, cmax=axis_max, cmin=axis_min))

@app.callback(Output("ws-device-instance", "send"), Input("ws-send-instance-buffer", "children"))
def send_to_instance(value): return value