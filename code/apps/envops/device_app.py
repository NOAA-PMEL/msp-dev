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
from datetime import datetime

from utils import get_registry_data, config, create_unified_shell, register_sidebar_callbacks

L = logging.getLogger(__name__)

# --- Initialize Isolated Dash App ---
app = dash.Dash(__name__, requests_pathname_prefix="/envds/envops/devices/", routes_pathname_prefix="/", suppress_callback_exceptions=True)
register_sidebar_callbacks(app)

datastore_url = f"datastore.{config.daq_id}-system.svc.cluster.local"

# --- Helper Functions (Adapted from sensor.py) ---
def get_all_sensors():
    url = f"http://{datastore_url}/device-instance/registry/get/"
    try:
        response = httpx.get(url, params={"device_type": "sensor"}, timeout=5.0)
        return response.json().get("results", [])
    except Exception as e:
        L.error(f"get_all_sensors error: {e}")
        return []

def get_device_data(device_id: str, device_type: str="sensor"):
    query = {"device_type": device_type, "device_id": device_id}
    url = f"http://{datastore_url}/device/data/get/"
    try:
        response = httpx.get(url, params=query, timeout=10.0)
        results = response.json()
        if "results" in results and results["results"]: return results["results"]
    except Exception: pass
    return []

def get_device_instance(device_id: str, device_type: str="sensor"):
    query = {"device_type": device_type, "device_id": device_id}
    url = f"http://{datastore_url}/device-instance/registry/get/"
    try:
        response = httpx.get(url, params=query, timeout=5.0)
        results = response.json()
        if "results" in results and results["results"]: return results["results"][0]
    except Exception: pass
    return {}

def get_device_definition(device_definition_id: str, device_type: str="sensor"):
    query = {"device_type": device_type, "device_definition_id": device_definition_id}
    url = f"http://{datastore_url}/device-definition/registry/get/"
    try:
        response = httpx.get(url, params=query, timeout=5.0)
        results = response.json()
        if "results" in results and results["results"]: return results["results"][0]
    except Exception: pass
    return {}

def get_device_definition_by_device_id(device_id: str, device_type: str="sensor"):
    device = get_device_instance(device_id=device_id, device_type=device_type)
    if device:
        try:
            device_definition_id = "::".join([device["make"], device["model"], device["version"]])
            return get_device_definition(device_definition_id=device_definition_id, device_type=device_type)
        except Exception: pass
    return {}

# --- Dynamic Builders (Adapted from sensor.py) ---
def build_tables(layout_options):
    table_list = []
    for ltype, dims in layout_options.items():
        for dim, options in dims.items():
            title = "Data"
            if ltype == "layout-settings":
                title = f"Device Settings"
                column_defs = [
                    {"field": "parameter", "headerName": "Setting Parameter", "editable": False, "pinned": "left"},
                    {"field": "description", "headerName": "Description", "editable": False},
                    {"field": "actual_value", "headerName": "Actual Value", "editable": False},
                    {"field": "requested_value", "headerName": "Requested Value", "editable": True, "cellEditorSelector": {"function": "determineSettingEditor(params)"}}
                ]
                table_list.append(dbc.AccordionItem([
                    dag.AgGrid(id={"type": "settings-table", "index": dim}, rowData=options.get("row-data-skeletons", []), columnDefs=column_defs, columnSizeOptions="autoSize", dashGridOptions={"domLayout": "autoHeight", "singleClickEdit": True, "rowSelection": "single"}),
                    dbc.Button("Submit Selected Setting", id={"type": "submit-setting-btn", "index": dim}, color="primary", className="mt-3")
                ], title=title))
            elif ltype == "layout-1d":
                title = f"Data 1-D ({dim})"
                table_list.append(dbc.AccordionItem([dag.AgGrid(id={"type": "data-table-1d", "index": dim}, rowData=[], columnDefs=options["table-column-defs"], columnSizeOptions="autoSize")], title=title))
            elif ltype == "layout-2d":
                title = f"Data 2-D (time, {dim})"
                table_list.append(dbc.AccordionItem([dag.AgGrid(id={"type": "data-table-2d", "index": f"time::{dim}"}, rowData=[], columnDefs=options["table-column-defs"], columnSizeOptions="autoSize")], title=title))
    return table_list

def build_graph_1d(dropdown_list, xaxis="time"):
    return dbc.Card([
        dbc.CardHeader([dcc.Dropdown(id={"type": "sensor-graph-1d-dropdown", "index": xaxis}, options=dropdown_list, value="")]),
        dcc.Graph(id={"type": "sensor-graph-1d", "index": xaxis}, figure=go.Figure(data=go.Scatter(x=[], y=[], type="scatter")), style={"height": 300}),
    ])

def build_graph_2d(dropdown_list, xaxis="time", yaxis="", zaxis=""):
    content = dbc.Row([
        dbc.Button("Submit", {"type": "graph-2d-z-axis-submit", "index": f"{xaxis}::{yaxis}"}),
        dbc.Label("z-axis min:"), dbc.Col(dbc.Input(type="number", id={"type": "graph-2d-z-axis-min", "index": f"{xaxis}::{yaxis}"})),
        dbc.Label("z-axis max:"), dbc.Col(dbc.Input(type="number", id={"type": "graph-2d-z-axis-max", "index": f"{xaxis}::{yaxis}"})),
    ])
    axes_settings = dbc.Accordion([dbc.AccordionItem([dbc.Card(children=[content])], title="Axes Settings")], start_collapsed=True)
    return dbc.Card([
        dbc.CardHeader([dcc.Dropdown(id={"type": "graph-2d-dropdown", "index": f"{xaxis}::{yaxis}"}, options=dropdown_list, value="")]),
        dbc.Row([
            axes_settings,
            dbc.Col(dcc.Graph(id={"type": "graph-2d-heatmap", "index": f"{xaxis}::{yaxis}"}, style={"height": 500})),
            dbc.Col(dcc.Graph(id={"type": "graph-2d-line", "index": f"{xaxis}::{yaxis}"}, style={"height": 500})),
        ]),
    ])

def build_graph_3d(dropdown_list, xaxis="", yaxis="", zaxis=""):
    content = dbc.Row([dbc.Button("Submit", {"type": "graph-3d-z-axis-submit", "index": f"{xaxis}::{yaxis}"}), dbc.Label("z-axis min:")])
    axes_settings = dbc.Accordion([dbc.AccordionItem([dbc.Card(children=[content])], title="Axes Settings")], start_collapsed=True)
    return dbc.Card([
        dbc.CardHeader([dcc.Dropdown(id={"type": "graph-3d-dropdown", "index": f"{xaxis}::{yaxis}"}, options=dropdown_list, value="")]),
        dbc.Row([
            axes_settings,
            dbc.Col(dcc.Graph(id={"type": "graph-3d-line", "index": f"{xaxis}::{yaxis}"}, style={"height": 500})),
            dbc.Col(dcc.Graph(id={"type": "graph-3d-heatmap", "index": f"{xaxis}::{yaxis}"}, style={"height": 500})),
        ]),
    ])

def build_graphs(layout_options):
    graph_list = []
    for ltype, dims in layout_options.items():
        for dim, options in dims.items():
            if ltype == "layout-1d":
                graph_list.append(dbc.AccordionItem([dbc.Row([build_graph_1d(options["variable-list"], xaxis=dim)])], title=f"Plots 1-D ({dim})"))
            elif ltype == "layout-2d":
                graph_list.append(dbc.AccordionItem([dbc.Row([build_graph_2d(options["variable-list"], xaxis="time", yaxis=dim)])], title=f"Plots 2-D (time, {dim})"))
            elif ltype == "layout-3d":
                axes = dim.split("::")
                graph_list.append(dbc.AccordionItem([dbc.Row([build_graph_3d(options["variable-list"], xaxis=axes[0], yaxis=axes[1])])], title=f"Plots 3-D ({axes[0]}, {axes[1]})"))
    return graph_list


# --- Main App Layout ---
app.layout = create_unified_shell(html.Div([
    dcc.Location(id="device-url", refresh=False),
    html.Div(id="device-page-content") 
]), active_item="ops")

@app.callback(
    Output("device-page-content", "children"),
    Input("device-url", "pathname")
)
def render_deployment_devices(pathname):
    if not pathname or "deployment/" not in pathname:
        return dbc.Alert("Select a deployment from the sidebar.", color="info", className="m-4")
    deployment_id = pathname.split("/")[-1]
    
    # Generate Dropdown with all available sensors
    sensors = get_all_sensors()
    sensor_options = [{"label": s.get("metadata", {}).get("name", "Unknown"), "value": s.get("metadata", {}).get("name")} for s in sensors if s.get("metadata", {}).get("name")]
    
    return html.Div([
        dbc.Row([
            dbc.Col([
                html.H2([html.I(className="bi bi-cpu me-2"), "Raw Device Telemetry"], className="fw-bold mb-0"),
                html.P(f"Deployment: {deployment_id}", className="text-muted mb-0")
            ]),
            dbc.Col([
                html.Label("Select an Instrument:", className="fw-bold text-muted small"),
                dcc.Dropdown(id="sensor-selector", options=sensor_options, placeholder="Select a device to view...", className="shadow-sm")
            ], width=4)
        ], className="mb-4 align-items-center border-bottom pb-3"),
        
        # Container for the dynamically generated sensor UI
        html.Div(id="sensor-ui-container")
    ], className="container-fluid mt-3")


@app.callback(
    Output("sensor-ui-container", "children"),
    Input("sensor-selector", "value")
)
def generate_sensor_ui(sensor_id):
    if not sensor_id:
        return html.Div(html.H5("Please select an instrument from the dropdown.", className="text-muted text-center mt-5"))
        
    parts = sensor_id.split("::")
    sensor_meta = {"device_id": sensor_id, "make": parts[0] if len(parts)>0 else "", "model": parts[1] if len(parts)>1 else "", "serial_number": parts[2] if len(parts)>2 else ""}
    sensor_definition = get_device_definition_by_device_id(device_id=sensor_id, device_type="sensor")

    layout_options = {
        "layout-settings": {"time": {"table-column-defs": [], "variable-list": [], "row-data-skeletons": []}},
        "layout-calibration": {"time": {"table-column-defs": [], "variable-list": []}},
        "layout-1d": {"time": {"table-column-defs": [], "variable-list": []}},
    }
    calibration_vars = []

    if sensor_definition:
        try:
            dimensions = sensor_definition.get("dimensions", {})
            multi_dim = len(dimensions.keys()) > 1

            for name, var in sensor_definition.get("variables", {}).items():
                var_type = var.get("attributes", {}).get("variable_type", {}).get("data")
                
                if var_type == "setting":
                    long_name = var.get("attributes", {}).get("long_name", {}).get("data", name)
                    control_metadata = {
                        "parameter": name, "description": long_name, "actual_value": "", "requested_value": "",
                        "type": var.get("type", "unknown"),
                        "allowed_values": [x.strip() for x in (var.get("attributes", {}).get("allowed_values", {}).get("data", "")).split(",")] if var.get("attributes", {}).get("allowed_values", {}).get("data") else None,
                    }
                    layout_options["layout-settings"]["time"]["row-data-skeletons"].append(control_metadata)
                elif var_type == "calibration":
                    calibration_vars.append(name)
                elif var_type == "main":
                    if "shape" not in var or "time" not in var["shape"]: continue
                    
                    long_name = var.get("attributes", {}).get("long_name", {}).get("data", name)
                    dtype = var.get("type", "unknown")
                    data_type = "number" if dtype in ["float", "double", "int"] else "boolean" if dtype == "bool" else "text"

                    cd = {"field": name, "headerName": long_name, "filter": False, "cellDataType": data_type}

                    if multi_dim and len(var["shape"]) == 2:
                        if "layout-2d" not in layout_options: layout_options["layout-2d"] = {}
                        dim_2d = [d for d in var["shape"] if d != "time"][0]
                        if dim_2d not in layout_options["layout-2d"]:
                            layout_options["layout-2d"][dim_2d] = {"table-column-defs": [], "variable-list": []}
                            layout_options["layout-2d"][dim_2d]["table-column-defs"].append({"field": dim_2d, "headerName": dim_2d, "filter": False, "cellDataType": "text", "pinned": "left"})
                        layout_options["layout-2d"][dim_2d]["table-column-defs"].append(cd)
                    elif multi_dim and len(var["shape"]) == 3:
                        if "layout-3d" not in layout_options: layout_options["layout-3d"] = {}
                        dims_3d = [d for d in var["shape"] if d != "time"]
                        dim_3d_key = f"{dims_3d[0]}::{dims_3d[1]}"
                        if dim_3d_key not in layout_options["layout-3d"]: layout_options["layout-3d"][dim_3d_key] = {"table-column-defs": [], "variable-list": []}
                        layout_options["layout-3d"][dim_3d_key]["table-column-defs"].append(cd)
                    else:
                        layout_options["layout-1d"]["time"]["table-column-defs"].append(cd)

            for ltype, dims in layout_options.items():
                for dim, options in dims.items():
                    if "table-column-defs" in options:
                        for cd in options["table-column-defs"]:
                            if cd["field"] in dimensions or cd.get("cellDataType") != "number": continue
                            layout_options[ltype][dim]["variable-list"].append({"label": cd["field"], "value": cd["field"]})

        except Exception as e:
            L.error(f"build layout error: {e}")

    initial_request = {
        "source": f"envds.{config.daq_id}.dashboard",
        "data": {}, "destpath": "envds/sensor/settings/request", "deviceid": sensor_id
    }

    ws_protocol = "wss://" if config.ws_use_tls.lower() == "true" else "ws://"
    ws_base = f"{ws_protocol}{config.external_hostname}:{config.ws_port}/envds/envops"

    return html.Div([
        dbc.Accordion(build_tables(layout_options), id="sensor-data-accordion", className="mb-4"),
        dbc.Accordion(build_graphs(layout_options), id="sensor-plot-accordion", className="mb-4"),
        dbc.Accordion([dbc.AccordionItem(html.Pre(id="calibration-display", children="Waiting for data...", style={"whiteSpace": "pre-wrap", "wordBreak": "break-all"}), title="Calibration Values")], id="sensor-calibration-accordion", start_collapsed=True),
        
        WebSocket(id="ws-sensor-instance", url=f"{ws_base}/ws/sensor/{sensor_id}"),
        html.Div(id="ws-send-instance-buffer", children=json.dumps(initial_request), style={"display": "none"}),
        
        dcc.Store(id="calibration-vars", data=calibration_vars),
        dcc.Store(id="sensor-definition", data=sensor_definition),
        dcc.Store(id="sensor-meta", data=sensor_meta),
        dcc.Store(id="graph-axes", data={}),
        dcc.Store(id="sensor-data-buffer", data={}),
        dcc.Store(id="sensor-settings-buffer", data={})
    ])


# --- Sub-Callbacks (Adapted from sensor.py) ---

@app.callback(
    Output("sensor-data-buffer", "data"), Output("sensor-settings-buffer", "data"),
    Input("ws-sensor-instance", "message")
)
def update_sensor_buffers(event):
    if event and "data" in event:
        try:
            event_data = json.loads(event["data"])
            if "data-update" in event_data and event_data["data-update"]: return [event_data["data-update"], no_update]
            if "settings-update" in event_data and event_data["settings-update"]: return [no_update, event_data["settings-update"]]
        except Exception: pass
    return [no_update, no_update]

@app.callback(
    Output({"type": "sensor-graph-1d", "index": MATCH}, "figure"),
    Input({"type": "sensor-graph-1d-dropdown", "index": MATCH}, "value"),
    [State("sensor-meta", "data"), State("graph-axes", "data"), State("sensor-definition", "data"), State({"type": "sensor-graph-1d-dropdown", "index": MATCH}, "id")]
)
def select_graph_1d(y_axis, sensor_meta, graph_axes, sensor_definition, graph_id):
    default_fig = go.Figure(layout={"xaxis": {"title": "Time"}, "yaxis": {"title": "Value"}, "template": "simple_white"})
    if not y_axis: return default_fig

    try:
        x, y = [], []
        results = get_device_data(device_id=sensor_meta.get("device_id"), device_type="sensor")
        if results:
            for doc in results:
                try:
                    x.append(doc["variables"]["time"]["data"])
                    y.append(doc["variables"][y_axis]["data"])
                except KeyError: pass

        units = ""
        try: units = f'({sensor_definition["variables"][y_axis]["attributes"]["units"]["data"]})'
        except Exception: pass

        return go.Figure(data=go.Scatter(x=x, y=y, type="scatter", mode="lines+markers"), layout={"xaxis": {"title": "Time"}, "yaxis": {"title": f"{y_axis} {units}".strip()}, "template": "simple_white"})
    except Exception: return default_fig

@app.callback(
    Output({"type": "sensor-graph-1d", "index": ALL}, "extendData"),
    Input("sensor-data-buffer", "data"),
    State({"type": "sensor-graph-1d-dropdown", "index": ALL}, "value"),
    prevent_initial_call=True
)
def update_graph_1d(sensor_data, y_axis_list):
    if not sensor_data: raise PreventUpdate
    figs_to_update = []
    for y_axis in y_axis_list:
        if not y_axis:
            figs_to_update.append(no_update)
            continue
        variables = sensor_data.get("variables", {})
        if "time" not in variables or y_axis not in variables:
            figs_to_update.append(no_update)
            continue
        x_val, y_val = variables["time"].get("data"), variables[y_axis].get("data")
        if x_val is None or y_val is None:
            figs_to_update.append(no_update)
            continue
        if isinstance(x_val, list) and len(x_val) > 0: x_val = x_val[-1]
        if isinstance(y_val, list) and len(y_val) > 0: y_val = y_val[-1]
        figs_to_update.append(({"x": [[x_val]], "y": [[y_val]]}, [0], 1000))
        
    if not any(f != no_update for f in figs_to_update): raise PreventUpdate
    return figs_to_update

@app.callback(
    Output("calibration-display", "children"),
    Input("sensor-data-buffer", "data"),
    [State("calibration-display", "children"), State("calibration-vars", "data")]
)
def update_calibration_display(sensor_data, current_display, cal_vars):
    if not sensor_data or not cal_vars: raise PreventUpdate
    try: cal_data = json.loads(current_display)
    except: cal_data = {}

    has_updates = False
    for name in cal_vars:
        if name in sensor_data.get("variables", {}):
            new_val = sensor_data["variables"][name].get("data")
            if cal_data.get(name) != new_val:
                cal_data[name] = new_val
                has_updates = True
                
    if not has_updates and current_display != "Waiting for data...": raise PreventUpdate
    if not cal_data: return "Waiting for data..."
    return json.dumps(cal_data, indent=2)

@app.callback(
    Output({"type": "data-table-1d", "index": ALL}, "rowTransaction"),
    Input("sensor-data-buffer", "data"),
    State({"type": "data-table-1d", "index": ALL}, "columnDefs")
)
def update_table_1d(sensor_data, col_defs_list):
    if not sensor_data: raise PreventUpdate
    transactions = []
    for col_defs in col_defs_list:
        data = {col["field"]: sensor_data.get("variables", {}).get(col["field"], {}).get("data", "") for col in col_defs}
        transactions.append({"add": [data], "addIndex": 0})
    if not transactions: raise PreventUpdate
    return transactions

@app.callback(
    Output("ws-send-instance-buffer", "children", allow_duplicate=True),
    Input({"type": "submit-setting-btn", "index": ALL}, "n_clicks"),
    State({"type": "settings-table", "index": ALL}, "selectedRows"),
    State("sensor-meta", "data"),
    prevent_initial_call=True
)
def submit_setting_change(n_clicks_list, selected_rows_list, sensor_meta):
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

    return json.dumps({
        "source": f"envds.{config.daq_id}.dashboard",
        "data": {"settings": {selected_row["parameter"]: {"requested": requested_val}}},
        "destpath": "envds/sensor/settings/request",
        "deviceid": sensor_meta["device_id"]
    })

@app.callback(Output("ws-sensor-instance", "send"), Input("ws-send-instance-buffer", "children"))
def send_to_instance(value): return value