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

# --- Initialize Isolated Dash App ---
app = dash.Dash(__name__, requests_pathname_prefix="/envds/envops/devices/", routes_pathname_prefix="/", suppress_callback_exceptions=True)
register_sidebar_callbacks(app)

datastore_url = f"datastore.{config.daq_id}-system.svc.cluster.local"

# --- Corrected Route-Splitting Helper Functions ---
def get_all_devices():
    """Fetches Sensors, Operational devices, and Controllers from the registry and tags them."""
    devices = []
    # 🟢 NEW: Added "operational" to the discovery loop
    for d_type in ["sensor", "operational", "controller"]:
        # Split routes based on legacy datastore setup
        path = "controller-instance" if d_type == "controller" else "device-instance"
        url = f"http://{datastore_url}/{path}/registry/get/"
        try:
            query = {"device_type": d_type} if d_type in ["sensor", "operational"] else {}
            response = httpx.get(url, params=query, timeout=5.0)
            items = response.json().get("results", [])
            for item in items:
                item["_device_type"] = d_type # Tag it so the UI knows how to route it
            devices.extend(items)
        except Exception as e:
            L.error(f"get_all_devices error for {d_type}: {e}")
    return devices

def get_device_data(device_id: str, device_type: str="sensor"):
    path = "controller" if device_type == "controller" else "device"
    query = {"controller_id": device_id} if device_type == "controller" else {"device_type": device_type, "device_id": device_id}
    url = f"http://{datastore_url}/{path}/data/get/"
    try:
        response = httpx.get(url, params=query, timeout=10.0)
        results = response.json()
        if "results" in results and results["results"]: return results["results"]
    except Exception as e:
        L.error(f"get_device_data error: {e}")
    return []

def get_device_instance(device_id: str, device_type: str="sensor"):
    path = "controller-instance" if device_type == "controller" else "device-instance"
    query = {"controller_id": device_id} if device_type == "controller" else {"device_type": device_type, "device_id": device_id}
    url = f"http://{datastore_url}/{path}/registry/get/"
    try:
        response = httpx.get(url, params=query, timeout=5.0)
        results = response.json()
        if "results" in results and results["results"]: return results["results"][0]
    except Exception as e:
        L.error(f"get_device_instance error: {e}")
    return {}

def get_device_definition(device_definition_id: str, device_type: str="sensor"):
    path = "controller-definition" if device_type == "controller" else "device-definition"
    query = {"controller_definition_id": device_definition_id} if device_type == "controller" else {"device_type": device_type, "device_definition_id": device_definition_id}
    url = f"http://{datastore_url}/{path}/registry/get/"
    try:
        response = httpx.get(url, params=query, timeout=5.0)
        results = response.json()
        if "results" in results and results["results"]: return results["results"][0]
    except Exception as e:
        L.error(f"get_device_definition error: {e}")
    return {}

def get_device_definition_by_device_id(device_id: str, device_type: str="sensor"):
    device = get_device_instance(device_id=device_id, device_type=device_type)
    if device:
        try:
            device_definition_id = "::".join([device["make"], device["model"], device["version"]])
            return get_device_definition(device_definition_id=device_definition_id, device_type=device_type)
        except Exception as e:
            L.error(f"get_device_definition_by_device_id error: {e}")
    return {}

# --- Dynamic Builders ---
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
        dbc.CardHeader([dcc.Dropdown(id={"type": "sensor-graph-1d-dropdown", "index": xaxis}, options=dropdown_list, value="", placeholder="Select variable to plot...")]),
        dcc.Graph(id={"type": "sensor-graph-1d", "index": xaxis}, figure=go.Figure(data=go.Scatter(x=[], y=[], type="scatter")), style={"height": 400}),
    ], className="shadow-sm border-0")

def build_graph_2d(dropdown_list, xaxis="time", yaxis="", zaxis=""):
    content = dbc.Row([
        dbc.Button("Submit Range", {"type": "graph-2d-z-axis-submit", "index": f"{xaxis}::{yaxis}"}, color="primary", className="mb-2"),
        dbc.Label("z-axis min:", className="small text-muted fw-bold"), dbc.Col(dbc.Input(type="number", id={"type": "graph-2d-z-axis-min", "index": f"{xaxis}::{yaxis}"}, className="mb-2")),
        dbc.Label("z-axis max:", className="small text-muted fw-bold"), dbc.Col(dbc.Input(type="number", id={"type": "graph-2d-z-axis-max", "index": f"{xaxis}::{yaxis}"})),
    ])
    axes_settings = dbc.Accordion([dbc.AccordionItem([dbc.Card(children=[content], className="border-0 shadow-sm p-3")], title="Axes Settings")], start_collapsed=True, className="mb-3")
    return dbc.Card([
        dbc.CardHeader([dcc.Dropdown(id={"type": "graph-2d-dropdown", "index": f"{xaxis}::{yaxis}"}, options=dropdown_list, value="", placeholder="Select variable to plot...")]),
        dbc.CardBody([
            axes_settings,
            dbc.Row([
                dbc.Col(dcc.Graph(id={"type": "graph-2d-heatmap", "index": f"{xaxis}::{yaxis}"}, style={"height": 500})),
                dbc.Col(dcc.Graph(id={"type": "graph-2d-line", "index": f"{xaxis}::{yaxis}"}, style={"height": 500})),
            ])
        ])
    ], className="shadow-sm border-0")

def build_graph_3d(dropdown_list, xaxis="", yaxis="", zaxis=""):
    return dbc.Card([
        dbc.CardHeader([dcc.Dropdown(id={"type": "graph-3d-dropdown", "index": f"{xaxis}::{yaxis}"}, options=dropdown_list, value="", placeholder="Select variable to plot...")]),
        dbc.Row([
            dbc.Col(dcc.Graph(id={"type": "graph-3d-line", "index": f"{xaxis}::{yaxis}"}, style={"height": 500})),
            dbc.Col(dcc.Graph(id={"type": "graph-3d-heatmap", "index": f"{xaxis}::{yaxis}"}, style={"height": 500})),
        ]),
    ])

def build_graphs(layout_options):
    graph_list = []
    for ltype, dims in layout_options.items():
        for dim, options in dims.items():
            if ltype == "layout-1d":
                graph_list.append(dbc.AccordionItem([build_graph_1d(options["variable-list"], xaxis=dim)], title=f"Plots 1-D ({dim})"))
            elif ltype == "layout-2d":
                graph_list.append(dbc.AccordionItem([build_graph_2d(options["variable-list"], xaxis="time", yaxis=dim)], title=f"Plots 2-D (time, {dim})"))
            elif ltype == "layout-3d":
                axes = dim.split("::")
                graph_list.append(dbc.AccordionItem([build_graph_3d(options["variable-list"], xaxis=axes[0], yaxis=axes[1])], title=f"Plots 3-D ({axes[0]}, {axes[1]})"))
    return graph_list


# --- Main App Layout ---
app.layout = create_unified_shell(html.Div([
    dcc.Location(id="device-url", refresh=False),
    html.Div(id="device-page-content") 
]), active_item="devices")

@app.callback(
    Output("device-page-content", "children"),
    Input("device-url", "pathname")
)
def render_global_devices(pathname):
    try:
        devices = get_all_devices()
        device_options = []
        
        for d in devices:
            make = d.get("make")
            model = d.get("model")
            sn = d.get("serial_number")
            
            if not make or not model or not sn: continue
            
            dtype = d.get("_device_type", "sensor")
            device_id = f"{make}::{model}::{sn}"
            
            # Embed the device type into the value so the UI knows how to route it
            dropdown_val = f"{dtype}::{device_id}"
            label = f"{make} {model} (SN: {sn}) [{dtype.capitalize()}]"
            
            device_options.append({"label": label, "value": dropdown_val})

        device_options = sorted(device_options, key=lambda d: d['label'])
        
        return html.Div([
            dbc.Row([
                dbc.Col([
                    html.H2([html.I(className="bi bi-cpu me-2"), "Fleet Device Diagnostics"], className="fw-bold mb-0"),
                    html.P("Global registry of all active sensors and controllers.", className="text-muted mb-0")
                ]),
                dbc.Col([
                    html.Label("Select an Instrument:", className="fw-bold text-muted small"),
                    dcc.Dropdown(id="device-selector", options=device_options, placeholder="Select a device to view...", className="shadow-sm")
                ], width=5)
            ], className="mb-4 align-items-center border-bottom pb-3"),
            
            html.Div(id="device-ui-container")
        ], className="container-fluid mt-3")
        
    except Exception as e:
        L.error(f"[DEVICE UI] Layout Crash: {traceback.format_exc()}")
        return html.Div([
            dbc.Alert([
                html.H4("🚨 Internal Server Error", className="alert-heading"),
                html.P("The dashboard encountered a fatal Python exception while building the layout:")
            ], color="danger", className="m-4 shadow-sm"),
            html.Pre(traceback.format_exc(), className="bg-dark text-danger p-3 mx-4 rounded shadow-sm border border-danger", style={"overflowX": "auto"})
        ])

@app.callback(
    Output("device-ui-container", "children"),
    Input("device-selector", "value")
)
def generate_device_ui(dropdown_val):
    if not dropdown_val:
        return html.Div(html.H5("Please select an instrument from the dropdown to load its UI and variable plots.", className="text-muted text-center mt-5"))
        
    parts = dropdown_val.split("::")
    device_type = parts[0]
    device_id = f"{parts[1]}::{parts[2]}::{parts[3]}"
    
    device_meta = {"device_id": device_id, "device_type": device_type, "make": parts[1], "model": parts[2], "serial_number": parts[3]}
    device_definition = get_device_definition_by_device_id(device_id=device_id, device_type=device_type)

    layout_options = {
        "layout-settings": {"time": {"table-column-defs": [], "variable-list": [], "row-data-skeletons": []}},
        "layout-calibration": {"time": {"table-column-defs": [], "variable-list": []}},
        "layout-1d": {"time": {"table-column-defs": [], "variable-list": []}},
    }
    calibration_vars = []

    if device_definition:
        try:
            dimensions = device_definition.get("dimensions", {})
            multi_dim = len(dimensions.keys()) > 1

            for name, var in device_definition.get("variables", {}).items():
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
                            # Ensure we append the descriptive headerName as the label for the dropdowns
                            label = cd.get("headerName", cd["field"])
                            layout_options[ltype][dim]["variable-list"].append({"label": label, "value": cd["field"]})

        except Exception as e:
            L.error(f"build layout error: {e}")

    # Set up the correct API payload for the settings buffer based on device type
    # 🟢 DYNAMIC MAPPING: Operational uses sensor streams for its backend configuration
    topic_type = "sensor" if device_type == "operational" else device_type
    id_field = "controllerid" if device_type == "controller" else "deviceid"
    initial_request = {
        "source": f"envds.{config.daq_id}.dashboard",
        "data": {}, "destpath": f"envds/{topic_type}/settings/request", 
        id_field: device_id
    }

    ws_protocol = "wss://" if str(config.ws_use_tls).lower() == "true" else "ws://"
    ws_base = f"{ws_protocol}{config.external_hostname}:{config.ws_port}/envds/envops"

    return html.Div([
        dbc.Accordion(build_tables(layout_options), id="device-data-accordion", className="mb-4", start_collapsed=True),
        dbc.Accordion(build_graphs(layout_options), id="device-plot-accordion", className="mb-4"),
        dbc.Accordion([dbc.AccordionItem(html.Pre(id="calibration-display", children="Waiting for data...", style={"whiteSpace": "pre-wrap", "wordBreak": "break-all"}), title="Calibration Values")], id="device-calibration-accordion", start_collapsed=True),
        
        # 🟢 DYNAMIC WEBSOCKET: Operational listens to the sensor stream
        WebSocket(id="ws-device-instance", url=f"{ws_base}/ws/{topic_type}/{device_id}"),
        html.Div(id="ws-send-instance-buffer", children=json.dumps(initial_request), style={"display": "none"}),
        
        dcc.Store(id="calibration-vars", data=calibration_vars),
        dcc.Store(id="device-definition", data=device_definition),
        dcc.Store(id="device-meta", data=device_meta),
        dcc.Store(id="graph-axes", data={}),
        dcc.Store(id="device-data-buffer", data={}),
        dcc.Store(id="device-settings-buffer", data={})
    ])


# --- Sub-Callbacks ---

@app.callback(
    Output("device-data-buffer", "data"), Output("device-settings-buffer", "data"),
    Input("ws-device-instance", "message")
)
def update_device_buffers(event):
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
    [State("device-meta", "data"), State("graph-axes", "data"), State("device-definition", "data"), State({"type": "sensor-graph-1d-dropdown", "index": MATCH}, "id")]
)
def select_graph_1d(y_axis, device_meta, graph_axes, device_definition, graph_id):
    default_fig = go.Figure(layout={"xaxis": {"title": "Time"}, "yaxis": {"title": "Value"}, "template": "simple_white"})
    if not y_axis: return default_fig

    try:
        x, y = [], []
        results = get_device_data(device_id=device_meta.get("device_id"), device_type=device_meta.get("device_type", "sensor"))
        if results:
            for doc in results:
                try:
                    x.append(doc["variables"]["time"]["data"])
                    y.append(doc["variables"][y_axis]["data"])
                except KeyError: pass

        units = ""
        try: units = f'({device_definition["variables"][y_axis]["attributes"]["units"]["data"]})'
        except Exception: pass

        return go.Figure(data=go.Scatter(x=x, y=y, type="scatter", mode="lines+markers"), layout={"xaxis": {"title": "Time"}, "yaxis": {"title": f"{y_axis} {units}".strip()}, "template": "simple_white"})
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
        if not y_axis:
            figs_to_update.append(no_update)
            continue
        variables = device_data.get("variables", {})
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
    Input("device-data-buffer", "data"),
    [State("calibration-display", "children"), State("calibration-vars", "data")]
)
def update_calibration_display(device_data, current_display, cal_vars):
    if not device_data or not cal_vars: raise PreventUpdate
    try: cal_data = json.loads(current_display)
    except: cal_data = {}

    has_updates = False
    for name in cal_vars:
        if name in device_data.get("variables", {}):
            new_val = device_data["variables"][name].get("data")
            if cal_data.get(name) != new_val:
                cal_data[name] = new_val
                has_updates = True
                
    if not has_updates and current_display != "Waiting for data...": raise PreventUpdate
    if not cal_data: return "Waiting for data..."
    return json.dumps(cal_data, indent=2)

@app.callback(
    Output({"type": "data-table-1d", "index": ALL}, "rowTransaction"),
    Input("device-data-buffer", "data"),
    State({"type": "data-table-1d", "index": ALL}, "columnDefs")
)
def update_table_1d(device_data, col_defs_list):
    if not device_data: raise PreventUpdate
    transactions = []
    for col_defs in col_defs_list:
        data = {}
        for col in col_defs:
            field = col["field"]
            val = device_data.get("variables", {}).get(field, {}).get("data", "")
            if isinstance(val, list) and len(val) > 0: val = val[-1]
            data[field] = val
        transactions.append({"add": [data], "addIndex": 0})
    if not transactions: raise PreventUpdate
    return transactions

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
    
    # 🟢 DYNAMIC MAPPING: Translate operational commands to the expected sensor format
    topic_type = "sensor" if dtype == "operational" else dtype
    id_field = "controllerid" if dtype == "controller" else "deviceid"

    # Use exact schema matching the hardware targets
    return json.dumps({
        "source": f"envds.{config.daq_id}.dashboard",
        "data": {"settings": selected_row["parameter"], "requested": requested_val},
        "destpath": f"envds/{topic_type}/settings/request",
        id_field: device_meta["device_id"]
    })

@app.callback(Output("ws-device-instance", "send"), Input("ws-send-instance-buffer", "children"))
def send_to_instance(value): return value

@app.callback(
    Output({"type": "data-table-2d", "index": ALL}, "rowData"), 
    Input("device-data-buffer", "data"),
    [State({"type": "data-table-2d", "index": ALL}, "rowData"), State({"type": "data-table-2d", "index": ALL}, "columnDefs"), State("device-definition", "data")]
)
def update_table_2d(device_data, row_data_list, col_defs_list, device_definition):
    if not device_data: raise PreventUpdate
    new_row_data_list = []
    for col_defs in col_defs_list:
        if not col_defs:
            new_row_data_list.append(dash.no_update)
            continue
        dim_2d = col_defs[0]["field"]
        dim_2d_is_coord = device_definition and dim_2d in device_definition.get("variables", {}) and device_definition["variables"][dim_2d].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate"
        
        if dim_2d_is_coord: dim_data = device_definition["variables"][dim_2d].get("data", [])
        else:
            if dim_2d not in device_data.get("variables", {}):
                new_row_data_list.append(dash.no_update)
                continue
            dim_data = device_data["variables"][dim_2d].get("data")
            if not dim_data:
                new_row_data_list.append(dash.no_update)
                continue

        row_data = []
        for index in range(0, len(dim_data)):
            data = {dim_2d: dim_data[index]}
            for col in col_defs[1:]:
                try: data[col["field"]] = device_data["variables"][col["field"]]["data"][index]
                except (KeyError, IndexError, TypeError): data[col["field"]] = None
            row_data.append(data)
        new_row_data_list.append(row_data)
        
    if all(r == dash.no_update for r in new_row_data_list): raise PreventUpdate
    return new_row_data_list

@app.callback(
    Output({"type": "graph-2d-heatmap", "index": MATCH}, "figure", allow_duplicate=True),
    [Input({"type": "graph-2d-z-axis-submit", "index": MATCH}, "n_clicks")],
    [State({"type": "graph-2d-z-axis-min", "index": MATCH}, "value"), State({"type": "graph-2d-z-axis-max", "index": MATCH}, "value"), State({"type": "graph-2d-heatmap", "index": MATCH}, "figure")],
    prevent_initial_call=True
)
def set_2d_z_axis_range(n, axis_min, axis_max, heatmap):
    return go.Figure(heatmap).update_layout(coloraxis=dict(cauto=False, cmax=axis_max, cmin=axis_min))