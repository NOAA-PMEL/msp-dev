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

# --- GLOBAL MEMORY CACHE ---
REGISTRY_CACHE = {
    "instances": {},
    "definitions": {}
}

# --- Route-Splitting Helper Functions ---
def get_all_devices():
    devices = []
    for d_type in ["sensor", "operational", "controller"]:
        path = "controller-instance" if d_type == "controller" else "device-instance"
        url = f"http://{datastore_url}/{path}/registry/get/"
        try:
            query = {"device_type": d_type} if d_type in ["sensor", "operational"] else {}
            response = httpx.get(url, params=query, timeout=10.0)
            items = response.json().get("results", [])
            for item in items:
                item["_device_type"] = d_type 
                make = item.get("make")
                model = item.get("model")
                sn = item.get("serial_number", item.get("serial_id"))
                if make and model and sn:
                    cache_key = f"{d_type}::{make}::{model}::{sn}"
                    REGISTRY_CACHE["instances"][cache_key] = item
            devices.extend(items)
        except Exception as e:
            L.error(f"get_all_devices error for {d_type}: {e}")
    return devices

def get_device_data(device_id: str, device_type: str="sensor"):
    path = "controller" if device_type == "controller" else "device"
    query = {"controller_id": device_id} if device_type == "controller" else {"device_type": device_type, "device_id": device_id}
    url = f"http://{datastore_url}/{path}/data/get/"
    try:
        response = httpx.get(url, params=query, timeout=30.0)
        results = response.json()
        if "results" in results and results["results"]: 
            L.info(f"[DEVICE HISTORY] Successfully fetched {len(results['results'])} historical records for {device_id}")
            return results["results"]
    except Exception as e:
        L.error(f"get_device_data error: {e}")
    return []

def get_device_instance(device_id: str, device_type: str="sensor"):
    path = "controller-instance" if device_type == "controller" else "device-instance"
    query = {"controller_id": device_id} if device_type == "controller" else {"device_type": device_type, "device_id": device_id}
    url = f"http://{datastore_url}/{path}/registry/get/"
    try:
        response = httpx.get(url, params=query, timeout=10.0)
        results = response.json()
        if "results" in results and results["results"]: return results["results"][0]
    except Exception as e:
        L.error(f"get_device_instance error: {e}")
    return {}

def get_device_definition_by_device_id(device_id: str, device_type: str="sensor"):
    cache_key = f"{device_type}::{device_id}"
    device = REGISTRY_CACHE["instances"].get(cache_key)
    if not device:
        device = get_device_instance(device_id, device_type)
        
    if device:
        try:
            version = device.get("version")
            if not version: return {}
            
            device_definition_id = f"{device['make']}::{device['model']}::{version}"
            def_cache_key = f"{device_type}::{device_definition_id}"
            
            if def_cache_key in REGISTRY_CACHE["definitions"]:
                return REGISTRY_CACHE["definitions"][def_cache_key]
                
            path = "controller-definition" if device_type == "controller" else "device-definition"
            query = {"controller_definition_id": device_definition_id} if device_type == "controller" else {"device_type": device_type, "device_definition_id": device_definition_id}
            url = f"http://{datastore_url}/{path}/registry/get/"
            
            response = httpx.get(url, params=query, timeout=10.0)
            results = response.json()
            if "results" in results and results["results"]: 
                dfn = results["results"][0]
                REGISTRY_CACHE["definitions"][def_cache_key] = dfn
                return dfn
        except Exception as e:
            L.error(f"get_device_definition_by_device_id error: {e}")
    return {}


# --- Dynamic Builders ---
def build_tables(layout_options):
    table_list = []
    for ltype, dims in layout_options.items():
        for dim, options in dims.items():
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
    
    # 🟢 HOISTED DATA PIPELINE
    dcc.Store(id="device-meta", data={}),
    dcc.Store(id="device-definition", data={}),
    dcc.Store(id="calibration-vars", data=[]),
    dcc.Store(id="graph-axes", data={}),
    dcc.Store(id="device-data-buffer", data={}),
    dcc.Store(id="device-settings-buffer", data={}),
    dcc.Store(id="last-time-store", data=None),
    
    # 🟢 FIX: The WebSocket is injected into this Div so it mounts fresh with the right URL.
    html.Div(id="device-ws-container", style={"display": "none"}),
    html.Div(id="ws-send-instance-buffer", style={"display": "none"}),
    
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
            sn = d.get("serial_number", d.get("serial_id"))
            if not make or not model or not sn: continue
            
            dtype = d.get("_device_type", "sensor")
            device_id = f"{make}::{model}::{sn}"
            device_options.append({"label": f"{make} {model} (SN: {sn}) [{dtype.capitalize()}]", "value": f"{dtype}::{device_id}"})

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
        return html.Div([dbc.Alert(f"Fatal Layout Error: {e}", color="danger")])

@app.callback(
    [
        Output("device-ui-container", "children"),
        Output("device-meta", "data"),
        Output("device-definition", "data"),
        Output("calibration-vars", "data"),
        Output("device-ws-container", "children"), # 🟢 FIX: Mounts a fresh WebSocket component
        Output("ws-send-instance-buffer", "children", allow_duplicate=True),
        Output("device-data-buffer", "data"),
        Output("device-settings-buffer", "data"),
        Output("last-time-store", "data")
    ],
    Input("device-selector", "value"),
    prevent_initial_call=True
)
def generate_device_ui(dropdown_val):
    if not dropdown_val:
        return (
            html.Div(html.H5("Please select an instrument from the dropdown to load its UI and variable plots.", className="text-muted text-center mt-5")),
            {}, {}, [], [], no_update, {}, {}, None
        )
        
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
                        "parameter": name, 
                        "description": long_name, 
                        "actual_value": "", 
                        "requested_value": "",
                        "type": var.get("type", "unknown"),
                        "allowed_values": [x.strip() for x in (var.get("attributes", {}).get("allowed_values", {}).get("data", "")).split(",")] if var.get("attributes", {}).get("allowed_values", {}).get("data") else None,
                        "min": var.get("attributes", {}).get("valid_min", {}).get("data", None),
                        "max": var.get("attributes", {}).get("valid_max", {}).get("data", None),
                        "step": var.get("attributes", {}).get("step_increment", {}).get("data", None)
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
                            label = cd.get("headerName", cd["field"])
                            layout_options[ltype][dim]["variable-list"].append({"label": label, "value": cd["field"]})

        except Exception as e:
            L.error(f"build layout error: {e}")

    topic_type = "sensor" if device_type == "operational" else device_type
    id_field = "controllerid" if device_type == "controller" else "deviceid"
    initial_request = {
        "source": f"envds.{config.daq_id}.dashboard",
        "data": {}, "destpath": f"envds/{topic_type}/settings/request", 
        id_field: device_id
    }

    ws_protocol = "wss://" if str(config.ws_use_tls).lower() == "true" else "ws://"
    ws_base = f"{ws_protocol}{config.external_hostname}:{config.ws_port}/envds/envops"
    
    ws_component = WebSocket(id="ws-device-instance", url=f"{ws_base}/ws/{topic_type}/{device_id}")
    L.info(f"[DEVICE UI] Connecting WebSocket to: {ws_component.url}")
    
    ui_container = html.Div([
        dbc.Accordion(build_tables(layout_options), id="device-data-accordion", className="mb-4", start_collapsed=True),
        dbc.Accordion(build_graphs(layout_options), id="device-plot-accordion", className="mb-4"),
        dbc.Accordion([dbc.AccordionItem(html.Pre(id="calibration-display", children="Waiting for data...", style={"whiteSpace": "pre-wrap", "wordBreak": "break-all"}), title="Calibration Values")], id="device-calibration-accordion", start_collapsed=True)
    ])

    return (
        ui_container, 
        device_meta, 
        device_definition, 
        calibration_vars, 
        ws_component, 
        json.dumps(initial_request), 
        {}, {}, None
    )


# --- Sub-Callbacks ---

@app.callback(
    Output("device-data-buffer", "data"), Output("device-settings-buffer", "data"), Output("last-time-store", "data"),
    Input("ws-device-instance", "message"), State("last-time-store", "data")
)
def update_device_buffers(event, last_time):
    if event and "data" in event:
        try:
            event_data = json.loads(event["data"])
            data_out = no_update
            settings_out = no_update
            new_last_time = no_update

            if "data-update" in event_data and event_data["data-update"]: 
                current_time = event_data["data-update"].get("variables", {}).get("time", {}).get("data")
                if isinstance(current_time, list) and len(current_time) > 0: current_time = current_time[-1]
                
                # 🟢 FIX: Ensure current_time actually exists before attempting to deduplicate it
                if current_time and current_time == last_time:
                    pass 
                else:
                    data_out = event_data["data-update"]
                    new_last_time = current_time or last_time
                    L.info(f"[DEVICE LIVE] Data Update Captured for time: {current_time}")

            if "settings-update" in event_data and event_data["settings-update"]: 
                settings_out = event_data["settings-update"]

            return [data_out, settings_out, new_last_time]
        except Exception as e: 
            L.error(f"[DEVICE LIVE] WebSocket message parsing error: {e}")
            pass
            
    return [no_update, no_update, no_update]


@app.callback(
    Output({"type": "sensor-graph-1d", "index": MATCH}, "figure"),
    Input({"type": "sensor-graph-1d-dropdown", "index": MATCH}, "value"),
    [State("device-meta", "data"), State("graph-axes", "data"), State("device-definition", "data"), State({"type": "sensor-graph-1d-dropdown", "index": MATCH}, "id")]
)
def select_graph_1d(y_axis, device_meta, graph_axes, device_definition, graph_id):
    default_fig = go.Figure(layout={"xaxis": {"title": "Time"}, "yaxis": {"title": "Value"}, "template": "simple_white"})
    if not y_axis or not device_meta: return default_fig

    try:
        x, y = [], []
        results = get_device_data(device_id=device_meta.get("device_id"), device_type=device_meta.get("device_type", "sensor"))
        if results:
            for doc in results:
                try:
                    time_data = doc["variables"]["time"]["data"]
                    y_data = doc["variables"][y_axis]["data"]
                    
                    if isinstance(time_data, list): x.extend(time_data)
                    else: x.append(time_data)
                    
                    if isinstance(y_data, list): y.extend(y_data)
                    else: y.append(y_data)
                except KeyError: pass

        units = ""
        try: units = f'({device_definition["variables"][y_axis]["attributes"]["units"]["data"]})'
        except Exception: pass

        return go.Figure(data=go.Scatter(x=x, y=y, type="scatter", mode="lines+markers"), layout={"xaxis": {"title": "Time"}, "yaxis": {"title": f"{y_axis} {units}".strip()}, "template": "simple_white"})
    except Exception as e: 
        L.error(f"[DEVICE PLOT] 1D render error: {e}")
        return default_fig


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
        
    if all(f == no_update for f in figs_to_update): raise PreventUpdate
    return figs_to_update


@app.callback(
    [Output({"type": "graph-2d-heatmap", "index": MATCH}, "figure", allow_duplicate=True), Output({"type": "graph-2d-line", "index": MATCH}, "figure", allow_duplicate=True)],
    Input({"type": "graph-2d-dropdown", "index": MATCH}, "value"),
    [State("device-meta", "data"), State("device-definition", "data"), State({"type": "graph-2d-dropdown", "index": MATCH}, "id")],
    prevent_initial_call=True,
)
def select_graph_2d(z_axis, device_meta, device_definition, graph_id):
    if not z_axis or not device_meta: raise PreventUpdate
    y_axis = graph_id["index"].split("::")[1]
    use_log = (y_axis == "diameter")
    
    x, y, orig_z = [], [], []
    y_is_coord = False
    
    if device_definition and y_axis in device_definition.get("variables", {}):
        if device_definition["variables"][y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
            y_is_coord = True
            y = device_definition["variables"][y_axis].get("data", [])

    results = get_device_data(device_id=device_meta.get("device_id"), device_type=device_meta.get("device_type", "sensor"))
    if not results: raise PreventUpdate

    for doc in results:
        try:
            t_data = doc["variables"]["time"]["data"]
            z_data = doc["variables"][z_axis]["data"]
            
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

    z = []
    for yi in range(len(y)):
        new_z = []
        for xi in range(len(x)):
            try: new_z.append(orig_z[xi][yi])
            except IndexError: new_z.append(None)
        z.append(new_z)

    y_units, z_units = "", ""
    try: y_units = f'({device_definition["variables"][y_axis]["attributes"]["units"]["data"]})'
    except Exception: pass
    try: z_units = f'({device_definition["variables"][z_axis]["attributes"]["units"]["data"]})'
    except Exception: pass

    heatmap = go.Figure(data=go.Heatmap(x=x, y=y, z=z, type="heatmap", colorscale="Rainbow"), layout={"xaxis": {"title": "Time"}, "yaxis": {"title": f"{y_axis} {y_units}".strip()}})
    scatter = go.Figure(data=[{"x": y, "y": orig_z[-1] if len(orig_z) > 0 else [], "type": "scatter"}], layout={"xaxis": {"title": f"{y_axis} {y_units}".strip()}, "yaxis": {"title": f"{z_axis} {z_units}".strip()}, "title": str(x[-1]) if len(x) > 0 else ""})

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
        
        if device_definition and y_axis in device_definition.get("variables", {}):
            if device_definition["variables"][y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate": y_is_coord = True

        if ("time" not in device_data.get("variables", {}) or (not y_is_coord and y_axis not in device_data.get("variables", {})) or z_axis not in device_data.get("variables", {})):
            heatmaps.append(no_update)
            continue

        x = device_data["variables"]["time"]["data"]
        if x in current_fig["data"][0].get("x", []):
            heatmaps.append(no_update)
            continue

        if not isinstance(x, list): x = [x]
        for nx in x: current_fig["data"][0]["x"].append(nx)
        
        y = current_fig["data"][0].get("y", [])
        if len(y) == 0: y = device_definition["variables"][y_axis].get("data", []) if y_is_coord else device_data["variables"][y_axis]["data"]
        
        orig_z = device_data["variables"][z_axis]["data"]
        if not isinstance(orig_z, list): orig_z = [orig_z]

        if len(x) > 1:
            z = []
            for yi, yval in enumerate(y):
                new_z = []
                for xi, xval in enumerate(x):
                    try: new_z.append(orig_z[xi][yi])
                    except IndexError: new_z.append(None)
                z.append(new_z)
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
        
        if device_definition and y_axis in device_definition.get("variables", {}):
            if device_definition["variables"][y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate": y_is_coord = True

        if ("time" not in device_data.get("variables", {}) or (not y_is_coord and y_axis not in device_data.get("variables", {})) or z_axis not in device_data.get("variables", {})):
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
    [Output({"type": "graph-3d-line", "index": MATCH}, "figure", allow_duplicate=True), Output({"type": "graph-3d-heatmap", "index": MATCH}, "figure", allow_duplicate=True)],
    Input({"type": "graph-3d-dropdown", "index": MATCH}, "value"),
    [State("device-meta", "data"), State("device-definition", "data"), State({"type": "graph-3d-dropdown", "index": MATCH}, "id")],
    prevent_initial_call=True,
)
def select_graph_3d(z_axis, device_meta, device_definition, graph_id):
    if not z_axis or not device_meta: raise PreventUpdate
    x_axis = graph_id["index"].split("::")[0]
    y_axis = graph_id["index"].split("::")[1]
    
    x_is_coord, y_is_coord = False, False
    x, y, z_history = [], [], []

    if device_definition:
        if x_axis in device_definition.get("variables", {}) and device_definition["variables"][x_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
            x_is_coord = True
            x = device_definition["variables"][x_axis].get("data", [])
        if y_axis in device_definition.get("variables", {}) and device_definition["variables"][y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
            y_is_coord = True
            y = device_definition["variables"][y_axis].get("data", [])

    results = get_device_data(device_id=device_meta.get("device_id"), device_type=device_meta.get("device_type", "sensor"))
    if not results: raise PreventUpdate

    for doc in results:
        try:
            t_data = doc["variables"]["time"]["data"]
            z_data = doc["variables"][z_axis]["data"]
            
            if isinstance(t_data, list):
                if not x_is_coord: x.extend(doc["variables"][x_axis]["data"])
                if not y_is_coord: y.extend(doc["variables"][y_axis]["data"])
                z_history.extend(z_data)
            else:
                if not x_is_coord: x.append(doc["variables"][x_axis]["data"])
                if not y_is_coord: y.append(doc["variables"][y_axis]["data"])
                z_history.append(z_data)
        except KeyError: continue

    if len(x) > 0 and isinstance(x[-1], list): x = x[-1]
    if len(y) > 0 and isinstance(y[-1], list): y = y[-1]
    if not z_history: raise PreventUpdate
        
    latest_z = z_history[-1] 
    z = []
    for yi in range(len(y)):
        new_row = []
        for xi in range(len(x)):
            try: new_row.append(latest_z[xi][yi])
            except IndexError: new_row.append(None)
        z.append(new_row)

    units = []
    for axis in [x_axis, y_axis, z_axis]:
        try: units.append(f'({device_definition["variables"][axis]["attributes"]["units"]["data"]})')
        except Exception: units.append('')

    scatter = go.Figure(data=go.Surface(z=z, x=x, y=y))
    scatter.update_scenes(xaxis_title_text=f"{x_axis} {units[0]}".strip(), yaxis_title_text=f"{y_axis} {units[1]}".strip(), zaxis_title_text=f"{z_axis} {units[2]}".strip())

    heatmap = go.Figure(data=go.Heatmap(z=z, x=x, y=y, type="heatmap", colorscale="Rainbow"))
    heatmap.update_layout(xaxis={"title": f"{x_axis} {units[0]}".strip()}, yaxis={"title": f"{y_axis} {units[1]}".strip()})
    
    if x_axis == "diameter": heatmap.update_xaxes(type="log")

    return [scatter, heatmap]


@app.callback(
    [Output({"type": "graph-3d-line", "index": ALL}, "figure"), Output({"type": "graph-3d-heatmap", "index": ALL}, "figure")],
    Input("device-data-buffer", "data"),
    [State({"type": "graph-3d-dropdown", "index": ALL}, "value"), State("device-definition", "data"), State({"type": "graph-3d-line", "index": ALL}, "figure"), State({"type": "graph-3d-heatmap", "index": ALL}, "figure"), State({"type": "graph-3d-dropdown", "index": ALL}, "id")],
    prevent_initial_call=True,
)
def update_graph_3d_plots(device_data, z_axis_list, device_definition, line_figs, heatmap_figs, graph_ids):
    if not device_data: raise PreventUpdate
    updated_lines, updated_heatmaps = [], []
    
    for z_axis, graph_id, line_fig, heatmap_fig in zip(z_axis_list, graph_ids, line_figs, heatmap_figs):
        if not z_axis or not line_fig or not heatmap_fig:
            updated_lines.append(no_update)
            updated_heatmaps.append(no_update)
            continue

        x_axis = graph_id["index"].split("::")[0]
        y_axis = graph_id["index"].split("::")[1]
        
        x_is_coord, y_is_coord = False, False
        if device_definition:
            if x_axis in device_definition.get("variables", {}) and device_definition["variables"][x_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate": x_is_coord = True
            if y_axis in device_definition.get("variables", {}) and device_definition["variables"][y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate": y_is_coord = True

        if ((not x_is_coord and x_axis not in device_data.get("variables", {})) or (not y_is_coord and y_axis not in device_data.get("variables", {})) or z_axis not in device_data.get("variables", {})):
            updated_lines.append(no_update)
            updated_heatmaps.append(no_update)
            continue

        x = device_definition["variables"][x_axis].get("data", []) if x_is_coord else device_data["variables"][x_axis]["data"]
        y = device_definition["variables"][y_axis].get("data", []) if y_is_coord else device_data["variables"][y_axis]["data"]
        latest_z = device_data["variables"][z_axis]["data"]

        z = []
        for yi in range(len(y)):
            new_row = []
            for xi in range(len(x)):
                try: new_row.append(latest_z[xi][yi])
                except IndexError: new_row.append(None)
            z.append(new_row)

        line_fig["data"][0]["z"] = z
        heatmap_fig["data"][0]["z"] = z
        
        if isinstance(x, list) and len(x) > 0: x_title = x[-1]
        else: x_title = x
        line_fig["layout"]["title"] = str(x_title)
        
        updated_lines.append(line_fig)
        updated_heatmaps.append(heatmap_fig)

    if all(l == no_update for l in updated_lines): raise PreventUpdate
    return updated_lines, updated_heatmaps


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
            field = col["field"]
            val = device_data.get("variables", {}).get(field, {}).get("data")
            
            if isinstance(val, list) and len(val) > 0: val = val[-1]
            if val == "": val = None
            
            data[field] = val
            
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
        if not col_defs:
            new_row_data_list.append(no_update)
            continue
            
        dim_2d = col_defs[0]["field"]
        dim_2d_is_coord = device_definition and dim_2d in device_definition.get("variables", {}) and device_definition["variables"][dim_2d].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate"
        
        if dim_2d_is_coord: dim_data = device_definition["variables"][dim_2d].get("data", [])
        else:
            if dim_2d not in device_data.get("variables", {}):
                new_row_data_list.append(no_update)
                continue
            dim_data = device_data["variables"][dim_2d].get("data")
            if not dim_data:
                new_row_data_list.append(no_update)
                continue

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
    updated_row_lists = []
    has_updates = False

    for rows in row_data_list:
        if not rows:
            updated_row_lists.append(no_update)
            continue
            
        grid_patched = False
        for row in rows:
            param_name = row["parameter"]
            if param_name in device_settings.get("settings", {}):
                param_data = device_settings["settings"][param_name]
                
                if isinstance(param_data, dict) and "data" in param_data:
                    actual_val = param_data["data"].get("actual", "")
                    req_val = param_data["data"].get("requested", "")
                elif isinstance(param_data, dict):
                    actual_val = param_data.get("actual", "")
                    req_val = param_data.get("requested", "")
                else: continue

                if str(row.get("actual_value")) != str(actual_val):
                    row["actual_value"] = actual_val
                    grid_patched = True
                    
                if row.get("requested_value") == "" or row.get("requested_value") is None:
                    row["requested_value"] = req_val
                    grid_patched = True
        
        if grid_patched:
            updated_row_lists.append(rows)
            has_updates = True
        else: updated_row_lists.append(no_update)

    if not has_updates: raise PreventUpdate
    return updated_row_lists


@app.callback(
    Output("calibration-display", "children"),
    Input("device-data-buffer", "data"),
    [State("calibration-display", "children"), State("calibration-vars", "data")],
    prevent_initial_call=True
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

    return json.dumps({
        "source": f"envds.{config.daq_id}.dashboard",
        "data": {"settings": {selected_row["parameter"]: {"requested": requested_val}}},
        "destpath": f"envds/{topic_type}/settings/request",
        id_field: device_meta["device_id"]
    })


@app.callback(
    Output({"type": "graph-2d-heatmap", "index": MATCH}, "figure", allow_duplicate=True),
    [Input({"type": "graph-2d-z-axis-submit", "index": MATCH}, "n_clicks")],
    [State({"type": "graph-2d-z-axis-min", "index": MATCH}, "value"), State({"type": "graph-2d-z-axis-max", "index": MATCH}, "value"), State({"type": "graph-2d-heatmap", "index": MATCH}, "figure")],
    prevent_initial_call=True
)
def set_2d_z_axis_range(n, axis_min, axis_max, heatmap):
    return go.Figure(heatmap).update_layout(coloraxis=dict(cauto=False, cmax=axis_max, cmin=axis_min))

@app.callback(Output("ws-device-instance", "send"), Input("ws-send-instance-buffer", "children"))
def send_to_instance(value): return value