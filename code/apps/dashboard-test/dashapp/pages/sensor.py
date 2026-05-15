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
)
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from dash_extensions import WebSocket
from pydantic import BaseSettings
from ulid import ULID
import dash_ag_grid as dag
import pandas as pd
import numpy as np
from logfmter import Logfmter
from collections import deque
import httpx
import traceback

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.DEBUG)

dash.register_page(
    __name__,
    path_template="/sensor/<sensor_id>",
    title="Sensors",
    nav_bar=False
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
 
    knative_broker: str = (
        "http://kafka-broker-ingress.knative-eventing.svc.cluster.local/default/default"
    )
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

link_url_base = f"{http_url_base}/msp/dashboardtest"

ws_send_buffer = html.Div(id="ws-send-instance-buffer", style={"display": "none"})


def build_tables(layout_options):
    table_list = []
    print(f"build_tables: {layout_options}")
    for ltype, dims in layout_options.items():
        for dim, options in dims.items():
            title = "Data"

            if ltype == "layout-settings":
                title = f"Device Settings"
                
                # Reshaped row definitions for the parameter layout grid
                column_defs = [
                    {"field": "parameter", "headerName": "Setting Parameter", "editable": False, "pinned": "left"},
                    {"field": "description", "headerName": "Description", "editable": False},
                    {
                        "field": "value", 
                        "headerName": "Actual Value", 
                        "editable": True,
                        "cellEditorSelector": {"function": "determineSettingEditor(params)"}
                    }
                ]
                
                table_list.append(
                    dbc.AccordionItem(
                        [
                            dag.AgGrid(
                                id={"type": "settings-table", "index": dim},
                                rowData=options.get("row-data-skeletons", []),
                                columnDefs=column_defs,
                                columnSizeOptions="autoSize",
                                dashGridOptions={"domLayout": "autoHeight", "singleClickEdit": True},
                                style={"height": None, "maxHeight": "500px", "overflow": "auto"}
                            )
                        ],
                        title=title,
                    )
                )

            elif ltype == "layout-1d":
                title = f"Data 1-D ({dim})"
                table_list.append(
                    dbc.AccordionItem(
                        [
                            dag.AgGrid(
                                id={"type": "data-table-1d", "index": dim},
                                rowData=[],
                                columnDefs=options["table-column-defs"],
                                columnSizeOptions="autoSize",
                            )
                        ],
                        title=title,
                    )
                )

            elif ltype == "layout-2d":
                title = f"Data 2-D (time, {dim})"
                table_list.append(
                    dbc.AccordionItem(
                        [
                            dag.AgGrid(
                                id={"type": "data-table-2d", "index": f"time::{dim}"},
                                rowData=[],
                                columnDefs=options["table-column-defs"],
                                columnSizeOptions="autoSize",
                            )
                        ],
                        title=title,
                    )
                )

    print(f"build_tables: {table_list}")
    return table_list


def build_graph_1d(dropdown_list, xaxis="time", yaxis=""):
    graph = dbc.Card(
        children=[
            dbc.CardHeader(
                children=[
                    dcc.Dropdown(
                        id={"type": "sensor-graph-1d-dropdown", "index": xaxis},
                        options=dropdown_list,
                        value="",
                    )
                ]
            ),
            dcc.Graph(
                id={"type": "sensor-graph-1d", "index": xaxis},
                figure=go.Figure(
                    data=go.Scatter(x=[], y=[], type="scatter")
                ),
                style={"height": 300},
            ),
        ]
    )
    return graph

def build_graph_2d(dropdown_list, xaxis="time", yaxis="", zaxis=""):
    content = dbc.Row(
        children=[
            dbc.Button("Submit", {"type": "graph-2d-z-axis-submit", "index": f"{xaxis}::{yaxis}"}),
            dbc.Label("z-axis min:"),
            dbc.Col(
                dbc.Input(
                    type="number",
                    id={"type": "graph-2d-z-axis-min", "index": f"{xaxis}::{yaxis}"},
                )
            ),
            dbc.Label("z-axis max:"),
            dbc.Col(
                dbc.Input(
                    type="number",
                    id={"type": "graph-2d-z-axis-max", "index": f"{xaxis}::{yaxis}"},
                )
            ),
        ]
    )

    axes_settings = dbc.Accordion(
        children=[
            dbc.AccordionItem(
                [dbc.Card(children=[content])],
                title="Axes Settings"
            )
        ],
        start_collapsed=True
    )

    graph = dbc.Card(
        children=[
            dbc.CardHeader(
                children=[
                    dcc.Dropdown(
                        id={"type": "graph-2d-dropdown", "index": f"{xaxis}::{yaxis}"},
                        options=dropdown_list,
                        value="",
                    )
                ]
            ),
            dbc.Row(
                children=[
                    axes_settings,
                    dbc.Col(
                        dcc.Graph(
                            id={
                                "type": "graph-2d-heatmap",
                                "index": f"{xaxis}::{yaxis}",
                            },
                            style={"height": 500},
                        )
                    ),
                    dbc.Col(
                        dcc.Graph(
                            id={"type": "graph-2d-line", "index": f"{xaxis}::{yaxis}"},
                            style={"height": 500},
                        )
                    ),
                ]
            ),
        ]
    )
    return graph


def build_graph_3d(dropdown_list, xaxis="", yaxis="", zaxis=""):
    content = dbc.Row(
        children=[
            dbc.Button("Submit", {"type": "graph-3d-z-axis-submit", "index": f"{xaxis}::{yaxis}"}),
            dbc.Label("z-axis min:"),
        ]
    )

    axes_settings = dbc.Accordion(
        children=[
            dbc.AccordionItem(
                [dbc.Card(children=[content])],
                title="Axes Settings"
            )
        ],
        start_collapsed=True
    )

    graph = dbc.Card(
        children=[
            dbc.CardHeader(
                children=[
                    dcc.Dropdown(
                        id={"type": "graph-3d-dropdown", "index": f"{xaxis}::{yaxis}"},
                        options=dropdown_list,
                        value="",
                    )
                ]
            ),
            dbc.Row(
                children=[
                    axes_settings,
                    dbc.Col(
                        dcc.Graph(
                            id={"type": "graph-3d-line", "index": f"{xaxis}::{yaxis}"},
                            style={"height": 500},
                        )
                    ),
                    dbc.Col(
                        dcc.Graph(
                            id={
                                "type": "graph-3d-heatmap",
                                "index": f"{xaxis}::{yaxis}",
                            },
                            style={"height": 500},
                        )
                    ),
                ]
            ),
        ]
    )
    return graph


def build_graphs(layout_options):
    graph_list = []
    print(f"build_graphs: {layout_options}")
    for ltype, dims in layout_options.items():
        print('dims', dims)
        for dim, options in dims.items():
            title = "Plots"
            if ltype == "layout-1d":
                title = f"Plots 1-D ({dim})"
                graph_list.append(
                    dbc.AccordionItem(
                        [
                            dbc.Row(
                                children=[
                                    build_graph_1d(
                                        options["variable-list"],
                                        xaxis=dim,
                                    )
                                ]
                            )
                        ],
                        title=title,
                    )
                )

            elif ltype == "layout-2d":
                title = f"Plots 2-D (time, {dim})"
                graph_list.append(
                    dbc.AccordionItem(
                        [
                            dbc.Row(
                                children=[
                                    build_graph_2d(
                                        options["variable-list"],
                                        xaxis="time",
                                        yaxis=dim,
                                    )
                                ]
                            )
                        ],
                        title=title,
                    )
                )
            
            elif ltype == "layout-3d":
                axes = dim.split("::")
                title = f"Plots 3-D ({axes[0]}, {axes[1]})"
                graph_list.append(
                    dbc.AccordionItem(
                        [
                            dbc.Row(
                                children=[
                                    build_graph_3d(
                                        options["variable-list"],
                                        xaxis=axes[0],
                                        yaxis=axes[1],
                                    )
                                ]
                            )
                        ],
                        title=title,
                    )
                )

    print(f"build_graphs list: {graph_list}")
    return graph_list

def get_device_data(device_id: str, device_type: str="sensor"):
    query = {"device_type": device_type, "device_id": device_id}
    url = f"http://{datastore_url}/device/data/get/"
    try:
        timeout = httpx.Timeout(30.0, read=None)
        response = httpx.get(url, params=query, timeout=timeout)
        results = response.json()
        if "results" in results and results["results"]:
            return results["results"]
    except Exception as e:
        L.error("get_device_data", extra={"reason": e})
    return []

def get_device_instance(device_id: str, device_type: str="sensor"):
    query = {"device_type": device_type, "device_id": device_id}
    url = f"http://{datastore_url}/device-instance/registry/get/"
    try:
        timeout = httpx.Timeout(30.0, read=None)
        response = httpx.get(url, params=query, timeout=timeout)
        results = response.json()
        if "results" in results and results["results"]:
            return results["results"][0]
    except Exception as e:
        L.error("get_device_instance", extra={"reason": e})
    return {}

def get_device_definition_by_device_id(device_id: str, device_type: str="sensor"):
    device = get_device_instance(device_id=device_id, device_type=device_type)
    if device:
        try:
            device_definition_id = "::".join([
                device["make"],
                device["model"],
                device["version"]
            ])
            return get_device_definition(device_definition_id=device_definition_id, device_type=device_type)
        except Exception as e:
            print("ERROR: get_device_definition_by_device_id", extra={"reason": e})
    return {}

def get_device_definition(device_definition_id: str, device_type: str="sensor"):
    query = {"device_type": device_type, "device_definition_id": device_definition_id}
    url = f"http://{datastore_url}/device-definition/registry/get/"
    try:
        timeout = httpx.Timeout(30.0, read=None)
        response = httpx.get(url, params=query, timeout=timeout)
        results = response.json()
        if "results" in results and results["results"]:
            return results["results"][0]
    except Exception as e:
        L.error("get_device_definition", extra={"reason": e})
        return {}


def layout(sensor_id=None):
    print(f"get_layout: {sensor_id}")
    sensor_definition = None
    if sensor_id:
        parts = sensor_id.split("::")
        sensor_meta = {
            "device_id": sensor_id,
            "make": parts[0],
            "model": parts[1],
            "serial_number": parts[2],
        }

        sensor_definition = get_device_definition_by_device_id(device_id=sensor_id, device_type="sensor")

    else:
        sensor_meta = {}
        sensor_definition = {}

    layout_options = {
        "layout-settings": {"time": {"table-column-defs": [], "variable-list": [], "row-data-skeletons": []}},
        "layout-calibration": {"time": {"table-column-defs": [], "variable-list": []}},
        "layout-1d": {"time": {"table-column-defs": [], "variable-list": []}},
    }
    
    calibration_vars = []

    if sensor_definition:
        try:
            dimensions = sensor_definition["dimensions"]
            multi_dim = False
            if len(dimensions.keys()) > 1:
                multi_dim = True

            for name, var in sensor_definition["variables"].items():
                var_type = var["attributes"].get("variable_type", {}).get("data")
                
                if var_type == "setting":
                    long_name = name
                    ln = var["attributes"].get("long_name", None)
                    if ln:
                        long_name = ln.get("data", name)

                    dtype = var.get("type", "unknown")
                    allowed_vals = var["attributes"].get("allowed_values", {}).get("data", None)
                    
                    # Extract numeric bounds
                    min_val = var["attributes"].get("valid_min", {}).get("data", None)
                    max_val = var["attributes"].get("valid_max", {}).get("data", None)
                    step_val = var["attributes"].get("step_increment", {}).get("data", None)

                    # Build meta properties to expose inside JavaScript function determineSettingEditor
                    control_metadata = {
                        "parameter": name,
                        "description": long_name,
                        "value": "",
                        "type": dtype,
                        "allowed_values": [x.strip() for x in allowed_vals.split(",")] if allowed_vals else None,
                        "min": min_val,
                        "max": max_val,
                        "step": step_val
                    }
                    layout_options["layout-settings"]["time"]["row-data-skeletons"].append(control_metadata)

                elif var_type == "calibration":
                    calibration_vars.append(name)

                elif var_type == "main":
                    if "shape" not in var:
                        continue
                    if "time" not in var["shape"]:
                        continue

                    long_name = name
                    ln = var["attributes"].get("long_name", None)
                    if ln:
                        long_name = ln.get("data", name)

                    dtype = var.get("type", "unknown")
                    data_type = "text"
                    if dtype in ["float", "double", "int"]:
                        data_type = "number"
                    elif dtype in ["str", "string", "char"]:
                        data_type = "text"
                    elif dtype in ["bool"]:
                        data_type = "boolean"

                    cd = {
                        "field": name,
                        "headerName": long_name,
                        "filter": False,
                        "cellDataType": data_type,
                    }

                    if multi_dim and len(var["shape"]) == 2:
                        if "layout-2d" not in layout_options:
                            layout_options["layout-2d"] = {}

                        dim_2d = [d for d in var["shape"] if d != "time"][0]

                        if dim_2d not in layout_options["layout-2d"]:
                            layout_options["layout-2d"][dim_2d] = {
                                "table-column-defs": [],
                                "variable-list": [],
                            }
                            dln = dim_2d
                            try:
                                dln = sensor_definition["attributes"][dim_2d]["long_name"]["data"]
                            except KeyError:
                                pass

                            data_type = "text"
                            try:
                                dtype = sensor_definition["variables"][dim_2d]["type"]
                                if dtype in ["float", "double", "int"]:
                                    data_type = "number"
                                elif dtype in ["str", "string", "char"]:
                                    data_type = "text"
                                elif dtype in ["bool"]:
                                    data_type = "boolean"
                            except KeyError:
                                pass

                            dcd = {
                                "field": dim_2d,
                                "headerName": dln,
                                "filter": False,
                                "cellDataType": data_type,
                                "pinned": "left",
                            }
                            layout_options["layout-2d"][dim_2d]["table-column-defs"].append(dcd)

                        layout_options["layout-2d"][dim_2d]["table-column-defs"].append(cd)

                    elif multi_dim and len(var["shape"]) == 3:
                        if "layout-3d" not in layout_options:
                            layout_options["layout-3d"] = {}

                        dims_3d = [d for d in var["shape"] if d != "time"]
                        dim_3d_key = f"{dims_3d[0]}::{dims_3d[1]}"

                        if dim_3d_key not in layout_options["layout-3d"]:
                            layout_options["layout-3d"][dim_3d_key] = {
                                "table-column-defs": [],
                                "variable-list": [],
                            }

                        layout_options["layout-3d"][dim_3d_key]["table-column-defs"].append(cd)
                    else:
                        layout_options["layout-1d"]["time"]["table-column-defs"].append(cd)
                else:
                    continue

            for ltype, dims in layout_options.items():
                for dim, options in dims.items():
                    if "table-column-defs" in options:
                        for cd in options["table-column-defs"]:
                            if cd["field"] in dimensions:
                                continue
                            if cd["cellDataType"] != "number":
                                continue
                            layout_options[ltype][dim]["variable-list"].append(
                                {"label": cd["field"], "value": cd["field"]}
                            )

        except KeyError as e:
            print(f"build column_defs error: {e}")

    layout = html.Div(
        [
            html.H1(f"Sensor: {sensor_id}"),
            dbc.Accordion(
                build_tables(layout_options),
                id="sensor-data-accordion",
            ),
            dbc.Accordion(
                build_graphs(layout_options),
                id="sensor-plot-accordion",
            ),
            dbc.Accordion(
                [
                    dbc.AccordionItem(
                        html.Pre(
                            id="calibration-display", 
                            children="Waiting for data...",
                            style={"whiteSpace": "pre-wrap", "wordBreak": "break-all"}
                        ),
                        title="Calibration Values"
                    )
                ],
                id="sensor-calibration-accordion",
                start_collapsed=True
            ),
            WebSocket(
                id="ws-sensor-instance",
                url=f"{ws_url_base}/msp/dashboardtest/ws/sensor/{sensor_id}"
            ),
            ws_send_buffer,
            dcc.Store(id="calibration-vars", data=calibration_vars),
            dcc.Store(id="sensor-definition", data=sensor_definition),
            dcc.Store(id="sensor-meta", data=sensor_meta),
            dcc.Store(id="graph-axes", data={}),
            dcc.Store(id="sensor-data-buffer", data={}),
            dcc.Store(id="sensor-settings-buffer", data={})
        ]
    )
    return layout

@callback(
    Output({"type": "sensor-graph-1d", "index": MATCH}, "figure"),
    Input({"type": "sensor-graph-1d-dropdown", "index": MATCH}, "value"),
    [
        State("sensor-meta", "data"),
        State("graph-axes", "data"),
        State("sensor-definition", "data"),
        State({"type": "sensor-graph-1d-dropdown", "index": MATCH}, "id"),
    ],
)
def select_graph_1d(y_axis, sensor_meta, graph_axes, sensor_definition, graph_id):
    default_fig = go.Figure(
        data=go.Scatter(x=[], y=[], type="scatter", mode="lines+markers"),
        layout={"xaxis": {"title": "Time"}, "yaxis": {"title": "Value"}}
    )

    if not y_axis:
        return default_fig

    try:
        if graph_axes is None:
            graph_axes = {}
        if "graph-1d" not in graph_axes:
            graph_axes["graph-1d"] = dict()
            
        graph_axes["graph-1d"][graph_id["index"]] = {"x-axis": "time", "y-axis": y_axis}

        x, y = [], []
        results = get_device_data(device_id=sensor_meta.get("device_id"), device_type="sensor")
        
        if results and len(results) > 0:
            for doc in results:
                try:
                    x.append(doc["variables"]["time"]["data"])
                    y.append(doc["variables"][y_axis]["data"])
                except KeyError:
                    continue

        units = ""
        try:
            unit_data = sensor_definition["variables"][y_axis]["attributes"]["units"]["data"]
            if unit_data:
                units = f'({unit_data})'
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
    [
        Output({"type": "graph-2d-heatmap", "index": MATCH}, "figure", allow_duplicate=True),
        Output({"type": "graph-2d-line", "index": MATCH}, "figure", allow_duplicate=True),
    ],
    Input({"type": "graph-2d-dropdown", "index": MATCH}, "value"),
    [
        State("sensor-meta", "data"),
        State("graph-axes", "data"),
        State("sensor-definition", "data"),
        State({"type": "graph-2d-dropdown", "index": MATCH}, "id"),
    ],
    prevent_initial_call=True,
)
def select_graph_2d(z_axis, sensor_meta, graph_axes, sensor_definition, graph_id):
    if not z_axis:
        raise PreventUpdate

    if "graph-2d" not in graph_axes:
        graph_axes["graph-2d"] = dict()
    
    y_axis = graph_id["index"].split("::")[1]
    use_log = (y_axis == "diameter")
    
    graph_axes["graph-2d"][graph_id["index"]] = {
        "x-axis": "time",
        "y-axis": y_axis,
        "z-axis": z_axis,
    }

    x, y, orig_z = [], [], []
    
    y_is_coord = False
    if sensor_definition and y_axis in sensor_definition.get("variables", {}):
        if sensor_definition["variables"][y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
            y_is_coord = True
            y = sensor_definition["variables"][y_axis].get("data", [])

    device_id = sensor_meta.get("device_id")
    results = get_device_data(device_id=device_id, device_type="sensor")

    if not results:
        raise PreventUpdate

    for doc in results:
        try:
            x.append(doc["variables"]["time"]["data"])
            if not y_is_coord:
                y.append(doc["variables"][y_axis]["data"])
            orig_z.append(doc["variables"][z_axis]["data"])
        except KeyError:
            continue

    if len(y) > 0 and isinstance(y[-1], list):
        y = y[-1]

    z = []
    for yi in range(len(y)):
        new_z = []
        for xi in range(len(x)):
            try:
                new_z.append(orig_z[xi][yi])
            except IndexError:
                new_z.append(None)
        z.append(new_z)

    y_units, z_units = "", ""
    try:
        y_units = f'({sensor_definition["variables"][y_axis]["attributes"]["units"]["data"]})'
    except Exception: pass
    try:
        z_units = f'({sensor_definition["variables"][z_axis]["attributes"]["units"]["data"]})'
    except Exception: pass

    heatmap = go.Figure(
        data=go.Heatmap(x=x, y=y, z=z, type="heatmap", colorscale="Rainbow"),
        layout={
            "xaxis": {"title": "Time"},
            "yaxis": {"title": f"{y_axis} {y_units}".strip()},
        },
    )
    if use_log:
        heatmap.update_yaxes(type="log")
        heatmap.update_layout(coloraxis=dict(cmax=None, cmin=None))

    scatter = go.Figure(
        data=[{"x": y, "y": orig_z[-1] if len(orig_z) > 0 else [], "type": "scatter"}],
        layout={
            "xaxis": {"title": f"{y_axis} {y_units}".strip()},
            "yaxis": {"title": f"{z_axis} {z_units}".strip()},
            "title": str(x[-1]) if len(x) > 0 else "",
        },
    )
    if use_log:
        scatter.update_xaxes(type="log")

    return [heatmap, scatter]


@callback(
    [
        Output({"type": "graph-3d-line", "index": MATCH}, "figure", allow_duplicate=True),
        Output({"type": "graph-3d-heatmap", "index": MATCH}, "figure", allow_duplicate=True)
    ],
    Input({"type": "graph-3d-dropdown", "index": MATCH}, "value"),
    [
        State("sensor-meta", "data"),
        State("graph-axes", "data"),
        State("sensor-definition", "data"),
        State({"type": "graph-3d-dropdown", "index": MATCH}, "id"),
    ],
    prevent_initial_call=True,
)
def select_graph_3d(z_axis, sensor_meta, graph_axes, sensor_definition, graph_id):
    if not z_axis:
        raise PreventUpdate

    if "graph-3d" not in graph_axes:
        graph_axes["graph-3d"] = dict()
    
    x_axis = graph_id["index"].split("::")[0]
    y_axis = graph_id["index"].split("::")[1]
    
    x_is_coord, y_is_coord = False, False
    x, y, z_history = [], [], []

    if sensor_definition:
        if x_axis in sensor_definition.get("variables", {}) and sensor_definition["variables"][x_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
            x_is_coord = True
            x = sensor_definition["variables"][x_axis].get("data", [])
        if y_axis in sensor_definition.get("variables", {}) and sensor_definition["variables"][y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
            y_is_coord = True
            y = sensor_definition["variables"][y_axis].get("data", [])

    device_id = sensor_meta.get("device_id")
    results = get_device_data(device_id=device_id, device_type="sensor")

    if not results:
        raise PreventUpdate

    for doc in results:
        try:
            if not x_is_coord:
                x.append(doc["variables"][x_axis]["data"])
            if not y_is_coord:
                y.append(doc["variables"][y_axis]["data"])
            z_history.append(doc["variables"][z_axis]["data"])
        except KeyError:
            continue

    if len(x) > 0 and isinstance(x[-1], list): x = x[-1]
    if len(y) > 0 and isinstance(y[-1], list): y = y[-1]

    if not z_history:
        raise PreventUpdate
        
    latest_z = z_history[-1] 
    z = []
    for yi in range(len(y)):
        new_row = []
        for xi in range(len(x)):
            try:
                new_row.append(latest_z[xi][yi])
            except IndexError:
                new_row.append(None)
        z.append(new_row)

    units = []
    for axis in [x_axis, y_axis, z_axis]:
        try:
            unit = f'({sensor_definition["variables"][axis]["attributes"]["units"]["data"]})'
            units.append(unit)
        except Exception:
            units.append('')

    scatter = go.Figure(data=go.Surface(z=z, x=x, y=y))
    scatter.update_scenes(
        xaxis_title_text=f"{x_axis} {units[0]}".strip(),
        yaxis_title_text=f"{y_axis} {units[1]}".strip(),
        zaxis_title_text=f"{z_axis} {units[2]}".strip()
    )

    heatmap = go.Figure(data=go.Heatmap(z=z, x=x, y=y, type="heatmap", colorscale="Rainbow"))
    heatmap.update_layout(
        xaxis={"title": f"{x_axis} {units[0]}".strip()},
        yaxis={"title": f"{y_axis} {units[1]}".strip()}
    )
    if x_axis == "diameter":
        heatmap.update_xaxes(type="log")

    return [scatter, heatmap]


@callback(
        Output("sensor-data-buffer", "data"),
        Output("sensor-settings-buffer", "data"),
        Input("ws-sensor-instance", "message")
          )
def update_sensor_buffers(event):
    if event is not None and "data" in event:
        event_data = json.loads(event["data"])
        print(f"update_sensor_buffers: {event_data}")
        if "data-update" in event_data:
            try:
                if event_data["data-update"]:
                    return [event_data["data-update"], dash.no_update]
            except Exception as event:
                print(f"data buffer update error: {event}")
            
        if "settings-update" in event_data:
            try:
                if event_data["settings-update"]:
                    return [dash.no_update, event_data["settings-update"]]
            except Exception as e:
                print(f"settings buffer update error: {e}")
        
    return [dash.no_update, dash.no_update]


@callback(
    Output({"type": "sensor-graph-1d", "index": ALL}, "extendData"),
    Input("sensor-data-buffer", "data"),
    [
        State({"type": "sensor-graph-1d-dropdown", "index": ALL}, "value"),
    ],
    prevent_initial_call=True
)
def update_graph_1d(sensor_data, y_axis_list):
    if not sensor_data:
        raise PreventUpdate

    try:
        figs_to_update = []
        for y_axis in y_axis_list:
            if not y_axis:
                figs_to_update.append(dash.no_update)
                continue

            variables = sensor_data.get("variables", {})
            if "time" not in variables or y_axis not in variables:
                figs_to_update.append(dash.no_update)
                continue

            x_val = variables["time"].get("data")
            y_val = variables[y_axis].get("data")

            if x_val is None or y_val is None:
                figs_to_update.append(dash.no_update)
                continue

            if isinstance(x_val, list) and len(x_val) > 0: x_val = x_val[-1]
            if isinstance(y_val, list) and len(y_val) > 0: y_val = y_val[-1]

            figs_to_update.append(
                ( {"x": [[x_val]], "y": [[y_val]]}, [0], 1000 )
            )

        if not any(f != dash.no_update for f in figs_to_update):
            raise PreventUpdate

        return figs_to_update

    except Exception as e:
        L.error(f"data update error graph: {e}")
        raise PreventUpdate

@callback(
    Output({"type": "graph-2d-heatmap", "index": ALL}, "figure", allow_duplicate=True),
    Input("sensor-data-buffer", "data"),
    [
        State({"type": "graph-2d-dropdown", "index": ALL}, "value"),
        State("graph-axes", "data"),
        State("sensor-definition", "data"),
        State({"type": "graph-2d-heatmap", "index": ALL}, "figure"),
        State({"type": "graph-2d-heatmap", "index": ALL}, "id"),
    ],
    prevent_initial_call=True,
)
def update_graph_2d_heatmap(
    sensor_data, z_axis_list, graph_axes, sensor_definition, current_figs, graph_ids
):
    if not sensor_data:
        raise PreventUpdate

    heatmaps = []
    for z_axis, graph_id, current_fig in zip(z_axis_list, graph_ids, current_figs):
        if not current_fig or not current_fig.get("data"):
            heatmaps.append(dash.no_update)
            continue

        y_axis = graph_id["index"].split("::")[1]
        
        y_is_coord = False
        if sensor_definition and y_axis in sensor_definition.get("variables", {}):
            if sensor_definition["variables"][y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
                y_is_coord = True

        if (
            "time" not in sensor_data.get("variables", {})
            or (not y_is_coord and y_axis not in sensor_data.get("variables", {}))
            or z_axis not in sensor_data.get("variables", {})
        ):
            heatmaps.append(dash.no_update)
            continue

        x = sensor_data["variables"]["time"]["data"]

        if x in current_fig["data"][0].get("x", []):
            heatmaps.append(dash.no_update)
            continue

        if not isinstance(x, list):
            x = [x]

        for nx in x:
            current_fig["data"][0]["x"].append(nx)
        
        y = current_fig["data"][0].get("y", [])
        if len(y) == 0:
            if y_is_coord:
                y = sensor_definition["variables"][y_axis].get("data", [])
            else:
                y = sensor_data["variables"][y_axis]["data"]
        
        orig_z = sensor_data["variables"][z_axis]["data"]
        if not isinstance(orig_z, list):
            orig_z = [orig_z]

        z = []
        if len(x) > 1:
            for yi, yval in enumerate(y):
                new_z = []
                for xi, xval in enumerate(x):
                    try:
                        new_z.append(orig_z[xi][yi])
                    except IndexError:
                        new_z.append(None)
                z.append(new_z)
        else:
            for yi, yval in enumerate(y):
                try:
                    current_fig["data"][0]["z"][yi].append(orig_z[yi])
                except IndexError:
                    pass
                z.append([orig_z[yi]] if len(orig_z)>yi else [None])

        heatmaps.append(current_fig)
        
    if all(h == dash.no_update for h in heatmaps):
        raise PreventUpdate
        
    return heatmaps

@callback(
    Output({"type": "graph-2d-line", "index": ALL}, "figure"),
    Input("sensor-data-buffer", "data"),
    [
        State({"type": "graph-2d-dropdown", "index": ALL}, "value"),
        State("graph-axes", "data"),
        State("sensor-definition", "data"),
        State({"type": "graph-2d-line", "index": ALL}, "figure"),
        State({"type": "graph-2d-line", "index": ALL}, "id"),
    ],
    prevent_initial_call=True,
)
def update_graph_2d_scatter(
    sensor_data, z_axis_list, graph_axes, sensor_definition, current_figs, graph_ids
):
    if not sensor_data:
        raise PreventUpdate

    scatters = []
    for z_axis, graph_id, current_fig in zip(z_axis_list, graph_ids, current_figs):
        if not current_fig or not current_fig.get("data"):
            scatters.append(dash.no_update)
            continue

        y_axis = graph_id["index"].split("::")[1]
        
        y_is_coord = False
        if sensor_definition and y_axis in sensor_definition.get("variables", {}):
            if sensor_definition["variables"][y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
                y_is_coord = True

        if (
            "time" not in sensor_data.get("variables", {})
            or (not y_is_coord and y_axis not in sensor_data.get("variables", {}))
            or z_axis not in sensor_data.get("variables", {})
        ):
            scatters.append(dash.no_update)
            continue

        x = sensor_data["variables"]["time"]["data"]
        
        if y_is_coord:
            y = sensor_definition["variables"][y_axis].get("data", [])
        else:
            y = sensor_data["variables"][y_axis]["data"]
            
        z = sensor_data["variables"][z_axis]["data"]

        current_fig["data"][0]["x"] = y
        current_fig["data"][0]["y"] = z
        if isinstance(x, list) and len(x) > 0:
            x = x[-1]
        current_fig["layout"]["title"] = str(x)
        
        scatters.append(current_fig)

    if all(s == dash.no_update for s in scatters):
        raise PreventUpdate
        
    return scatters


@callback(
    [
        Output({"type": "graph-3d-line", "index": ALL}, "figure"),
        Output({"type": "graph-3d-heatmap", "index": ALL}, "figure")
    ],
    Input("sensor-data-buffer", "data"),
    [
        State({"type": "graph-3d-dropdown", "index": ALL}, "value"),
        State("sensor-definition", "data"),
        State({"type": "graph-3d-line", "index": ALL}, "figure"),
        State({"type": "graph-3d-heatmap", "index": ALL}, "figure"),
        State({"type": "graph-3d-dropdown", "index": ALL}, "id"),
    ],
    prevent_initial_call=True,
)
def update_graph_3d_plots(
    sensor_data, z_axis_list, sensor_definition, line_figs, heatmap_figs, graph_ids
):
    if not sensor_data:
        raise PreventUpdate

    updated_lines, updated_heatmaps = [], []
    
    for z_axis, graph_id, line_fig, heatmap_fig in zip(z_axis_list, graph_ids, line_figs, heatmap_figs):
        if not z_axis or not line_fig or not heatmap_fig:
            updated_lines.append(dash.no_update)
            updated_heatmaps.append(dash.no_update)
            continue

        x_axis = graph_id["index"].split("::")[0]
        y_axis = graph_id["index"].split("::")[1]
        
        x_is_coord, y_is_coord = False, False
        if sensor_definition:
            if x_axis in sensor_definition.get("variables", {}) and sensor_definition["variables"][x_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
                x_is_coord = True
            if y_axis in sensor_definition.get("variables", {}) and sensor_definition["variables"][y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
                y_is_coord = True

        if (
            (not x_is_coord and x_axis not in sensor_data.get("variables", {}))
            or (not y_is_coord and y_axis not in sensor_data.get("variables", {}))
            or z_axis not in sensor_data.get("variables", {})
        ):
            updated_lines.append(dash.no_update)
            updated_heatmaps.append(dash.no_update)
            continue

        if x_is_coord:
            x = sensor_definition["variables"][x_axis].get("data", [])
        else:
            x = sensor_data["variables"][x_axis]["data"]
            
        if y_is_coord:
            y = sensor_definition["variables"][y_axis].get("data", [])
        else:
            y = sensor_data["variables"][y_axis]["data"]
            
        latest_z = sensor_data["variables"][z_axis]["data"]

        z = []
        for yi in range(len(y)):
            new_row = []
            for xi in range(len(x)):
                try:
                    new_row.append(latest_z[xi][yi])
                except IndexError:
                    new_row.append(None)
            z.append(new_row)

        line_fig["data"][0]["z"] = z
        heatmap_fig["data"][0]["z"] = z
        
        if isinstance(x, list) and len(x) > 0: x_title = x[-1]
        else: x_title = x
        
        line_fig["layout"]["title"] = str(x_title)
        
        updated_lines.append(line_fig)
        updated_heatmaps.append(heatmap_fig)

    if all(l == dash.no_update for l in updated_lines):
        raise PreventUpdate
        
    return updated_lines, updated_heatmaps


@callback(
    Output("ws-send-instance-buffer", "children"),
    Input({"type": "settings-table", "index": ALL}, "cellValueChanged"),
    State("sensor-meta", "data")
)
def handle_setting_cell_changed(changed_cells, sensor_meta):
    """Detect vertical settings table grid edits and compile a structured CloudEvent settings request."""
    if not changed_cells or not any(x for x in changed_cells):
        raise PreventUpdate

    try:
        # Extract row info from vertical property change event
        event_entry = [x for x in changed_cells if x is not None][0][0]
        col_id = event_entry["data"]["parameter"]
        raw_val = event_entry["data"]["value"]
        
        # Cast to proper dynamic types based on context definitions
        try:
            if event_entry["data"]["type"] == "int":
                requested_val = int(raw_val)
            elif event_entry["data"]["type"] == "float":
                requested_val = float(raw_val)
            elif raw_val in ["True", "False"]:
                requested_val = raw_val == "True"
            else:
                requested_val = str(raw_val)
        except (ValueError, TypeError):
            requested_val = raw_val

        event = {
            "source": f"envds.{config.daq_id}.dashboard",
            "data": {"settings": {col_id: {"requested": requested_val}}},
            "destpath": "envds/sensor/settings/request",
            "deviceid": sensor_meta["device_id"]
        }
        print(f"Generated settings control request event: {event}")
        return json.dumps(event)
        
    except Exception as e:
        print(f"vertical property cell editing error: {e}")
        print(traceback.format_exc())
        raise PreventUpdate


@callback(
    Output({"type": "settings-table", "index": ALL}, "rowData"), 
    Input("sensor-settings-buffer", "data"),
    State({"type": "settings-table", "index": ALL}, "rowData"),
)
def update_settings_table(sensor_settings, row_data_list):
    """Live-update parameter layout rows when the instrument broadcasts actual setting updates."""
    if not sensor_settings or not row_data_list:
        raise PreventUpdate

    updated_row_lists = []
    has_updates = False

    try:
        for rows in row_data_list:
            if not rows:
                updated_row_lists.append(dash.no_update)
                continue
                
            grid_patched = False
            # Iterate vertical skeleton list elements
            for row in rows:
                param_name = row["parameter"]
                if param_name in sensor_settings.get("settings", {}):
                    # Safely map context update safely back to table view row
                    actual_val = sensor_settings["settings"][param_name]["data"].get("actual", "")
                    if row["value"] != actual_val:
                        row["value"] = actual_val
                        grid_patched = True
            
            if grid_patched:
                updated_row_lists.append(rows)
                has_updates = True
            else:
                updated_row_lists.append(dash.no_update)

        if not has_updates:
            raise PreventUpdate
            
        return updated_row_lists

    except Exception as e:
        print(f"settings-table live update pipeline failure: {e}")
        raise PreventUpdate


@callback(
    Output("calibration-display", "children"),
    Input("sensor-data-buffer", "data"),
    [
        State("calibration-display", "children"),
        State("calibration-vars", "data"),
    ]
)
def update_calibration_display(sensor_data, current_display, cal_vars):
    if not sensor_data or not cal_vars:
        raise PreventUpdate

    try:
        cal_data = json.loads(current_display)
    except:
        cal_data = {}

    has_updates = False
    for name in cal_vars:
        if name in sensor_data.get("variables", {}):
            new_val = sensor_data["variables"][name].get("data")
            if cal_data.get(name) != new_val:
                cal_data[name] = new_val
                has_updates = True
    
    if not has_updates and current_display != "Waiting for data...":
        raise PreventUpdate
        
    if not cal_data:
        return "Waiting for data..."
        
    return json.dumps(cal_data, indent=2)


@callback(
    Output(
        {"type": "data-table-1d", "index": ALL}, "rowTransaction"
    ),
    Input("sensor-data-buffer", "data"),
    [
        State({"type": "data-table-1d", "index": ALL}, "columnDefs"),
    ],
)
def update_table_1d(sensor_data, col_defs_list):
    if not sensor_data:
        raise PreventUpdate

    print('sensor data', sensor_data)
    transactions = []
    
    try:
        for col_defs in col_defs_list:
            data = {}
            for col in col_defs:
                name = col["field"]
                if name in sensor_data.get("variables", {}):
                    data[name] = sensor_data["variables"][name].get("data", "")
                else:
                    data[name] = ""
            
            transactions.append({"add": [data], "addIndex": 0})

        if len(transactions) == 0:
            raise PreventUpdate
            
        print('row transactions', transactions)
        return transactions

    except Exception as e:
        print(f"data update error table: {e}")
        print(traceback.format_exc())
        raise PreventUpdate

@callback(
    Output(
        {"type": "data-table-2d", "index": ALL}, "rowData"
    ), 
    Input("sensor-data-buffer", "data"),
    [
        State({"type": "data-table-2d", "index": ALL}, "rowData"),
        State({"type": "data-table-2d", "index": ALL}, "columnDefs"),
        State("sensor-definition", "data"),
    ],
)
def update_table_2d(sensor_data, row_data_list, col_defs_list, sensor_definition):
    if not sensor_data:
        raise PreventUpdate
        
    new_row_data_list = []
    for col_defs in col_defs_list:
        if not col_defs:
            new_row_data_list.append(dash.no_update)
            continue
            
        dim_2d = col_defs[0]["field"]
        
        dim_2d_is_coord = False
        if sensor_definition and dim_2d in sensor_definition.get("variables", {}):
            if sensor_definition["variables"][dim_2d].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
                dim_2d_is_coord = True
        
        if dim_2d_is_coord:
            dim_data = sensor_definition["variables"][dim_2d].get("data", [])
        else:
            if dim_2d not in sensor_data.get("variables", {}):
                new_row_data_list.append(dash.no_update)
                continue
            dim_data = sensor_data["variables"][dim_2d].get("data")
            if not dim_data:
                new_row_data_list.append(dash.no_update)
                continue

        row_data = []
        for index in range(0, len(dim_data)):
            data = {}
            for col in col_defs:
                name = col["field"]
                if name == dim_2d:
                    data[name] = dim_data[index]
                else:
                    try:
                        data[name] = sensor_data["variables"][name]["data"][index]
                    except (KeyError, IndexError, TypeError):
                        data[name] = None
            row_data.append(data)
        new_row_data_list.append(row_data)
        
    if all(r == dash.no_update for r in new_row_data_list):
        raise PreventUpdate
        
    return new_row_data_list


@callback(
    Output(
        {"type": "graph-2d-heatmap", "index": MATCH}, "figure", allow_duplicate=True
    ),
    [Input({"type": "graph-2d-z-axis-submit", "index": MATCH}, "n_clicks")],
    [
        State({"type": "graph-2d-z-axis-min", "index": MATCH}, "value"),
        State({"type": "graph-2d-z-axis-max", "index": MATCH}, "value"),
        State({"type": "graph-2d-heatmap", "index": MATCH}, "figure"),
    ],
    prevent_initial_call=True,
)
def set_2d_z_axis_range(n, axis_min, axis_max, heatmap):

    print(f"z-axis range: min={axis_min}, max={axis_max}")
    fig = go.Figure(heatmap)
    fig = fig.update_layout(coloraxis=dict(cauto=False, cmax=axis_max, cmin=axis_min))
    print(f"update fig: {fig}")

    return fig

@callback(
    Output("ws-sensor-instance", "send"), Input("ws-send-instance-buffer", "children")
)
def send_to_instance(value):
    print(f"sending: {value}")
    return value