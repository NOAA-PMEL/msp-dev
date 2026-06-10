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
    path_template="/controller/<controller_id>",
    title="Controller Telemetry & Settings",
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
        env_prefix = "ENVOPS_"
        case_sensitive = False

config = Settings()

datastore_url = f"datastore.{config.daq_id}-system.svc.cluster.local"

http_url_base = f"http://{config.external_hostname}:{config.http_port}"
if config.http_use_tls:
    http_url_base = f"https://{config.external_hostname}:{config.https_port}"
ws_url_base = f"ws://{config.external_hostname}:{config.ws_port}"
if config.ws_use_tls:
    ws_url_base = f"wss://{config.external_hostname}:{config.wss_port}"

# --- UI BUILDERS ---

def build_tables(layout_options):
    table_list = []
    for ltype, dims in layout_options.items():
        for dim, options in dims.items():
            title = "Data"

            if ltype == "layout-settings":
                title = f"Hardware Controls & Configuration"
                
                column_defs = [
                    {"field": "parameter", "headerName": "Control Parameter", "editable": False, "pinned": "left", "width": 250, "checkboxSelection": True},
                    {"field": "description", "headerName": "Description", "editable": False, "flex": 1},
                    {"field": "actual_value", "headerName": "Current State", "editable": False, "width": 150},
                    {
                        "field": "requested_value", 
                        "headerName": "Target Value (Click to Edit)", 
                        "editable": True,
                        "width": 220,
                        "cellStyle": {"backgroundColor": "#f8f9fa", "border": "1px dashed #0d6efd", "cursor": "text"}
                    }
                ]
                
                table_list.append(
                    dbc.AccordionItem(
                        [
                            dag.AgGrid(
                                id={"type": "controller-settings-table", "index": dim},
                                rowData=options.get("row-data-skeletons", []),
                                columnDefs=column_defs,
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
                            dbc.Button([html.I(className="bi bi-send-check me-2"), "Transmit Selected Control"], id={"type": "controller-submit-setting-btn", "index": dim}, color="primary", className="fw-bold shadow-sm")
                        ],
                        title=title,
                    )
                )

            elif ltype == "layout-1d":
                title = f"1-Dimensional Data Stream ({dim})"
                table_list.append(
                    dbc.AccordionItem(
                        [
                            dag.AgGrid(
                                id={"type": "controller-data-table-1d", "index": dim},
                                rowData=[],
                                columnDefs=options["table-column-defs"],
                                columnSize="autoSize",
                                defaultColDef={"resizable": True, "minWidth": 120},
                                dashGridOptions={
                                    "autoSizeStrategy": {"type": "fitCellContents"}
                                },
                                style={"height": "400px", "width": "100%"},
                                className="ag-theme-alpine shadow-sm border"
                            )
                        ],
                        title=title,
                    )
                )

            elif ltype == "layout-2d":
                title = f"2-Dimensional Data Stream (time, {dim})"
                table_list.append(
                    dbc.AccordionItem(
                        [
                            dag.AgGrid(
                                id={"type": "controller-data-table-2d", "index": f"time::{dim}"},
                                rowData=[],
                                columnDefs=options["table-column-defs"],
                                columnSize="autoSize",
                                defaultColDef={"resizable": True, "minWidth": 120},
                                dashGridOptions={
                                    "autoSizeStrategy": {"type": "fitCellContents"}
                                },
                                style={"height": "400px", "width": "100%"},
                                className="ag-theme-alpine shadow-sm border"
                            )
                        ],
                        title=title,
                    )
                )
    return table_list

def build_graph_1d(dropdown_list, xaxis="time"):
    return dbc.Card([
        dbc.CardHeader([
            html.Span([html.I(className="bi bi-funnel me-2"), "Y-Axis Variable:"], className="small fw-bold text-muted me-2 text-uppercase"),
            dcc.Dropdown(
                id={"type": "controller-graph-1d-dropdown", "index": xaxis},
                options=dropdown_list, value="", className="mt-1 shadow-sm"
            )
        ], className="bg-light border-bottom"),
        dbc.CardBody([
            dcc.Graph(
                id={"type": "controller-graph-1d", "index": xaxis},
                figure=go.Figure(data=go.Scatter(x=[], y=[], type="scatter")),
                style={"height": 400},
            )
        ], className="p-0")
    ], className="border-0 shadow-sm mb-3")

def build_graph_2d(dropdown_list, xaxis="time", yaxis=""):
    content = dbc.Row([
        dbc.Col([dbc.Label("Z-Axis Min:", className="small fw-bold text-muted mb-0")], width=2, align="center"),
        dbc.Col([dbc.Input(type="number", id={"type": "controller-graph-2d-z-axis-min", "index": f"{xaxis}::{yaxis}"}, size="sm")], width=3),
        dbc.Col([dbc.Label("Z-Axis Max:", className="small fw-bold text-muted mb-0")], width=2, align="center"),
        dbc.Col([dbc.Input(type="number", id={"type": "controller-graph-2d-z-axis-max", "index": f"{xaxis}::{yaxis}"}, size="sm")], width=3),
        dbc.Col([dbc.Button("Apply", id={"type": "controller-graph-2d-z-axis-submit", "index": f"{xaxis}::{yaxis}"}, color="primary", size="sm", className="fw-bold w-100")], width=2)
    ], className="g-2 mb-2")

    axes_settings = dbc.Accordion([
        dbc.AccordionItem([content], title="Axes Limits Override", class_name="small")
    ], start_collapsed=True, flush=True, className="border-bottom")

    return dbc.Card([
        dbc.CardHeader([
            html.Span([html.I(className="bi bi-funnel me-2"), "Z-Axis Variable:"], className="small fw-bold text-muted me-2 text-uppercase"),
            dcc.Dropdown(id={"type": "controller-graph-2d-dropdown", "index": f"{xaxis}::{yaxis}"}, options=dropdown_list, value="", className="mt-1 shadow-sm")
        ], className="bg-light border-bottom"),
        dbc.CardBody([
            axes_settings,
            dbc.Row([
                dbc.Col(dcc.Graph(id={"type": "controller-graph-2d-heatmap", "index": f"{xaxis}::{yaxis}"}, style={"height": 450})),
                dbc.Col(dcc.Graph(id={"type": "controller-graph-2d-line", "index": f"{xaxis}::{yaxis}"}, style={"height": 450})),
            ])
        ], className="p-0")
    ], className="border-0 shadow-sm mb-3")

def build_graph_3d(dropdown_list, xaxis="", yaxis="", zaxis=""):
    content = dbc.Row([
        dbc.Col([dbc.Label("Z-Axis Min:", className="small fw-bold text-muted mb-0")], width=3, align="center"),
        dbc.Col([dbc.Input(type="number", id={"type": "controller-graph-3d-z-axis-min", "index": f"{xaxis}::{yaxis}"}, size="sm")], width=4),
        dbc.Col([dbc.Button("Apply", id={"type": "controller-graph-3d-z-axis-submit", "index": f"{xaxis}::{yaxis}"}, color="primary", size="sm", className="fw-bold w-100")], width=5)
    ], className="g-2 mb-2")

    axes_settings = dbc.Accordion([
        dbc.AccordionItem([content], title="Axes Limits Override", class_name="small")
    ], start_collapsed=True, flush=True, className="border-bottom")

    return dbc.Card([
        dbc.CardHeader([
            html.Span([html.I(className="bi bi-funnel me-2"), "Z-Axis Variable:"], className="small fw-bold text-muted me-2 text-uppercase"),
            dcc.Dropdown(id={"type": "controller-graph-3d-dropdown", "index": f"{xaxis}::{yaxis}"}, options=dropdown_list, value="", className="mt-1 shadow-sm")
        ], className="bg-light border-bottom"),
        dbc.CardBody([
            axes_settings,
            dbc.Row([
                dbc.Col(dcc.Graph(id={"type": "controller-graph-3d-line", "index": f"{xaxis}::{yaxis}"}, style={"height": 450})),
                dbc.Col(dcc.Graph(id={"type": "controller-graph-3d-heatmap", "index": f"{xaxis}::{yaxis}"}, style={"height": 450})),
            ])
        ], className="p-0")
    ], className="border-0 shadow-sm mb-3")

def build_graphs(layout_options):
    graph_list = []
    for ltype, dims in layout_options.items():
        for dim, options in dims.items():
            if ltype == "layout-1d":
                title = f"1-Dimensional Plots ({dim})"
                graph_list.append(dbc.AccordionItem([build_graph_1d(options["variable-list"], xaxis=dim)], title=title))
            elif ltype == "layout-2d":
                title = f"2-Dimensional Plots (time, {dim})"
                graph_list.append(dbc.AccordionItem([build_graph_2d(options["variable-list"], xaxis="time", yaxis=dim)], title=title))
            elif ltype == "layout-3d":
                axes = dim.split("::")
                title = f"3-Dimensional Plots ({axes[0]}, {axes[1]})"
                graph_list.append(dbc.AccordionItem([build_graph_3d(options["variable-list"], xaxis=axes[0], yaxis=axes[1])], title=title))
    return graph_list

# --- DATA FETCHERS ---

def get_controller_data(controller_id: str):
    query = {"controller_id": controller_id}
    url = f"http://{datastore_url}/controller/data/get/"
    try:
        timeout = httpx.Timeout(30.0, read=None)
        response = httpx.get(url, params=query, timeout=timeout)
        results = response.json()
        if "results" in results and results["results"]:
            return results["results"]
    except Exception as e:
        L.error("get_controller_data", extra={"reason": e})
    return []

def get_controller_instance(controller_id: str):
    query = {"controller_id": controller_id}
    url = f"http://{datastore_url}/controller-instance/registry/get/"
    try:
        timeout = httpx.Timeout(30.0, read=None)
        response = httpx.get(url, params=query, timeout=timeout)
        results = response.json()
        if "results" in results and results["results"]:
            return results["results"][0]
    except Exception as e:
        L.error("get_controller_instance", extra={"reason": e})
    return {}

def get_controller_definition_by_device_id(controller_id: str):
    controller = get_controller_instance(controller_id=controller_id)
    if controller:
        try:
            version_str = controller.get("version", controller.get("format_version", "1.0.0"))

            controller_definition_id = "::".join([
                controller["make"],
                controller["model"],
                version_str
            ])
            return get_controller_definition(controller_definition_id=controller_definition_id)
        except Exception as e:
            print("ERROR: get_controller_definition_by_device_id", extra={"reason": e})
    return {}

def get_controller_definition(controller_definition_id: str):
    query = {"controller_definition_id": controller_definition_id}
    url = f"http://{datastore_url}/controller-definition/registry/get/"
    try:
        timeout = httpx.Timeout(30.0, read=None)
        response = httpx.get(url, params=query, timeout=timeout)
        results = response.json()
        if "results" in results and results["results"]:
            return results["results"][0]
    except Exception as e:
        L.error("get_controller_definition", extra={"reason": e})
        return {}
    
# --- LAYOUT ---

def layout(controller_id=None):
    controller_definition = None
    if controller_id:
        parts = controller_id.split("::")
        controller_meta = {
            "device_id": controller_id,
            "make": parts[0],
            "model": parts[1],
            "serial_number": parts[2],
        }
        controller_definition = get_controller_definition_by_device_id(controller_id=controller_id)
    else:
        controller_meta = {}
        controller_definition = {}

    layout_options = {
        "layout-settings": {"time": {"table-column-defs": [], "variable-list": [], "row-data-skeletons": []}},
        "layout-calibration": {"time": {"table-column-defs": [], "variable-list": []}},
        "layout-1d": {"time": {"table-column-defs": [], "variable-list": []}},
    }
    
    calibration_vars = []

    if controller_definition:
        try:
            dimensions = controller_definition["dimensions"]
            multi_dim = len(dimensions.keys()) > 1

            for name, var in controller_definition["variables"].items():
                var_type = var["attributes"].get("variable_type", {}).get("data")
                
                if var_type == "setting":
                    long_name = name
                    ln = var["attributes"].get("long_name", None)
                    if ln: long_name = ln.get("data", name)

                    dtype = var.get("type", "unknown")
                    allowed_vals = var["attributes"].get("allowed_values", {}).get("data", None)
                    min_val = var["attributes"].get("valid_min", {}).get("data", None)
                    max_val = var["attributes"].get("valid_max", {}).get("data", None)
                    step_val = var["attributes"].get("step_increment", {}).get("data", None)

                    control_metadata = {
                        "parameter": name,
                        "description": long_name,
                        "actual_value": "--",
                        "requested_value": "",
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
                            dln = controller_definition.get("attributes", {}).get(dim_2d, {}).get("long_name", {}).get("data", dim_2d)
                            d_dtype = controller_definition.get("variables", {}).get(dim_2d, {}).get("type", "unknown")
                            d_data_type = "number" if d_dtype in ["float", "double", "int"] else "boolean" if d_dtype == "bool" else "text"
                            
                            layout_options["layout-2d"][dim_2d]["table-column-defs"].append(
                                {"field": dim_2d, "headerName": dln, "filter": False, "cellDataType": d_data_type, "pinned": "left"}
                            )
                        layout_options["layout-2d"][dim_2d]["table-column-defs"].append(cd)

                    elif multi_dim and len(var["shape"]) == 3:
                        if "layout-3d" not in layout_options: layout_options["layout-3d"] = {}
                        dims_3d = [d for d in var["shape"] if d != "time"]
                        dim_3d_key = f"{dims_3d[0]}::{dims_3d[1]}"

                        if dim_3d_key not in layout_options["layout-3d"]:
                            layout_options["layout-3d"][dim_3d_key] = {"table-column-defs": [], "variable-list": []}
                        layout_options["layout-3d"][dim_3d_key]["table-column-defs"].append(cd)
                    else:
                        layout_options["layout-1d"]["time"]["table-column-defs"].append(cd)

            for ltype, dims in layout_options.items():
                for dim, options in dims.items():
                    if "table-column-defs" in options:
                        for cd in options["table-column-defs"]:
                            if cd["field"] in dimensions or cd["cellDataType"] != "number": continue
                            layout_options[ltype][dim]["variable-list"].append({"label": cd["field"], "value": cd["field"]})

        except KeyError as e:
            print(f"build column_defs error: {e}")

    initial_request = {
        "source": f"envds.{config.daq_id}.dashboard",
        "data": {},
        "destpath": "envds/controller/settings/request",
        "controllerid": controller_meta.get("device_id", "")
    }

    display_name = f"{controller_meta.get('make', '')} {controller_meta.get('model', controller_id.split('::')[-1] if controller_id else '')}"

    return html.Div([
        # --- HEADER STRIP ---
        dbc.Row([
            dbc.Col([
                html.H2([html.I(className="bi bi-cpu me-3 text-primary"), f"{display_name}"], className="text-dark fw-bold mb-0"),
                html.P(f"Controller ID: {controller_id}", className="text-muted small font-monospace mt-1 mb-0")
            ], width=8),
            dbc.Col(
                dbc.Button(
                    [html.I(className="bi bi-arrow-left me-2"), "Back to Registry"], 
                    href=dash.get_relative_path("/assets"), 
                    color="secondary", outline=True, className="float-end fw-bold shadow-sm"
                ), width=4, className="text-end align-self-center"
            )
        ], className="mb-4 mt-3 border-bottom pb-3"),

        # --- TABLES & CONTROLS ---
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H6([html.I(className="bi bi-sliders me-2"), "Hardware Controls & Data Streams"], className="mb-0 text-primary fw-bold"), className="p-2 bg-white border-bottom-0"),
                    dbc.CardBody([
                        dbc.Accordion(
                            build_tables(layout_options),
                            id="controller-data-accordion",
                            always_open=True,
                            flush=True,
                            className="border-top"
                        )
                    ], className="p-0 bg-light")
                ], className="shadow-sm border-0 mb-4")
            ], width=12)
        ]),

        # --- PLOTS ---
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H6([html.I(className="bi bi-graph-up me-2"), "Live Telemetry Plots"], className="mb-0 text-primary fw-bold"), className="p-2 bg-white border-bottom-0"),
                    dbc.CardBody([
                        dbc.Accordion(
                            build_graphs(layout_options),
                            id="controller-plot-accordion",
                            always_open=True,
                            flush=True,
                            className="border-top"
                        )
                    ], className="p-0")
                ], className="shadow-sm border-0 mb-4")
            ], width=12)
        ]),
        
        # --- CALIBRATION ---
        dbc.Row([
            dbc.Col([
                dbc.Card([
                    dbc.CardHeader(html.H6([html.I(className="bi bi-tools me-2"), "Calibration Values"], className="mb-0 text-primary fw-bold"), className="p-2 bg-white border-bottom-0"),
                    dbc.CardBody([
                        html.Pre(
                            id="controller-calibration-display", 
                            children="Waiting for data...",
                            className="bg-white p-3 border rounded text-dark font-monospace small mb-0 shadow-sm",
                            style={"whiteSpace": "pre-wrap", "wordBreak": "break-all", "maxHeight": "300px", "overflowY": "auto"}
                        )
                    ], className="p-3 bg-light")
                ], className="shadow-sm border-0 mb-4")
            ], width=12)
        ]),

        # --- WEBSOCKETS & STORES ---
        WebSocket(id="ws-controller-instance", url=f"{ws_url_base}/envds/envops/ws/controller/{controller_id}"),
        html.Div(id="ws-send-controller-buffer", children=json.dumps(initial_request), style={"display": "none"}),
        dcc.Store(id="controller-calibration-vars", data=calibration_vars),
        dcc.Store(id="controller-definition", data=controller_definition),
        dcc.Store(id="controller-meta", data=controller_meta),
        dcc.Store(id="controller-graph-axes", data={}),
        dcc.Store(id="controller-data-buffer", data={}),
        dcc.Store(id="controller-settings-buffer", data={})
    ])

# --- CALLBACKS ---

@callback(
    Output({"type": "controller-graph-1d", "index": MATCH}, "figure"),
    Input({"type": "controller-graph-1d-dropdown", "index": MATCH}, "value"),
    [
        State("controller-meta", "data"),
        State("controller-graph-axes", "data"),
        State("controller-definition", "data"),
        State({"type": "controller-graph-1d-dropdown", "index": MATCH}, "id"),
    ],
)
def select_graph_1d(y_axis, controller_meta, graph_axes, controller_definition, graph_id):
    default_fig = go.Figure(
        data=go.Scatter(x=[], y=[], type="scatter", mode="lines+markers"),
        layout={"xaxis": {"title": "Time"}, "yaxis": {"title": "Value"}}
    )

    if not y_axis: return default_fig

    try:
        if graph_axes is None: graph_axes = {}
        if "graph-1d" not in graph_axes: graph_axes["graph-1d"] = dict()
            
        graph_axes["graph-1d"][graph_id["index"]] = {"x-axis": "time", "y-axis": y_axis}

        x, y = [], []
        results = get_controller_data(controller_id=controller_meta.get("device_id"))
        
        if results and len(results) > 0:
            for doc in results:
                try:
                    x.append(doc["variables"]["time"]["data"])
                    y.append(doc["variables"][y_axis]["data"])
                except KeyError: continue

        units = ""
        try:
            unit_data = controller_definition["variables"][y_axis]["attributes"]["units"]["data"]
            if unit_data: units = f'({unit_data})'
        except Exception: pass

        fig = go.Figure(
            data=go.Scatter(x=x, y=y, type="scatter", mode="lines+markers"),
            layout={"xaxis": {"title": "Time"}, "yaxis": {"title": f"{y_axis} {units}".strip()}},
        )
        return fig
    except Exception as e:
        L.error(f"select_graph_1d error: {e}")
        return default_fig


@callback(
    [
        Output({"type": "controller-graph-2d-heatmap", "index": MATCH}, "figure", allow_duplicate=True),
        Output({"type": "controller-graph-2d-line", "index": MATCH}, "figure", allow_duplicate=True),
    ],
    Input({"type": "controller-graph-2d-dropdown", "index": MATCH}, "value"),
    [
        State("controller-meta", "data"),
        State("controller-graph-axes", "data"),
        State("controller-definition", "data"),
        State({"type": "controller-graph-2d-dropdown", "index": MATCH}, "id"),
    ],
    prevent_initial_call=True,
)
def select_graph_2d(z_axis, controller_meta, graph_axes, controller_definition, graph_id):
    if not z_axis: raise PreventUpdate

    if "graph-2d" not in graph_axes: graph_axes["graph-2d"] = dict()
    
    y_axis = graph_id["index"].split("::")[1]
    use_log = (y_axis == "diameter")
    
    graph_axes["graph-2d"][graph_id["index"]] = {"x-axis": "time", "y-axis": y_axis, "z-axis": z_axis}

    x, y, orig_z = [], [], []
    y_is_coord = False
    if controller_definition and y_axis in controller_definition.get("variables", {}):
        if controller_definition["variables"][y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
            y_is_coord = True
            y = controller_definition["variables"][y_axis].get("data", [])

    results = get_controller_data(controller_id=controller_meta.get("device_id"))
    if not results: raise PreventUpdate

    for doc in results:
        try:
            x.append(doc["variables"]["time"]["data"])
            if not y_is_coord: y.append(doc["variables"][y_axis]["data"])
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

    y_units, z_units = "", ""
    try: y_units = f'({controller_definition["variables"][y_axis]["attributes"]["units"]["data"]})'
    except Exception: pass
    try: z_units = f'({controller_definition["variables"][z_axis]["attributes"]["units"]["data"]})'
    except Exception: pass

    heatmap = go.Figure(
        data=go.Heatmap(x=x, y=y, z=z, type="heatmap", colorscale="Rainbow"),
        layout={"xaxis": {"title": "Time"}, "yaxis": {"title": f"{y_axis} {y_units}".strip()}},
    )
    if use_log:
        heatmap.update_yaxes(type="log")
        heatmap.update_layout(coloraxis=dict(cmax=None, cmin=None))

    scatter = go.Figure(
        data=[{"x": y, "y": orig_z[-1] if len(orig_z) > 0 else [], "type": "scatter"}],
        layout={"xaxis": {"title": f"{y_axis} {y_units}".strip()}, "yaxis": {"title": f"{z_axis} {z_units}".strip()}, "title": str(x[-1]) if len(x) > 0 else ""},
    )
    if use_log: scatter.update_xaxes(type="log")

    return [heatmap, scatter]


@callback(
    [
        Output({"type": "controller-graph-3d-line", "index": MATCH}, "figure", allow_duplicate=True),
        Output({"type": "controller-graph-3d-heatmap", "index": MATCH}, "figure", allow_duplicate=True)
    ],
    Input({"type": "controller-graph-3d-dropdown", "index": MATCH}, "value"),
    [
        State("controller-meta", "data"),
        State("controller-graph-axes", "data"),
        State("controller-definition", "data"),
        State({"type": "controller-graph-3d-dropdown", "index": MATCH}, "id"),
    ],
    prevent_initial_call=True,
)
def select_graph_3d(z_axis, controller_meta, graph_axes, controller_definition, graph_id):
    if not z_axis: raise PreventUpdate

    if "graph-3d" not in graph_axes: graph_axes["graph-3d"] = dict()
    
    x_axis = graph_id["index"].split("::")[0]
    y_axis = graph_id["index"].split("::")[1]
    
    x_is_coord, y_is_coord = False, False
    x, y, z_history = [], [], []

    if controller_definition:
        if x_axis in controller_definition.get("variables", {}) and controller_definition["variables"][x_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
            x_is_coord = True
            x = controller_definition["variables"][x_axis].get("data", [])
        if y_axis in controller_definition.get("variables", {}) and controller_definition["variables"][y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
            y_is_coord = True
            y = controller_definition["variables"][y_axis].get("data", [])

    results = get_controller_data(controller_id=controller_meta.get("device_id"))
    if not results: raise PreventUpdate

    for doc in results:
        try:
            if not x_is_coord: x.append(doc["variables"][x_axis]["data"])
            if not y_is_coord: y.append(doc["variables"][y_axis]["data"])
            z_history.append(doc["variables"][z_axis]["data"])
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
        try:
            unit = f'({controller_definition["variables"][axis]["attributes"]["units"]["data"]})'
            units.append(unit)
        except Exception: units.append('')

    scatter = go.Figure(data=go.Surface(z=z, x=x, y=y))
    scatter.update_scenes(
        xaxis_title_text=f"{x_axis} {units[0]}".strip(),
        yaxis_title_text=f"{y_axis} {units[1]}".strip(),
        zaxis_title_text=f"{z_axis} {units[2]}".strip()
    )

    heatmap = go.Figure(data=go.Heatmap(z=z, x=x, y=y, type="heatmap", colorscale="Rainbow"))
    heatmap.update_layout(xaxis={"title": f"{x_axis} {units[0]}".strip()}, yaxis={"title": f"{y_axis} {units[1]}".strip()})
    if x_axis == "diameter": heatmap.update_xaxes(type="log")

    return [scatter, heatmap]


# --- HELPER: Flattens nested dicts into scalars for graphing ---
def sanitize_payload(payload):
    if "variables" in payload:
        for k, v in payload["variables"].items():
            if "data" in v:
                val = v["data"]
                if isinstance(val, dict):
                    # Extract the actual or requested value
                    v["data"] = val.get("actual", val.get("requested", val))
                elif isinstance(val, list):
                    v["data"] = [
                        item.get("actual", item.get("requested", item)) if isinstance(item, dict) else item 
                        for item in val
                    ]
    return payload

@callback(
    Output("controller-data-buffer", "data"),
    Output("controller-settings-buffer", "data"),
    Input("ws-controller-instance", "message")
)
def update_controller_buffers(event):
    if event is not None and "data" in event:
        try:
            # Parse the payload sent by main.py (which is already ce.data)
            payload = json.loads(event["data"])
            
            if "variables" in payload: 
                return [sanitize_payload(payload), dash.no_update]
            elif "settings" in payload: 
                return [dash.no_update, payload]
                
        except Exception as e:
            L.error(f"Controller buffer parse error: {e}")
            
    return [dash.no_update, dash.no_update]


@callback(
    Output({"type": "controller-graph-1d", "index": ALL}, "extendData"),
    Input("controller-data-buffer", "data"),
    [State({"type": "controller-graph-1d-dropdown", "index": ALL}, "value")],
    prevent_initial_call=True
)
def update_graph_1d(controller_data, y_axis_list):
    if not controller_data: raise PreventUpdate

    try:
        figs_to_update = []
        for y_axis in y_axis_list:
            if not y_axis:
                figs_to_update.append(dash.no_update)
                continue

            variables = controller_data.get("variables", {})
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

            figs_to_update.append(( {"x": [[x_val]], "y": [[y_val]]}, [0], 1000 ))

        if not any(f != dash.no_update for f in figs_to_update): raise PreventUpdate
        return figs_to_update
    except Exception as e:
        L.error(f"data update error graph: {e}")
        raise PreventUpdate

@callback(
    Output({"type": "controller-graph-2d-heatmap", "index": ALL}, "figure", allow_duplicate=True),
    Input("controller-data-buffer", "data"),
    [
        State({"type": "controller-graph-2d-dropdown", "index": ALL}, "value"),
        State("controller-graph-axes", "data"),
        State("controller-definition", "data"),
        State({"type": "controller-graph-2d-heatmap", "index": ALL}, "figure"),
        State({"type": "controller-graph-2d-heatmap", "index": ALL}, "id"),
    ],
    prevent_initial_call=True,
)
def update_graph_2d_heatmap(controller_data, z_axis_list, graph_axes, controller_definition, current_figs, graph_ids):
    if not controller_data: raise PreventUpdate

    heatmaps = []
    for z_axis, graph_id, current_fig in zip(z_axis_list, graph_ids, current_figs):
        if not current_fig or not current_fig.get("data"):
            heatmaps.append(dash.no_update)
            continue

        y_axis = graph_id["index"].split("::")[1]
        
        y_is_coord = False
        if controller_definition and y_axis in controller_definition.get("variables", {}):
            if controller_definition["variables"][y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
                y_is_coord = True

        if ("time" not in controller_data.get("variables", {}) or (not y_is_coord and y_axis not in controller_data.get("variables", {})) or z_axis not in controller_data.get("variables", {})):
            heatmaps.append(dash.no_update)
            continue

        x = controller_data["variables"]["time"]["data"]
        if x in current_fig["data"][0].get("x", []):
            heatmaps.append(dash.no_update)
            continue

        if not isinstance(x, list): x = [x]
        for nx in x: current_fig["data"][0]["x"].append(nx)
        
        y = current_fig["data"][0].get("y", [])
        if len(y) == 0:
            if y_is_coord: y = controller_definition["variables"][y_axis].get("data", [])
            else: y = controller_data["variables"][y_axis]["data"]
        
        orig_z = controller_data["variables"][z_axis]["data"]
        if not isinstance(orig_z, list): orig_z = [orig_z]

        z = []
        if len(x) > 1:
            for yi, yval in enumerate(y):
                new_z = []
                for xi, xval in enumerate(x):
                    try: new_z.append(orig_z[xi][yi])
                    except IndexError: new_z.append(None)
                z.append(new_z)
        else:
            for yi, yval in enumerate(y):
                try: current_fig["data"][0]["z"][yi].append(orig_z[yi])
                except IndexError: pass
                z.append([orig_z[yi]] if len(orig_z)>yi else [None])

        heatmaps.append(current_fig)
        
    if all(h == dash.no_update for h in heatmaps): raise PreventUpdate
    return heatmaps

@callback(
    Output({"type": "controller-graph-2d-line", "index": ALL}, "figure"),
    Input("controller-data-buffer", "data"),
    [
        State({"type": "controller-graph-2d-dropdown", "index": ALL}, "value"),
        State("controller-graph-axes", "data"),
        State("controller-definition", "data"),
        State({"type": "controller-graph-2d-line", "index": ALL}, "figure"),
        State({"type": "controller-graph-2d-line", "index": ALL}, "id"),
    ],
    prevent_initial_call=True,
)
def update_graph_2d_scatter(controller_data, z_axis_list, graph_axes, controller_definition, current_figs, graph_ids):
    if not controller_data: raise PreventUpdate

    scatters = []
    for z_axis, graph_id, current_fig in zip(z_axis_list, graph_ids, current_figs):
        if not current_fig or not current_fig.get("data"):
            scatters.append(dash.no_update)
            continue

        y_axis = graph_id["index"].split("::")[1]
        
        y_is_coord = False
        if controller_definition and y_axis in controller_definition.get("variables", {}):
            if controller_definition["variables"][y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
                y_is_coord = True

        if ("time" not in controller_data.get("variables", {}) or (not y_is_coord and y_axis not in controller_data.get("variables", {})) or z_axis not in controller_data.get("variables", {})):
            scatters.append(dash.no_update)
            continue

        x = controller_data["variables"]["time"]["data"]
        if y_is_coord: y = controller_definition["variables"][y_axis].get("data", [])
        else: y = controller_data["variables"][y_axis]["data"]
            
        z = controller_data["variables"][z_axis]["data"]

        current_fig["data"][0]["x"] = y
        current_fig["data"][0]["y"] = z
        if isinstance(x, list) and len(x) > 0: x = x[-1]
        current_fig["layout"]["title"] = str(x)
        scatters.append(current_fig)

    if all(s == dash.no_update for s in scatters): raise PreventUpdate
    return scatters


@callback(
    [
        Output({"type": "controller-graph-3d-line", "index": ALL}, "figure"),
        Output({"type": "controller-graph-3d-heatmap", "index": ALL}, "figure")
    ],
    Input("controller-data-buffer", "data"),
    [
        State({"type": "controller-graph-3d-dropdown", "index": ALL}, "value"),
        State("controller-definition", "data"),
        State({"type": "controller-graph-3d-line", "index": ALL}, "figure"),
        State({"type": "controller-graph-3d-heatmap", "index": ALL}, "figure"),
        State({"type": "controller-graph-3d-dropdown", "index": ALL}, "id"),
    ],
    prevent_initial_call=True,
)
def update_graph_3d_plots(controller_data, z_axis_list, controller_definition, line_figs, heatmap_figs, graph_ids):
    if not controller_data: raise PreventUpdate

    updated_lines, updated_heatmaps = [], []
    
    for z_axis, graph_id, line_fig, heatmap_fig in zip(z_axis_list, graph_ids, line_figs, heatmap_figs):
        if not z_axis or not line_fig or not heatmap_fig:
            updated_lines.append(dash.no_update)
            updated_heatmaps.append(dash.no_update)
            continue

        x_axis = graph_id["index"].split("::")[0]
        y_axis = graph_id["index"].split("::")[1]
        
        x_is_coord, y_is_coord = False, False
        if controller_definition:
            if x_axis in controller_definition.get("variables", {}) and controller_definition["variables"][x_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
                x_is_coord = True
            if y_axis in controller_definition.get("variables", {}) and controller_definition["variables"][y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
                y_is_coord = True

        if ((not x_is_coord and x_axis not in controller_data.get("variables", {})) or (not y_is_coord and y_axis not in controller_data.get("variables", {})) or z_axis not in controller_data.get("variables", {})):
            updated_lines.append(dash.no_update)
            updated_heatmaps.append(dash.no_update)
            continue

        if x_is_coord: x = controller_definition["variables"][x_axis].get("data", [])
        else: x = controller_data["variables"][x_axis]["data"]
            
        if y_is_coord: y = controller_definition["variables"][y_axis].get("data", [])
        else: y = controller_data["variables"][y_axis]["data"]
            
        latest_z = controller_data["variables"][z_axis]["data"]

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

    if all(l == dash.no_update for l in updated_lines): raise PreventUpdate
    return updated_lines, updated_heatmaps


@callback(
    Output("ws-send-controller-buffer", "children", allow_duplicate=True),
    Input({"type": "controller-submit-setting-btn", "index": ALL}, "n_clicks"),
    State({"type": "controller-settings-table", "index": ALL}, "selectedRows"),
    State("controller-meta", "data"),
    prevent_initial_call=True
)
def submit_setting_change(n_clicks_list, selected_rows_list, controller_meta):
    print(f"\n--- CONTROLLER APPLY BUTTON CLICKED ---")
    print(f"Clicks List: {n_clicks_list}")
    print(f"Selected Rows List: {selected_rows_list}")
    
    if not any(n for n in n_clicks_list if n): 
        print("Aborting: No valid clicks.")
        raise PreventUpdate

    selected_row = None
    for rows in selected_rows_list:
        if rows and len(rows) > 0:
            selected_row = rows[0]
            break
            
    if not selected_row: 
        print("Aborting: No row selected across any table.")
        raise PreventUpdate
        
    col_id = selected_row["parameter"]
    raw_val = selected_row.get("requested_value")
    
    print(f"Targeting: {col_id} | Raw Requested Value: '{raw_val}'")
    
    if raw_val is None or raw_val == "": 
        print("Aborting: requested_value is empty.")
        raise PreventUpdate
        
    if str(raw_val).lower() in ["true", "on", "1"]:
        requested_val = 1 if selected_row.get("type") == "int" else True
    elif str(raw_val).lower() in ["false", "off", "0"]:
        requested_val = 0 if selected_row.get("type") == "int" else False
    else:
        try:
            if selected_row["type"] == "int": requested_val = int(raw_val)
            elif selected_row["type"] == "float": requested_val = float(raw_val)
            else: requested_val = str(raw_val)
        except (ValueError, TypeError):
            requested_val = raw_val

    event = {
        "source": f"envds.{config.daq_id}.dashboard",
        "data": {"settings": {col_id: {"requested": requested_val}}},
        "destpath": "envds/controller/settings/request",
        "controllerid": controller_meta["device_id"] 
    }
    print(f"SUCCESS! Transmitting: {json.dumps(event)}")
    return json.dumps(event)

@callback(
    Output({"type": "controller-settings-table", "index": ALL}, "rowData"), 
    Input("controller-settings-buffer", "data"),
    State({"type": "controller-settings-table", "index": ALL}, "rowData"),
    prevent_initial_call=True
)
def update_settings_table(controller_settings, row_data_list):
    if not controller_settings or not row_data_list: raise PreventUpdate

    updated_row_lists = []
    has_updates = False

    try:
        for rows in row_data_list:
            if not rows:
                updated_row_lists.append(dash.no_update)
                continue
                
            grid_patched = False
            new_rows = [] 
            
            for row in rows:
                new_row = row.copy() # THE FIX: Break the memory reference pointer!
                param_name = new_row["parameter"]
                
                if param_name in controller_settings.get("settings", {}):
                    param_data = controller_settings["settings"][param_name]
                    if isinstance(param_data, dict) and "data" in param_data:
                        actual_val = param_data["data"].get("actual", "")
                        req_val = param_data["data"].get("requested", "")
                    elif isinstance(param_data, dict):
                        actual_val = param_data.get("actual", "")
                        req_val = param_data.get("requested", "")
                    else: 
                        new_rows.append(new_row)
                        continue

                    if str(new_row.get("actual_value")) != str(actual_val):
                        new_row["actual_value"] = actual_val
                        grid_patched = True
                        
                    if new_row.get("requested_value") == "" or new_row.get("requested_value") is None:
                        new_row["requested_value"] = req_val
                        grid_patched = True
                
                new_rows.append(new_row)
            
            if grid_patched:
                updated_row_lists.append(new_rows)
                has_updates = True
            else: 
                updated_row_lists.append(dash.no_update)

        if not has_updates: raise PreventUpdate
        return updated_row_lists
        
    except Exception as e:
        print(f"settings-table live update failure: {e}")
        raise PreventUpdate


@callback(
    Output("controller-calibration-display", "children"),
    Input("controller-data-buffer", "data"),
    [
        State("controller-calibration-display", "children"),
        State("controller-calibration-vars", "data"),
    ]
)
def update_calibration_display(controller_data, current_display, cal_vars):
    if not controller_data or not cal_vars: raise PreventUpdate

    try: cal_data = json.loads(current_display)
    except: cal_data = {}

    has_updates = False
    for name in cal_vars:
        if name in controller_data.get("variables", {}):
            new_val = controller_data["variables"][name].get("data")
            if cal_data.get(name) != new_val:
                cal_data[name] = new_val
                has_updates = True
    
    if not has_updates and current_display != "Waiting for data...": raise PreventUpdate
    if not cal_data: return "Waiting for data..."
    return json.dumps(cal_data, indent=2)


@callback(
    Output({"type": "controller-data-table-1d", "index": ALL}, "rowTransaction"),
    Input("controller-data-buffer", "data"),
    [State({"type": "controller-data-table-1d", "index": ALL}, "columnDefs")],
)
def update_table_1d(controller_data, col_defs_list):
    if not controller_data: raise PreventUpdate

    transactions = []
    try:
        for col_defs in col_defs_list:
            data = {}
            for col in col_defs:
                name = col["field"]
                if name in controller_data.get("variables", {}): data[name] = controller_data["variables"][name].get("data", "")
                else: data[name] = ""
            transactions.append({"add": [data], "addIndex": 0})

        if len(transactions) == 0: raise PreventUpdate
        return transactions
    except Exception as e:
        print(f"data update error table: {e}")
        raise PreventUpdate


@callback(
    Output({"type": "controller-data-table-2d", "index": ALL}, "rowData"), 
    Input("controller-data-buffer", "data"),
    [
        State({"type": "controller-data-table-2d", "index": ALL}, "rowData"),
        State({"type": "controller-data-table-2d", "index": ALL}, "columnDefs"),
        State("controller-definition", "data"),
    ],
)
def update_table_2d(controller_data, row_data_list, col_defs_list, controller_definition):
    if not controller_data: raise PreventUpdate
        
    new_row_data_list = []
    for col_defs in col_defs_list:
        if not col_defs:
            new_row_data_list.append(dash.no_update)
            continue
            
        dim_2d = col_defs[0]["field"]
        dim_2d_is_coord = False
        if controller_definition and dim_2d in controller_definition.get("variables", {}):
            if controller_definition["variables"][dim_2d].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
                dim_2d_is_coord = True
        
        if dim_2d_is_coord: dim_data = controller_definition["variables"][dim_2d].get("data", [])
        else:
            if dim_2d not in controller_data.get("variables", {}):
                new_row_data_list.append(dash.no_update)
                continue
            dim_data = controller_data["variables"][dim_2d].get("data")
            if not dim_data:
                new_row_data_list.append(dash.no_update)
                continue

        row_data = []
        for index in range(0, len(dim_data)):
            data = {}
            for col in col_defs:
                name = col["field"]
                if name == dim_2d: data[name] = dim_data[index]
                else:
                    try: data[name] = controller_data["variables"][name]["data"][index]
                    except (KeyError, IndexError, TypeError): data[name] = None
            row_data.append(data)
        new_row_data_list.append(row_data)
        
    if all(r == dash.no_update for r in new_row_data_list): raise PreventUpdate
    return new_row_data_list


@callback(
    Output({"type": "controller-graph-2d-heatmap", "index": MATCH}, "figure", allow_duplicate=True),
    [Input({"type": "controller-graph-2d-z-axis-submit", "index": MATCH}, "n_clicks")],
    [
        State({"type": "controller-graph-2d-z-axis-min", "index": MATCH}, "value"),
        State({"type": "controller-graph-2d-z-axis-max", "index": MATCH}, "value"),
        State({"type": "controller-graph-2d-heatmap", "index": MATCH}, "figure"),
    ],
    prevent_initial_call=True,
)
def set_2d_z_axis_range(n, axis_min, axis_max, heatmap):
    fig = go.Figure(heatmap)
    fig = fig.update_layout(coloraxis=dict(cauto=False, cmax=axis_max, cmin=axis_min))
    return fig

@callback(
    Output("ws-controller-instance", "send"), Input("ws-send-controller-buffer", "children")
)
def send_to_instance(value):
    return value