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
# import pymongo
from collections import deque
import httpx
import traceback
# import xarray as xr

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.DEBUG)

dash.register_page(
    __name__,
    path_template="/system_data",
    title="System Data",  # , prevent_initial_callbacks=True
    order=1
)
 

class Settings(BaseSettings):
    # host: str = "0.0.0.0"
    # port: int = 8787
    # debug: bool = False
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

# TODO: add readOnly user for this connection

datastore_url = f"datastore.{config.daq_id}-system"

http_url_base = f"http://{config.external_hostname}:{config.http_port}"
if config.http_use_tls:
    http_url_base = f"https://{config.external_hostname}:{config.https_port}"
ws_url_base = f"ws://{config.external_hostname}:{config.ws_port}"
if config.ws_use_tls:
    ws_url_base = f"wss://{config.external_hostname}:{config.wss_port}"

link_url_base = f"{http_url_base}/msp/dashboardtest"

ws_send_buffer = html.Div(id="ws-send-instance-buffer", style={"display": "none"})


# def build_tables(layout_options):

#     table_list = []
#     print(f"build_tables: {layout_options}")
#     for ltype, dims in layout_options.items():
#         for dim, options in dims.items():
#             title = "Data"

#             if ltype == "layout-1d":
#                 title = f"Data 1-D ({dim})"

#                 # TODO: make the ids work for multiple dims
#                 table_list.append(
#                     dbc.AccordionItem(
#                         [
#                             dag.AgGrid(
#                                 id={"type": "data-table-1d", "index": dim},
#                                 rowData=[],
#                                 columnDefs=layout_options["layout-1d"][dim][
#                                     "table-column-defs"
#                                 ],
#                                 columnSizeOptions="autoSize",  # "autoSize", "autoSizeSkip", "sizeToFit", "responsiveSizeToFit"
#                             )
#                         ],
#                         title=title,
#                     )
#                 )
#                 print(f"build_tables: {table_list}")

#             elif ltype == "layout-2d":
#                 title = f"Data 2-D (time, {dim})"
#                 table_list.append(
#                     dbc.AccordionItem(
#                         [
#                             dag.AgGrid(
#                                 id={"type": "data-table-2d", "index": f"time::{dim}"},
#                                 rowData=[],
#                                 columnDefs=layout_options["layout-2d"][dim][
#                                     "table-column-defs"
#                                 ],
#                                 columnSizeOptions="autoSize",  # "autoSize", "autoSizeSkip", "sizeToFit", "responsiveSizeToFit"
#                             )
#                         ],
#                         title=title,
#                     )
#                 )

#     print(f"build_tables: {table_list}")

#     return table_list


def build_tables(table_columns_dict):
    L.debug(f"ENTERING build_tables ---")
    L.debug(f"table_columns_dict looks like: {table_columns_dict}")
    table_list = []
    for varset_id, columns in table_columns_dict.items():
        L.debug(f"Varset {varset_id} and column len {len(columns)}")
        table_list.append(
            dbc.AccordionItem(
                [
                    dag.AgGrid(
                        id={"type": "data-table-1d", "index": varset_id}, 
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
                        id={"type": "graph-1d-dropdown", "index": xaxis},
                        options=dropdown_list,
                        value="",
                    )
                ]
            ),
            dcc.Graph(
                id={"type": "graph-1d", "index": xaxis},
                figure=go.Figure(
                    data=go.Scatter(x=[], y=[], type="scatter")
                    # {
                    #     "x": [],
                    #     "y": [],
                    #     "type": "scatter",
                    # }
                ),
                style={"height": 600},
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
                title="Axes Settings",
                # start_collapsed=True
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
                title="Axes Settings",
                # start_collapsed=True
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




def build_graph_settings_2d():

    collapse = html.Div(
        [
            dbc.Button(
                "Graph Settings",
                id="collapse-button",
                className="mb-3",
                color="primary",
                n_clicks=0,
            ),
            dbc.Collapse(
                dbc.Card(dbc.CardBody("This content is hidden in the collapse")),
                id="collapse",
                is_open=False,
            ),
        ]
    )


def build_graph_settings_3d():

    collapse = html.Div(
        [
            dbc.Button(
                "Graph Settings",
                id="collapse-button",
                className="mb-3",
                color="primary",
                n_clicks=0,
            ),
            dbc.Collapse(
                dbc.Card(dbc.CardBody("This content is hidden in the collapse")),
                id="collapse",
                is_open=False,
            ),
        ]
    )


# def build_graphs(layout_options):

#     graph_list = []
#     print(f"build_graphs: {layout_options}")
#     for ltype, dims in layout_options.items():
#         print('dims', dims)
#         for dim, options in dims.items():
#             title = "Plots"
#             if ltype == "layout-1d":
#                 title = f"Plots 1-D ({dim})"

#                 # TODO: make the ids work for multiple dims
#                 graph_list.append(
#                     dbc.AccordionItem(
#                         [
#                             dbc.Row(
#                                 children=[
#                                     build_graph_1d(
#                                         layout_options["layout-1d"][dim][
#                                             "variable-list"
#                                         ],
#                                         xaxis=dim,
#                                     )
#                                 ]
#                             )
#                         ],
#                         title=title,
#                     )
#                 )
#                 print(f"build_graphs: {graph_list}")

#             elif ltype == "layout-2d":
#                 title = f"Plots 2-D (time, {dim})"
#                 graph_list.append(
#                     dbc.AccordionItem(
#                         [
#                             dbc.Row(
#                                 children=[
#                                     build_graph_2d(
#                                         layout_options["layout-2d"][dim][
#                                             "variable-list"
#                                         ],
#                                         xaxis="time",
#                                         yaxis=dim,
#                                     )
#                                 ]
#                             )
#                         ],
#                         title=title,
#                     )
#                 )
            
#         if ltype == "layout-3d":
#             title = f"Plots 3-D ({list(dims.keys())[0]}, {list(dims.keys())[1]})"
#             # title = f"Plots 3-D (time, {dim})"
#             graph_list.append(
#                 dbc.AccordionItem(
#                     [
#                         dbc.Row(
#                             children=[
#                                 build_graph_3d(
#                                     layout_options["layout-3d"][dim][
#                                         "variable-list"
#                                     ],
#                                     # xaxis="time",
#                                     xaxis =list(dims.keys())[0],
#                                     yaxis=list(dims.keys())[1],
#                                     # yaxis=dim
#                                     # zaxis=list(dims.keys())[1]
#                                 )
#                             ]
#                         )
#                     ],
#                     title=title,
#                 )
#             )

#     print(f"build_tables: {graph_list}")

#     return graph_list

def build_graphs(shared_dropdown_list, unique_varsets):
    return [
        dbc.AccordionItem(
            [
                dbc.Row(
                    children=[
                        dcc.Checklist(
                            # Important: This index ("shared") must exactly match 
                            # the index your dropdown uses inside build_graph_1d!
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






@callback(
    Output("variableset-store", "data"),
    Input("variableset-interval", "n_intervals"),
    State("variableset-store", "data")
)
def update_variableset_list(count, current_sets):
    update = False
    new_data = []
    
    try:
        # url = f"http://{datastore_url}/variableset-definition/registry/get/"
        url = f"http://{datastore_url}/variableset-definition/registry/ids/get/"
        L.debug(f"variableset-definition-get: {url}")
        response = httpx.get(url)
        results = response.json()
        L.debug(f"variableset-results: {results}")
        if "results" in results and results["results"]:
            for id in results["results"]:
                if id is not None:
                    parts = id.split("::")
                    # sensor_def = {
                    #     "variab": id,
                    #     "make": parts[0],
                    #     "model": parts[1],
                    #     "version": parts[2],
                    # }
                    # variableset = parts[0]
                    variableset = id
                    L.debug(f"variableset-definition-get_1: {variableset}")
                    if variableset not in current_sets:
                        current_sets.append(variableset)
                        update = True
                    new_data.append(variableset)


        remove_data = []
        for index, data in enumerate(current_sets):
            if data not in new_data:
                update = True
                remove_data.insert(0, index)
        for index in remove_data:
            current_sets.pop(index)

        if update:
            return current_sets
        else:
            return dash.no_update

    except Exception as e:
        L.error(f"update_sensor_definitions error: {e}")
        L.error(traceback.format_exc())
        return dash.no_update
    


@callback(
    Output("variableset-defs-store", "data"),
    Input("variableset-store", "data"),
    State("variableset-defs-store", "data")
)
def update_variableset_defs(varset_ids, current_defs):
    if varset_ids is None:
        raise PreventUpdate
    
    current_defs = current_defs or {}
    updated = False

    for varset_id in varset_ids:
        if varset_id not in current_defs:
            try:
                def_url = f"http://{datastore_url}/variableset-definition/registry/get/"
                query_params = {"variableset_definition_id": varset_id}
                L.debug(f"Fetching definition: {def_url}")

                response = httpx.get(
                    def_url,
                    params = query_params
                    )
                                
                if response.status_code == 200:
                    results = response.json()
                    current_defs[varset_id] = results
                    updated = True
                    L.debug(f"Varset definition: {results}")

                else:
                    L.error(f"Failed to fetch {varset_id}: Status {response.status_code}")
                    
            except Exception as e:
                L.error(f"Error fetching definition for {varset_id}: {e}")
    
    
    keys_to_remove = [k for k in current_defs.keys() if k not in varset_ids]
    for key in keys_to_remove:
        del current_defs[key]
        updated = True

    # 3. Only trigger downstream callbacks if the dictionary actually changed
    if updated:
        return current_defs
    else:
        return dash.no_update



@callback(
        Output("variableset-data-buffer", "data"),
        Input("ws-variableset-instance", "message")
          )
def update_variableset_buffers(event):
    L.debug(f"update_variableset_buffers: {event}")
    if event is not None and "data" in event:
        event_data = json.loads(event["data"])
        L.debug(f"update_variableset_buffers: {event}")
        return [event_data]
        # if "data-update" in event_data:
        #     try:
        #         # msg = json.loads(event["data-update"])
        #         # print(f"update_controller_data: {event_data}")
        #         if event_data["data-update"]:
        #             return [event_data["data-update"]]
        #     except Exception as event:
        #         L.debug(f"data buffer update error: {event}")
            
    return [dash.no_update]




# def layout(platform=None):
#     print(f"get_layout: {platform}")

#     for varset_id, definition in all_variablesets.items():

#         layout_options = {
#             "layout-1d": {"time": {"table-column-defs": [], "variable-list": []}},
#         }

#         if definition:
#             try:
#                 dimensions = definition["dimensions"]
#                 multi_dim = False
#                 if len(dimensions.keys()) > 1:
#                     multi_dim = True

#                 for name, var in definition["variables"].items():
#                     if var["attributes"]["variable_type"]["data"] == "setting":
#                         long_name = name
#                         ln = var["attributes"].get("long_name", None)
#                         if ln:
#                             long_name = ln.get("data", name)

#                         # get data type
#                         dtype = var.get("type", "unknown")
#                         print(f"dtype = {dtype}")
#                         data_type = "text"
#                         if dtype in ["float", "double", "int"]:
#                             data_type = "number"
#                         elif dtype in ["str", "string", "char"]:
#                             data_type = "text"
#                         elif dtype in ["bool"]:
#                             data_type = "boolean"

#                         cd = {
#                             "field": name,
#                             "headerName": long_name,
#                             "filter": False,
#                             "cellDataType": data_type,
#                             # "cellRendererSelector": {'function': 'component_selector(params)'}
#                         }
#                         layout_options["layout-settings"]["time"]["table-column-defs"].append(cd)

#                     # only get the data variables for main
#                     elif var["attributes"]["variable_type"]["data"] != "main":
#                         continue
#                     # if name in dimensions:
#                     #     continue
#                     if "shape" not in var:
#                         continue
#                     if "time" not in var["shape"]:
#                         continue
#                     # if len(var["shape"]) > 1 or "time" not in var["shape"]:
#                     #     continue

#                     long_name = name
#                     ln = var["attributes"].get("long_name", None)
#                     if ln:
#                         long_name = ln.get("data", name)

#                     # get data type
#                     dtype = var.get("type", "unknown")
#                     print(f"dtype = {dtype}")
#                     data_type = "text"
#                     if dtype in ["float", "double", "int"]:
#                         data_type = "number"
#                     elif dtype in ["str", "string", "char"]:
#                         data_type = "text"
#                     elif dtype in ["bool"]:
#                         data_type = "boolean"

#                     # column_defs_1d.append(
#                     cd = {
#                         "field": name,
#                         "headerName": long_name,
#                         "filter": False,
#                         "cellDataType": data_type,
#                     }
#                     # )

#                     if multi_dim and len(var["shape"]) == 2:
#                         if "layout-2d" not in layout_options:
#                             layout_options["layout-2d"] = (
#                                 {}
#                             )  # {dim_2d: {"table-column-defs": [], "variable-list": []}}

#                         dim_2d = [d for d in var["shape"] if d != "time"][0]

#                         # if no entry for second dim, create
#                         if dim_2d not in layout_options["layout-2d"]:
#                             layout_options["layout-2d"][dim_2d] = {
#                                 "table-column-defs": [],
#                                 "variable-list": [],
#                             }
#                             dln = dim_2d
#                             try:
#                                 dln = definition["attributes"][dim_2d]["long_name"]["data"]
#                             except KeyError:
#                                 pass

#                             # print(f"layout: {ln}")
#                             data_type = "text"
#                             try:
#                                 dtype = definition["variables"][dim_2d]["type"]
#                                 if dtype in ["float", "double", "int"]:
#                                     data_type = "number"
#                                 elif dtype in ["str", "string", "char"]:
#                                     data_type = "text"
#                                 elif dtype in ["bool"]:
#                                     data_type = "boolean"
#                             except KeyError:
#                                 pass
#                             # print(f"layout: {data_type}")

#                             dcd = {
#                                 "field": dim_2d,
#                                 "headerName": dln,
#                                 "filter": False,
#                                 "cellDataType": data_type,
#                                 "pinned": "left",
#                             }
#                             layout_options["layout-2d"][dim_2d]["table-column-defs"].append(dcd)

#                         layout_options["layout-2d"][dim_2d]["table-column-defs"].append(cd)

#                     elif multi_dim and len(var["shape"]) == 3:
#                         if "layout-3d" not in layout_options:
#                             layout_options["layout-3d"] = (
#                                 {}
#                             )  # {dim_2d: {"table-column-defs": [], "variable-list": []}}

#                         dim_3d = [d for d in var["shape"] if d != "time"][0]

#                         # if no entry for second dim, create
#                         if dim_3d not in layout_options["layout-3d"]:
#                             layout_options["layout-3d"][dim_3d] = {
#                                 "table-column-defs": [],
#                                 "variable-list": [],
#                             }
#                             dln = dim_3d
#                             try:
#                                 dln = definition["attributes"][dim_3d]["long_name"]["data"]
#                             except KeyError:
#                                 pass

#                             # print(f"layout: {ln}")
#                             data_type = "text"
#                             try:
#                                 dtype = definition["variables"][dim_3d]["type"]
#                                 if dtype in ["float", "double", "int"]:
#                                     data_type = "number"
#                                 elif dtype in ["str", "string", "char"]:
#                                     data_type = "text"
#                                 elif dtype in ["bool"]:
#                                     data_type = "boolean"
#                             except KeyError:
#                                 pass
#                             # print(f"layout: {data_type}")

#                             dcd = {
#                                 "field": dim_3d,
#                                 "headerName": dln,
#                                 "filter": False,
#                                 "cellDataType": data_type,
#                                 "pinned": "left",
#                             }
#                             layout_options["layout-3d"][dim_3d]["table-column-defs"].append(dcd)

#                         layout_options["layout-3d"][dim_3d]["table-column-defs"].append(cd)
#                         # continue
#                     else:
#                         print('cd', cd)
#                         layout_options["layout-1d"]["time"]["table-column-defs"].append(cd)

#                 for ltype, dims in layout_options.items():
#                     # for gtype, options in layout_options.items():
#                     for dim, options in dims.items():
#                         for cd in options["table-column-defs"]:
#                             if cd["field"] in dimensions:
#                                 continue
#                             if cd["cellDataType"] != "number":
#                                 continue
#                             layout_options[ltype][dim]["variable-list"].append(
#                                 {
#                                     # "label": cd["field"], "value": cd["field"]
#                                     "label": f"{varset_id} - {cd['field']}", 
#                                     "value": f"{varset_id}::{cd['field']}"
#                                 }
#                             )

#             except KeyError as e:
#                 print(f"build column_defs error: {e}")



# def layout(platform=None):
#     print(f"get_layout: {platform}")

#     # 1. INITIALIZE OUTSIDE THE LOOP
#     # Notice that "table-column-defs" is now a dictionary. 
#     # This maps varset_id -> [list of columns for that specific table]
#     layout_options = {
#         "layout-1d": {
#             "time": {
#                 "table-column-defs": {}, # Changed to dict to keep tables separate
#                 "variable-list": []      # Kept as list for the combined graph dropdown
#             }
#         },
#         "layout-settings": {
#             "time": {
#                 "table-column-defs": {}  # Added to prevent your KeyError
#             }
#         }
#     }

#     for varset_id, definition in all_variablesets.items():
#         if not definition:
#             continue

#         # 2. INITIALIZE THE COLUMN LIST FOR THIS SPECIFIC DATASET
#         layout_options["layout-1d"]["time"]["table-column-defs"][varset_id] = []
#         layout_options["layout-settings"]["time"]["table-column-defs"][varset_id] = []

#         try:
#             dimensions = definition["dimensions"]
#             multi_dim = len(dimensions.keys()) > 1

#             for name, var in definition["variables"].items():
                
#                 # --- SETTINGS VARIABLES ---
#                 if var["attributes"]["variable_type"]["data"] == "setting":
#                     long_name = name
#                     ln = var["attributes"].get("long_name", None)
#                     if ln:
#                         long_name = ln.get("data", name)

#                     dtype = var.get("type", "unknown")
#                     data_type = "text"
#                     if dtype in ["float", "double", "int"]:
#                         data_type = "number"
#                     elif dtype in ["str", "string", "char"]:
#                         data_type = "text"
#                     elif dtype in ["bool"]:
#                         data_type = "boolean"

#                     cd = {
#                         "field": name,
#                         "headerName": long_name,
#                         "filter": False,
#                         "cellDataType": data_type,
#                     }
#                     # Append to this specific dataset's settings table
#                     layout_options["layout-settings"]["time"]["table-column-defs"][varset_id].append(cd)

#                 # --- MAIN VARIABLES ---
#                 elif var["attributes"]["variable_type"]["data"] == "main":
#                     if "shape" not in var or "time" not in var["shape"]:
#                         continue

#                     long_name = name
#                     ln = var["attributes"].get("long_name", None)
#                     if ln:
#                         long_name = ln.get("data", name)

#                     dtype = var.get("type", "unknown")
#                     data_type = "text"
#                     if dtype in ["float", "double", "int"]:
#                         data_type = "number"
#                     elif dtype in ["str", "string", "char"]:
#                         data_type = "text"
#                     elif dtype in ["bool"]:
#                         data_type = "boolean"

#                     cd = {
#                         "field": name,
#                         "headerName": long_name,
#                         "filter": False,
#                         "cellDataType": data_type,
#                     }

#                     if multi_dim and len(var["shape"]) == 2:
#                         # Add your 2D logic here (make sure to use varset_id as the key like above)
#                         pass 
#                     elif multi_dim and len(var["shape"]) == 3:
#                         # Add your 3D logic here
#                         pass
#                     else:
#                         # 3. APPEND COLUMN TO THIS SPECIFIC VARSET'S TABLE
#                         layout_options["layout-1d"]["time"]["table-column-defs"][varset_id].append(cd)

#                         # 4. BUILD THE GRAPH DROPDOWN IMMEDIATELY
#                         # If it's a number and not a dimension, add it to the shared list
#                         if data_type == "number" and name not in dimensions:
#                             layout_options["layout-1d"]["time"]["variable-list"].append(
#                                 {
#                                     "label": f"{varset_id} - {name}", 
#                                     "value": f"{varset_id}::{name}"
#                                 }
#                             )

#         except KeyError as e:
#             print(f"build column_defs error: {e}")
            

#     layout = html.Div(
#         [
#             html.H1(f"System Data"),
#             dbc.Accordion(
#                 build_tables(layout_options),
#                 id="system-data-accordion",
#             ),
#             dbc.Accordion(
#                 build_graphs(layout_options),
 
#                 id="sensor-plot-accordion",
#             ),
#             dcc.Store(id="graph-axes", data={}),
#             WebSocket(
#                 id="ws-variableset-instance",
#                 # url=f"{ws_url_base}/msp/dashboardtest/ws/variableset/{variableset_id}"
#                 url=f"{ws_url_base}/msp/dashboardtest/ws/variableset/raz1::main"
#             ),
#             dcc.Store(id='variableset-store', data=[]),
#             dcc.Store(id='variableset-data-buffer', data={}),
#             dcc.Interval(
#                 id='variableset-interval',
#                 interval=5*1000,
#                 n_intervals=0
#             )
#         ]
#     )

#     request = {"client-request": "start-updates"}
#     return layout






def layout(platform=None):
    print(f"get_layout: {platform}")
    L.debug("Starting initial API fetch for layout generation...")

    all_variablesets = {}
    current_sets = []

    # 1. Fetch the initial data using the logic from your callback
    try:
        # Fetch the list of IDs
        url = f"http://{datastore_url}/variableset-definition/registry/ids/get/"
        response = httpx.get(url)
        results = response.json()
        
        if "results" in results and results["results"]:
            for varset_id in results["results"]:
                if varset_id is not None:
                    L.debug(f"layout function varset_id: {varset_id}")
                    # Keep track of the IDs for the dcc.Store baseline
                    current_sets.append(varset_id)
                    
                    # Fetch the full definition for this specific ID
                    # ** Adjust this URL to match your API's definition endpoint **
                    def_url = f"http://{datastore_url}/variableset-definition/registry/get/"
                    query_params = {"variableset_definition_id": varset_id}
                    def_response = httpx.get(
                        def_url,
                        params = query_params
                        )
                    L.debug(f"def response status code: {def_response.status_code}")
                    L.debug(f"layout varset def: {def_response}")
                    
                    if def_response.status_code == 200:
                        new_varset_def = def_response.json().get("results", {})[0]
                        all_variablesets[varset_id] = new_varset_def
                    else:
                        L.error(f"API REJECTION for {varset_id}")
                        
    except Exception as e:
        L.error(f"Initial variableset fetch error: {e}")
        L.error(traceback.format_exc())

    # 2. Create separate containers for the tables and the shared graph
    table_columns_1d = {}
    shared_graph_dropdown = []

    # 3. Parse the fetched definitions
    for varset_id, varset_definition in all_variablesets.items():
        L.debug(f"layout varset {varset_id}, {varset_definition}")
        if not varset_definition:
            continue
            
        # Initialize the column list for this specific table
        table_columns_1d[varset_id] = []

        table_columns_1d[varset_id].append({
            "field": "time",
            "headerName": "Time",
            "filter": False,
            "cellDataType": "text",
        })

        try:
            # dimensions = varset_definition.get("dimensions", {})

            
            for name, var in varset_definition.get("variables", {}).items():
                
                # Check variable type attributes
                # var_type = var.get("attributes", {}).get("variable_type", {}).get("data")
                
                # if var_type in ["setting", "main"]:
                    
                #     # If it's a main variable, ensure it has the required shape/time
                #     if var_type == "main":
                #         if "shape" not in var or "time" not in var["shape"]:
                #             continue

                # Get names
                long_name = name
                ln = var.get("attributes", {}).get("long_name", None)
                if ln:
                    long_name = ln.get("data", name)
                
                unit_val = var.get("attributes", {}).get("units", {}).get("data")
                if unit_val:
                    long_name = f"{long_name} ({unit_val})"

                # Get data type
                dtype = var.get("type", "unknown")
                data_type = "text"
                if dtype in ["float", "double", "int"]:
                    data_type = "number"
                elif dtype in ["str", "string", "char"]:
                    data_type = "text"
                elif dtype in ["bool"]:
                    data_type = "boolean"

                # Build column definition
                cd = {
                    "field": name,
                    "headerName": long_name,
                    "filter": False,
                    "cellDataType": data_type,
                }

                # Add the column to this specific dataset's table
                table_columns_1d[varset_id].append(cd)

                # Add the variable to the shared graph dropdown
                # if data_type == "number" and name not in dimensions:
                if data_type == "number":
                    shared_graph_dropdown.append(
                        {
                            "label": f"{long_name} - {varset_id}", 
                            "value": f"{varset_id}::{name}"
                        }
                    )

        except KeyError as e:
            L.error(f"build column_defs error for {varset_id}: {e}")
    
    unique_varsets = list(all_variablesets.keys())

    # 4. Build the layout
    layout_div = html.Div(
        [
            html.H1("System Data"),
            dbc.Accordion(
                build_tables(table_columns_1d), # Uses the updated minimal build_tables
                id="system-data-accordion",
            ),
            dbc.Accordion(
                build_graphs(shared_graph_dropdown, unique_varsets), # Uses the updated minimal build_graphs
                id="sensor-plot-accordion",
            ),

            dcc.Store(id="master-dropdown-options", data=shared_graph_dropdown),
            dcc.Store(id="graph-axes", data={}),
            WebSocket(
                id="ws-variableset-instance",
                # You might need to dynamically handle this if multiple WebSockets are needed
                url=f"{ws_url_base}/msp/dashboardtest/ws/variableset/raz1::main" 
            ),
            
            # Pre-populate the store with the IDs we just fetched so the interval 
            # callback has a baseline and doesn't immediately fire an update
            dcc.Store(id='variableset-store', data=current_sets),
            dcc.Store(id='variableset-defs-store', data=all_variablesets),            
            dcc.Store(id='variableset-data-buffer', data={}),
            dcc.Interval(
                id='variableset-interval',
                interval=5*1000,
                n_intervals=0
            )
        ]
    )

    return layout_div




@callback(
    Output({"type": "graph-1d-dropdown", "index": MATCH}, "options"),
    Input({"type": "graph-varset-filter", "index": MATCH}, "value"),
    State("master-dropdown-options", "data"),
    prevent_initial_call=False
)
def filter_graph_dropdown(selected_varsets, master_options):
    if not master_options:
        return dash.no_update
        
    # If the user unchecks every box, clear the dropdown completely
    if not selected_varsets:
        return [] 
        
    filtered_options = []
    
    for opt in master_options:
        # Split the value to check which dataset it belongs to
        try:
            varset_id, variable_name = opt["value"].rsplit("::", 1)
        except ValueError:
            continue
            
        # If the ID matches one of the checked boxes, keep it in the list!
        if varset_id in selected_varsets:
            filtered_options.append(opt)
            
    return filtered_options






@callback(
    Output(
        {"type": "graph-1d", "index": MATCH}, "figure"
    ),  # Output("graph-axes", "data")],
    Input({"type": "graph-1d-dropdown", "index": MATCH}, "value"),
    [
        State("graph-axes", "data"),
        State("variableset-defs-store", "data"),
        State({"type": "graph-1d-dropdown", "index": MATCH}, "id"),
    ],
)
def select_graph_1d(selected_value, graph_axes, variableset_defs, graph_id):
    if not selected_value:
        return {"x": [], "y": [], "type": "scatter"}
    
    try:
        varset_id, y_axis = selected_value.rsplit("::", 1)

        if "graph-1d" not in graph_axes:
            graph_axes["graph-1d"] = dict()
        graph_axes["graph-1d"][graph_id["index"]] = {"x-axis": "time", "y-axis": y_axis}

        units = ""

        try:
            unit_data = variableset_defs[varset_id]["variables"][y_axis].get("attributes", {}).get("units", {}).get("data")
            
            if unit_data:
                units = f'({unit_data})'

        except KeyError:
            pass

        fig = go.Figure(
                data=go.Scatter(x=[], y=[], type="scatter"),
                layout={
                    "xaxis": {"title": "Time"},
                    "yaxis": {"title": f"{y_axis} {units}"},
                },
            )

        return fig 

    except Exception as e:
        print(f"select_graph_1d error: {e}")
        L.error(traceback.format_exc())
        return dash.no_update  # , dash.no_update]


# @callback(
#     [
#         Output(
#             {"type": "graph-2d-heatmap", "index": MATCH}, "figure", allow_duplicate=True
#         ),
#         Output(
#             {"type": "graph-2d-line", "index": MATCH}, "figure", allow_duplicate=True
#         ),
#     ],
#     Input({"type": "graph-2d-dropdown", "index": MATCH}, "value"),
#     [
#         State("sensor-meta", "data"),
#         State("graph-axes", "data"),
#         State("sensor-definition", "data"),
#         State({"type": "graph-2d-dropdown", "index": MATCH}, "id"),
#     ],
#     prevent_initial_call=True,
# )
# def select_graph_2d(z_axis, sensor_meta, graph_axes, sensor_definition, graph_id):
#     print(f"select_graph_2d: {z_axis}, {sensor_meta}, {graph_axes}, {graph_id}")
#     try:
#         if "graph-2d" not in graph_axes:
#             graph_axes["graph-2d"] = dict()
#         y_axis = graph_id["index"].split("::")[1]
#         use_log = False
#         if y_axis == "diameter":
#             use_log = True
#         graph_axes["graph-2d"][graph_id["index"]] = {
#             "x-axis": "time",
#             "y-axis": y_axis,
#             "z-axis": z_axis,
#         }
#         print(f"select_graph_2d: {graph_axes}")

#         x = []
#         y = []
#         z = []
#         orig_z = []
#         query = {
#             "make": sensor_meta["make"],
#             "model": sensor_meta["model"],
#             "serial_number": sensor_meta["serial_number"],
#         }
#         sort = {"variables.time.data": 1}
#         device_id = f'{sensor_meta["make"]}::{sensor_meta["model"]}::{sensor_meta["serial_number"]}'
#         results = get_device_data(device_id=device_id)
#         print(f"2d results: {results}")
#         if results is None or len(results) == 0:
#             print("results = None")
#             raise PreventUpdate

#         elif results and len(results) > 0:
#             print("results = good")
#             for doc in results:
#                 try:
#                     x.append(doc["variables"]["time"]["data"])
#                     y.append(doc["variables"][y_axis]["data"])
#                     orig_z.append(doc["variables"][z_axis]["data"])
#                 except KeyError:
#                     continue

#         print('2d x', x)
#         print('2d y', y)
#         print('2d og z', z)
#         units = ""
#         try:
#             y_units = f'({sensor_definition["variables"][y_axis]["attributes"]["units"]["data"]})'
#             z_units = f'({sensor_definition["variables"][z_axis]["attributes"]["units"]["data"]})'
#         except KeyError:
#             pass

#         if isinstance(y[-1], list):
#             y = y[-1]

#         for yi, yval in enumerate(y):
#             new_z = []
#             for xi, xval in enumerate(x):
#                 new_z.append(orig_z[xi][yi])
#             z.append(new_z)

#         heatmap = go.Figure(
#             data=go.Heatmap(
#                 x=x, y=y, z=z, type="heatmap", colorscale="Rainbow"
#             ),
#             layout={
#                 "xaxis": {"title": "Time"},
#                 "yaxis": {"title": f"{y_axis} {y_units}"},
#             },
#         )
#         if use_log:
#             heatmap.update_yaxes(type="log")
#             heatmap.update_layout(coloraxis=dict(cmax=None, cmin=None))
#         print(f"heatmap figure: {heatmap}")
#         scatter = go.Figure(
#             data=[{"x": y, "y": z[-1], "type": "scatter"}],
#             layout={
#                 "xaxis": {"title": f"{y_axis} {y_units}"},
#                 "yaxis": {"title": f"{z_axis} {z_units}"},
#                 "title": str(x[-1]),
#             },
#         )
#         if use_log:
#             scatter.update_xaxes(type="log")
#         print(f"scatter figure: {scatter}")

#         return [heatmap, scatter]  # , graph_axes]
#     except Exception as e:
#         print(f"select_graph_2d error: {e}")
#         raise PreventUpdate



# @callback(
#     [
#         Output(
#             {"type": "graph-3d-line", "index": MATCH}, "figure", allow_duplicate=True
#         ),
#         Output(
#             {"type": "graph-3d-heatmap", "index": MATCH}, "figure", allow_duplicate=True
#         )
#     ],
#     Input({"type": "graph-3d-dropdown", "index": MATCH}, "value"),
#     [
#         State("sensor-meta", "data"),
#         State("graph-axes", "data"),
#         State("sensor-definition", "data"),
#         State({"type": "graph-3d-dropdown", "index": MATCH}, "id"),
#     ],
#     prevent_initial_call=True,
# )
# def select_graph_3d(z_axis, sensor_meta, graph_axes, sensor_definition, graph_id):
#     try:
#         if "graph-3d" not in graph_axes:
#             graph_axes["graph-3d"] = dict()
#         x_axis = graph_id["index"].split("::")[0]
#         y_axis = graph_id["index"].split("::")[1]
#         # z_axis = graph_id["index"].split("::")[2]
#         use_log = False
#         if x_axis == "diameter":
#             use_log = True
#         graph_axes["graph-3d"][graph_id["index"]] = {
#             # "x-axis": "time",
#             "x-axis": x_axis,
#             "y-axis": y_axis,
#             "z-axis": z_axis,
#         }
#         print(f"select_graph_3d: {graph_axes}")

#         x = []
#         y = []
#         z = []
        
#         device_id = f'{sensor_meta["make"]}::{sensor_meta["model"]}::{sensor_meta["serial_number"]}'
#         results = get_device_data(device_id=device_id)
#         print(f"3d results: {results}")
#         if results is None or len(results) == 0:
#             raise PreventUpdate

#         elif results and len(results) > 0:
#             print("results = good")
#             for doc in results:
#                 try:
#                     x.append(doc["variables"][x_axis]["data"])
#                     y.append(doc["variables"][y_axis]["data"])
#                     z.append(doc["variables"][z_axis]["data"])
#                 except KeyError:
#                     continue

#         units = []
#         for axis in [x_axis, y_axis, z_axis]:
#             try:
#                 unit = f'({sensor_definition["variables"][axis]["attributes"]["units"]["data"]})'
#                 units.append(unit)
#             except Exception:
#                 unit = ''
#                 units.append(unit)
        
#         print('units', units)
        
#         if isinstance(x[-1], list):
#             x = x[-1]

#         if isinstance(y[-1], list):
#             y = y[-1]
        
#         if isinstance(z[-1], list):
#             z = z[-1]

#         heatmap = go.Figure(
#             data=go.Heatmap(
#                 x=x, y=y, z=z, type="heatmap", colorscale="Rainbow"
#             ),
#             layout={
#                 "xaxis": {"title": f"{x_axis} {units[0]}"},
#                 "yaxis": {"title": f"{y_axis} {units[1]}"},
#                 # "colorscale": "rainbow"
#             },
#         )
#         if use_log:
#             heatmap.update_xaxes(type="log")
#             heatmap.update_layout(coloraxis=dict(cmax=None, cmin=None))
        
#         scatter = go.Figure(
#             data = go.Surface(z=z, x=x, y=y)
#         )
#         scatter.update_scenes(
#             xaxis_title_text = f"{x_axis} {units[0]}",
#             yaxis_title_text = f"{y_axis} {units[1]}",
#             zaxis_title_text = f"{z_axis} {units[2]}"
#         )

#         return [scatter, heatmap]
#         # return PreventUpdate
#     except Exception as e:
#         print(f"select_graph_3d error: {e}")
#         print(traceback.format_exc())
#         raise PreventUpdate


# @callback(
#         Output("sensor-data-buffer", "data"),
#         Output("sensor-settings-buffer", "data"),
#         Input("ws-sensor-instance", "message")
#           )
# def update_sensor_buffers(event):
#     if event is not None and "data" in event:
#         event_data = json.loads(event["data"])
#         print(f"update_sensor_buffers: {event_data}")
#         if "data-update" in event_data:
#             try:
#                 if event_data["data-update"]:
#                     return [event_data["data-update"], dash.no_update]
#             except Exception as event:
#                 print(f"data buffer update error: {event}")
            
#         if "settings-update" in event_data:
#             try:
#                 if event_data["settings-update"]:
#                     return [dash.no_update, event_data["settings-update"]]
#             except Exception as e:
#                 print(f"settings buffer update error: {e}")
        
#     return [dash.no_update, dash.no_update]


# @callback(
#     Output({"type": "graph-1d", "index": ALL}, "extendData"),
#     Input("variableset-data-buffer", "data"),
#     [
#         State({"type": "graph-1d-dropdown", "index": ALL}, "value"),
#         # State("graph-axes", "data"),
#         # State({"type": "graph-1d", "index": ALL}, "figure"),
#     ],
# )
# def update_graph_1d(buffer_data, selected_values):
#     if not buffer_data:
#         raise PreventUpdate

#     try:
#         L.debug(f"update graph buffer data {buffer_data}")
#         # NOTE: You need a way to identify WHICH dataset just arrived in the buffer.
#         # Assuming your websocket payload includes a field identifying the varset_id:
#         # e.g., incoming_varset_id = buffer_data.get("device_id") 
#         # or    incoming_varset_id = buffer_data.get("id")
#         buffer_data = buffer_data[0]
#         incoming_varset_id = buffer_data.get("attributes", {}).get("variablesetfullid", {}) # Adjust this key to match your actual payload!
        
#         if isinstance(incoming_varset_id, dict):
#             incoming_varset_id = incoming_varset_id.get("data")
#         else:
#             incoming_varset_id = incoming_varset_id

#         figs_to_update = []

#         if incoming_varset_id:

#             for selected_value in selected_values:
#                 # If the dropdown is empty, don't update this graph
#                 if not selected_value:
#                     figs_to_update.append(dash.no_update)
#                     continue

#                 varset_id, y_axis = selected_value.rsplit("::", 1)

#                 # ONLY extend the graph if the incoming data belongs to the selected dataset
#                 if incoming_varset_id != varset_id:
#                     figs_to_update.append(dash.no_update)
#                     continue

#                 # Ensure the data has time and the selected variable
#                 if "time" not in buffer_data.get("variables", {}) or y_axis not in buffer_data.get("variables", {}):
#                     figs_to_update.append(dash.no_update)
#                     continue

#                 # Extract the live data points
#                 x_val = buffer_data.get("variables", {}).get("time", {}).get("data", {})
#                 y_val = buffer_data.get("variables", {}).get(y_axis, {}).get("data", {})

#                 # Dash's extendData expects lists of lists: {"x": [[new_x]], "y": [[new_y]]}
#                 figs_to_update.append({"x": [[x_val]], "y": [[y_val]]})

#             return figs_to_update
        
#         else:
#             raise PreventUpdate

#     except Exception as e:
#         print(f"data update error graph: {e}")
#         L.error(traceback.format_exc())
#         raise PreventUpdate



@callback(
    Output({"type": "graph-1d", "index": ALL}, "extendData"),
    Input("variableset-data-buffer", "data"),
    [
        State({"type": "graph-1d-dropdown", "index": ALL}, "value"),
    ],
)
def update_graph_1d(buffer_data, selected_values):
    if not buffer_data:
        raise PreventUpdate

    try:
        L.debug(f"update graph buffer data {buffer_data}")

        incoming_varset_id = buffer_data[0].get("variablesetfullid", {})
        L.debug(f"incoming_varset_id {incoming_varset_id}")
        buffer_data = buffer_data[0].get("data-update", {})
        L.debug(f"buffer_data {buffer_data}")

       
        # buffer_data = buffer_data[0].get("data", {})
        
        # Safely extract the ID
        # incoming_varset_id = buffer_data.get("attributes", {}).get("variablesetfullid", {})
        if isinstance(incoming_varset_id, dict):
            incoming_varset_id = incoming_varset_id.get("data")
            
        figs_to_update = []

        if incoming_varset_id:
            for selected_value in selected_values:
                # If the dropdown is empty, don't update this graph
                if not selected_value:
                    figs_to_update.append(dash.no_update)
                    continue

                # Safely split varset_id and y_axis
                try:
                    varset_id, y_axis = selected_value.rsplit("::", 1)
                except ValueError:
                    figs_to_update.append(dash.no_update)
                    continue

                L.debug(f"GRAPH ROUTING: Incoming '{incoming_varset_id}' vs Selected '{varset_id}'")

                # ONLY extend the graph if the incoming data belongs to the selected dataset
                if str(incoming_varset_id) != str(varset_id):
                    figs_to_update.append(dash.no_update)
                    continue

                # Ensure the data has time and the selected variable
                variables = buffer_data.get("variables", {})

                
                time_data = variables.get("time", {})
                if not time_data:
                    time_data = buffer_data.get("time", {}) # Check root if not in variables
                
                # CHECKPOINT 2: Did we find the data?
                L.debug(f"GRAPH DATA CHECK: Time found? {bool(time_data)} | '{y_axis}' found? {y_axis in variables}")
                
                if not time_data or y_axis not in variables:
                    figs_to_update.append(dash.no_update)
                    continue

                # FIX 1: Extract the live data points (removed the {} fallback)
                x_val = variables.get("time", {}).get("data")
                y_val = variables.get(y_axis, {}).get("data")

                if isinstance(x_val, list) and len(x_val) > 0: x_val = x_val[0]
                if isinstance(y_val, list) and len(y_val) > 0: y_val = y_val[0]

                # CHECKPOINT 3: What is actually being sent to the graph?
                L.debug(f"GRAPH APPENDING DATA: x=[{x_val}], y=[{y_val}]")

                # FIX 2: extendData requires a tuple: ( {data_dict}, [target_traces], max_points )
                # [0] targets the first line on the graph. 1000 limits the line to 1000 points.
                figs_to_update.append(
                    ( {"x": [[x_val]], "y": [[y_val]]}, [0], 1000 )
                )

            return figs_to_update
        
        else:
            return [dash.no_update] * len(selected_values)

    except Exception as e:
        print(f"data update error graph: {e}")
        L.error(traceback.format_exc())
        return [dash.no_update] * len(selected_values)



# @callback(
#     Output({"type": "graph-2d-heatmap", "index": ALL}, "figure", allow_duplicate=True),
#     Input("sensor-data-buffer", "data"),
#     [
#         State({"type": "graph-2d-dropdown", "index": ALL}, "value"),
#         State("graph-axes", "data"),
#         State("sensor-definition", "data"),
#         State({"type": "graph-2d-heatmap", "index": ALL}, "figure"),
#         State({"type": "graph-2d-heatmap", "index": ALL}, "id"),
#     ],
#     prevent_initial_call=True,
# )
# def update_graph_2d_heatmap(
#     sensor_data, z_axis_list, graph_axes, sensor_definition, current_figs, graph_ids
# ):

#     try:
#         heatmaps = []
#         if sensor_data:
#             print(f"update_2d_heatmap: {z_axis_list}, {graph_ids}")
#             for z_axis, graph_id, current_fig in zip(
#                 z_axis_list, graph_ids, current_figs
#             ):
#                 y_axis = graph_id["index"].split("::")[1]
#                 print(f"y_axis, z_axis: {y_axis}, {z_axis}")
#                 if (
#                     "time" not in sensor_data["variables"]
#                     or y_axis not in sensor_data["variables"]
#                     or z_axis not in sensor_data["variables"]
#                 ):
#                     raise PreventUpdate

#                 x = sensor_data["variables"]["time"]["data"]

#                 if x in current_fig["data"][0]["x"]:
#                     print("don't update")
#                     raise PreventUpdate

#                 print("start x,y,z")
#                 if not isinstance(x, list):
#                     x = [x]
#                 print(f"x: {x}")

#                 # work around until extendData works
#                 new_x = current_fig["data"][0]["x"]
#                 for nx in x:
#                     current_fig["data"][0]["x"].append(nx)
#                 print(f"new x: {new_x}")
#                 y = current_fig["data"][0]["y"]
#                 if len(y) == 0:
#                     y = sensor_data["variables"][y_axis]["data"]
#                 if not isinstance(y, list):
#                     y = [y]
#                 print(f"y: {y}")
#                 orig_z = sensor_data["variables"][z_axis]["data"]
#                 if not isinstance(orig_z, list):
#                     orig_z = [orig_z]
#                 print(f"update: {[x]}, {[y]}, {[orig_z]}")

#                 # work around until extendData works
#                 z = []
#                 new_z = current_fig["data"][0]["z"]
#                 if len(x) > 1:
#                     for yi, yval in enumerate(y):
#                         z.append([])
#                         new_z = []
#                         for xi, xval in enumerate(x):
#                             new_z.append(orig_z[xi][yi])
#                         z.append(new_z)
#                 else:
#                     for yi, yval in enumerate(y):
#                         current_fig["data"][0]["z"][yi].append(orig_z[yi])
#                         z.append([orig_z[yi]])

#                 units = ""
#                 try:
#                     y_units = f'({sensor_definition["variables"][y_axis]["attributes"]["units"]["data"]})'
#                     z_units = f'({sensor_definition["variables"][z_axis]["attributes"]["units"]["data"]})'
#                 except KeyError:
#                     pass

#                 print(f'change data: "x": {[x]}, "y": {[y]}, "z": {[z]}')
#                 heatmaps.append(current_fig)

#             print(f"heatmaps: {heatmaps}")
#             if len(heatmaps) == 0:
#                 raise PreventUpdate
#             return heatmaps

#     except Exception as e:
#         print(f"heatmap update error: {e}")

#     raise PreventUpdate


# @callback(
#     Output({"type": "graph-2d-line", "index": ALL}, "figure"),
#     Input("sensor-data-buffer", "data"),
#     [
#         State({"type": "graph-2d-dropdown", "index": ALL}, "value"),
#         State("graph-axes", "data"),
#         State("sensor-definition", "data"),
#         State({"type": "graph-2d-line", "index": ALL}, "figure"),
#         State({"type": "graph-2d-line", "index": ALL}, "id"),
#     ],
#     prevent_initial_call=True,
# )
# def update_graph_2d_scatter(
#     sensor_data, z_axis_list, graph_axes, sensor_definition, current_figs, graph_ids
# ):

#     try:
#         scatters = []
#         if sensor_data:
#             print(f"sensor_data: {sensor_data}")
#             for z_axis, graph_id, current_fig in zip(
#                 z_axis_list, graph_ids, current_figs
#             ):
#                 y_axis = graph_id["index"].split("::")[1]
#                 if (
#                     "time" not in sensor_data["variables"]
#                     or y_axis not in sensor_data["variables"]
#                     or z_axis not in sensor_data["variables"]
#                 ):
#                     raise PreventUpdate

#                 x = sensor_data["variables"]["time"]["data"]
#                 y = sensor_data["variables"][y_axis]["data"]
#                 z = sensor_data["variables"][z_axis]["data"]
#                 print(f"scatter update: {x}, {y}, {z}")

#                 current_fig["data"][0]["x"] = y
#                 current_fig["data"][0]["y"] = z
#                 if isinstance(x, list):
#                     x = x[-1]
#                 current_fig["layout"]["title"] = str(x)
#                 print(f"scatter current_fig: {current_fig}")
#                 scatters.append(current_fig)

#             return scatters

#     except Exception as e:
#         print(f"scatter update error: {e}")
#         # return dash.no_update
#         # return dash.no_update
#     raise PreventUpdate
#     # return dash.no_update




# @callback(
#     Output({"type": "graph-3d-line", "index": ALL}, "figure"),
#     Input("sensor-data-buffer", "data"),
#     [
#         State({"type": "graph-3d-dropdown", "index": ALL}, "value"),
#         State("graph-axes", "data"),
#         State("sensor-definition", "data"),
#         State({"type": "graph-3d-line", "index": ALL}, "figure"),
#         State({"type": "graph-3d-line", "index": ALL}, "id"),
#     ],
#     prevent_initial_call=True,
# )
# def update_graph_3d_scatter(
#     sensor_data, z_axis_list, graph_axes, sensor_definition, current_figs, graph_ids
# ):

#     try:
#         scatters = []
#         if sensor_data:
#             print(f"sensor_data: {sensor_data}")
#             for z_axis, graph_id, current_fig in zip(
#                 z_axis_list, graph_ids, current_figs
#             ):
#                 x_axis = graph_id["index"].split("::")[0]
#                 y_axis = graph_id["index"].split("::")[1]
#                 if (
#                     # "time" not in sensor_data["variables"]
#                     x_axis not in sensor_data["variables"]
#                     or y_axis not in sensor_data["variables"]
#                     or z_axis not in sensor_data["variables"]
#                 ):
#                     raise PreventUpdate

#                 x = sensor_data["variables"][x_axis]["data"]
#                 y = sensor_data["variables"][y_axis]["data"]
#                 z = sensor_data["variables"][z_axis]["data"]
#                 print(f"scatter update: {x}, {y}, {z}")

#                 current_fig["data"][0]["z"] = z
#                 if isinstance(x, list):
#                     x = x[-1]
#                 current_fig["layout"]["title"] = str(x)
#                 print(f"scatter current_fig: {current_fig}")
#                 scatters.append(current_fig)

#             return scatters

#     except Exception as e:
#         print(f"scatter update error: {e}")
#     raise PreventUpdate


# @callback(
#     Output(
#         {"type": "data-table-1d", "index": ALL}, "rowData"
#     ),
#     Input("variableset-data-buffer", "data"),
#     [
#         State({"type": "data-table-1d", "index": ALL}, "rowData"),
#         State({"type": "data-table-1d", "index": ALL}, "columnDefs"),
#         State({"type": "data-table-1d", "index": ALL}, "id")
#     ],  # , dcc.Store("sensor-definition", "data")],
# )
# def update_table_1d(buffer_data, row_data_list, col_defs_list, table_ids):
#     if not buffer_data:
#         raise PreventUpdate
    
#     try:
    
#         L.debug(f"update table buffer data {buffer_data}")

#         # Get the ID of the incoming data (adjust "id" to match your websocket payload key)
#         buffer_data = buffer_data[0]
#         L.debug(f"LIVE DATA KEYS: {buffer_data.keys()}")
#         incoming_varset_id = buffer_data.get("attributes", {}).get("variablesetfullid", {})

#         if incoming_varset_id:
#             new_row_data_list = []
            
#             for row_data, col_defs, table_id in zip(row_data_list, col_defs_list, table_ids):
#                 # ONLY process data if this table belongs to this dataset
#                 if table_id["index"] == incoming_varset_id:
#                     data = {}
#                     for col in col_defs:
#                         name = col["field"]
#                         if name in buffer_data.get("variables", {}):
#                             data[name] = buffer_data["variables"][name]["data"]
#                         else:
#                             data[name] = ""

#                     if len(row_data) > 30:
#                         new_row_data_list.append(row_data[:30])
#                     else:
#                         new_row_data_list.append(row_data)
#                 else:
#                     # Table doesn't match; keep existing data untouched
#                     new_row_data_list.append(row_data)

#             return new_row_data_list
        
#         else:
#             raise PreventUpdate
    
#     except Exception as e:
#         print(f"data update error table: {e}")
#         L.error(traceback.format_exc())
#         raise PreventUpdate



@callback(
    Output(
        {"type": "data-table-1d", "index": ALL}, "rowData"
    ),
    Input("variableset-data-buffer", "data"),
    [
        State({"type": "data-table-1d", "index": ALL}, "rowData"),
        State({"type": "data-table-1d", "index": ALL}, "columnDefs"),
        State({"type": "data-table-1d", "index": ALL}, "id")
    ],
)
def update_table_1d(buffer_data, row_data_list, col_defs_list, table_ids):
    if not buffer_data:
        raise PreventUpdate
    
    try:
        # buffer_data = buffer_data[0]
        L.debug(f"initial buffer data {buffer_data}")
        id_dict = buffer_data[0].get("variablesetfullid", {})
        L.debug(f"id_dict {id_dict}")
        buffer_data = buffer_data[0].get("data-update", {})
        L.debug(f"buffer_data table {buffer_data}")
        
        # --- DEBUG 1: See what the top level looks like ---
        L.debug(f"LIVE DATA KEYS: {buffer_data.keys()}")
        L.debug(f"ATTRIBUTE KEYS: {buffer_data.get('attributes', {}).keys()}")
        
        # FIX 1: Drill all the way down to the actual string, safely!
        # id_dict = buffer_data.get("attributes", {}).get("variablesetfullid", {})
        
        # If it's a dictionary, grab the 'data' key. If it's just a string, keep it.
        if isinstance(id_dict, dict):
            incoming_varset_id = id_dict.get("data")
        else:
            incoming_varset_id = id_dict

        # --- DEBUG 2: Verify we extracted a string like "raz1::main" ---
        L.debug(f"EXTRACTED INCOMING ID: '{incoming_varset_id}'")

        if incoming_varset_id:
            new_row_data_list = []
            
            for row_data, col_defs, table_id in zip(row_data_list, col_defs_list, table_ids):
                
                # --- DEBUG 3: Watch the routing logic ---
                L.debug(f"Comparing Table '{table_id['index']}' to Incoming '{incoming_varset_id}'")
                
                if str(table_id["index"]) == str(incoming_varset_id):
                    
                    # FIX 2: Safely extract variables to prevent the KeyError crash
                    variables = buffer_data.get("variables", {})
                    L.debug(f"EXTRACTED VARIABLES: '{variables}'")
                    
                    data = {}
                    for col in col_defs:
                        L.debug(f"col: '{col}'")
                        name = col["field"]

                        if name == "time":
                            # 1st attempt: Look inside 'variables' (where your previous log showed it)
                            t_val = variables.get("time", {}).get("data")
                            
                            # 2nd attempt: Look at the root level (just in case the backend moves it)
                            if not t_val:
                                t_val = buffer_data.get("time", {}).get("data", "")
                                
                            data[name] = t_val
                            continue

                        if name in variables:
                            # Safely extract the data point
                            data[name] = variables[name].get("data", "")
                        else:
                            data[name] = ""

                    # --- DEBUG 4: Verify the row was built successfully ---
                    L.debug(f"BUILT NEW ROW: {data}")

                    # FIX 3: Actually insert the new row into the table!
                    row_data.insert(0, data)

                    if len(row_data) > 30:
                        new_row_data_list.append(row_data[:30])
                    else:
                        new_row_data_list.append(row_data)
                else:
                    new_row_data_list.append(row_data)

            return new_row_data_list
        
        else:
            return [dash.no_update] * len(table_ids)
    
    except Exception as e:
        L.error(f"data update error table: {e}")
        L.error(traceback.format_exc())
        return [dash.no_update] * len(table_ids)


# @callback(
#     Output(
#         {"type": "data-table-2d", "index": ALL}, "rowData"
#     ),  # , Output("active-sensor-changes", "data")],
#     Input("sensor-data-buffer", "data"),
#     [
#         State({"type": "data-table-2d", "index": ALL}, "rowData"),
#         State({"type": "data-table-2d", "index": ALL}, "columnDefs"),
#     ],  # , dcc.Store("sensor-definition", "data")],
# )
# def update_table_2d(sensor_data, row_data_list, col_defs_list):  # , sensor_definition):
#     if sensor_data:
#         new_row_data_list = []
#         try:
#             for col_defs in col_defs_list:
#                 dim_2d = col_defs[0]["field"]
#                 # row_data = [{}]*len(sensor_data["variables"][dim_2d]["data"])
#                 row_data = []
#                 for index in range(0, len(sensor_data["variables"][dim_2d]["data"])):
#                     data = {}
#                     for col in col_defs:
#                         name = col["field"]
#                         data[name] = sensor_data["variables"][name]["data"][index]
#                     row_data.append(data)
#                 # print(f"row_data: {row_data}")
#                 new_row_data_list.append(row_data)
#             return new_row_data_list

    
#         except Exception as e:
#             print(f"data update error: {e}")
#             # return dash.no_update
#         raise PreventUpdate
#         # return [dash.no_update for i in range(0,len(col_defs_list))]
#         # return row_data
#     else:
#         # return dash.no_update
#         raise PreventUpdate
    

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


# @callback(
#     Output("ws-sensor-instance", "send"), Input("ws-send-instance-buffer", "children")
# )
# def send_to_instance(value):
#     print(f"sending: {value}")
#     return value