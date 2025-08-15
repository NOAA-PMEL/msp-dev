# from datetime import datetime, timezone
# import json
# import logging
# import dash
# import plotly.express as px
# import plotly.graph_objs as go
# from dash import (
#     html,
#     callback,
#     dcc,
#     Input,
#     Output,
#     dash_table,
#     State,
#     MATCH,
#     ALL,
#     Patch,
# )
# from dash.exceptions import PreventUpdate
# import dash_bootstrap_components as dbc
# from dash_extensions import WebSocket
# from pydantic import BaseSettings
# from ulid import ULID
# import dash_ag_grid as dag
# import pandas as pd
# from logfmter import Logfmter
# import traceback
# from envds.daq.event import DAQEvent

# # import pymongo
# from collections import deque
# import httpx

# handler = logging.StreamHandler()
# handler.setFormatter(Logfmter())
# logging.basicConfig(handlers=[handler])
# L = logging.getLogger(__name__)
# L.setLevel(logging.DEBUG)

# dash.register_page(
#     __name__,
#     path_template="/operational/<operational_id>",
#     title="Operational Devices",  # , prevent_initial_callbacks=True
# )


# class Settings(BaseSettings):
#     # host: str = "0.0.0.0"
#     # port: int = 8787
#     # debug: bool = False
#     daq_id: str = "default"
#     ws_hostname: str = "localhost:8080"
#     knative_broker: str = (
#         "http://kafka-broker-ingress.knative-eventing.svc.cluster.local/default/default"
#     )
#     # mongodb_data_user_name: str = ""
#     # mongodb_data_user_password: str = ""
#     # mongodb_registry_user_name: str = ""
#     # mongodb_registry_user_password: str = ""
#     # mongodb_data_connection: str = (
#     #     "mongodb://uasdaq:password@uasdaq-mongodb-0.uasdaq-mongodb-svc.mongodb.svc.cluster.local:27017,uasdaq-mongodb-1.uasdaq-mongodb-svc.mongodb.svc.cluster.local:27017,uasdaq-mongodb-2.uasdaq-mongodb-svc.mongodb.svc.cluster.local:27017/data?replicaSet=uasdaq-mongodb&ssl=false"
#     # )
#     # mongodb_registry_connection: str = (
#     #     "mongodb://uasdaq:password@uasdaq-mongodb-0.uasdaq-mongodb-svc.mongodb.svc.cluster.local:27017,uasdaq-mongodb-1.uasdaq-mongodb-svc.mongodb.svc.cluster.local:27017,uasdaq-mongodb-2.uasdaq-mongodb-svc.mongodb.svc.cluster.local:27017/registry?replicaSet=uasdaq-mongodb&ssl=false"
#     # )
#     # erddap_http_connection: str = (
#     #     "http://uasdaq.pmel.noaa.gov/uasdaq/dataserver/erddap"
#     # )
#     # erddap_https_connection: str = (
#     #     "https://uasdaq.pmel.noaa.gov/uasdaq/dataserver/erddap"
#     # )
#     # erddap_author: str = "fake_author"

#     dry_run: bool = False

#     class Config:
#         env_prefix = "DASHBOARD_"
#         case_sensitive = False


# config = Settings()

# # TODO: add readOnly user for this connection

# # combine secrets to get complete connection string
# # if "<username>" in config.mongodb_data_connection:
# #     mongodb_data_conn = config.mongodb_data_connection.replace(
# #         "<username>", config.mongodb_data_user_name
# #     )
# #     config = config.copy(update={"mongodb_data_connection": mongodb_data_conn})

# # if "<password>" in config.mongodb_data_connection:
# #     mongodb_data_conn = config.mongodb_data_connection.replace(
# #         "<password>", config.mongodb_data_user_password
# #     )
# #     config = config.copy(update={"mongodb_data_connection": mongodb_data_conn})

# # if "<username>" in config.mongodb_registry_connection:
# #     mongodb_registry_conn = config.mongodb_registry_connection.replace(
# #         "<username>", config.mongodb_registry_user_name
# #     )
# #     config = config.copy(update={"mongodb_registry_connection": mongodb_registry_conn})

# # if "<password>" in config.mongodb_registry_connection:
# #     mongodb_registry_conn = config.mongodb_registry_connection.replace(
# #         "<password>", config.mongodb_registry_user_password
# #     )
# #     config = config.copy(update={"mongodb_registry_connection": mongodb_registry_conn})

# # db_client = pymongo.MongoClient(
# # # self.client = AsyncIOMotorClient(
# #     config.mongodb_registry_connection,
# #     # connect=True,
# #     # tls=True,
# #     # tlsAllowInvalidCertificates=True
# # )
# # print(db_client)


# # class DBClient:
# #     def __init__(self, connection: str, db_type: str = "mongodb") -> None:
# #         self.db_type = db_type
# #         self.client = None
# #         self.connection = connection

# #     def connect(self):
# #         if self.db_type == "mongodb":
# #             self.connect_mongo()
# #         # return self.client

# #     def connect_mongo(self):
# #         if not self.client:
# #             try:
# #                 self.client = pymongo.MongoClient(
# #                     # self.client = AsyncIOMotorClient(
# #                     self.connection,
# #                     # connect=True,
# #                     # tls=True,
# #                     # tlsAllowInvalidCertificates=True
# #                 )
# #             except pymongo.errors.ConnectionError:
# #                 self.client = None
# #             # L.info("mongo client", extra={"connection": self.connection, "client": self.client})
# #             # L.info(await self.client.server_info())
# #         # return self.client

# #     def get_db(self, database: str):
# #         self.connect()
# #         if self.client:
# #             return self.client[database]
# #         return None

# #     def get_collection(self, database: str, collection: str):
# #         # L.info("get_collection")
# #         db = self.get_db(database)
# #         # L.info(f"get_collection:db = {db}")
# #         if db is not None:
# #             try:
# #                 db_coll = db[collection]
# #                 # L.info(f"get_collection:db:collection = {db_coll}")
# #                 return db_coll
# #             except Exception as e:
# #                 print(f"get_collection error: {e}")
# #         return None

# #     def find_one(self, database: str, collection: str, query: dict):
# #         self.connect()
# #         if self.client:
# #             # db = self.client[database]
# #             # db_collection = db[collection]
# #             db_collection = self.get_collection(
# #                 database=database, collection=collection
# #             )
# #             result = db_collection.find_one(query)
# #             if result:
# #                 update = {"last_update": datetime.now(tz=timezone.utc)}
# #                 self.client.update_one(database, collection, result, update)
# #             return result
# #         return None

# #     def find(
# #         self, database: str, collection: str, query: dict, sort=None, refresh=True
# #     ):
# #         self.connect()
# #         if self.client:
# #             # db = self.client[database]
# #             # db_collection = db[collection]
# #             db_collection = self.get_collection(
# #                 database=database, collection=collection
# #             )
# #             if sort:
# #                 find_result = db_collection.find(query).sort(sort)
# #             else:
# #                 find_result = db_collection.find(query)
# #             # print(f"find result: {result}")
# #             result = []
# #             for r in find_result:
# #                 result.append(r)
# #                 # print(f"r: {r}")

# #             if result and refresh:
# #                 for r in result:
# #                     # print(f"r: {r}")
# #                     update = {"last_update": datetime.now(tz=timezone.utc)}
# #                     self.update_one(database, collection, r, update)

# #             return result
# #         return None

# #     def insert_one(self, database: str, collection: str, document: dict):
# #         self.connect()
# #         if self.client:
# #             db = self.client[database]
# #             sensor_defs = db[collection]
# #             result = sensor_defs.insert_one(document)
# #             return result
# #         return None

# #     def update_one(
# #         self,
# #         database: str,
# #         collection: str,
# #         document: dict,
# #         update: dict,
# #         filter: dict = None,
# #         upsert=False,
# #     ):
# #         self.connect()
# #         if self.client:
# #             db = self.client[database]
# #             sensor = db[collection]
# #             if filter is None:
# #                 filter = document
# #             set_update = {"$set": update}
# #             if upsert:
# #                 set_update["$setOnInsert"] = document
# #             result = sensor.update_one(filter=filter, update=set_update, upsert=upsert)
# #             return result
# #         return None


# # db_data_client = DBClient(connection=config.mongodb_data_connection)
# # db_registry_client = DBClient(connection=config.mongodb_registry_connection)

# datastore_url = f"datastore.{config.daq_id}-system"
# link_url_base = f"http://{config.ws_hostname}/msp/dashboardtest"

# # websocket = WebSocket(
# #     id="ws-sensor", url=f"ws://uasdaq.pmel.noaa.gov/uasdaq/dashboard/ws/sensor/main"
# # )
# ws_send_buffer = html.Div(id="ws-send-instance-buffer", style={"display": "none"})


# def build_tables(layout_options):

#     table_list = []
#     print(f"build_tables: {layout_options}")
#     for ltype, dims in layout_options.items():
#         for dim, options in dims.items():
#             title = "Data"

#             if ltype == "layout-settings":
#                 title = f"Operational Device Settings"
#                 table_list.append(
#                     dbc.AccordionItem(
#                         [
#                             dag.AgGrid(
#                                 id={"type": "settings-table", "index": dim},
#                                 rowData=[],
#                                 columnDefs=layout_options["layout-settings"][dim]["table-column-defs"],
#                                 columnSizeOptions="autoSize",  # "autoSize", "autoSizeSkip", "sizeToFit", "responsiveSizeToFit"
#                             )
#                         ],
#                         title=title,
#                     )
#                 )

#             elif ltype == "layout-1d":
#                 title = f"Data 1-D ({dim})"
#                 # TODO: make the ids work for multiple dims
#                 table_list.append(
#                     dbc.AccordionItem(
#                         [
#                             dag.AgGrid(
#                                 id={"type": "operational-data-table-1d", "index": dim},
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
#                                 id={
#                                     "type": "operational-data-table-2d",
#                                     "index": f"time::{dim}",
#                                 },
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


# # def build_graph_1d(dropdown_list):
# def build_graph_1d(dropdown_list, xaxis="time", yaxis=""):

#     graph = dbc.Card(
#         children=[
#             dbc.CardHeader(
#                 children=[
#                     dcc.Dropdown(
#                         id={"type": "operational-graph-1d-dropdown", "index": xaxis},
#                         options=dropdown_list,
#                         value="",
#                     )
#                 ]
#             ),
#             dcc.Graph(
#                 id={"type": "operational-graph-1d", "index": xaxis},
#                 figure=go.Figure(
#                     data=go.Scatter(x=[], y=[], type="scatter")
#                     # {
#                     #     "x": [],
#                     #     "y": [],
#                     #     "type": "scatter",
#                     # }
#                 ),
#                 style={"height": 300},
#             ),
#         ]
#     )

#     return graph


# def build_graph_2d(dropdown_list, xaxis="time", yaxis="", zaxis=""):

#     content = dbc.Row(
#         children=[
#             dbc.Button(
#                 "Submit",
#                 {
#                     "type": "operational-graph-2d-z-axis-submit",
#                     "index": f"{xaxis}::{yaxis}",
#                 },
#             ),
#             dbc.Label("z-axis min:"),
#             dbc.Col(
#                 dbc.Input(
#                     type="number",
#                     id={
#                         "type": "operational-graph-2d-z-axis-min",
#                         "index": f"{xaxis}::{yaxis}",
#                     },
#                 )
#             ),
#             dbc.Label("z-axis max:"),
#             dbc.Col(
#                 dbc.Input(
#                     type="number",
#                     id={
#                         "type": "operational-graph-2d-z-axis-max",
#                         "index": f"{xaxis}::{yaxis}",
#                     },
#                 )
#             ),
#         ]
#     )

#     # axes_collapse = html.Div(
#     #     [

#     #         dbc.Button(
#     #             "Axes Settings",
#     #             id={"type": "graph-2d-axes-settings", "index": f"{xaxis}::{yaxis}"},
#     #             className="mb-3",
#     #             # color="primary",
#     #             n_clicks=0,
#     #         ),
#     #         dbc.Collapse(
#     #             dbc.Card(children=[content]),
#     #             id={"type": "graph-2d-axes-collapse", "index": f"{xaxis}::{yaxis}"},
#     #             is_open=False,
#     #         ),
#     #     ]
#     # )

#     axes_settings = dbc.Accordion(
#         children=[
#             dbc.AccordionItem(
#                 [dbc.Card(children=[content])],
#                 title="Axes Settings",
#                 # start_collapsed=True
#             )
#         ],
#         start_collapsed=True,
#     )

#     graph = dbc.Card(
#         children=[
#             dbc.CardHeader(
#                 children=[
#                     dcc.Dropdown(
#                         id={
#                             "type": "operational-graph-2d-dropdown",
#                             "index": f"{xaxis}::{yaxis}",
#                         },
#                         options=dropdown_list,
#                         value="",
#                     )
#                 ]
#             ),
#             dbc.Row(
#                 children=[
#                     axes_settings,
#                     dbc.Col(
#                         dcc.Graph(
#                             id={
#                                 "type": "operational-graph-2d-heatmap",
#                                 "index": f"{xaxis}::{yaxis}",
#                             },
#                             # figure=go.Figure(
#                             #     data=go.Heatmap(x=[], y=[], z=[], type="heatmap")
#                             # ),
#                             # figure=[
#                             #     {
#                             #         "x": [],
#                             #         "y": [],
#                             #         "type": "scatter",
#                             #     }
#                             #     }
#                             # ],
#                             style={"height": 500},
#                         )
#                     ),
#                     dbc.Col(
#                         dcc.Graph(
#                             id={
#                                 "type": "operational-graph-2d-line",
#                                 "index": f"{xaxis}::{yaxis}",
#                             },
#                             # figure=go.Figure(
#                             #     data=go.Line(x=[], y=[], type="line")
#                             # ),
#                             # figure=[
#                             #     {
#                             #         "x": [],
#                             #         "y": [],
#                             #         "type": "scatter",
#                             #     }
#                             # ],
#                             style={"height": 500},
#                         )
#                     ),
#                 ]
#             ),
#             # dbc.Col(children=[
#             #     dcc.Graph(
#             #         id={"type": "graph-2d-heatmap", "index": f"{xaxis}::{yaxis}"},
#             #         # figure=go.Figure(
#             #         #     data=go.Heatmap(x=[], y=[], z=[], type="heatmap")
#             #         # ),
#             #         # figure=[
#             #         #     {
#             #         #         "x": [],
#             #         #         "y": [],
#             #         #         "type": "scatter",
#             #         #     }
#             #         #     }
#             #         # ],
#             #         style={"height": 600},
#             #     ),
#             #     dcc.Graph(
#             #         id={"type": "graph-2d-line", "index": f"{xaxis}::{yaxis}"},
#             #         # figure=go.Figure(
#             #         #     data=go.Line(x=[], y=[], type="line")
#             #         # ),
#             #         # figure=[
#             #         #     {
#             #         #         "x": [],
#             #         #         "y": [],
#             #         #         "type": "scatter",
#             #         #     }
#             #         # ],
#             #         style={"height": 600},
#             #     )
#             # ])
#             # dcc.Graph(
#             #     id="graph-2d",
#             #     figure=go.Figure(
#             #         data=go.Heatmap(x=[], y=[], z=[], type="heatmap")
#             #     ),
#             #     # figure=[
#             #     #     {
#             #     #         "x": [],
#             #     #         "y": [],
#             #     #         "type": "scatter",
#             #     #     }
#             #     # ],
#             #     style={"height": 300},
#             # ),
#         ]
#     )

#     return graph


# def build_graph_settings_2d():

#     collapse = html.Div(
#         [
#             dbc.Button(
#                 "Graph Settings",
#                 id="collapse-button",
#                 className="mb-3",
#                 color="primary",
#                 n_clicks=0,
#             ),
#             dbc.Collapse(
#                 dbc.Card(dbc.CardBody("This content is hidden in the collapse")),
#                 id="collapse",
#                 is_open=False,
#             ),
#         ]
#     )


# def build_graphs(layout_options):

#     graph_list = []
#     print(f"build_graphs: {layout_options}")
#     for ltype, dims in layout_options.items():
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
#                 title = f"Data 2-D (time, {dim})"
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

#     print(f"build_tables: {graph_list}")

#     return graph_list


# def get_operational_data(operational_id: str):

#     query = {"operational_id": operational_id}
#     url = f"http://{datastore_url}/operational/data/get/"
#     print(f"operational-data-get: {url}, query: {query}")
#     try:
#         response = httpx.get(url, params=query)
#         results = response.json()
#         # print(f"results: {results}")
#         if "results" in results and results["results"]:
#             return results["results"]
#     except Exception as e:
#         L.error("get_operational_data", extra={"reason": e})
#     return []


# def get_operational_instance(operational_id: str, device_type: str = "operational"):

#     query = {"operational_id": operational_id}
#     url = f"http://{datastore_url}/operational-instance/registry/get/"
#     L.debug("get_operational_instance", extra={"url": url, "query": query})
#     try:
#         response = httpx.get(url, params=query)
#         results = response.json()
#         # print(f"controller_instance results: {results}")
#         L.debug("get_operational_instance", extra={"results": results})
#         if "results" in results and results["results"]:
#             return results["results"][0]
#     except Exception as e:
#         L.error("get_operational_instance", extra={"reason": e})

#     return {}


# def get_operational_definition_by_id(operational_id: str):

#     device = get_operational_instance(operational_id=operational_id)
#     if device:
#         try:
#             operational_definition_id = "::".join(
#                 [device["make"], device["model"], device["version"]]
#             )
#             print(f"operational_definition_id: {operational_definition_id}")
#             return get_operational_definition(
#                 operational_definition_id=operational_definition_id
#             )

#         except Exception as e:
#             print("ERROR: get_operational_definition_by_id", extra={"reason": e})

#     return {}


# def get_operational_definition(operational_definition_id: str):

#     query = {"operational_definition_id": operational_definition_id}
#     url = f"http://{datastore_url}/operational-definition/registry/get/"
#     print(f"operational-definition-get: {url}")
#     try:
#         response = httpx.get(url, params=query)
#         results = response.json()
#         print(f"operational_definition results: {results}")
#         if "results" in results and results["results"]:
#             return results["results"][0]
#     except Exception as e:
#         L.error("get_operational_definition", extra={"reason": e})

#         return {}


# def layout(operational_id=None):
#     print(f"get_layout: {operational_id}")
#     operational_definition = None
#     if operational_id:
#         parts = operational_id.split("::")
#         # print(f"get_layout: {parts}")
#         operational_meta = {
#             "operational_id": operational_id,
#             "make": parts[0],
#             "model": parts[1],
#             "serial_number": parts[2],
#         }
#         # print(f"get_layout: {sensor_meta}")
#         # query = {"make": sensor_meta["make"], "model": sensor_meta["model"]}
#         # response = httpx.get(f"http://{datastore_url}/device-definition/registry/get", params=query)
#         # print(f"response: {response}")
#         # # print(f"get_layout: {query}")

#         # # TODO: replace with datastore call
#         # results = []
#         # # results = db_registry_client.find(
#         # #     database="registry", collection="sensor_definition", query=query
#         # # )
#         # if len(results) > 0:
#         #     sensor_definition = results[0]
#         #     if len(results) > 1:
#         #         for sdef in results[1:]:
#         #             try:
#         #                 if sdef["version"] > sensor_definition["version"]:
#         #                     sensor_definition = sdef
#         #             except KeyError:
#         #                 pass

#         operational_definition = get_operational_definition_by_id(
#             operational_id=operational_id
#         )
#         print(f"operational_definition: {operational_definition}")
#         # else:
#         #     sensor_definition = None

#         # print(f"sensor def: {sdef}")
#     else:
#         # sensor_id = "AerosolDynamics::MAGIC250::142"
#         # parts = sensor_id.split("::")
#         operational_meta = {}
#         operational_definition = {}

#     layout_options = {
#         "layout-settings": {"time": {"table-column-defs": [], "variable-list": []}},
#         "layout-1d": {"time": {"table-column-defs": [], "variable-list": []}},
#     }
#     column_defs_1d = []  # deque([], maxlen=10)
#     dropdown_list_1d = []
#     column_defs_1d = []  # deque([], maxlen=10)
#     dropdown_list_2d = []
#     if operational_definition:
#         try:
#             dimensions = operational_definition["dimensions"]
#             is_2d = False
#             if len(dimensions.keys()) > 1:
#                 is_2d = True
#                 if "layout-2d" not in layout_options:
#                     layout_options["layout-2d"] = (
#                         {}
#                     )  # {dim_2d: {"table-column-defs": [], "variable-list": []}}

#                 # for d in dimensions.keys():
#                 #     if d == "time":
#                 #         continue
#                 #     if "columns_2d" not in graph_options:
#                 #         graph_options["columns_2d"] = dict()
#                 #     graph_options["columns_2d"] = {d:[]}

#             print(f"layout: {layout_options}")
#             if is_2d:
#                 for d in dimensions.keys():
#                     if d == "time":
#                         cd = {
#                             "field": "time",
#                             "headerName": "Time",
#                             "filter": False,
#                             "cellDataType": "text",
#                             "pinned": "left",
#                         }
#                         layout_options["layout-1d"]["time"]["table-column-defs"].append(
#                             cd
#                         )

#                     # if d != "time":
#                     else:
#                         layout_options["layout-2d"][d] = {
#                             "table-column-defs": [],
#                             "variable-list": [],
#                         }
#                         ln = d
#                         try:
#                             ln = operational_definition["attributes"][d]["long_name"][
#                                 "data"
#                             ]
#                         except KeyError:
#                             pass

#                         print(f"layout: {ln}")
#                         data_type = "text"
#                         try:
#                             dtype = operational_definition["variables"][d]["type"]
#                             if dtype in ["float", "double", "int"]:
#                                 data_type = "number"
#                             elif dtype in ["str", "string", "char"]:
#                                 data_type = "text"
#                             elif dtype in ["bool"]:
#                                 data_type = "boolean"
#                         except KeyError:
#                             pass
#                         print(f"layout: {data_type}")

#                         cd = {
#                             "field": d,
#                             "headerName": ln,
#                             "filter": False,
#                             "cellDataType": data_type,
#                             "pinned": "left",
#                         }
#                         layout_options["layout-2d"][d]["table-column-defs"].append(cd)
#                         print(f"layout: {layout_options}")
#             else:
#                 if "time" in dimensions:
#                     # column_defs_1d.append(
#                     cd = {
#                         "field": "time",
#                         "headerName": "Time",
#                         "filter": False,
#                         "cellDataType": "text",
#                         "pinned": "left",
#                     }
#                     layout_options["layout-1d"]["time"]["table-column-defs"].append(cd)
#                     print(f"layout: {layout_options}")
            
#             # make settings table
#             for name, var in operational_definition["variables"].items():
#                 if var["attributes"]["variable_type"]["data"] == "setting":
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

#                     cd = {
#                         "field": name,
#                         "headerName": long_name,
#                         "filter": False,
#                         "cellDataType": data_type,
#                         # "cellRenderer": "DBC_Switch",
#                     }
#                     layout_options["layout-settings"]["time"]["table-column-defs"].append(cd)

#             for name, var in operational_definition["variables"].items():
#                 # only get the data variables for main
#                 if var["attributes"]["variable_type"]["data"] != "main":
#                     continue
#                 if name in dimensions:
#                     continue
#                 if "shape" not in var:
#                     continue
#                 if "time" not in var["shape"]:
#                     continue
#                 # if len(var["shape"]) > 1 or "time" not in var["shape"]:
#                 #     continue

#                 long_name = name
#                 ln = var["attributes"].get("long_name", None)
#                 if ln:
#                     long_name = ln.get("data", name)

#                 # get data type
#                 dtype = var.get("type", "unknown")
#                 print(f"dtype = {dtype}")
#                 data_type = "text"
#                 if dtype in ["float", "double", "int"]:
#                     data_type = "number"
#                 elif dtype in ["str", "string", "char"]:
#                     data_type = "text"
#                 elif dtype in ["bool"]:
#                     data_type = "boolean"

#                 # column_defs_1d.append(
#                 cd = {
#                     "field": name,
#                     "headerName": long_name,
#                     "filter": False,
#                     "cellDataType": data_type,
#                 }
#                 # )

#                 if is_2d and len(var["shape"]) == 2:
#                     dim_2d = [d for d in var["shape"] if d != "time"][0]
#                     layout_options["layout-2d"][dim_2d]["table-column-defs"].append(cd)
#                 else:
#                     layout_options["layout-1d"]["time"]["table-column-defs"].append(cd)

#             for ltype, dims in layout_options.items():
#                 # for gtype, options in layout_options.items():
#                 for dim, options in dims.items():
#                     for cd in options["table-column-defs"]:
#                         if cd["field"] in dimensions:
#                             continue
#                         if cd["cellDataType"] != "number":
#                             continue
#                         layout_options[ltype][dim]["variable-list"].append(
#                             {"label": cd["field"], "value": cd["field"]}
#                         )

#             # # dropdown_list = []
#             # for cd in column_defs_1d:
#             #     if cd["field"] == "time":
#             #         continue
#             #     if cd["cellDataType"] != "number":
#             #         continue
#             #     dropdown_list_1d.append({"label": cd["field"], "value": cd["field"]})

#         except KeyError as e:
#             print(f"build column_defs error: {e}")

#     # print("here:1")
#     layout = html.Div(
#         [
#             # html.Div(id="dbc-switch-value-changed"),
#             html.H1(f"Operational Device: {operational_id}"),
#             # get_button,
#             # build_tables(layout_options)
#             dbc.Accordion(
#                 build_tables(layout_options),
#                 # [
#                 #     dbc.AccordionItem(
#                 #         [
#                 #             dag.AgGrid(
#                 #                 id="controller-data-table-1d",
#                 #                 rowData=[],
#                 #                 columnDefs=layout_options["layout-1d"]["time"]["table-column-defs"],
#                 #                 columnSizeOptions="autoSize",  # "autoSize", "autoSizeSkip", "sizeToFit", "responsiveSizeToFit"
#                 #             )
#                 #         ],
#                 #         title="Data: 1-D",
#                 #     ),
#                 #     # dbc.AccordionItem(
#                 #     #     [
#                 #     #         dag.AgGrid(
#                 #     #             id="active-sensor-table",
#                 #     #             rowData=[],
#                 #     #             columnDefs=[
#                 #     #                 {
#                 #     #                     "field": "make",
#                 #     #                     "headerName": "Make/Mfg",
#                 #     #                     "filter": True,
#                 #     #                 },
#                 #     #                 {
#                 #     #                     "field": "model",
#                 #     #                     "headerName": "Model",
#                 #     #                     "filter": True,
#                 #     #                 },
#                 #     #                 {
#                 #     #                     "field": "serial_number",
#                 #     #                     "headerName": "Serial Number",
#                 #     #                     "filter": True,
#                 #     #                 },
#                 #     #                 {
#                 #     #                     "field": "sampling_sytem",
#                 #     #                     "headerName": "Sampling System",
#                 #     #                     "filter": True,
#                 #     #                 },
#                 #     #             ],
#                 #     #         )
#                 #     #     ],
#                 #     #     title="Active Sensors",
#                 #     # ),
#                 # ],
#                 id="operational-data-accordion",
#             ),
#             dbc.Accordion(
#                 build_graphs(layout_options),
#                 # [
#                 #     dbc.AccordionItem(
#                 #         [
#                 #             dbc.Row(
#                 #                 children=[
#                 #                     build_graph_1d(
#                 #                         layout_options["layout-1d"]["time"][
#                 #                             "variable-list"
#                 #                         ]
#                 #                     )
#                 #                     # dbc.Card(
#                 #                     #     children=[
#                 #                     #         dbc.CardHeader(
#                 #                     #             children=[
#                 #                     #                 dcc.Dropdown(
#                 #                     #                     id="controller-graph-1d-dropdown",
#                 #                     #                     options=dropdown_list_1d,
#                 #                     #                     value=""
#                 #                     #                 )
#                 #                     #             ]
#                 #                     #         ),
#                 #                     #         dcc.Graph(
#                 #                     #             id="graph-1d",
#                 #                     #             figure=[
#                 #                     #                 {
#                 #                     #                     "x": [],
#                 #                     #                     "y": [],
#                 #                     #                     "type": "scatter",
#                 #                     #                 }
#                 #                     #             ],
#                 #                     #             style={"height": 300},
#                 #                     #         ),
#                 #                     #     ]
#                 #                     # )
#                 #                 ]
#                 #             )
#                 #         ],
#                 #         title="Data: 1-D",
#                 #     ),
#                 #     # dbc.AccordionItem(
#                 #     #     [
#                 #     #         dbc.Row(
#                 #     #             children=[
#                 #     #                 build_graph_2d(graph_options["layout-2d"]["variable-list"])
#                 #     #             ]
#                 #     #         )
#                 #     #     ],
#                 #     #     title="Data: 1-D",
#                 #     # ),
#                 # ],
#                 id="operational-plot-accordion",
#             ),
#             WebSocket(
#                 id="ws-operational-instance",
#                 # url=f"ws://uasdaq.pmel.noaa.gov/uasdaq/dashboard/ws/sensor/{sensor_id}",
#                 # url=f"ws://uasdaq.pmel.noaa.gov/uasdaq/dashboard/ws/sensor/{sensor_id}",
#                 # url=f"wss://k8s.pmel-dev.oarcloud.noaa.gov:443/uasdaq/dashboard/ws/sensor/{sensor_id}"
#                 # url=f"ws://mspbase01:8080/msp/dashboardtest/ws/sensor/{sensor_id}"
#                 url=f"ws://{config.ws_hostname}/msp/dashboardtest/ws/operational/{operational_id}",
#             ),
#             ws_send_buffer,
#             dcc.Store(id="operational-definition", data=operational_definition),
#             dcc.Store(id="operational-meta", data=operational_meta),
#             dcc.Store(id="operational-graph-axes", data={}),
#             dcc.Store(id="operational-data-buffer", data={}),
#             dcc.Store(id="operational-settings-buffer", data={}),
#             # dcc.Interval(id="test-interval", interval=(10*1000)),
#             # dcc.Interval(
#             #     id="table-update-interval", interval=(5 * 1000), n_intervals=0
#             # ),
#             # dcc.Interval(id="active-sensor-update-interval", interval=(5*1000), n_intervals=0),
#             # html.Div(id="test-output")
#         ]
#     )
#     # print("here:2")

#     request = {"client-request": "start-updates"}
#     # print("here:3")
#     # print(f"sent request: {request}")
#     # ws_send_buffer.children = json.dumps(request)
#     # print("here:4")
#     # websocket.send("test programatically")
#     return layout


# # layout = get_layout  # ()

# # @callback(
# #     Output("test-output", "children"),
# #     Input("test-interval", "n_intervals")
# # )
# # def test_read(num_intervals):

# #     docs = []
# #     db_registry_client.connect()
# #     if db_registry_client:
# #         sensor_def_registry = db_registry_client.get_collection("registry", "sensor_definition")
# #         for doc in sensor_def_registry.find().sort("_id"):
# #             if doc:
# #                 docs.append(doc)
# #             print(f"sensor defintion: {doc}")
# #         print(f"Number of docs: {len(docs)}, {docs}")
# #     return doc


# # @callback(
# #     Output("sensor-defs-table", "rowData"),
# #     Input("table-update-interval", "n_intervals"),
# #     State("sensor-defs-table", "rowData"),
# # )
# # def update_sensor_definitions(count, table_data):
# #     # print(f"sensor_def: {count}")
# #     update = False
# #     new_data = []
# #     try:
# #         db_registry_client.connect()
# #         if db_registry_client:
# #             sensor_def_registry = db_registry_client.get_collection(
# #                 "registry", "sensor_definition"
# #             )
# #             for doc in sensor_def_registry.find():
# #                 if doc is not None:
# #                     # print(f"doc: {doc}")
# #                     id = doc["_id"]
# #                     make = doc["make"]
# #                     model = doc["model"]
# #                     version = doc["version"]

# #                     sensor_def = {
# #                         "sensor-def-id": id,
# #                         "make": make,
# #                         "model": model,
# #                         "version": version,
# #                     }
# #                     if sensor_def not in table_data:
# #                         table_data.append(sensor_def)
# #                         update = True
# #                     new_data.append(sensor_def)

# #             remove_data = []
# #             for index, data in enumerate(table_data):
# #                 if data not in new_data:
# #                     update = True
# #                     remove_data.insert(0,index)
# #             for index in remove_data:
# #                 table_data.pop(index)

# #             if update:
# #                 return table_data
# #             else:
# #                 return dash.no_update

# #     except Exception as e:
# #         print(f"update_sensor_definitions error: {e}")
# #         return dash.no_update


# # @callback(
# #     Output("active-sensor-table", "rowData"),
# #     Input("table-update-interval", "n_intervals"),
# #     State("active-sensor-table", "rowData"),
# # )
# # def update_active_sensors(count, table_data):

# #     # print(f"active_sensor: {count}")
# #     update = False
# #     new_data = []
# #     try:
# #         db_registry_client.connect()
# #         if db_registry_client:
# #             # print(db_registry_client)
# #             sensor_registry = db_registry_client.get_collection("registry", "sensor")
# #             # print(sensor_registry)
# #             for doc in sensor_registry.find():
# #                 if doc is not None:
# #                     # print(f"doc: {doc}")
# #                     make = doc["make"]
# #                     model = doc["model"]
# #                     serial_number = doc["serial_number"]
# #                     version = doc["version"]
# #                     id = "::".join([make, model, serial_number])
# #                     sampling_system = "unknown"

# #                     sensor = {
# #                         "sensor-id": id,
# #                         "make": make,
# #                         "model": model,
# #                         "serial_number": serial_number,
# #                         "sampling_system": sampling_system,
# #                     }
# #                     if sensor not in table_data:
# #                         table_data.append(sensor)
# #                         update = True
# #                     new_data.append(sensor)

# #             remove_data = []
# #             for index, data in enumerate(table_data):
# #                 if data not in new_data:
# #                     update = True
# #                     remove_data.insert(0,index)
# #             for index in remove_data:
# #                 table_data.pop(index)

# #             if update:
# #                 return table_data
# #             else:
# #                 return dash.no_update

# #     except Exception as e:
# #         print(f"update_active_sensors error: {e}")
# #         return dash.no_update


# @callback(
#     # [Output({"type": "graph-1d", "index": MATCH}, "figure"), Output("graph-axes", "data")],
#     Output(
#         {"type": "operational-graph-1d", "index": MATCH}, "figure"
#     ),  # Output("graph-axes", "data")],
#     Input({"type": "operational-graph-1d-dropdown", "index": MATCH}, "value"),
#     [
#         State("operational-meta", "data"),
#         State("operational-graph-axes", "data"),
#         State("operational-definition", "data"),
#         State({"type": "operational-graph-1d-dropdown", "index": MATCH}, "id"),
#     ],
# )
# def select_graph_1d(
#     y_axis, operational_meta, graph_axes, operational_definition, graph_id
# ):
#     print(f"select_graph_1d: {y_axis}, {operational_meta}, {graph_axes}")
#     # print(f"current_fig: {current_fig}")
#     try:
#         if "operational-graph-1d" not in graph_axes:
#             graph_axes["operational-graph-1d"] = dict()
#         graph_axes["operational-graph-1d"][graph_id["index"]] = {
#             "x-axis": "time",
#             "y-axis": y_axis,
#         }
#         print(f"select_graph_1d: {graph_axes}")

#         x = []
#         y = []
#         query = {
#             "operational_id": operational_meta["operational_id"],
#             # "make": sensor_meta["make"],
#             # "model": sensor_meta["model"],
#             # "serial_number": sensor_meta["serial_number"],
#         }
#         # sort = {"variables.time.data": 1}
#         results = get_operational_data(operational_id=operational_meta["operational_id"])
#         # results = httpx.get(f"{datastore_url}/sensor/data/get", params=query)
#         # # results = db_data_client.find("data", "sensor", query, sort)
#         print(f"***results: {results}")
#         if results is None or len(results) == 0:
#             print("results = None")
#             # return [{"x": [], "y": [], "type": "scatter"}, graph_axes]
#             return {"x": [], "y": [], "type": "scatter"}  # , graph_axes]
#         if results and len(results) > 0:
#             print("results = good")
#             for doc in results:
#                 try:
#                     x.append(doc["variables"]["time"]["data"])
#                     y.append(doc["variables"][y_axis]["data"])
#                 except KeyError:
#                     continue

#         # print(f"x,y: {x}, {y}")
#         # # fig = go.Figure(data=[go.Scatter(x=x, y=y)])
#         # print(f"go fig: {fig}")
#         # fig = dict(data=[{'x': x, 'y': y}])
#         units = ""
#         try:
#             units = f'({operational_definition["variables"][y_axis]["attributes"]["units"]["data"]})'
#         except KeyError:
#             pass

#         # fig = {
#         #     "data": [{"x": x, "y": y, "type": "scatter"}],
#         #     "layout": {
#         #         "xaxis": {"title": "Time"},
#         #         "yaxis": {"title": f"{y_axis} {units}"},
#         #     },
#         # }
#         fig = go.Figure(
#             data=go.Scatter(x=x, y=y, type="scatter"),
#             layout={
#                 "xaxis": {"title": "Time"},
#                 "yaxis": {"title": f"{y_axis} {units}"},
#             },
#         )
#         # print(f"go fig: {fig}")
#         # return [fig, graph_axes]
#         return fig  # , graph_axes]
#     except Exception as e:
#         print(f"select_graph_1d error: {e}")
#         # return [dash.no_update, dash.no_update]
#         return dash.no_update  # , dash.no_update]


# # @callback(
# #     # [Output({"type": "graph-1d", "index": MATCH}, "figure"), Output("graph-axes", "data")],
# #     [
# #         Output(
# #             {"type": "graph-2d-heatmap", "index": MATCH}, "figure", allow_duplicate=True
# #         ),
# #         Output(
# #             {"type": "graph-2d-line", "index": MATCH}, "figure", allow_duplicate=True
# #         ),
# #     ],
# #     Input({"type": "graph-2d-dropdown", "index": MATCH}, "value"),
# #     [
# #         State("controller-meta", "data"),
# #         State("graph-axes", "data"),
# #         State("controller-definition", "data"),
# #         State({"type": "graph-2d-dropdown", "index": MATCH}, "id"),
# #     ],
# #     prevent_initial_call=True,
# # )
# # def select_graph_2d(z_axis, controller_meta, graph_axes, controller_definition, graph_id):
# #     print(f"select_graph_2d: {z_axis}, {controller_meta}, {graph_axes}, {graph_id}")
# #     # print(f"current_fig: {current_fig}")
# #     try:
# #         if "graph-2d" not in graph_axes:
# #             graph_axes["graph-2d"] = dict()
# #         y_axis = graph_id["index"].split("::")[1]
# #         use_log = False
# #         if y_axis == "diameter":
# #             use_log = True
# #         graph_axes["graph-2d"][graph_id["index"]] = {
# #             "x-axis": "time",
# #             "y-axis": y_axis,
# #             "z-axis": z_axis,
# #         }
# #         print(f"select_graph_2d: {graph_axes}")

# #         x = []
# #         y = []
# #         z = []
# #         orig_z = []
# #         query = {
# #             "make": controller_meta["make"],
# #             "model": controller_meta["model"],
# #             "serial_number": controller_meta["serial_number"],
# #         }
# #         sort = {"variables.time.data": 1}
# #         results = httpx.get(f"{datastore_url}/controller/data/get", params=query)
# #         # results = db_data_client.find("data", "sensor", query, sort, refresh=False)
# #         print(f"2d results: {results}")
# #         if results is None or len(results) == 0:
# #             print("results = None")
# #             # return [{"x": [], "y": [], "type": "scatter"}, graph_axes]
# #             # return {"x": [], "y": [], "type": "scatter"}#, graph_axes]
# #             raise PreventUpdate

# #         elif results and len(results) > 0:
# #             print("results = good")
# #             for doc in results:
# #                 try:
# #                     x.append(doc["variables"]["time"]["data"])
# #                     y.append(doc["variables"][y_axis]["data"])
# #                     orig_z.append(doc["variables"][z_axis]["data"])
# #                 except KeyError:
# #                     continue

# #         # print(f"x,y: {x}, {y}")
# #         # # fig = go.Figure(data=[go.Scatter(x=x, y=y)])
# #         # print(f"go fig: {fig}")
# #         # fig = dict(data=[{'x': x, 'y': y}])
# #         units = ""
# #         try:
# #             y_units = f'({controller_definition["variables"][y_axis]["attributes"]["units"]["data"]})'
# #             z_units = f'({controller_definition["variables"][z_axis]["attributes"]["units"]["data"]})'
# #         except KeyError:
# #             pass

# #         # fig = {
# #         #     "data": [{"x": x, "y": y, "type": "scatter"}],
# #         #     "layout": {
# #         #         "xaxis": {"title": "Time"},
# #         #         "yaxis": {"title": f"{y_axis} {units}"},
# #         #     },
# #         # }
# #         if isinstance(y[-1], list):
# #             y = y[-1]

# #         for yi, yval in enumerate(y):
# #             # z.append([])
# #             new_z = []
# #             for xi, xval in enumerate(x):
# #                 new_z.append(orig_z[xi][yi])
# #             z.append(new_z)

# #         heatmap = go.Figure(
# #             data=go.Heatmap(
# #                 x=x, y=y, z=z, type="heatmap", colorscale="Rainbow"
# #             ),
# #             # data=[{"x": x, "y": y, "z": z, "type": "heatmap"}],
# #             layout={
# #                 "xaxis": {"title": "Time"},
# #                 "yaxis": {"title": f"{y_axis} {y_units}"},
# #                 # "yaxis": {"title": f"{y_axis} {y_units}"},
# #                 # "colorscale": "rainbow"
# #             },
# #         )
# #         if use_log:
# #             heatmap.update_yaxes(type="log")
# #             heatmap.update_layout(coloraxis=dict(cmax=None, cmin=None))
# #         print(f"heatmap figure: {heatmap}")
# #         scatter = go.Figure(
# #             # data=go.Scatter(x=y, y=z[-1], type="scatter"),
# #             data=[{"x": y, "y": z[-1], "type": "scatter"}],
# #             layout={
# #                 "xaxis": {"title": f"{y_axis} {y_units}"},
# #                 "yaxis": {"title": f"{z_axis} {z_units}"},
# #                 "title": str(x[-1]),
# #                 # "yaxis": {"title": f"{y_axis} {y_units}"},
# #                 # "colorscale": "rainbow"
# #             },
# #         )
# #         if use_log:
# #             scatter.update_xaxes(type="log")
# #         print(f"scatter figure: {scatter}")

# #         # print(f"go fig: {fig}")
# #         # return [fig, graph_axes]
# #         return [heatmap, scatter]  # , graph_axes]
# #     except Exception as e:
# #         print(f"select_graph_2d error: {e}")
# #         # return [dash.no_update, dash.no_update]
# #         raise PreventUpdate
# #         # return [dash.no_update, dash.no_update]


# @callback(
#     [
#         Output("operational-data-buffer", "data"),
#         Output("operational-settings-buffer", "data"),
#     ],
#     Input("ws-operational-instance", "message"),
# )
# def update_operational_buffers(event):
#     if event is not None and "data" in event:
#         # print(f"update_controller_buffers: {event}")
#         event_data = json.loads(event["data"])
#         print(f"update_operational_buffers: {event_data}")
#         if "data-update" in event_data:
#             try:
#                 # msg = json.loads(event["data-update"])
#                 # print(f"update_controller_data: {event_data}")
#                 if event_data["data-update"]:
#                     return [event_data["data-update"], dash.no_update]
#             except Exception as event:
#                 print(f"data buffer update error: {event}")
            
#         elif "settings-update" in event_data:
#             try:
#                 # msg = json.loads(event["settings"])
#                 # print(f"update_controller_settings: {event_data}")
#                 if event_data["settings-update"]:
#                     return [dash.no_update,event_data["settings-update"]]
#             except Exception as e:
#                 print(f"settings buffer update error: {e}")
#             # return dash.no_update
#             # return dash.no_update
#     return [dash.no_update, dash.no_update]


# @callback(
#     Output({"type": "operational-graph-1d", "index": ALL}, "extendData"),
#     Input("operational-data-buffer", "data"),
#     [
#         State({"type": "operational-graph-1d-dropdown", "index": ALL}, "value"),
#         State("operational-graph-axes", "data"),
#         State({"type": "operational-graph-1d", "index": ALL}, "figure"),
#     ],
# )
# def update_graph_1d(operational_data, y_axis_list, graph_axes, current_figs):

#     # # may need this later with multiple plots but I think it will still loop through drop downs
#     # if "graph-1d" not in graph_axes:
#     #     return dash.no_update

#     # axes = graph_axes["graph-1d"]# = {"x-axis": "time", "y-axis": y_axis}
#     try:
#         figs = []
#         if operational_data:
#             print(f"operational_data: {operational_data}")
#             for y_axis in y_axis_list:
#                 if (
#                     "time" not in operational_data["variables"]
#                     or y_axis not in operational_data["variables"]
#                 ):
#                     return dash.no_update

#                 x = [operational_data["variables"]["time"]["data"]]
#                 y = [operational_data["variables"][y_axis]["data"]]
#                 print(f"update: {[x]}, {[y]}")
#                 figs.append({"x": [x], "y": [y]})
#             # return {"x": [x], "y": [y]}
#             return figs

#     except Exception as e:
#         print(f"data update error: {e}")
#         # return dash.no_update
#         # return dash.no_update
#     raise PreventUpdate
#     # return dash.no_update


# # @callback(
# #     # Output({"type": "graph-2d-heatmap", "index": ALL}, "extendData"),
# #     Output({"type": "graph-2d-heatmap", "index": ALL}, "figure", allow_duplicate=True),
# #     Input("controller-data-buffer", "data"),
# #     [
# #         State({"type": "graph-2d-dropdown", "index": ALL}, "value"),
# #         State("graph-axes", "data"),
# #         State("controller-definition", "data"),
# #         State({"type": "graph-2d-heatmap", "index": ALL}, "figure"),
# #         State({"type": "graph-2d-heatmap", "index": ALL}, "id"),
# #     ],
# #     prevent_initial_call=True,
# # )
# # def update_graph_2d_heatmap(
# #     controller_data, z_axis_list, graph_axes, controller_definition, current_figs, graph_ids
# # ):

# #     # # may need this later with multiple plots but I think it will still loop through drop downs
# #     # if "graph-1d" not in graph_axes:
# #     #     return dash.no_update

# #     # axes = graph_axes["graph-1d"]# = {"x-axis": "time", "y-axis": y_axis}
# #     try:
# #         heatmaps = []
# #         if controller_data:
# #             print(f"update_2d_heatmap: {z_axis_list}, {graph_ids}")
# #             for z_axis, graph_id, current_fig in zip(
# #                 z_axis_list, graph_ids, current_figs
# #             ):
# #                 y_axis = graph_id["index"].split("::")[1]
# #                 print(f"y_axis, z_axis: {y_axis}, {z_axis}")
# #                 if (
# #                     "time" not in controller_data["variables"]
# #                     or y_axis not in controller_data["variables"]
# #                     or z_axis not in controller_data["variables"]
# #                 ):
# #                     raise PreventUpdate
# #                     # heatmaps.append(dash.no_update)
# #                     # # scatters.append(dash.no_update)
# #                     # continue
# #                     # return dash.no_update

# #                 x = controller_data["variables"]["time"]["data"]
# #                 # print(f"current x, z: {x}, {current_fig['data'][0]['x']}, {current_fig['data'][0]['z']}")
# #                 # print(f"current x, y, z: {x}, {len(current_fig['data'][0]['x'])}, {len(current_fig['data'][0]['y'])}, {len(current_fig['data'][0]['z'])}")
# #                 # print(f"current x, y, z: {x}, {current_fig['data'][0]['x']}, {current_fig['data'][0]['y']}, {current_fig['data'][0]['z']}")

# #                 # print(f"current fig: {current_fig}")

# #                 if x in current_fig["data"][0]["x"]:
# #                     print("don't update")
# #                     raise PreventUpdate

# #                 print("start x,y,z")
# #                 if not isinstance(x, list):
# #                     x = [x]
# #                 print(f"x: {x}")

# #                 # work around until extendData works
# #                 new_x = current_fig["data"][0]["x"]
# #                 for nx in x:
# #                     current_fig["data"][0]["x"].append(nx)
# #                 print(f"new x: {new_x}")
# #                 y = current_fig["data"][0]["y"]
# #                 if len(y) == 0:
# #                     y = controller_data["variables"][y_axis]["data"]
# #                 if not isinstance(y, list):
# #                     y = [y]
# #                 print(f"y: {y}")
# #                 orig_z = controller_data["variables"][z_axis]["data"]
# #                 if not isinstance(orig_z, list):
# #                     orig_z = [orig_z]
# #                 print(f"update: {[x]}, {[y]}, {[orig_z]}")

# #                 # work around until extendData works
# #                 z = []
# #                 new_z = current_fig["data"][0]["z"]
# #                 if len(x) > 1:
# #                     for yi, yval in enumerate(y):
# #                         z.append([])
# #                         new_z = []
# #                         for xi, xval in enumerate(x):
# #                             new_z.append(orig_z[xi][yi])
# #                         z.append(new_z)
# #                 else:
# #                     for yi, yval in enumerate(y):
# #                         current_fig["data"][0]["z"][yi].append(orig_z[yi])
# #                         # z.append([])
# #                         # new_z = []
# #                         # for xi, xval in enumerate(x):
# #                         #     new_z.append(orig_z[xi][yi])
# #                         z.append([orig_z[yi]])

# #                 units = ""
# #                 try:
# #                     y_units = f'({controller_definition["variables"][y_axis]["attributes"]["units"]["data"]})'
# #                     z_units = f'({controller_definition["variables"][z_axis]["attributes"]["units"]["data"]})'
# #                 except KeyError:
# #                     pass

# #                 # patched_heatmap = Patch()
# #                 # print(f"patched heatmap: {patched_heatmap}")
# #                 # if x not in patched_heatmap["data"][0]["x"]:
# #                 #     patched_heatmap["data"][0]["x"].append(x)
# #                 #     patched_heatmap["data"][0]["z"].append(z)

# #                 print(f'change data: "x": {[x]}, "y": {[y]}, "z": {[z]}')
# #                 # heatmaps.append({"x": [x], "y": [y], "z": [[z]]})
# #                 # heatmaps.append({"x": [x], "y": [y], "z": [z]})

# #                 # if len(current_fig["data"][0]["y"]) == 0:
# #                 #     heatmaps.append({"x": [x], "y": [y], "z": [[z]]})
# #                 # else:
# #                 #     heatmaps.append({"x": [x], "z": [[z]]})

# #                 # heatmaps.append({"data": [{"x": new_x, "y": y, "z": new_z}]})

# #                 # heatmaps.append(patched_heatmap)
# #                 heatmaps.append(current_fig)
# #                 # new_fig = go.Figure(
# #                 #     data=go.Heatmap(x=new_x, y=y, z=new_z, type="heatmap"),
# #                 #     layout={
# #                 #         "xaxis": {"title": f"{y_axis} {y_units}"},
# #                 #         "yaxis": {"title": f"{z_axis} {z_units}"},
# #                 #         # "yaxis": {"title": f"{y_axis} {y_units}"},
# #                 #         # "colorscale": "rainbow"
# #                 #     }
# #                 # )

# #                 # heatmaps.append(
# #                 # )
# #             # return {"x": [x], "y": [y]}
# #             print(f"heatmaps: {heatmaps}")
# #             if len(heatmaps) == 0:
# #                 raise PreventUpdate
# #             return heatmaps

# #     except Exception as e:
# #         print(f"heatmap update error: {e}")
# #         # return dash.no_update
# #         # return dash.no_update
# #     raise PreventUpdate
# #     # return dash.no_update


# # @callback(
# #     Output({"type": "graph-2d-line", "index": ALL}, "figure"),
# #     Input("controller-data-buffer", "data"),
# #     [
# #         State({"type": "graph-2d-dropdown", "index": ALL}, "value"),
# #         State("graph-axes", "data"),
# #         State("controller-definition", "data"),
# #         State({"type": "graph-2d-line", "index": ALL}, "figure"),
# #         State({"type": "graph-2d-line", "index": ALL}, "id"),
# #     ],
# #     prevent_initial_call=True,
# # )
# # def update_graph_2d_scatter(
# #     controller_data, z_axis_list, graph_axes, controller_definition, current_figs, graph_ids
# # ):

# #     # # may need this later with multiple plots but I think it will still loop through drop downs
# #     # if "graph-1d" not in graph_axes:
# #     #     return dash.no_update

# #     # axes = graph_axes["graph-1d"]# = {"x-axis": "time", "y-axis": y_axis}
# #     try:
# #         # heatmaps = []
# #         scatters = []
# #         if controller_data:
# #             print(f"controller_data: {controller_data}")
# #             for z_axis, graph_id, current_fig in zip(
# #                 z_axis_list, graph_ids, current_figs
# #             ):
# #                 y_axis = graph_id["index"].split("::")[1]
# #                 if (
# #                     "time" not in controller_data["variables"]
# #                     or y_axis not in controller_data["variables"]
# #                     or z_axis not in controller_data["variables"]
# #                 ):
# #                     raise PreventUpdate
# #                     # heatmaps.append(dash.no_update)
# #                     # scatters.append(dash.no_update)
# #                     # continue
# #                     # return dash.no_update

# #                 x = controller_data["variables"]["time"]["data"]
# #                 y = controller_data["variables"][y_axis]["data"]
# #                 z = controller_data["variables"][z_axis]["data"]
# #                 print(f"scatter update: {x}, {y}, {z}")

# #                 # units = ""
# #                 # try:
# #                 #     y_units = f'({sensor_definition["variables"][y_axis]["attributes"]["units"]["data"]})'
# #                 #     z_units = f'({sensor_definition["variables"][z_axis]["attributes"]["units"]["data"]})'
# #                 # except KeyError:
# #                 #     pass

# #                 # patched_scatter = Patch()
# #                 # patched_scatter["data"][0]["x"] = y
# #                 # patched_scatter["data"][0]["y"] = z
# #                 # patched_scatter["layout"]["title"] = str(x[-1])
# #                 # heatmaps.append({"x": [x], "y": [y], "z": [z]})

# #                 current_fig["data"][0]["x"] = y
# #                 current_fig["data"][0]["y"] = z
# #                 if isinstance(x, list):
# #                     x = x[-1]
# #                 current_fig["layout"]["title"] = str(x)
# #                 print(f"scatter current_fig: {current_fig}")
# #                 scatters.append(current_fig)
# #                 # scatters.append(
# #                 #     go.Figure(
# #                 #         data=go.Scatter(x=y, y=z, type="scatter"),
# #                 #         layout={
# #                 #             "xaxis": {"title": f"{y_axis} {y_units}"},
# #                 #             "yaxis": {"title": f"{z_axis} {z_units}"},
# #                 #             "title": str(x[-1])
# #                 #             # "yaxis": {"title": f"{y_axis} {y_units}"},
# #                 #             # "colorscale": "rainbow"
# #                 #         }
# #                 #     )
# #                 # )
# #                 # scatters.append(patched_scatter)

# #             # return {"x": [x], "y": [y]}

# #             return scatters

# #     except Exception as e:
# #         print(f"scatter update error: {e}")
# #         # return dash.no_update
# #         # return dash.no_update
# #     raise PreventUpdate
# #     # return dash.no_update

# @callback(
#     # Output("dbc-switch-value-changed", "children"),
#     Output("ws-send-instance-buffer", "children"),
#     Input({"type": "settings-table", "index": ALL}, "cellRendererData"),
#     State("operational-meta", "data")
# )
# def get_requested_setting(changed_component, operational_meta):
#     print('COMPONENT CHANGED', json.dumps(changed_component))
#     requested_val = int(changed_component[0]["value"])
#     col_id = changed_component[0]["colId"]
#     try:
#         event = {
#             "source": "testsource",
#             "data": {"settings": col_id, "requested": requested_val},
#             "destpath": "envds/operational/settings/request",
#             "operationalid": operational_meta["operational_id"]
#         }
#     except Exception as e:
#             print(f"data update error: {e}")
#     return json.dumps(changed_component), json.dumps(event)


# @callback(
#     Output({"type": "settings-table", "index": ALL}, "rowData"), 
#     Output({"type": "settings-table", "index": ALL}, "columnDefs"),
#     Input("operational-settings-buffer", "data"),
#     [
#         State({"type": "settings-table", "index": ALL}, "rowData"),
#         State({"type": "settings-table", "index": ALL}, "columnDefs"),
#         State("operational-definition", "data")
#     ],
# )
# def update_settings_table(operational_settings, row_data_list, col_defs_list, operational_definition):
#     if operational_settings:
#             new_row_data_list = []
#             new_column_defs = []
#             try:
#                 for row_data, col_defs in zip(row_data_list, col_defs_list):
#                     print('col defs', col_defs)
#                     data = {}
#                     for col in col_defs:
#                         print('col', col)
#                         name = col["field"]
#                         if name in operational_settings["settings"]:
#                             data[name] = operational_settings["settings"][name]["data"]["actual"]
#                         else:
#                             data[name] = ""

#                         # Make sure the settings contain integers
#                         if operational_definition["variables"][name]["attributes"]["valid_min"]["type"] == "int":
#                             min_val = operational_definition["variables"][name]["attributes"]["valid_min"]["data"]
#                             max_val = operational_definition["variables"][name]["attributes"]["valid_max"]["data"]
#                             step_val = operational_definition["variables"][name]["attributes"]["step_increment"]["data"]

#                             # Check if the setting should be set up as a boolean switch
#                             if min_val == 0:
#                                 if max_val == 1:
#                                     if step_val == 1:
#                                         col["cellRenderer"] = "DBC_Switch"
#                                         col["cellRendererParams"] = {"color": "success"}
                            
#                             # Check if the setting should be set up as numeric input
#                             elif max_val > 1:
#                                 col["cellRenderer"] = "DCC_Input"
#                                 col["cellRendererParams"] = {"min": min_val, "max": max_val, "step": step_val}

#                         new_column_defs.append(col)
#                     row_data.insert(0, data)
#                     new_row_data_list.append(row_data[0:1])
#                 if len(new_row_data_list) == 0:
#                     raise PreventUpdate
#                 return new_row_data_list, [new_column_defs]
            
#             except Exception as e:
#                 print(f"data update error: {e}")
#                 print(traceback.format_exc())
#             raise PreventUpdate
#     else:
#         raise PreventUpdate


# @callback(
#     Output(
#         {"type": "operational-data-table-1d", "index": ALL}, "rowData"
#     ),  # , Output("active-sensor-changes", "data")],
#     Input("operational-data-buffer", "data"),
#     # Input("ws-sensor-instance", "message"),
#     [
#         State({"type": "operational-data-table-1d", "index": ALL}, "rowData"),
#         State({"type": "operational-data-table-1d", "index": ALL}, "columnDefs"),
#     ],  # , dcc.Store("sensor-definition", "data")],
#     # prevent_initial_call=True,
# )
# def update_table_1d(
#     operational_data, row_data_list, col_defs_list
# ):  # , sensor_definition):
#     # sensor_def_data_changes = []
#     # active_sensor_data_changes = []
#     # # print(f"message data: {e}")
#     # print(f"sensor_def_data: {sensor_def_data}")
#     # print(f"row_data: {type(row_data)}, {row_data}, col_defs: {col_defs}")
#     # if e is not None and "data" in e:
#     if operational_data:
#         new_row_data_list = []
#         try:
#             # sensor_data = json.loads(e["data"])
#             for row_data, col_defs in zip(row_data_list, col_defs_list):
#                 data = {}
#                 # print(f"row, col: {row_data}, {col_defs}")
#                 for col in col_defs:
#                     name = col["field"]
#                     # print(f"name: {name}")
#                     if name in operational_data["variables"]:
#                         # print(f"variable: {msg["variables"][name]["data"]}")
#                         data[name] = operational_data["variables"][name]["data"]
#                         # print(f"data: {data}")
#                     else:
#                         data[name] = ""
#                 # if row_data is None:
#                 #     row_data = []
#                 # print(f"row_data1: {type(row_data), {row_data}}")
#                 row_data.insert(0, data)
#                 # print(f"row_data2: {type(row_data), {row_data}}")
#                 # row_data = row_data.append(data)
#                 # test_row_data = []
#                 # test_row_data.append(data)
#                 # print(f"row-data: {test_row_data}")

#                 # limit size of table to 30 rows
#                 if len(row_data) > 30:
#                     # return row_data[:30]
#                     new_row_data_list.append(row_data[:30])
#                 else:
#                     # return row_data
#                     new_row_data_list.append(row_data)
#                 # return dash.no_update
#             # row_data_list.append(row_data)
#             # print(f"row_data_list: {row_data_list}")
#             if len(new_row_data_list) == 0:
#                 raise PreventUpdate
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
#         # return [dash.no_update for i in range(0,len(col_defs_list))]
#         # return dash.no_update


# # @callback(
# #     Output(
# #         {"type": "controller-data-table-2d", "index": ALL}, "rowData"
# #     ),  # , Output("active-sensor-changes", "data")],
# #     Input("controller-data-buffer", "data"),
# #     # Input("ws-controller-instance", "message"),
# #     [
# #         State({"type": "controller-data-table-2d", "index": ALL}, "rowData"),
# #         State({"type": "controller-data-table-2d", "index": ALL}, "columnDefs"),
# #     ],  # , dcc.Store("controller-definition", "data")],
# #     # prevent_initial_call=True,
# # )
# # def update_table_2d(controller_data, row_data_list, col_defs_list):  # , controller_definition):
# #     # controller_def_data_changes = []
# #     # active_controller_data_changes = []
# #     # # print(f"message data: {e}")
# #     # print(f"controller_def_data: {controller_def_data}")
# #     # print(f"row_data: {type(row_data)}, {row_data}, col_defs: {col_defs}")
# #     # if e is not None and "data" in e:
# #     if controller_data:
# #         new_row_data_list = []
# #         try:
# #             for col_defs in col_defs_list:
# #                 dim_2d = col_defs[0]["field"]
# #                 # row_data = [{}]*len(controller_data["variables"][dim_2d]["data"])
# #                 row_data = []
# #                 for index in range(0, len(controller_data["variables"][dim_2d]["data"])):
# #                     data = {}
# #                     for col in col_defs:
# #                         name = col["field"]
# #                         data[name] = controller_data["variables"][name]["data"][index]
# #                         # print(f"row_data: {index} {name} : {row_data[index][name]} : {row_data[index]}")
# #                     # print(f"row_data: {row_data}")
# #                     # print(f"data: {data}")
# #                     row_data.append(data)
# #                 # print(f"row_data: {row_data}")
# #                 new_row_data_list.append(row_data)
# #             # print(f"new_row_data_list: {new_row_data_list}")
# #             return new_row_data_list

# #             # # controller_data = json.loads(e["data"])
# #             # for row_data, col_defs in zip(row_data_list,col_defs_list):
# #             #     # dim_2d = table_id.split("::")[1]
# #             #     data = {}
# #             #     # print(f"row, col: {row_data}, {col_defs}")
# #             #     for col in col_defs:
# #             #         name = col["field"]
# #             #         # print(f"name: {name}")
# #             #         if name in sensor_data["variables"]:
# #             #             # print(f"variable: {msg["variables"][name]["data"]}")
# #             #             data[name] = sensor_data["variables"][name]["data"]
# #             #             # print(f"data: {data}")
# #             #         else:
# #             #             data[name] = ""
# #             #     # if row_data is None:
# #             #     #     row_data = []
# #             #     # print(f"row_data1: {type(row_data), {row_data}}")

# #             #     for i in range(0, len(data[col_defs[0]["field"]])):

# #             #     row_data.insert(0, data)
# #             #     # print(f"row_data2: {type(row_data), {row_data}}")
# #             #     # row_data = row_data.append(data)
# #             #     # test_row_data = []
# #             #     # test_row_data.append(data)
# #             #     # print(f"row-data: {test_row_data}")

# #             #     # limit size of table to 30 rows
# #             #     if len(row_data) > 30:
# #             #         # return row_data[:30]
# #             #         new_row_data_list.append(row_data[:30])
# #             #     else:
# #             #         # return row_data
# #             #         new_row_data_list.append(row_data)
# #             #     # return dash.no_update
# #             # # row_data_list.append(row_data)
# #             # # print(f"row_data_list: {row_data_list}")
# #             # if len(new_row_data_list) == 0:
# #             #     raise PreventUpdate
# #             # return new_row_data_list

# #         except Exception as e:
# #             print(f"data update error: {e}")
# #             # return dash.no_update
# #         raise PreventUpdate
# #         # return [dash.no_update for i in range(0,len(col_defs_list))]
# #         # return row_data
# #     else:
# #         # return dash.no_update
# #         raise PreventUpdate
# #         # return [dash.no_update for i in range(0,len(col_defs_list))]
# #         # return dash.no_update


# # #    content = dbc.Row(children=[

# # #         dbc.Label("z-axis min:"),
# # #         dbc.Col(dbc.Input(type="number", id={"type": "graph-2d-z-axis-min", "index": f"{xaxis}::{yaxis}"})),
# # #         dbc.Label("z-axis max:"),
# # #         dbc.Col(dbc.Input(type="number", id={"type": "graph-2d-z-axis-max", "index": f"{xaxis}::{yaxis}"})),
# # #     ])

# # #     axes_collapse = html.Div(
# # #         [

# # #             dbc.Button(
# # #                 "Axes Settings",
# # #                 id={"type": "graph-2d-axes-settings", "index": f"{xaxis}::{yaxis}"},
# # #                 className="mb-3",
# # #                 # color="primary",
# # #                 n_clicks=0,
# # #             ),
# # #             dbc.Collapse(
# # #                 dbc.Card(dbc.CardBody(childern=[content])),
# # #                 id={"type": "graph-2d-axes-collapse", "index": f"{xaxis}::{yaxis}"},
# # #                 is_open=False,
# # #             ),
# # #         ]
# # #     )


# # @callback(
# #     Output(
# #         {"type": "graph-2d-heatmap", "index": MATCH}, "figure", allow_duplicate=True
# #     ),
# #     [Input({"type": "graph-2d-z-axis-submit", "index": MATCH}, "n_clicks")],
# #     [
# #         State({"type": "graph-2d-z-axis-min", "index": MATCH}, "value"),
# #         State({"type": "graph-2d-z-axis-max", "index": MATCH}, "value"),
# #         State({"type": "graph-2d-heatmap", "index": MATCH}, "figure"),
# #     ],
# #     prevent_initial_call=True,
# # )
# # def set_2d_z_axis_range(n, axis_min, axis_max, heatmap):

# #     print(f"z-axis range: min={axis_min}, max={axis_max}")
# #     fig = go.Figure(heatmap)
# #     fig = fig.update_layout(coloraxis=dict(cauto=False, cmax=axis_max, cmin=axis_min))
# #     print(f"update fig: {fig}")
# #     # try:
# #     #     heatmap['layout']['template']['layout']['coloraxis']['cmin'] = axis_min
# #     #     heatmap['layout']['template']['layout']['coloraxis']['cmix'] = axis_max
# #     #     print(f"heatmap: {heatmap['layout']['template']['layout']['coloraxis']}")
# #     #     # for k,v in heatmap['layout']["template"]['coloraxis'].items():
# #     #     #     print(f"[{k}]: [{v.keys()}]")
# #     # except KeyError:
# #     #     pass
# #     # # heatmap.update_layout(coloraxis=dict(cmax=axis_max, cmin=axis_min))

# #     return fig


# # # @callback(
# # #     Output(
# # #         {"type": "graph-2d-heatmap", "index": MATCH}, "figure", allow_duplicate=True
# # #     ),
# # #     [Input({"type": "graph-2d-z-axis-max", "index": MATCH}, "value")],
# # #     [
# # #         State({"type": "graph-2d-heatmap", "index": MATCH}, "figure"),
# # #         State({"type": "graph-2d-z-axis-min", "index": MATCH}, "value"),
# # #     ],
# # #     prevent_initial_call=True,
# # # )
# # # def set_2d_z_axis_max(axis_max, heatmap, axis_min):

# # #     print(f"z-axis range: min={axis_min}, max={axis_max}")
# # #     heatmap.update_layout(coloraxis=dict(cmax=axis_max, cmin=axis_min))

# # #     return heatmap


# @callback(
#     Output("ws-operational-instance", "send"),
#     Input("ws-send-instance-buffer", "children"),
# )
# def send_to_instance(value):
#     print(f"sending: {value}")
#     return value
