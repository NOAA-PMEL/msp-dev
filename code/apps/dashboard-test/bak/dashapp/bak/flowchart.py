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
# from dash_extensions import Mermaid

# # import pymongo
# from collections import deque
# import httpx

# handler = logging.StreamHandler()
# handler.setFormatter(Logfmter())
# logging.basicConfig(handlers=[handler])
# L = logging.getLogger(__name__)
# L.setLevel(logging.DEBUG)


# class Settings(BaseSettings):
#     host: str = "0.0.0.0"
#     port: int = 8787
#     debug: bool = False
#     daq_id: str = "default"
#     ws_hostname: str = "localhost:8080"
#     knative_broker: str = (
#         "http://kafka-broker-ingress.knative-eventing.svc.cluster.local/default/default"
#     )

#     class Config:
#         env_prefix = "DASHBOARD_"
#         case_sensitive = False

# config = Settings()
# datastore_url = f"datastore.{config.daq_id}-system"
# link_url_base = f"http://{config.ws_hostname}/msp/dashboardtest"

# dash.register_page(
#     __name__,
#     path_template="/flowchart",
#     title="Flowchart",  # , prevent_initial_callbacks=True
# )


# layout = html.Div([
#     Mermaid(id='flowchart-output'),
#     html.Div(id='output-area'),
#     dcc.Interval(
#                 id="table-update-interval", interval=(5 * 1000), n_intervals=0
#             )
# ])


# @callback(
#     Output("active-sensors", "data"),
#     Input("table-update-interval", "n_intervals"),
#     State("active-sensors", "data")
# )
# def update_active_sensors(count, active_sensors):

#     update = False
#     new_data = []
#     rel_path = dash.get_relative_path("/")
#     print(f"*** rel_path: {rel_path}")
#     try:
#         query = {"device_type": "sensor"}
#         url = f"http://{datastore_url}/device-instance/registry/get/"
#         print(f"device-definition-get: {url}")
#         response = httpx.get(url, params=query)
#         results = response.json()
#         # print(f"results: {results}")
#         if "results" in results and results["results"]:
#             print('results', results)
#             for doc in results["results"]:
#                 make = doc["make"]
#                 model = doc["model"]
#                 serial_number = doc["serial_number"]
#                 version = doc["version"]
#                 sensor_id = "::".join([make, model, serial_number])
#                 sampling_system_id = "unknown::unknown::unknown"

#                 sensor = {
#                     "sensor_id": f"[{sensor_id}]({link_url_base}/dash/sensor/{sensor_id})",
#                     "make": make,
#                     "model": model,
#                     "serial_number": serial_number,
#                     "sampling_system_id": f"[{sampling_system_id}]{link_url_base}/uasdaq/dashboard/dash/sampling-system/{sampling_system_id})",
#                 }
#                 if sensor not in active_sensors:
#                     active_sensors.append(sensor)
#                     update = True

#         if update:
#             print('sensor update detected')
#             return active_sensors
#         else:
#             return dash.no_update

#     except Exception as e:
#         print(f"update_active_sensors error: {e}")
#         return dash.no_update


# # @callback(
# #     Output("active-controllers", "data"),
# #     Input("table-update-interval", "n_intervals"),
# #     State("active-controllers", "data")
# # )
# # def update_active_controllers(count, active_controllers):

# #     update = False
# #     new_data = []
# #     rel_path = dash.get_relative_path("/")
# #     print(f"*** rel_path: {rel_path}")
# #     try:
# #         pass

# #         query = {"device_type": "controller"}
# #         url = f"http://{datastore_url}/controller-instance/registry/get/"
# #         print(f"controller-definition-get: {url}")
# #         response = httpx.get(url, params=query)
# #         results = response.json()
# #         print(f"results: {results}")
# #         if "results" in results and results["results"]:
# #             for doc in results["results"]:
# #                 make = doc["make"]
# #                 model = doc["model"]
# #                 serial_number = doc["serial_number"]
# #                 version = doc["version"]
# #                 controller_id = "::".join([make, model, serial_number])
# #                 sampling_system_id = "unknown::unknown::unknown"

# #                 controller = {
# #                     "controller_id": f"[{controller_id}]({link_url_base}/dash/controller/{controller_id})",
# #                     "make": make,
# #                     "model": model,
# #                     "serial_number": serial_number,
# #                     "sampling_system_id": f"[{sampling_system_id}]{link_url_base}/uasdaq/dashboard/dash/sampling-system/{sampling_system_id})",
# #                 }
# #                 if controller not in active_controllers:
# #                     active_controllers.append(controller)
# #                     update = True

# #         if update:
# #             return active_controllers
# #         else:
# #             return dash.no_update


# #     except Exception as e:
# #         print(f"update_active_controllers error: {e}")
# #         return dash.no_update




# @callback(
#     Output('flowchart-output', 'chart'),
#     Output('output-area', 'children'),
#     Input('active-sensors', 'data'),
#     # Input('active-controllers', 'data')
# )
# def update_output(active_sensors):
#     mermaid_code = "flowchart TB;\n"
#     print('mermaid code 1', mermaid_code)
#     mermaid_code += f"subgraph Main['Main'];\n"
#     try:
#         for sensor in active_sensors:
#             # some check of sampling system id / location will need to be done here
#             mermaid_code += f"  {sensor['model']}[{sensor['model']}];\n"
        
#             print('mermaid code', mermaid_code)
    
#     except Exception as e:
#         pass

#     # try:
#     #     for controller in active_controllers:
#     #         mermaid_code += f"  {controller['model']}[{controller['model']}];\n"
    
#     # except Exception as e:
#     #     pass
    
#     mermaid_code += "end"

#         # mermaid_code = f"""
#         # flowchart TB
#         # subgraph Main["Main"]
#         #     A[{sensor}]
#         # end
#         # """
#     # mermaid_code = f"""
#     # flowchart TB
#     #     subgraph Main["Main"]
#     #             a2["a2"]
#     #             a1["a1"]
#     #     end
#     #     subgraph Physics["Physics"]
#     #             b2["b2"]
#     #             b1["b1"]
#     #     end
#     #         c1["c1"] -----> a2 & c2["c2"]
#     #         a1 --> a2
#     #         b1 --> b2
#     #         one --> two
#     #         two ---> c2
#     # """
#     # to_return = f'active sensors{active_sensors}'
#     return mermaid_code, mermaid_code
    
