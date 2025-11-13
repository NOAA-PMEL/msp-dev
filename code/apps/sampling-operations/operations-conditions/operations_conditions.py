import asyncio
import json
import logging
import math
import sys
from time import sleep
from typing import List

# import numpy as np
from ulid import ULID
from pathlib import Path
import os

import httpx
from logfmter import Logfmter

# from registry import registry
# from flask import Flask, request
from pydantic import BaseSettings, BaseModel, Field
from cloudevents.http import CloudEvent, from_http, from_json, to_json
from cloudevents.conversion import to_structured  # , from_http
from cloudevents.exceptions import InvalidStructuredJSON
from aiomqtt import Client, MqttError

# # from cloudevents.http.conversion import from_http
# from cloudevents.conversion import to_structured  # , from_http
# from cloudevents.exceptions import InvalidStructuredJSON

from datetime import datetime, timedelta, timezone
from envds.util.util import (
    get_datetime_string,
    get_datetime,
    datetime_to_string,
    string_to_datetime,
    get_datetime_with_delta,
    string_to_timestamp,
    timestamp_to_string,
    time_to_next,
    round_to_nearest_N_seconds,
    seconds_elapsed,
)

# from envds.daq.event import DAQEvent
# from envds.daq.types import DAQEventType as det
from envds.sampling.event import SamplingEvent
from envds.sampling.types import SamplingEventType as sampet


# import pymongo

import uvicorn

# from datastore_requests import (
#     DataStoreQuery,
#     DataUpdate,
#     DataRequest,
#     DeviceDefinitionUpdate,
#     DeviceDefinitionRequest,
#     DeviceInstanceUpdate,
#     DeviceInstanceRequest,
#     DatastoreRequest,
#     ControllerInstanceUpdate,
#     ControllerInstanceRequest,
#     ControllerDataRequest,
#     ControllerDataUpdate,
#     ControllerDefinitionRequest,
#     ControllerDefinitionUpdate
# )

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.INFO)


# test


class OperationsConditionsConfig(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8080
    debug: bool = True
    # knative_broker: str = (
    #     "http://kafka-broker-ingress.knative-eventing.svc.cluster.local/default/default"
    # )
    # mongodb_user_name: str = ""
    # mongodb_user_password: str = ""
    # mongodb_connection: str = (
    #     "mongodb://uasdaq:password@uasdaq-mongodb-0.uasdaq-mongodb-svc.mongodb.svc.cluster.local:27017,uasdaq-mongodb-1.uasdaq-mongodb-svc.mongodb.svc.cluster.local:27017,uasdaq-mongodb-2.uasdaq-mongodb-svc.mongodb.svc.cluster.local:27017/data?replicaSet=uasdaq-mongodb&ssl=false"
    # )
    # erddap_http_connection: str = (
    #     "http://uasdaq.pmel.noaa.gov/uasdaq/dataserver/erddap"
    # )
    # erddap_https_connection: str = (
    #     "https://uasdaq.pmel.noaa.gov/uasdaq/dataserver/erddap"
    # )
    # # erddap_author: str = "fake_author"

    # TODO fix ns prefix
    daq_id: str | None = None

    mqtt_broker: str = "mosquitto.default"
    mqtt_port: int = 1883
    # mqtt_topic_filter: str = 'aws-id/acg-daq/+'
    mqtt_topic_subscriptions: str = (
        "envds/+/+/+/data/#"  # ['envds/+/+/+/data/#', 'envds/+/+/+/status/#', 'envds/+/+/+/setting/#', 'envds/+/+/+/control/#']
    )
    # mqtt_client_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    mqtt_client_id: str = Field(str(ULID()))

    knative_broker: str | None = None

    class Config:
        env_prefix = "OPERATIONS_MANAGER_"
        case_sensitive = False


class OperationsConditions:
    """docstring for OperationsConditions."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug("OperationsConditions instantiated")

        # self.sampling_mode_control = True
        # self.sampling_modes = dict()
        # self.sampling_states = dict()
        self.sampling_conditions = dict()

        # self.sampling_actions = dict()

        # # current mode
        # self.platform_sampling_mode = None



        # self.platforms = dict()
        # self.platform_layouts = dict()
        # self.variablemaps = dict()

        # this is cache for variable mapping
        # self.variablesets = {
        #     "sources": dict(),
        #     "variablesets": dict(),
        #     "indices": dict(),
        # }
        # print("here:6")

        # self.index_ready_buffer = asyncio.Queue()
        # self.index_monitor_tasks = dict()

        self.config = OperationsConditionsConfig()
        self.configure()
        print("here:7")

        self.http_client = None

        self.mqtt_buffer = asyncio.Queue()
        # asyncio.create_task(self.get_from_mqtt_loop())
        # asyncio.create_task(self.handle_mqtt_buffer())

        # asyncio.create_tasks(self.sampling_mode_monitor())
        # asyncio.create_tasks(self.sampling_state_monitor())
        asyncio.create_tasks(self.sampling_condition_monitor())
        # asyncio.create_tasks(self.sampling_action_monitor())

        print("OperationsConditions: init: here:8")


    def configure(self):
        # set clients

        self.logger.debug("configure", extra={"self.config": self.config})

        try:
            # load operating conditions and actions
            #   load payloads

            # with open("/app/config/sampling_modes.json", "r") as f:
            #     sampling_modes = json.load(f)

            # for sampling_mode in sampling_modes:
            #     if sampling_mode["kind"] not in self.sampling_modes:
            #         self.sampling_modes[sampling_mode["kind"]] = dict()
            #     if sampling_mode["name"] not in self.sampling_modes[sampling_mode["kind"]]:
            #         self.sampling_modes[sampling_mode["kind"]][sampling_mode["name"]] = sampling_mode

            # self.logger.debug("configure", extra={"sampling_modes": self.sampling_modes})

            # with open("/app/config/sampling_states.json", "r") as f:
            #     sampling_states = json.load(f)

            # for sampling_state in sampling_states:
            #     if sampling_state["kind"] not in self.sampling_states:
            #         self.sampling_states[sampling_state["kind"]] = dict()
            #     if sampling_state["name"] not in self.sampling_states[sampling_state["kind"]]:
            #         self.sampling_states[sampling_state["kind"]][sampling_state["name"]] = sampling_state

            # self.logger.debug("configure", extra={"sampling_states": self.sampling_states})

            # with open("/app/config/sampling_conditions.json", "r") as f:
            #     sampling_conditions = json.load(f)

            # for sampling_condition in sampling_conditions:
            #     if sampling_condition["kind"] not in self.sampling_conditions:
            #         self.sampling_conditions[sampling_condition["kind"]] = dict()
            #     if sampling_condition["name"] not in self.sampling_conditions[sampling_condition["kind"]]:
            #         self.sampling_conditions[sampling_condition["kind"]][sampling_condition["name"]] = sampling_condition

            self.logger.debug("configure", extra={"sampling_conditions": self.sampling_conditions})

        except Exception as e:
            self.logger.error("configure error", extra={"reason": e})

    def open_http_client(self):
        # create a new client for each request
        self.http_client = httpx.AsyncClient()

    async def send_event(self, ce):
        try:
            self.logger.debug(ce)  # , extra=template)
            if not self.http_client:
                self.open_http_client()
            try:
                timeout = httpx.Timeout(5.0, read=0.1)
                headers, body = to_structured(ce)
                self.logger.debug(
                    "send_event",
                    extra={
                        "broker": self.config.knative_broker,
                        "h": headers,
                        "b": body,
                    },
                )
                # send to knative broker
                # async with httpx.AsyncClient() as client:
                #     r = await client.post(
                #         self.config.knative_broker,
                #         headers=headers,
                #         data=body,
                #         timeout=timeout,
                #     )

                r = await self.http_client.post(
                    self.config.knative_broker,
                    headers=headers,
                    data=body,
                    timeout=timeout,
                )

                r.raise_for_status()
            except InvalidStructuredJSON:
                self.logger.error(f"INVALID MSG: {ce}")
            except httpx.TimeoutException:
                pass
            except httpx.HTTPError as e:
                self.logger.error(f"HTTP Error when posting to {e.request.url!r}: {e}")
        except Exception as e:
            print("error", e)
        await asyncio.sleep(0.01)

    async def submit_request(self, path: str, query: dict):
        try:
            self.logger.debug("submit_request", extra={"path": path, "query": query})
            # results = httpx.get(f"http://{self.datastore_url}/{path}/", params=query)
            results = await self.http_client.get(f"http://{self.datastore_url}/{path}/", params=query)
            self.logger.debug("submit_request", extra={"results": results.json()})
            return results.json()
        except Exception as e:
            self.logger.error("submit_request", extra={"reason": e})
            return {}

    async def get_from_mqtt_loop(self):
        reconnect = 10
        while True:
            try:
                self.logger.debug("listen", extra={"config": self.config})
                client_id = str(ULID())
                async with Client(
                    self.config.mqtt_broker,
                    port=self.config.mqtt_port,
                    identifier=client_id,
                ) as self.client:
                    # for topic in self.config.mqtt_topic_subscriptions.split("\n"):
                    for topic in self.config.mqtt_topic_subscriptions.split(","):
                        # print(f"run - topic: {topic.strip()}")
                        # self.logger.debug("run", extra={"topic": topic})
                        if topic.strip():
                            self.logger.debug("subscribe", extra={"topic": topic.strip()})
                            await self.client.subscribe(
                                f"$share/sampling-system/{topic.strip()}"
                            )

                        # await client.subscribe(config.mqtt_topic_subscription, qos=2)
                    # async with client.messages() as messages:
                    async for message in self.client.messages:  # () as messages:

                        try:
                            ce = from_json(message.payload)
                            topic = message.topic.value
                            ce["sourcepath"] = topic
                            await self.mqtt_buffer.put(ce)
                            self.logger.debug(
                                "get_from_mqtt_loop",
                                extra={"cetype": ce["type"], "topic": topic},
                            )
                        except Exception as e:
                            self.logger.error("get_from_mqtt_loop", extra={"reason": e})
                        # try:
                        #     self.logger.debug("listen", extra={"payload_type": type(ce), "ce": ce})
                        #     await self.send_to_knbroker(ce)
                        # except Exception as e:
                        #     self.logger.error("Error sending to knbroker", extra={"reason": e})
            except MqttError as error:
                self.logger.error(
                    f"{error}. Trying again in {reconnect} seconds",
                    extra={
                        k: v
                        for k, v in self.config.dict().items()
                        if k.lower().startswith("mqtt_")
                    },
                )
                await asyncio.sleep(reconnect)
            finally:
                await asyncio.sleep(0.0001)

    async def handle_mqtt_buffer(self):
        while True:
            try:
                ce = await self.mqtt_buffer.get()
                self.logger.debug("handle_mqtt_buffer", extra={"ce": ce})
                if ce["type"] == "envds.data.update":
                    self.logger.debug("handle_mqtt_buffer", extra={"ce-type": ce["type"]})
                    await self.device_data_update(ce)
                elif ce["type"] == "envds.controller.data.update":
                    await self.controller_data_update(ce)

            except Exception as e:
                self.logger.error("handle_mqtt_buffer", extra={"reason": e})

            await asyncio.sleep(0.0001)

    async def handle_condition_request(self, ce:CloudEvent):
        
        # parse request and evaluate criteria

        #   get source data from datastore
        # query = {}
        # results = await self.submit_request(
        #     path="device-definition/registry/get", query=query
        # )
        # # results = httpx.get(f"http://{self.datastore_url}/device-definition/registry/get/", parmams=query)
        # self.logger.debug("get_device_definitions_loop", extra={"results": results})

        # compare result with current:
        #   if changed, send immediate update
        #   else, send update at regularly scheduled interval
        
        pass

    # this probably won't happen for conditions unless there is another layer of resources
    async def handle_condition_update(self, ce:CloudEvent):
        pass


    # async def sampling_mode_monitor(self):
    #     while True:

    #         # get current sampling mode
    #         #   or trigger off mode updates
            
    #         await asyncio.sleep(1)

    # # TODO change this for sampling-system
    # async def device_data_update(self, ce: CloudEvent):

    #     try:
    #         attributes = ce.data["attributes"]
    #         # dimensions = ce.data["dimensions"]
    #         # variables = ce.data["variables"]

    #         make = attributes["make"]["data"]
    #         model = attributes["model"]["data"]
    #         serial_number = attributes["serial_number"]["data"]
    #         device_id = "::".join([make, model, serial_number])
    #         self.logger.debug("device_data_update", extra={"device_id": device_id})
    #         # if device_id in self.variablesets["sources"]:
    #         await self.update_by_source(
    #             source_id=device_id, source_data=ce
    #         )

    #     except Exception as e:
    #         self.logger.error("device_data_update", extra={"reason": e})
    #     pass

    # def get_variablemap_id(self, vm:dict):
    #     try:
    #         variablemap_type = vm["data"]["attributes"]["variablemap_index_type"]
    #         if variablemap_type == "Platform":
    #             variablemap_type_id = vm["data"]["attributes"]["platform"]
    #         else:
    #             return ""
            
    #         # variable_map_type_id = vm["data"]["attributes"]["variablemap_index_type_id"]
    #         variablemap_name = vm["metadata"]["name"]
    #         valid_config_time = vm["data"]["attributes"]["valid_config_time"]

    #         return "::".join([variablemap_type_id, variablemap_name, valid_config_time])
    #     except Exception as e:
    #         self.logger.error("get_variable_map_id", extra={"reason": e})
    #         return ""

    # def get_variableset_id(self, variableset_name:str, variableset:dict):
    #     try:
    #         variablemap_id = variableset["data"]["attributes"]["variablemap_id"]
            
    #         # variableset_name = variableset["metadata"]["name"]

    #         return "::".join([variablemap_id, variableset_name])
        
    #     except Exception as e:
    #         self.logger.error("get_variable_map_id", extra={"reason": e})
    #         return ""

    # def get_id_components(self, vm_id:str=None, vs_id:str=None) -> dict:
    #     try:
    #         if vs_id:
    #             parts = vs_id.split("::")
    #             comp = {
    #                 "variablemap_index_type_id": parts[0],
    #                 "variablemap_name": parts[1],
    #                 "valid_config_time": parts[2],
    #                 "variableset_name": parts[3],

    #             }
    #             return comp
    #         elif vm_id:
    #             parts = vm_id.split("::")
    #             comp = {
    #                 "variablemap_index_type_id": parts[0],
    #                 "variablemap_name": parts[1],
    #                 "valid_config_time": parts[2],
    #             }
    #             return comp
    #     except Exception as e:
    #         self.logger.error("get_id_components", extra={"reason": e})
        
    #     return None

    # # TODO change this for sampling-system
    # async def controller_data_update(self, ce: CloudEvent):

    #     try:
    #         attributes = ce.data["attributes"]

    #         make = attributes["make"]["data"]
    #         model = attributes["model"]["data"]
    #         serial_number = attributes["serial_number"]["data"]

    #         controller_id = "::".join([make, model, serial_number])
    #         # if controller_id in self.variablesets["sources"]:
    #         await self.update_by_source(
    #             source_id=controller_id, source_data=ce
    #         )

    #     except Exception as e:
    #         self.logger.error("device_data_update", extra={"reason": e})
    #     pass

    # def is_valid_variable_set(self, vs_id: str, time:str) -> bool:
    #     valid_vs_id = self.get_valid_variableset_id_by_time(self, variableset_id=vs_id, source_time=time)
    #     return vs_id == valid_vs_id

    # async def get_valid_variablemaps(self, target_time:str):
    #     try:
    #         self.logger.debug("get_valid_variablemaps", extra={"target_time": target_time})
    #         valid_variablesets = []
    #         for vmtype_name, vmtype in self.variablemaps.items():
    #             for vmtype_type_name, vm_type_type in vmtype.items():
    #                 for vm_name, vm in vm_type_type.items():

    #                     current = ""
    #                     for vm_valid_config_time in vm.keys():
    #                         self.logger.debug("get_valid_variablemaps", extra={"vm_name": vm_name, "valid_config_time": vm_valid_config_time})
    #                         # vm_parts = vm_id.split("::")
    #                         # vm_valid_config_time = vm_parts[2]
    #                         if target_time > vm_valid_config_time and vm_valid_config_time > current:
    #                             current = vm_valid_config_time
    #                     if current:
    #                         valid_variablesets.append(
    #                             {
    #                                 "variablemap_type": vmtype_name,
    #                                 "variablemap_type_name": vmtype_type_name,
    #                                 "variablemap_name": vm_name,
    #                                 "valid_config_time": current,
    #                                 "variablemap": vm[current]
    #                             }
    #                         )

    #     except Exception as e:
    #         self.logger.error("get_valid_variablemaps", extra={"reason": e})
    #         valid_variablesets = []
        
    #     self.logger.debug("get_valid_variablemaps", extra={"valid_variable_sets": valid_variablesets})
    #     return valid_variablesets
    
    # async def update_by_source(self, source_id:str, source_data: CloudEvent):
    #     try:
    #         # print("here:1")
    #         # self.logger.debug("update_by_source", extra={"source_id": source_id})
    #         # print("here:2")
    #         source_time = source_data.data["variables"]["time"]["data"]
    #         # print("here:3")
    #         self.logger.debug("update_by_source", extra={"source_time": source_time})
    #         # print("here:4")
    #         vm_list = await self.get_valid_variablemaps(target_time=source_time)
    #         # print(f"here:5 {vm_list}")
    #         for vm in vm_list:
    #             print(f"here:6 {vm}")
    #             # self.logger.debug("update_by_source", extra={"vm": vm})
    #             # print("here:7")
    #             variablemap = vm["variablemap"]
    #             # print("here:8")
    #             self.logger.debug("update_by_source", extra={"source_id": source_id, "vm": variablemap.keys()})
    #             # print("here:9")
    #             await self.update_variableset_by_source(variablemap=variablemap, source_id=source_id, source_data=source_data)
    #             # print("here:10")

    #     except Exception as e:
    #         # print("here:11")

    #         self.logger.error("update_by_source", extra={"reason": e})
    #         # print("here:12")

    # async def update_variableset_by_source(self, variablemap:dict, source_id:str, source_data:CloudEvent):

    #     try:
    #         print(f"update_variableset_by_source: variablemap = {variablemap}")
    #         self.logger.debug("update_variableset_by_source", extra={"source_id": source_id})
    #         source_time = source_data.data["variables"]["time"]["data"]
    #         self.logger.debug("update_variableset_by_source", extra={"source_time": source_time})
    #         for k,v in variablemap.items():
    #             print(f"***variablemap[{k}] = {v}")
    #             if k == "source_id" and v == source_id:
    #                 print("***YES***")
            
    #         if source_id not in variablemap["sources"]:
    #             print(f"!!! source_id: {source_id} not in variablemap[sources]: {variablemap['sources'].keys()}")
    #             return
            
    #         # print(f"update_variableset_by_source: {variablemap['sources']}")
    #         for src_xref in variablemap["sources"][source_id]:
    #             self.logger.debug("update_variableset_by_source", extra={"source_xref": src_xref})
    #             print(f"update_variableset_by_source: variablemap = {variablemap}")
    #             for k in variablemap.keys():
    #                 print(f"update_variableset_by_source: variablemap[{k}] = {variablemap[k]}")
    #             variableset = variablemap["variablesets"][src_xref["variableset"]]
    #             index_type = variableset["attributes"]["index_type"]["data"]
    #             index_value = variableset["attributes"]["index_value"]["data"]
    #             # if index_type not in variablemap["indexed"]["data"]:
    #             if index_type not in variablemap["indexed"]:
    #                 # variablemap["indexed"]["data"][index_type] = dict()
    #                 variablemap["indexed"][index_type] = dict()
    #             # if index_value not in variablemap["indexed"]["data"][index_type]:
    #             if index_value not in variablemap["indexed"][index_type]:
    #                 # variablemap["indexed"]["data"][index_type][index_value] = dict()
    #                 variablemap["indexed"][index_type][index_value] = dict()
    #             if "variablesets" not in variablemap["indexed"][index_type][index_value]:
    #                 variablemap["indexed"][index_type][index_value]["variablesets"] = []
    #             if "data" not in variablemap["indexed"][index_type][index_value]:
    #                 variablemap["indexed"][index_type][index_value]["data"] = dict()

    #             if index_type == "time":
    #                 self.logger.debug("update_variableset_by_source", extra={"index_value": index_value, "source_time": source_time})

    #                 indexed_time = await self.get_indexed_time_value(
    #                     index_time=index_value,
    #                     source_time=source_time)
    #                 self.logger.debug("update_variableset_by_source", extra={"indexed_time": indexed_time})
                    
    #                 # if indexed_time not in variablemap["indexed"]["data"][index_type][index_value]:
    #                 #     variablemap["indexed"]["data"][index_type][index_value][indexed_time] = dict()
    #                 if indexed_time not in variablemap["indexed"][index_type][index_value]["data"]:
    #                     variablemap["indexed"][index_type][index_value]["data"][indexed_time] = dict()
    #                 print(f"update_variableset_by_source: variablemap = {variablemap}")
                    
    #                 # if (vs_name:=src_xref["variableset"]) not in variablemap["indexed"]["data"][indexed_time]:
    #                 #     variablemap["indexed"]["data"][indexed_time][vs_name] = dict()
    #                 vs_name = src_xref["variableset"]
    #                 if vs_name not in variablemap["indexed"][index_type][index_value]["variablesets"]:
    #                     variablemap["indexed"][index_type][index_value]["variablesets"].append(vs_name)

    #                 map_type = src_xref["map_type"]
    #                 if map_type not in variablemap["indexed"][index_type][index_value]["data"][indexed_time]:
    #                     variablemap["indexed"][index_type][index_value]["data"][indexed_time][map_type] = dict()
    #                 if vs_name not in variablemap["indexed"][index_type][index_value]["data"][indexed_time][map_type]:
    #                     variablemap["indexed"][index_type][index_value]["data"][indexed_time][map_type][vs_name] = dict()
    #                 print(f"update_variableset_by_source: variablemap = {variablemap}")

    #                 # if src_xref["map_type"] == "direct":
    #                 #     # if "direct" not in variablemap["indexed"]["data"][indexed_time][vs_name]:
    #                 #     #     variablemap["indexed"]["data"][indexed_time][vs_name]["direct"] = dict()
    #                 #     if "direct" not in variablemap["indexed"][indexed_time][index_type][index_value]["data"][indexed_time]:
    #                 #         variablemap["indexed"][index_type][index_value]["data"][indexed_time]["direct"] = dict()
    #                 #     if vs_name not in variablemap["indexed"][index_type][index_value]["data"][indexed_time]["direct"][vs_name]:
    #                 #         variablemap["indexed"][index_type][index_value]["data"][indexed_time]["direct"][vs_name] = dict()

                    
    #                 if map_type == "direct":
    #                     print(f"update_variableset_by_source: vs_name = {vs_name}")
    #                     direct_map = variablemap["indexed"][index_type][index_value]["data"][indexed_time][map_type][vs_name]
    #                     self.logger.debug("update_variableset_by_source", extra={"direct_map": direct_map})
    #                     if (v_name:=src_xref["variable"]) not in direct_map:
    #                         direct_map[v_name] = []
    #                     self.logger.debug("update_variableset_by_source", extra={"direct_map": direct_map})
    #                     self.logger.debug("update_variableset_by_source", extra={"variablemap": variablemap["variablesets"]})
    #                     source_v = variablemap["variablesets"][vs_name]["variables"][v_name]["attributes"]["source_variable"]["data"]
    #                     self.logger.debug("update_variableset_by_source", extra={"source_data": source_data.data})
    #                     direct_map[v_name].append(
    #                         source_data.data["variables"][source_v]["data"]
    #                     )
    #                     self.logger.debug("update_variableset_by_source", extra={"direct_map": direct_map})

    #                 # self.logger.debug("update_variableset_by_source", extra={"vm": variablemap["indexed"]["data"][indexed_time][vs_name]["direct"][v_name]})
    #                 # print(f'!!!source_data: {variablemap["indexed"]["data"][indexed_time][vs_name]["direct"][v_name]}')
    #                 # print(f'!!!source_data: {variablemap["indexed"]["data"]}')

    #     except Exception as e:
    #         self.logger.error("update_variableset_by_source", extra={"reason": e})


    # async def update_variable_by_id(
    #     # self, vs_map: dict, source_id: str, source_data: CloudEvent
    #     self, vs_id: str, vs_variable: str, source_data: CloudEvent
    # ):

    #     try:
    #         # for vs_id, var_map in vs_map.items():
    #         #     # parts = vs_id.split("::")
    #         #     vs_cfg_time = self.get_id_components(vs_id=vs_id)["valid_config_time"]
    #         #     # vs_cfg_time = parts[2]

    #         #     source_time = source_data.data["variables"]["time"]["data"]
    #         #     target_vs_id = await self.get_valid_variableset_id_by_time(
    #         #         variableset_id=vs_id, source_time=source_time
    #         #     )

    #         #     if vs_id != target_vs_id:
    #         #         continue

    #         source_time = source_data.data["variables"]["time"]["data"]
    #         # vs = self.variablesets["variablesets"][vs_id]["data"]
    #         vm = vs["attributes"]["variablemap_id"]
    #         vs = self.variablesets["variablesets"][vs_id]
    #         vm = vs["data"]["attributes"]["variablemap_id"]
    #         vm = self.variablemaps[vs["attributes"]["variablemap_id"]]["data"]

    #         vs_var = vs["data"]["variables"][vs_variable]

    #         # direct can use details from variableset, if other, need to use variablemap?
    #         if vs_var["map_type"] == "direct":
    #             # direct_var = vs_var["direct_value"]["source_variable"]
    #             source_var = vs_var["source_variable"]
    #             # source_var = source_data.data["variables"][source_var]
    #             index_type = vs["data"]["attributes"]["index_type"]
    #             index_value = vs["data"]["attributes"]["index_value"]
    #             if index_type == "time":
    #                 indexed_value = await self.get_indexed_time_value(
    #                     index_time=vs["data"]["attributes"]["index_value"],
    #                     source_time=source_time)
                
    #             if index_type not in vs["index"]:
    #                 vs["index"][index_type]
    #             if index_value not in vs["index"][index_type]:
    #                 vs["index"][index_type][index_value] = index_value
    #             if vs_variable not in vs["index"][index_type][index_value][index_value]:
    #                 vs["index"][index_type][index_value][index_value][vs_variable] = []
    #             vs["index"][index_type][index_value][index_value][vs_variable].append(
    #                 source_data.data["variables"][source_var]["data"]
    #             )
                

    #         #     # get index value
    #         #     index = variablegroup[vg_name]["index"]
    #         #     index_type = vs["attributes"]["index_type"]
    #         #     # index_value = vs["attributes"]["index_type"]
    #         #     index_value = await self.get_index_value(
    #         #         index=index, source_data=source_data
    #         #     )
    #         #     if index_value not in mapped_var["data"]:
    #         #         mapped_var["data"][index_value] = []

    #         #     # append data
    #         #     mapped_var["data"][index_value].append(
    #         #         source_data.data["variables"][source_var]["data"]
    #         #     )



    #         # parts = vs_id.split(":")
    #         # vm_name = parts[0]
    #         # vg_name = parts[1]
    #         # var_name = parts[3]

    #         # target_time = source_data.data["variables"]["time"]["data"]
    #         # target_vm = await self.get_variablemap_by_revision_time(
    #         #     variablemap=vm_name, target_time=target_time
    #         # )

    #         # # variablegroup = self.platform_variablesets["maps"][vm_name]["variablegroups"][vg_name]
    #         # variablegroup = target_vm["variablegroups"][vg_name]
    #         # # mapped_var = self.platform_variablesets["maps"][vm_name]["variablegroups"][vg_name]["variables"][var_name]
    #         # mapped_var = variablegroup["variables"][var_name]
    #         # if mapped_var["map_type"] == "direct":
    #         #     direct_var = mapped_var["direct_value"]["source_variable"]
    #         #     source_var = mapped_var["source"][direct_var]["source_variable"]
    #         #     # source_var = source_data.data["variables"][source_var]

    #         #     # get index value
    #         #     index = variablegroup[vg_name]["index"]
    #         #     index_value = await self.get_index_value(
    #         #         index=index, source_data=source_data
    #         #     )
    #         #     if index_value not in mapped_var["data"]:
    #         #         mapped_var["data"][index_value] = []

    #         #     # append data
    #         #     mapped_var["data"][index_value].append(
    #         #         source_data.data["variables"][source_var]["data"]
    #         #     )

    #     except Exception as e:
    #         self.logger.error("update_variable_by_id", extra={"reason": e})

    # async def get_indexed_value(self, index_type: str, index_value, source_val:str):
    #     if index_type == "time":
    #         return await self.get_indexed_time_value(source_time=source_val)
    #     else:
    #         return None


    # async def get_indexed_time_value(self, index_time: int, source_time: str):

    #     # source_time = source_data.data["variables"]["time"]["data"]
        
    #     indexed_time = self.get_timebase_period(
    #         dt_string=source_time, timebase=index_time
    #     )
    #     return indexed_time

    # async def get_index_value(self, index: dict, source_time: str):

    #     if index["index_type"] == "time":
    #         tb = index["index_value"]

    #         # source_time = source_data.data["variables"]["time"]["data"]
    #         tb_time = self.round_to_nearest_N_seconds(
    #             dt_string=source_time, timebase=tb
    #         )

    # def get_timebase_period(self, dt_string: str, timebase: int) -> str:
    #     self.logger.debug("get_timebase_period", extra={"dt_string": dt_string, "timebase": timebase})
    #     dt = string_to_datetime(dt_string)
    #     self.logger.debug("get_timebase_period", extra={"dt": dt})
    #     dt_period = round_to_nearest_N_seconds(dt=dt, Nsec=timebase)
    #     self.logger.debug("get_timebase_period", extra={"dt_period": dt_period})
    #     self.logger.debug("get_timebase_period", extra={"period": datetime_to_string(dt_period)})
    #     if dt_period:
    #         return datetime_to_string(dt_period)
    #     else:
    #         return ""


    # async def index_time_monitor(self, timebase: int):

    #     # while True:
    #     #     dt = get_datetime()
    #     #     self.logger.debug("index_time_monitor", extra={"current_dt": dt, "timebase": timebase})
    #     #     self.logger.debug("index_time_monitor", extra={"current_dt_period": round_to_nearest_N_seconds(dt=dt, Nsec=timebase)})
    #     #     await asyncio.sleep(1)

    #     try:
    #         current_dt_period = round_to_nearest_N_seconds(dt=get_datetime(), Nsec=timebase)
    #         self.logger.debug("index_time_monitor", extra={"current_dt": current_dt_period, "timebase": timebase})
    #         last_dt_period = None
    #         if timebase <= 5:
    #             threshhold_direct = 0.75 * timebase
    #             threshhold_final = 0.9 * timebase
    #             update_threshhold = 0.8 * timebase
    #         else:
    #             threshhold_direct = 0.6 * timebase
    #             threshhold_final = 0.75 * timebase
    #             update_threshhold = 0.7 * timebase
    #     except Exception as e:
    #         self.logger.error("index_time_monitor-init", extra={"reason": e})

    #     while True:
    #         try:
    #             # create timestamp for current interval and save to index values
    #             # self.logger.debug("index_time_monitor: here")
    #             dt_period = round_to_nearest_N_seconds(dt=get_datetime(), Nsec=timebase)
    #             # self.logger.debug("index_time_monitor", extra={"dt_period": dt_period})

    #             # self.logger.debug("index_time_monitor", extra={"timebase": timebase, "current_dt": current_dt_period, "dt_period": dt_period, "last_dt": last_dt_period})
    #             if dt_period != current_dt_period:
    #                 # self.logger.debug("index_time_monitor", extra={"timebase": timebase, "current_dt": current_dt_period, "last_dt": last_dt_period})
    #                 last_dt_period = current_dt_period
    #                 current_dt_period = dt_period
    #                 # self.logger.debug("index_time_monitor", extra={"timebase": timebase, "current_dt": current_dt_period, "last_dt": last_dt_period})
    #             # await asyncio.sleep(1)
    #             # continue
    #             # # get list of vs from self.variablesets["indices"]["time"][timebase]
    #             # get all? valid variable maps based on time?
    #             # loop through list of vs in each valid vm and update if index_type and value match


    #             # current_time_period = timestamp_to_string(current_dt_period)
    #             # target_vm = await self.get_variablemap_by_revision_time(
    #             #     variablemap=variablemap, target_time=current_time_period
    #             # )

    #             # if current_time_period not in self.platform_variablesets["maps"][variablemap]["indices"]["timebase"][timebase]:
    #             # if current_time_period not in target_vm["indices"]["timebase"][timebase]:
    #             #     # self.platform_variablesets["maps"][variablemap]["indices"]["timebase"][timebase].append(current_time_period)
    #             #     target_vm["indices"]["timebase"][timebase].append(current_time_period)

    #             # check if current time is greater than threshold to create previous interval variableset
    #             #   e.g., if tb=1, wait for next second, if tb>1, wait for 0.6*tb to pass (tb=10, wait for 6sec to pass)
    #             if last_dt_period:
    #                 # self.logger.debug("index_time_monitor", extra={"timebase": timebase, "current_dt": current_dt_period, "last_dt": last_dt_period})
    #                 if seconds_elapsed(initial_dt=last_dt_period) >= update_threshhold:
    #                     self.logger.debug("index_time_monitor", extra={"timebase": timebase, "current_dt": current_dt_period, "last_dt": last_dt_period})
    #                     last_time_period = datetime_to_string(last_dt_period)
    #                     update = {
    #                         # "variablemap": variablemap,
    #                         # "variablemap_revision_time": target_vm["revision-time"],
    #                         "index_type": "time",
    #                         "index_value": timebase,
    #                         "update_type": "update",
    #                         "index_ready": last_time_period,
    #                     }                   
    #                     self.logger.debug("index_time_monitor", extra={"upate": update})
    #                     await self.index_ready_buffer.put(update)
    #                     last_dt_period = None

    #                 # if seconds_elapsed(initial_dt=last_dt_period) >= threshhold_direct:
    #                 #     update = {
    #                 #         # "variablemap": variablemap,
    #                 #         # "variablemap_revision_time": target_vm["revision-time"],
    #                 #         "index_type": "time",
    #                 #         "index_value": timebase,
    #                 #         "update_type": "direct",
    #                 #         "index_ready": last_time_period,
    #                 #     }
    #                 #     # await self.direct_timebase_ready_buffer.put(last_time_period)
    #                 #     await self.index_ready_buffer.put(update)

    #                 # elif seconds_elapsed(initial_dt=last_dt_period) >= threshhold_final:
    #                 #     update = {
    #                 #         # "variablemap": variablemap,
    #                 #         # "variablemap_revision_time": target_vm["revision-time"],
    #                 #         "index_type": "time",
    #                 #         "index_value": timebase,
    #                 #         "update_type": "final",
    #                 #         "index_ready": last_time_period,
    #                 #     }
    #                     # await self.direct_timebase_ready_buffer.put(last_time_period)
    #                     # await self.index_ready_buffer.put(update)
    #         except Exception as e:
    #             self.logger.error("index_time_monitor", extra={"reason": e})
        
    #             # await asyncio.sleep(time_to_next(timebase))
    #         await asyncio.sleep(0.1)

    # async def get_valid_variableset_id_by_time(self, variableset_id:str, source_time: str = "") -> str:
        
    #     if source_time == "":
    #         source_time = get_datetime_string()

    #     vs_parts=variableset_id.split("::")
    #     vs_valid_config_time = vs_parts[2]

    #     current = ""
    #     for vm_id in self.variablemaps["maps"].keys():
    #         vm_parts = vm_id.split("::")
    #         vm_valid_config_time = vm_parts[2]
    #         if source_time > vm_valid_config_time and vm_valid_config_time > current:
    #             current = vm_valid_config_time
        
    #     if current == "":
    #         return ""
        
    #     vs_parts[2] = current
    #     return "::".join(vs_parts)
    
    #     # for vm_id self.variablemaps["maps"].keys():
    #     #     if target_time > cfg_time and cfg_time > current:
    #     #         current = cfg_time

    #     # if current == "":
    #     #     return None
       

    # async def get_variablemap_by_revision_time(
    #     self, variablemap: str, target_time: str = ""
    # ) -> dict:
    #     if target_time == "":
    #         target_time = get_datetime_string()

    #     current = ""
    #     for cfg_time, vm in self.variablesets["maps"][variablemap].items():
    #         if target_time > cfg_time and cfg_time > current:
    #             current = cfg_time

    #     if current == "":
    #         return None

    #     return self.variablesets["maps"][variablemap][current]


    # async def update_direct_variable_by_time_index(self, variablemap:dict, variableset_name:str, variableset_record:dict, variable_name:str, time_index: dict):
    #     pass
    #     try:

    #         index_type = time_index["index_type"]
    #         index_value = time_index["index_value"]
    #         update_type = time_index["update_type"]
    #         target_time = time_index["index_ready"]

    #         # variableset = variablemap["variablesets"][variableset_name]
    #         # indexed_data = variablemap["indexed"]["data"][time_index["index_ready"]][variableset_name]
    #         indexed_data = variablemap["indexed"][index_type][index_value]["data"][target_time][variableset_name]
            
    #         #TODO handle multi-d data
    #         if len(indexed_data) == 0:
    #             if variableset_record["type"] in ["string", "str", "char"]:
    #                 val = ""
    #             else:
    #                 val = None
    #         elif len(indexed_data) == 1:
    #             val = indexed_data[0]
    #         else:
    #             if variableset_record["type"] in ["string", "str", "char"]:
    #                 val = indexed_data[0]
    #             else:
    #                 val = round(
    #                     sum(indexed_data)
    #                     / len(indexed_data),
    #                     3,
    #                 )

    #         variableset_record[variable_name]["data"] = val
    #     except Exception as e:
    #         self.logger.error("update_direct_variable_by_time_index", extra={"reason": e})

    #     return

    # async def update_variablesets_by_time_index(self, variablemap:dict, time_index: dict):

    #     variable_updates = {
    #         "direct": self.update_direct_variable_by_time_index,
    #     }

    #     try:
    #         self.logger.debug("update_variablesets_by_time_index", extra={"time_index": time_index})
    #         # print(f"update_variablesets_by_time_index: {variablemap}")
    #         # vm_name = time_index["variablemap"]
    #         # vm_cfg_time = time_index["variablemap_revision_time"]
    #         # target_vm = self.variablesets["maps"][vm_name][vm_cfg_time]
    #         # index_value = time_index["index_value"]
    #         index_type = time_index["index_type"]
    #         index_value = time_index["index_value"]
    #         update_type = time_index["update_type"]
    #         target_time = time_index["index_ready"]

    #         if target_time not in variablemap["indexed"][index_type][index_value]["data"]:
    #             return
            
    #         target_variablesets = variablemap["indexed"][index_type][index_value]["data"][target_time]

    #         for map_type in ["direct", "priority", "aggregate", "calculated"]: #direct, calculated, priority and aggregate
    #             if map_type not in target_variablesets:
    #                 continue

    #             self.logger.debug("update_variablesets_by_time_index", extra={"map_type": map_type})

    #             for vs_name, vs_data in target_variablesets[map_type].items():
    #                 variableset = variablemap["variablesets"][vs_name].copy()
    #                 for v_name, v_data in vs_data.items():
    #                     variable_updates[map_type](
    #                         variablemap=variablemap,
    #                         variableset_name=vs_name,
    #                         variableset_record=variableset,
    #                         variable_name=v_name,
    #                         time_index=time_index
    #                         )



    #         #         indexed_data = variablemap["indexed"]["data"][target_time][vs_name]
    #         #         variableset = variablemap["variablesets"][vs_name].copy()

    #         # for vs_name in variablemap["indexed"][index_type][index_value]:
    #         #     if target_time not in variablemap["indexed"]["data"] or vs_name not in variablemap["indexed"]["data"][target_time]:
    #         #         continue
    #         #     indexed_data = variablemap["indexed"]["data"][target_time][vs_name]
    #         #     variableset = variablemap["variablesets"][vs_name].copy()

    #         #     for map_type in ["direct", "priority", "aggregate", "calculated"]: #direct, calculated, priority and aggregate
    #         #         for v_name, v in variableset["variables"].items():
    #         #             if v["map_type"] == map_type:
    #         #                 variable_updates[map_type](variablemap=variablemap, variableset_name=vs_name, variableset_record=variableset, variable_name=v_name, time_index=time_index)
    #         #                 # if map_type == "direct":
    #         #                 #     self.update_direct_variable_by_time_index(variableset=variableset, time_index=time_index)
    #         #                 # elif map_type == "priority":
    #         #                 #     continue
    #         #                 # elif map_type == "aggregate":
    #         #                 #     continue
    #         #                 # elif map_type == "calulated":
    #         #                 #     continue


    #                 self.logger.debug("update_variablesets_by_time_index", extra={"vs_record": variableset})
                    
    #                 varset_id = self.get_variableset_id(variableset_name=vs_name, variableset=variableset)
    #                 source_id = (
    #                     f"envds.{self.config.daq_id}.variableset::{varset_id}"
    #                 )
    #                 source_topic = source_id.replace(".", "/")
    #                 if variableset:
    #                     event = SamplingEvent.create_variableset_data_update(
    #                         # source="sensor.mockco-mock1-1234", data=record
    #                         source=source_id,
    #                         data=variableset,
    #                     )
    #                     destpath = f"{source_topic}/data/update"
    #                     event["destpath"] = destpath
    #                     self.logger.debug(
    #                         "update_variablesets_by_time_index",
    #                         extra={"data": event, "destpath": destpath},
    #                     )
                        
    #                     await self.send_event(event)

    #         # Once processed, remove indexed data
    #         self.logger.debug("update_variablesets_by_time_index", extra={"indexed_data": variablemap["indexed"][index_type][index_value]["data"]})
    #         variablemap["indexed"][index_type][index_value]["data"].pop(target_time,None)
    #         self.logger.debug("update_variablesets_by_time_index", extra={"indexed_data": variablemap["indexed"][index_type][index_value]["data"]})


    #         #     if update_type == "direct" and update_type in indexed_data:
    #         #         if v_name not in indexed_data[update_type]:
    #         #             continue



    #         #         variablemap["indexed"]["data"][indexed_time][vs_name]["direct"][v_name].append(
    #         #             source_data.data["variables"][source_v]["data"]

    #         # # for vg in self.platform_variablesets["maps"][vm_name]["indices"][index_type][index_value]["variablegroups"]:
    #         # for vg in target_vm["indices"][index_type][index_value]["variablegroups"]:
    #         #     var_set = {
    #         #         "attributes": {
    #         #             "variablemap": {"type": "string", "data": vm_name},
    #         #             "variablemap_revision_time": {
    #         #                 "type": "string",
    #         #                 "data": vm_cfg_time,
    #         #             },
    #         #             "variablegroup": {"type": "string", "data": vg},
    #         #             "index_type": {"type": "string", "data": index_type},
    #         #             "index_value": {"type": "int", "data": index_value},
    #         #         },
    #         #         "dimensions": {"time": 1},
    #         #         "variables": {},
    #         #     }

    #         #     time_var = {
    #         #         "type": "str",
    #         #         "shape": ["time"],
    #         #         "attributes": {
    #         #             # how to make sure these are always using proper config?
    #         #             "variablemap": {"type": "string", "data": vm_name},
    #         #             "variablemap_revision_time": {
    #         #                 "type": "string",
    #         #                 "data": vm_cfg_time,
    #         #             },
    #         #             "variablegroup": {"type": "string", "data": vg},
    #         #             "index_type": {"type": "string", "data": index_type},
    #         #             "index_value": {"type": "int", "data": index_value},
    #         #         },
    #         #         "data": target_time,
    #         #     }
    #         #     var_set["variables"]["time"] = time_var

    #         #     # for name, variable in self.platform_variablesets["maps"][vm_name]["indices"][index_type][index_value]["variablegroups"][vg]["variables"].items():
    #         #     for name, variable in target_vm["indices"][index_type][index_value][
    #         #         "variablegroups"
    #         #     ][vg]["variables"].items():
    #         #         map_type = variable["map_type"]
    #         #         source = variable["source"]
    #         #         index_method = variable["index_method"]
    #         #         attributes = variable["attributes"]
    #         #         if map_type == "direct":
    #         #             source_variable = variable["direct_value"]["source_variable"]

    #         #             mapped_var = {
    #         #                 "type": "float",
    #         #                 "shape": ["time"],
    #         #                 "attributes": {
    #         #                     # how to make sure these are always using proper config?
    #         #                     "source_type": {
    #         #                         "type": "string",
    #         #                         "data": source[source_variable]["source_type"],
    #         #                     },
    #         #                     "source_id": {
    #         #                         "type": "string",
    #         #                         "data": source[source_variable]["source_id"],
    #         #                     },
    #         #                     "source_variable": {
    #         #                         "type": "string",
    #         #                         "data": source[source_variable]["source_variable"],
    #         #                     },
    #         #                 },
    #         #             }

    #         #             if len(variable["data"][index_value]) == 0:
    #         #                 if variable["type"] in ["string", "str", "char"]:
    #         #                     val = ""
    #         #                 else:
    #         #                     val = None
    #         #             elif len(variable["data"][index_value]) == 1:
    #         #                 val = variable["data"][index_value][0]
    #         #             else:
    #         #                 if variable["type"] in ["string", "str", "char"]:
    #         #                     val = variable["data"][index_value][0]
    #         #                 else:
    #         #                     val = round(
    #         #                         sum(variable["data"][index_value])
    #         #                         / len(variable["data"][index_value]),
    #         #                         3,
    #         #                     )
    #         #             mapped_var["data"] = val

    #         #             var_set["variables"][name] = mapped_var

    #         #             varset_id = f"{vm_name}::{vm_cfg_time}::{vg}"
    #         #             source_id = (
    #         #                 f"envds.{self.config.daq_id}.variableset::{varset_id}"
    #         #             )
    #         #             source_topic = source_id.replace(".", "/")
    #         #             if var_set:
    #         #                 event = SamplingEvent.create_variableset_update(
    #         #                     # source="sensor.mockco-mock1-1234", data=record
    #         #                     source=source_id,
    #         #                     data=var_set,
    #         #                 )
    #         #                 destpath = f"{source_topic}/data/update"
    #         #                 event["destpath"] = destpath
    #         #                 self.logger.debug(
    #         #                     "update_timebase_variableset_by_index",
    #         #                     extra={"data": event, "destpath": destpath},
    #         #                 )
    #         #                 # message = Message(data=event, destpath=destpath)
    #         #                 # message = event
    #         #                 # self.logger.debug("default_data_loop", extra={"m": message})
    #         #                 await self.send_event(event)

    #         #         else:
    #         #             continue  # TODO fill in for other types

    #     except Exception as e:
    #         self.logger.error("update_timebase_variableset_by_index", extra={"reason": e})

    # async def index_monitor(self):
        while True: 
            update = await self.index_ready_buffer.get()
            try:
                self.logger.debug("index_monitor", extra={"update": update})
                index_type = update["index_type"]
                index_value = update["index_value"]
                update_type = update["update_type"]
                target_time = update["index_ready"]

                vm_list = await self.get_valid_variablemaps(target_time=target_time)
                self.logger.debug("index_monitor", extra={"len": len(vm_list), "vm_list": vm_list})
                for vm in vm_list:
                    # await self.update_variableset_by_source(variablemap=vm, source_id=source_id, source_data=source_data)
                    print(f"index_monitor:vm = {vm}")
                    variablemap = vm["variablemap"]
                    print(f"index_monitor: variablemap = {variablemap}")

                    index_type = update["index_type"]
                    if index_type == "time":
                        print(f"index_monitor: index_type = {index_type}")
                        await self.update_variablesets_by_time_index(
                            variablemap=variablemap, time_index=update
                        )
                    # # handle timebase update
                    # vm_name = update["variablemap"]
                    # vm_cfg_time =  update["variablemap_revision_time"]
                    # target_vm = self.platform_variablesets["maps"][vm_name][vm_cfg_time]
                    # index_value = update["index_value"]
                    # # for vg in self.platform_variablesets["maps"][vm_name]["indices"][index_type][index_value]["variablegroups"]:
                    # for vg in target_vm["indices"][index_type][index_value]["variablegroups"]:
                    #     var_set = {
                    #         "attributes": {
                    #             "variablemap": {"type": "string", "data": vm_name},
                    #             "variablemap_revision_time": {"type": "string", "data": vm_cfg_time},
                    #             "variablegroup": {"type": "string", "data": vg},
                    #             "index_type": {"type": "string", "data": index_type},
                    #             "index_value": {"type": "int", "data": index_value}
                    #         },
                    #         "dimensions": {"time": 1},
                    #         "variables": {}
                    #     }

                    #     # for name, variable in self.platform_variablesets["maps"][vm_name]["indices"][index_type][index_value]["variablegroups"][vg]["variables"].items():
                    #     for name, variable in target_vm["indices"][index_type][index_value]["variablegroups"][vg]["variables"].items():
                    #         map_type = variable["map_type"]
                    #         source = variable["source"]
                    #         index_method = variable["index_method"]
                    #         attributes = variable["attributes"]
                    #         if map_type == "direct":
                    #             source_variable = variable["direct_value"]["source_variable"]
                    #         else:
                    #             continue # TODO fill in for other types
                    #         mapped_var = {
                    #             "type": "float",
                    #             "shape": ["time"],
                    #             "attributes": {
                    #                 # how to make sure these are always using proper config?
                    #                 "source_type": {"type": "string", "data": source[source_variable]["source_type"]},
                    #                 "source_id": {"type": "string", "data": source[source_variable]["source_id"]},
                    #                 "source_variable": {"type": "string", "data": source[source_variable]["source_variable"]},
                    #             }
                    #         }

                    pass
                else:
                    pass

            except Exception as e:
                self.logger.error("index_monitor", extra={"reason": e})



async def shutdown():
    print("shutting down")
    # for task in task_list:
    #     print(f"cancel: {task}")
    #     task.cancel()


async def main(config):
    config = uvicorn.Config(
        "main:app",
        host=config.host,
        port=config.port,
        # log_level=server_config.log_level,
        root_path="/msp/sampling-system",
        # log_config=dict_config,
    )

    server = uvicorn.Server(config)
    # test = logging.getLogger()
    # test.info("test")
    L.info(f"server: {server}")
    await server.serve()

    print("starting shutdown...")
    await shutdown()
    print("done.")


if __name__ == "__main__":
    # app.run(debug=config.debug, host=config.host, port=config.port)
    # app.run()
    config = OperationsConditionsConfig()
    print(config)
    # asyncio.run(main(config))

    try:
        index = sys.argv.index("--host")
        host = sys.argv[index + 1]
        config.host = host
    except (ValueError, IndexError):
        pass

    try:
        index = sys.argv.index("--port")
        port = sys.argv[index + 1]
        config.port = int(port)
    except (ValueError, IndexError):
        pass

    try:
        index = sys.argv.index("--log_level")
        ll = sys.argv[index + 1]
        config.log_level = ll
    except (ValueError, IndexError):
        pass
    print("going to run(main)")
    asyncio.run(main(config))
