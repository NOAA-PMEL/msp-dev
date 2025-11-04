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


class SamplingSystemConfig(BaseSettings):
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
        env_prefix = "SAMPLING_SYSTEM_"
        case_sensitive = False


class SamplingSystem:
    """docstring for TestClass."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("TestClass instantiated")
        self.logger.setLevel(logging.DEBUG)

        self.platforms = dict()
        self.platform_layouts = dict()
        self.variablemaps = dict()

        # this is cache for variable mapping
        self.variablesets = {
            "sources": dict(),
            "variablesets": dict(),
            "indices": dict(),
        }

        self.config = SamplingSystemConfig()
        self.configure()

        self.mqtt_buffer = asyncio.Queue()
        asyncio.create_task(self.get_from_mqtt_loop())
        asyncio.create_task(self.handle_mqtt_buffer())

        self.index_monitor_tasks = dict()

    def configure(self):
        # set clients

        self.logger.debug("configure", extra={"self.config": self.config})

        try:
            # load resource configmaps
            #   load payloads
            with open("/app/config/platforms.json", "r") as f:
                platforms = json.load(f)

            for platform in platforms:
                if platform["kind"] != "Platform":
                    continue
                if (name := platform["metadata"]["name"]) not in self.platforms:
                    self.platforms[name] = platform
            self.logger.debug("configure", extra={"platforms": self.platforms})
            # load layout configmaps

            # load variablemap configmaps
            with open("/app/config/platform_variablemaps.json", "r") as f:
                variablemaps = json.load(f)

            # TODO allow for multiple configs of a given map that are retrieved from datastore or loaded

            for vm in variablemaps:
                if vm["kind"] != "PlatformVariableMap":
                    continue
                vm_name = vm["metadata"]["name"]
                platform_name = vm["metadata"]["platform"]
                sampling_namespace = vm["metadata"]["sampling_namespace"]
                valid_config_time = vm["metadata"]["valid_config_time"]
                
                if "platform" not in self.variablemaps:
                    self.variablemaps["platform"] = dict()
                if platform_name not in self.variablemaps["platform"]:
                    self.variablemaps["platform"][platform_name] = dict()
                if vm_name not in self.variablemaps["platform"][platform_name]:
                    self.variablemaps["platform"][platform_name][vm_name] = dict()
                if valid_config_time not in self.variablemaps["platform"][platform_name][vm_name]:
                    self.variablemaps["platform"][platform_name][vm_name][valid_config_time] = {
                        "variablemap": vm,
                        "variablesets": dict(),
                        "indexed": dict(),
                        "sources": dict()
                    }

                vm_data = vm["data"]
                current_vm = self.variablemaps["platform"][platform_name][vm_name][valid_config_time]
                for vs_name, vs_def in vm_data["variablesets"].items():
                    if vs_name not in current_vm["variablesets"]:
                        current_vm["variablesets"][vs_name] = {
                            "attributes": dict(),
                            "dimensions": dict(),
                            "variables": dict()
                        }
                    current_vm["variablesets"][vs_name]["attributes"] = vm_data["attributes"].copy()
                    current_vm["variablesets"][vs_name]["attributes"]["index_type"] = vs_def["index"]["index_type"]
                    current_vm["variablesets"][vs_name]["attributes"]["index_value"] = vs_def["index"]["index_value"]
                    current_vm["variablesets"][vs_name]["attributes"]["variablemap_kind"] = vm["kind"]

                    current_vm["variablesets"][vs_name]["dimensions"] = {"time": 0}

                    # add variables for variableset
                    for v_name, v in vm_data["variables"].items():
                        if v["variableset"] == vs_name:
                            current_vm["variablesets"][vs_name]["variables"][v_name] = {
                                "type": v["type"],
                                "shape": v["shape"],
                                "attributes": v["attributes"].copy(),
                            }

                            # add in missing dimensions
                            for sh in v["shape"]:
                                if sh not in current_vm["variablesets"][vs_name]["dimensions"]:
                                    current_vm["variablesets"][vs_name]["dimensions"][sh] = 0

                            # add extra attributes
                            current_v =  current_vm["variablesets"][vs_name]["variables"][v_name]
                            current_v["attributes"]["map_type"] = v["map_type"]
                            if v["map_type"] == "direct":
                                current_v["attributes"]["source_type"] = v["source"]["source_type"]
                                current_v["attributes"]["source_ide"] = v["source"]["source_id"]
                                current_v["attributes"]["source_variable"] = v["source"]["source_variable"]
                                
                                # add x-ref source_id->variable
                                # if "direct" not in current_vm["sources"]:
                                #     current_vm["sources"]["direct"] = dict()
                                if v["source"]["source_id"] not in current_vm["sources"]:
                                    current_vm["sources"][v["source"]["source_id"]] = []
                                source_entry = {"variableset": vs_name, "variable": v_name, "map_type": "direct"}
                                if source_entry not in current_vm["sources"]["direct"][v["source"]["source_id"]]:
                                    current_vm["sources"]["direct"][v["source"]["source_id"]].append(source_entry)

                    # this holds list of variablesets for each index and indexed variable data
                    if vs_def["index"]["index_type"] not in current_vm["indexed"]:
                        current_vm["indexed"][vs_def["index"]["index_type"]] = {
                            vs_def["index"]["index_value"]: {
                                "variablesets": [],
                                "data": dict()
                            }
                        }
                        # add variableset to list for x-ref
                        if vs_name not in current_vm["indexed"][vs_def["index"]["index_type"]][vs_def["index"]["index_value"]]:
                            current_vm["indexed"][vs_def["index"]["index_type"]][vs_def["index"]["index_value"]].append(vs_name)
                
                        
                        # create indexing task for current index if necessary
                        if (index_type:=vs_def["index"]["index_type"]) not in self.index_monitor_tasks:
                            if index_type == "time":
                                index_value = vs_def["index"]["index_value"]
                                self.index_monitor_tasks[index_type] = {
                                    index_value: asyncio.create_task(
                                        self.index_time_monitor(
                                            self,
                                            # variablemap=vm_id,
                                            timebase=index_value,
                                        )
                                    )
                                }



            #     attributes = vm_data["attributes"]
            #     variablemap_type = attributes["variablemap_type"]
            #     if variablemap_type == "Platform":
            #         variablemap_type_id = attributes["platform"]
            #     valid_config_time = attributes["valid_config_time"]
            #     variablemap_id = "::".join(
            #         [variablemap_type_id, vm_id, valid_config_time]
            #     )

            #     revision = attributes["revision"]


            # for vm in variablemaps:
            #     if vm["kind"] != "VariableMap":
            #         continue
            #     vm_id = vm["metadata"]["name"]
            #     attributes = vm["data"]["attributes"]
            #     variablemap_type = attributes["variablemap_type"]
            #     if variablemap_type == "Platform":
            #         variablemap_type_id = attributes["platform"]
            #     valid_config_time = attributes["valid_config_time"]
            #     variablemap_id = "::".join(
            #         [variablemap_type_id, vm_id, valid_config_time]
            #     )

            #     revision = attributes["revision"]

            #     # vm_cfg_time = vm["revision-time"]
            #     # if vm_name not in self.variablemaps["maps"]:
            #     if variablemap_id not in self.variablemaps:
            #         self.variablemaps[variablemap_id] = vm
            #     # if vm_cfg_time not in self.variablemaps[vm_name]:
            #     #     self.variablemaps[name][vm_name][vm_cfg_time] = vm
            #     else:
            #         try:
            #             # replace with newer revision
            #             if (
            #                 self.variablemaps[name]["data"]["attributes"]["revision"]
            #                 < revision
            #             ):
            #                 self.variablemaps[variablemap_id] = vm
            #         except Exception:
            #             # replace anyway as there is something wrong
            #             self.variablemaps[variablemap_id] = vm

            #     # TODO: send event to datastore to save definition

            # # TODO retrieve all variable maps from datastore?

            # # build variableset(s) from latest revision of variablemap(s)
            # for vm_id, vm in self.variablemaps.items():
            #     # for vm_cfg_time, vm in vm.items():
            #     # parts = vm_id.split("::")
            #     # valid_config_time = parts[2]
            #     valid_config_time = self.get_id_components(vm_id=vm_id)["valid_config_time"]

            #     for vs_name, vs in vm["data"]["variablesets"].items():

            #         # make sure it's a known index for now
            #         if vs["index"]["index_type"] != "time":
            #             continue

            #         vs_id = "::".join([vm_id, vs_name])
            #         # vs_id = self.get_variableset_id(vs)
            #         if vs_id not in self.variablesets["variablesets"]:
            #             self.variablesets["variablesets"][vs_id] = dict()
            #             # {
            #             #     "data": dict(),
            #             #     "index": dict()
            #             # }
            #         index_type = vs["index"]["index_type"]
            #         index_value = vs["index"]["index_value"]

            #         current_vs = self.variablesets["variablesets"][vs_id] = {
            #             "apiVersion": "envds.sampling.system/v1",
            #             "kind": "VariableSet",
            #             "metadata": {
            #                 "name": vs_name,
            #                 "sampling_namespace": vm["data"]["attributes"]["sampling_namespace"]
            #             },
            #             "data": dict()
            #         }

            #         current_vs["data"]["attributes"] = {
            #             "variablemap_id": vm_id,
            #             "valid_config_time": vm["data"]["attributes"][
            #                 "valid_config_time"
            #             ],
            #             "revision": vm["data"]["attributes"]["valid_config_time"],
            #             "index_type": index_type,
            #             "index_value": index_value,
            #         }

            #         if index_type == "time":
            #             current_vs["data"]["dimensions"] = {"time": 0}
            #         else:
            #             # don't know what to do with other index types yet
            #             continue

            #         current_vs["data"]["variables"] = dict()

            #         time_var = {
            #             "type": "str",
            #             "shape": ["time"],
            #             "attributes": {
            #                 # how to make sure these are always using proper config?
            #                 "long_name": {"type": "string", "data": "Time"}
            #             },
            #             "data": None,
            #         }
            #         current_vs["data"]["variables"]["time"] = time_var

            #         valid_config_var = {
            #             "type": "str",
            #             "shape": ["time"],
            #             "attributes": {
            #                 # how to make sure these are always using proper config?
            #                 "long_name": {
            #                     "type": "string",
            #                     "data": "Valid Configuration Start Time",
            #                 }
            #             },
            #             "data": None,
            #         }
            #         current_vs["data"]["variables"]["valid_config_time"] = valid_config_var

            #         revision_var = {
            #             "type": "int",
            #             "shape": ["time"],
            #             "attributes": {
            #                 # how to make sure these are always using proper config?
            #                 "long_name": {
            #                     "type": "string",
            #                     "data": "Revision of Valid Configuration Start Time",
            #                 }
            #             },
            #             "data": None,
            #         }
            #         current_vs["data"]["variables"]["revison"] = valid_config_var

            #         for v_name, v in vm["data"]["variables"].items():
            #             if vs_name == v["variableset"]:
            #                 current_vs["data"]["variables"][v_name] = {
            #                     "type": v["type"],
            #                     "shape": v["shape"],
            #                     "attributes": v["attributes"],
            #                     "data": None,
            #                 }
            #                 attibutes = current_vs["data"]["variables"][v_name]["attributes"]

            #                 # add extra dimensions if needed
            #                 for sh in v["shape"]:
            #                     if sh not in current_vs["data"]["dimensions"]:
            #                         current_vs["data"]["dimensions"]["sh"] = 0

            #                 attributes["map_type"] = {
            #                     "type": "string",
            #                     "data": v["map_type"],
            #                 }
            #                 if v["map_type"] == "direct":
            #                     attributes["source_type"] = {
            #                         "type": "string",
            #                         "data": v["source"]["source_type"],
            #                     }
            #                     attributes["source_id"] = {
            #                         "type": "string",
            #                         "data": v["source"]["source_id"],
            #                     }
            #                     attributes["source_variable"] = {
            #                         "type": "string",
            #                         "data": v["source"]["source_variable"],
            #                     }

            #                     v_source_id = v["source"]["source_id"]
            #                     if "direct" not in self.variablesets["sources"]:
            #                         self.variablesets["sources"]["direct"] = dict()
            #                     if v_source_id not in self.variablesets["sources"]:
            #                         self.variablesets["sources"]["direct"][v_source_id] = dict()
            #                     if (
            #                         vs_id
            #                         not in self.variablesets["sources"]["direct"][v_source_id]
            #                     ):
            #                         self.variablesets["sources"]["direct"][v_source_id][vs_id] = {
            #                             "valid_config_time": valid_config_time,
            #                             "variable": v_name
            #                         }

            #         # create indexing task for current index if necessary
            #         if index_type not in self.index_monitor_tasks:
            #             if index_type == "time":
            #                 self.index_monitor_tasks[index_type] = {
            #                     index_value: asyncio.create_task(
            #                         self.index_time_monitor(
            #                             self,
            #                             variablemap=vm_id,
            #                             timebase=index_value,
            #                         )
            #                     )
            #                 }

            #         # TODO: is this what I want to do?
            #         # add variableset to list based on index
            #         if index_type not in self.variablesets["indices"]:
            #             self.variablesets["indices"][index_type] = dict()
            #         if index_value not in self.variablesets["indices"][index_type]:
            #             self.variablesets["indices"][index_type][index_value] = []
            #         if (
            #             vs_id
            #             not in self.variablesets["indices"][index_type][index_value]
            #         ):
            #             vs_id not in self.variablesets["indices"][index_type][
            #                 index_value
            #             ].append(vs_id)

            #     # if vm_id not in self.variablesets["maps"]:
            #     #     self.variablesets["maps"][vm_id] = dict()
            #     # if vm_cfg_time not in self.variablesets["maps"][vm_id]:
            #     #     self.variablesets["maps"][vm_id][vm_cfg_time] = {
            #     #         "variablegroups": dict(),
            #     #         "indices": dict(),
            #     #         # "sources": dict()
            #     #     }

            #     # # add each variable group
            #     # for vg_name, vg in vm["variablegroups"].items():
            #     #     if vg_name not in self.variablesets:
            #     #         self.variablesets["maps"][vm_id][vm_cfg_time][
            #     #             "variablegroups"
            #     #         ][vg_name] = {index: vg["index"], "variables": dict()}

            #     #     index_type = vg["index"]["index_type"]
            #     #     index_value = vg["index"]["index_value"]

            #     #     # create index and add vg_name to index for x-ref
            #     #     if (
            #     #         index_type
            #     #         not in self.variablesets[vm_id][vm_cfg_time][
            #     #             "indices"
            #     #         ]
            #     #     ):
            #     #         self.variablesets["maps"][vm_id][vm_cfg_time][
            #     #             "indices"
            #     #         ][index_type] = dict()

            #     #     # start index monitors
            #     #     if index_type not in self.index_monitor_tasks:
            #     #         self.index_monitor_tasks[index_type] = {
            #     #             index_value: asyncio.create_task(
            #     #                 self.index_timebase_monitor(
            #     #                     self,
            #     #                     variablemap=vm_id,
            #     #                     timebase=index_value,
            #     #                 )
            #     #             )
            #     #         }

            #     #     if (
            #     #         index_value
            #     #         not in self.variablesets["maps"][vm_id][
            #     #             vm_cfg_time
            #     #         ]["indices"][index_type]
            #     #     ):
            #     #         self.variablesets["maps"][vm_id][vm_cfg_time][
            #     #             "indices"
            #     #         ][index_type][index_value] = {
            #     #             "variablegroups": [],
            #     #             "data": [],
            #     #         }
            #     #         if (
            #     #             vg_name
            #     #             not in self.variablesets["maps"][vm_id][
            #     #                 vm_cfg_time
            #     #             ]["indices"][index_type][index_value]["variablegroups"]
            #     #         ):
            #     #             self.variablesets["maps"][vm_id][
            #     #                 vm_cfg_time
            #     #             ]["indices"][index_type][index_value][
            #     #                 "variablegroups"
            #     #             ].append(
            #     #                 vg_name
            #     #             )

            #     #     # for index_name, index_value in vm["variablegroups"][vg_name].items():
            #     #     #     # if index_name not in self.platform_variablesets["maps"][vm_name]["variablegroups"][vg_name]["index"]:
            #     #     #     #     self.platform_variablesets["maps"][vm_name]["variablegroups"][vg_name]["index"][index_name] = index_value
            #     #     #     if index_name not in self.platform_variablesets[vm_name]["indices"]:
            #     #     #         self.platform_variablesets["maps"][vm_name]["indices"][index_name] = dict()
            #     #     #     if index_value not in self.platform_variablesets["maps"][vm_name]["indices"][index_name]:
            #     #     #         self.platform_variablesets["maps"][vm_name]["indices"][index_name][index_value] = []

            #     # # for vg_name in vm["variablegroups"]:
            #     # #     if vg_name not in self.platform_variablesets[vm_name]["variablegroups"]:
            #     # #         self.platform_variablesets[vm_name]["variablegroups"][vg_name] = {
            #     # #             "timebase": vm["variablegroup"]["timebase"],
            #     # #             "variables": dict()
            #     # #         }

            #     # # add variables to each variable group
            #     # for name, variable in vm["variables"].items():
            #     #     vg_name = variable["variablegroup"]
            #     #     if (
            #     #         name
            #     #         not in self.variablesets["maps"][vm_id][
            #     #             vm_cfg_time
            #     #         ]["variablegroups"]["variables"][vg_name]
            #     #     ):
            #     #         self.variablesets["maps"][vm_id][vm_cfg_time][
            #     #             "variablegroups"
            #     #         ][vg_name]["variables"][name] = {
            #     #             "map": variable,  # grab whole thing for now, not sure what we'll need
            #     #             "data": dict(),
            #     #             "value": dict(),
            #     #         }

            #     #     # add source and map_id for x-ref
            #     #     for source_name, source in variable["sources"].items():
            #     #         if (
            #     #             vm_cfg_time
            #     #             not in self.variablesets["sources"]
            #     #         ):
            #     #             self.variablesets["sources"][
            #     #                 vm_cfg_time
            #     #             ] = dict()
            #     #         if (
            #     #             source["source_id"]
            #     #             not in self.variablesets["sources"][
            #     #                 vm_cfg_time
            #     #             ]
            #     #         ):
            #     #             self.variablesets["sources"][vm_cfg_time][
            #     #                 "source_id"
            #     #             ] = []
            #     #         # build map_id
            #     #         map_id = f"{vm_id}::{vg_name}::{name}"
            #     #         if (
            #     #             map_id
            #     #             not in self.variablesets["sources"][
            #     #                 vm_cfg_time
            #     #             ]["source_id"]
            #     #         ):
            #     #             self.variablesets["sources"][vm_cfg_time][
            #     #                 "source_id"
            #     #             ].append(map_id)

        except Exception as e:
            self.logger.error("configure error", extra={"reason": e})

    async def send_event(self, ce):
        try:
            self.logger.debug(ce)  # , extra=template)
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
                async with httpx.AsyncClient() as client:
                    r = await client.post(
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

                if ce["type"] == "envds.data.update":
                    await self.device_data_update(ce)
                elif ce["type"] == "envds.controller.data.update":
                    await self.controller_data_update(ce)

            except Exception as e:
                self.logger.error("handle_mqtt_buffer", extra={"reason": e})

            await asyncio.sleep(0.0001)

    # TODO change this for sampling-system
    async def device_data_update(self, ce: CloudEvent):

        try:
            attributes = ce.data["attributes"]
            # dimensions = ce.data["dimensions"]
            # variables = ce.data["variables"]

            make = attributes["make"]["data"]
            model = attributes["model"]["data"]
            serial_number = attributes["serial_number"]["data"]
            device_id = "::".join([make, model, serial_number])
            if device_id in self.variablesets["sources"]:
                await self.update_variableset_by_source(
                    source_id=device_id, source_data=ce
                )

        except Exception as e:
            self.logger.error("device_data_update", extra={"reason": e})
        pass

    def get_variablemap_id(self, vm:dict):
        try:
            variablemap_type = vm["data"]["attributes"]["variablemap_index_type"]
            if variablemap_type == "Platform":
                variablemap_type_id = vm["data"]["attributes"]["platform"]
            else:
                return ""
            
            # variable_map_type_id = vm["data"]["attributes"]["variablemap_index_type_id"]
            variablemap_name = vm["metadata"]["name"]
            valid_config_time = vm["data"]["attributes"]["valid_config_time"]

            return "::".join([variablemap_type_id, variablemap_name, valid_config_time])
        except Exception as e:
            self.logger.error("get_variable_map_id", extra={"reason": e})
            return ""

    def get_variableset_id(self, vs:dict):
        try:
            variablemap_id = vs["data"]["attributes"]["variablemap_id"]
            
            variableset_name = vs["metadata"]["name"]

            return "::".join([variablemap_id, variableset_name])
        
        except Exception as e:
            self.logger.error("get_variable_map_id", extra={"reason": e})
            return ""

    def get_id_components(self, vm_id:str=None, vs_id:str=None) -> dict:
        try:
            if vs_id:
                parts = vs_id.split("::")
                comp = {
                    "variablemap_index_type_id": parts[0],
                    "variablemap_name": parts[1],
                    "valid_config_time": parts[2],
                    "variableset_name": parts[3],

                }
                return comp
            elif vm_id:
                parts = vm_id.split("::")
                comp = {
                    "variablemap_index_type_id": parts[0],
                    "variablemap_name": parts[1],
                    "valid_config_time": parts[2],
                }
                return comp
        except Exception as e:
            self.logger.error("get_id_components", extra={"reason": e})
        
        return None

    # TODO change this for sampling-system
    async def controller_data_update(self, ce: CloudEvent):

        try:
            attributes = ce.data["attributes"]

            make = attributes["make"]["data"]
            model = attributes["model"]["data"]
            serial_number = attributes["serial_number"]["data"]

            controller_id = "::".join([make, model, serial_number])
            if controller_id in self.variablesets["sources"]:
                await self.update_variableset_by_source(
                    source_id=controller_id, source_data=ce
                )

        except Exception as e:
            self.logger.error("device_data_update", extra={"reason": e})
        pass

    def is_valid_variable_set(self, vs_id: str, time:str) -> bool:
        valid_vs_id = self.get_valid_variableset_id_by_time(self, variableset_id=vs_id, source_time=time)
        return vs_id == valid_vs_id

    async def update_by_source(self, source_id:str, source_data: CloudEvent):
        try:
            source_time = source_data.data["variables"]["time"]["data"]
            vm_list = await self.get_valid_variablemaps(time=source_time)
            for vm in vm_list:
                await self.update_variableset_by_source(variablemap=vm, source_id=source_id, source_data=source_data)

        except Exception as e:
            self.logger.error("update_by_source", extra={"reason": e})

    async def update_variableset_by_source(self, variablemap:dict, source_id:str, source_data:CloudEvent):

        try:
            source_time = source_data.data["variables"]["time"]["data"]

            for src_xref in variablemap["source"][source_id]:
                variableset = variablemap["variablesets"][src_xref["variableset"]]
                index_type = variableset["attributes"]["index_type"]
                index_value = variableset["attributes"]["index_value"]
                if index_type not in variablemap["indexed"]["data"]:
                    variablemap["indexed"]["data"][index_type] = dict()
                if index_value not in variablemap["indexed"]["data"][index_type]:
                    variablemap["indexed"]["data"][index_type][index_value] = dict()

                if index_type == "time":
                    indexed_time = await self.get_indexed_time_value(
                        index_time=index_value,
                        source_time=source_time)
                    
                    if indexed_time not in variablemap["indexed"]["data"][index_type][index_value]:
                        variablemap["indexed"]["data"][index_type][index_value][indexed_time] = dict()

                    if (vs_name:=src_xref["variableset"]) not in variablemap["indexed"]["data"][indexed_time]:
                        variablemap["indexed"]["data"][indexed_time][vs_name] = dict()
                    if src_xref["map_type"] == "direct":
                        if "direct" not in variablemap["indexed"]["data"][indexed_time][vs_name]:
                            variablemap["indexed"]["data"][indexed_time][vs_name]["direct"] = dict()
                        if (v_name:=src_xref["variable"]) not in variablemap["indexed"]["data"][indexed_time][vs_name]["direct"]:
                            variablemap["indexed"]["data"][indexed_time][vs_name]["direct"][v_name] = []
                        source_v = variablemap["variables"][v_name]["attributes"]["soruce_variable"]
                        variablemap["indexed"]["data"][indexed_time][vs_name]["direct"][v_name].append(
                            source_data.data["variables"][source_v]["data"]
                        )

        except Exception as e:
            self.logger.error("update_variableset_by_source", extra={"reason": e})


    # async def update_variableset_by_source(
    #     self, source_id: str, source_data: CloudEvent
    # ):
    #     pass
    #     if source_id not in self.variablesets["sources"]:
    #         return
    #     # for vs_id in self.variablesets["sources"][source_id]:
    #     #     self.update_variable_by_id(
    #     #         vs_id=vs_id, source_id=source_id, source_data=source_data
    #     #     )
    #     self.update_variable_by_id(vs_map=self.variablesets["sources"][source_id], source_id=source_id, source_data=source_data)
    #     # loop through mapped vars in source_id
    #     #
    #     for vs_id, var_map in self.variablesets["sources"]["direct"].items():
    #         source_time = source_data.data["variables"]["time"]["data"]
    #         if self.is_valid_variable_set(vs_id=vs_id, time=source_time):
    #             self.update_variable_by_id(vs_id=vs_id, vs_variable=var_map["variable"], source_data=source_data)

    async def update_variable_by_id(
        # self, vs_map: dict, source_id: str, source_data: CloudEvent
        self, vs_id: str, vs_variable: str, source_data: CloudEvent
    ):

        try:
            # for vs_id, var_map in vs_map.items():
            #     # parts = vs_id.split("::")
            #     vs_cfg_time = self.get_id_components(vs_id=vs_id)["valid_config_time"]
            #     # vs_cfg_time = parts[2]

            #     source_time = source_data.data["variables"]["time"]["data"]
            #     target_vs_id = await self.get_valid_variableset_id_by_time(
            #         variableset_id=vs_id, source_time=source_time
            #     )

            #     if vs_id != target_vs_id:
            #         continue

            source_time = source_data.data["variables"]["time"]["data"]
            # vs = self.variablesets["variablesets"][vs_id]["data"]
            vm = vs["attributes"]["variablemap_id"]
            vs = self.variablesets["variablesets"][vs_id]
            vm = vs["data"]["attributes"]["variablemap_id"]
            vm = self.variablemaps[vs["attributes"]["variablemap_id"]]["data"]

            vs_var = vs["data"]["variables"][vs_variable]

            # direct can use details from variableset, if other, need to use variablemap?
            if vs_var["map_type"] == "direct":
                # direct_var = vs_var["direct_value"]["source_variable"]
                source_var = vs_var["source_variable"]
                # source_var = source_data.data["variables"][source_var]
                index_type = vs["data"]["attributes"]["index_type"]
                index_value = vs["data"]["attributes"]["index_value"]
                if index_type == "time":
                    indexed_value = await self.get_indexed_time_value(
                        index_time=vs["data"]["attributes"]["index_value"],
                        source_time=source_time)
                
                if index_type not in vs["index"]:
                    vs["index"][index_type]
                if index_value not in vs["index"][index_type]:
                    vs["index"][index_type][index_value] = index_value
                if vs_variable not in vs["index"][index_type][index_value][index_value]:
                    vs["index"][index_type][index_value][index_value][vs_variable] = []
                vs["index"][index_type][index_value][index_value][vs_variable].append(
                    source_data.data["variables"][source_var]["data"]
                )
                

            #     # get index value
            #     index = variablegroup[vg_name]["index"]
            #     index_type = vs["attributes"]["index_type"]
            #     # index_value = vs["attributes"]["index_type"]
            #     index_value = await self.get_index_value(
            #         index=index, source_data=source_data
            #     )
            #     if index_value not in mapped_var["data"]:
            #         mapped_var["data"][index_value] = []

            #     # append data
            #     mapped_var["data"][index_value].append(
            #         source_data.data["variables"][source_var]["data"]
            #     )



            # parts = vs_id.split(":")
            # vm_name = parts[0]
            # vg_name = parts[1]
            # var_name = parts[3]

            # target_time = source_data.data["variables"]["time"]["data"]
            # target_vm = await self.get_variablemap_by_revision_time(
            #     variablemap=vm_name, target_time=target_time
            # )

            # # variablegroup = self.platform_variablesets["maps"][vm_name]["variablegroups"][vg_name]
            # variablegroup = target_vm["variablegroups"][vg_name]
            # # mapped_var = self.platform_variablesets["maps"][vm_name]["variablegroups"][vg_name]["variables"][var_name]
            # mapped_var = variablegroup["variables"][var_name]
            # if mapped_var["map_type"] == "direct":
            #     direct_var = mapped_var["direct_value"]["source_variable"]
            #     source_var = mapped_var["source"][direct_var]["source_variable"]
            #     # source_var = source_data.data["variables"][source_var]

            #     # get index value
            #     index = variablegroup[vg_name]["index"]
            #     index_value = await self.get_index_value(
            #         index=index, source_data=source_data
            #     )
            #     if index_value not in mapped_var["data"]:
            #         mapped_var["data"][index_value] = []

            #     # append data
            #     mapped_var["data"][index_value].append(
            #         source_data.data["variables"][source_var]["data"]
            #     )

        except Exception as e:
            self.logger.error("update_variable_by_id", extra={"reason": e})

    async def get_indexed_value(self, index_type: str, index_value, source_val:str):
        if index_type == "time":
            return await self.get_indexed_time_value(source_time=source_val)
        else:
            return None


    async def get_indexed_time_value(self, index_time: int, source_time: str):

        # source_time = source_data.data["variables"]["time"]["data"]
            indexed_time = self.get_timebase_period(
                dt_string=source_time, timebase=index_time
            )
            return indexed_time

    async def get_index_value(self, index: dict, source_time: str):

        if index["index_type"] == "time":
            tb = index["index_value"]

            # source_time = source_data.data["variables"]["time"]["data"]
            tb_time = self.round_to_nearest_N_seconds(
                dt_string=source_time, timebase=tb
            )

    def get_timebase_period(self, dt_string: str, timebase: int) -> str:
        dt = string_to_timestamp(dt_string)
        dt_period = round_to_nearest_N_seconds(dt=dt, Nsec=timebase)
        if dt_period:
            return timestamp_to_string(dt_period)
        else:
            return ""

    # def round_to_nearest_N_seconds(self, dt_string:str, timebase:int) -> str:

    #     try:
    #         dt = string_to_timestamp(dt_string)

    #         # print(f"dt = {dt}")
    #         # Calculate the total number of seconds since the start of the day
    #         total_seconds = (dt - dt.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
    #         # print(f"total_seconds = {total_seconds}")

    #         # Calculate the number of 15-second intervals
    #         intervals = total_seconds / timebase
    #         # print(f"intervals = {intervals}")

    #         # Round to the nearest interval
    #         rounded_intervals = round(intervals)
    #         # print(f"rounded_intervals = {rounded_intervals}")

    #         # Calculate the rounded total seconds
    #         rounded_total_seconds = rounded_intervals * timebase
    #         # print(f"rounded_total_seconds = {rounded_total_seconds}")

    #         # Create a new datetime object with the rounded seconds
    #         rounded_dt = dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(seconds=rounded_total_seconds)
    #         # print(f"rounded_dt = {rounded_dt}")
    #         result = timestamp_to_string(rounded_dt)
    #         self.logger.debug("round_to_nearest_N_seconds", extra={"orig": dt_string, "rounded": result})
    #         # return

    #     except Exception as e:
    #         self.logger.error("round_to_nearest_N_seconds", extra={"reason": e})
    #         result = ""
    #     return result

    async def index_time_monitor(self, timebase: int):
        current_dt_period = round_to_nearest_N_seconds(dt=get_datetime(), Nsec=timebase)

        last_dt_period = None
        if timebase <= 5:
            threshhold_direct = 0.75 * timebase
            threshhold_final = 0.9 * timebase
            update_threshhold = 0.8 * timebase
        else:
            threshhold_direct = 0.6 * timebase
            threshhold_final = 0.75 * timebase
            update_threshhold = 0.7 * timebase

        while True:
            # create timestamp for current interval and save to index values
            dt_period = round_to_nearest_N_seconds(dt=get_datetime(), Nsec=timebase)
            if dt_period != current_dt_period:
                last_dt_period = current_dt_period
                current_dt_period = dt_period

            # # get list of vs from self.variablesets["indices"]["time"][timebase]
            # get all? valid variable maps based on time?
            # loop through list of vs in each valid vm and update if index_type and value match


            # current_time_period = timestamp_to_string(current_dt_period)
            # target_vm = await self.get_variablemap_by_revision_time(
            #     variablemap=variablemap, target_time=current_time_period
            # )

            # if current_time_period not in self.platform_variablesets["maps"][variablemap]["indices"]["timebase"][timebase]:
            # if current_time_period not in target_vm["indices"]["timebase"][timebase]:
            #     # self.platform_variablesets["maps"][variablemap]["indices"]["timebase"][timebase].append(current_time_period)
            #     target_vm["indices"]["timebase"][timebase].append(current_time_period)

            # check if current time is greater than threshold to create previous interval variableset
            #   e.g., if tb=1, wait for next second, if tb>1, wait for 0.6*tb to pass (tb=10, wait for 6sec to pass)
            if last_dt_period:
                last_time_period = timestamp_to_string(last_time_period)
                if seconds_elapsed(initial_dt=last_dt_period) >= update_threshhold:
                    update = {
                        # "variablemap": variablemap,
                        # "variablemap_revision_time": target_vm["revision-time"],
                        "index_type": "time",
                        "index_value": timebase,
                        "update_type": "update",
                        "index_ready": last_time_period,
                    }                   
                    await self.index_ready_buffer.put(update)
                # if seconds_elapsed(initial_dt=last_dt_period) >= threshhold_direct:
                #     update = {
                #         # "variablemap": variablemap,
                #         # "variablemap_revision_time": target_vm["revision-time"],
                #         "index_type": "time",
                #         "index_value": timebase,
                #         "update_type": "direct",
                #         "index_ready": last_time_period,
                #     }
                #     # await self.direct_timebase_ready_buffer.put(last_time_period)
                #     await self.index_ready_buffer.put(update)

                # elif seconds_elapsed(initial_dt=last_dt_period) >= threshhold_final:
                #     update = {
                #         # "variablemap": variablemap,
                #         # "variablemap_revision_time": target_vm["revision-time"],
                #         "index_type": "time",
                #         "index_value": timebase,
                #         "update_type": "final",
                #         "index_ready": last_time_period,
                #     }
                    # await self.direct_timebase_ready_buffer.put(last_time_period)
                    # await self.index_ready_buffer.put(update)

            # await asyncio.sleep(time_to_next(timebase))
            await asyncio.sleep(0.1)

    # async def index_time_monitor_bak(self, variablemap: str, timebase: int):
    #     current_dt_period = round_to_nearest_N_seconds(dt=get_datetime(), Nsec=timebase)

    #     last_dt_period = None
    #     if timebase == 1:
    #         threshhold_direct = 0.75 * timebase
    #         threshhold_other = 0.9 * timebase
    #     else:
    #         threshhold_direct = 0.6 * timebase
    #         threshhold_other = 0.75 * timebase

    #     while True:
    #         # create timestamp for current interval and save to index values
    #         dt_period = round_to_nearest_N_seconds(dt=get_datetime(), Nsec=timebase)
    #         if dt_period != current_dt_period:
    #             last_dt_period = current_dt_period
    #             current_dt_period = dt_period

    #         # # get list of vs from self.variablesets["indices"]["time"][timebase]
    #         # get all? valid variable maps based on time?
    #         # loop through list of vs in each valid vm and update if index_type and value match


    #         current_time_period = timestamp_to_string(current_dt_period)
    #         target_vm = await self.get_variablemap_by_revision_time(
    #             variablemap=variablemap, target_time=current_time_period
    #         )

    #         # if current_time_period not in self.platform_variablesets["maps"][variablemap]["indices"]["timebase"][timebase]:
    #         if current_time_period not in target_vm["indices"]["timebase"][timebase]:
    #             # self.platform_variablesets["maps"][variablemap]["indices"]["timebase"][timebase].append(current_time_period)
    #             target_vm["indices"]["timebase"][timebase].append(current_time_period)

    #         # check if current time is greater than threshold to create previous interval variableset
    #         #   e.g., if tb=1, wait for next second, if tb>1, wait for 0.6*tb to pass (tb=10, wait for 6sec to pass)
    #         if last_dt_period:
    #             last_time_period = timestamp_to_string(last_time_period)
    #             if seconds_elapsed(initial_dt=last_dt_period) >= threshhold_direct:
    #                 update = {
    #                     "variablemap": variablemap,
    #                     "variablemap_revision_time": target_vm["revision-time"],
    #                     "index_type": "timebase",
    #                     "index_value": timebase,
    #                     "threshhold_type": "direct",
    #                     "index_ready": last_time_period,
    #                 }
    #                 # await self.direct_timebase_ready_buffer.put(last_time_period)
    #                 await self.index_ready_buffer.put(update)

    #         # await asyncio.sleep(time_to_next(timebase))
    #         await asyncio.sleep(0.1)

    async def get_valid_variableset_id_by_time(self, variableset_id:str, source_time: str = "") -> str:
        
        if source_time == "":
            source_time = get_datetime_string()

        vs_parts=variableset_id.split("::")
        vs_valid_config_time = vs_parts[2]

        current = ""
        for vm_id in self.variablemaps["maps"].keys():
            vm_parts = vm_id.split("::")
            vm_valid_config_time = vm_parts[2]
            if source_time > vm_valid_config_time and vm_valid_config_time > current:
                current = vm_valid_config_time
        
        if current == "":
            return ""
        
        vs_parts[2] = current
        return "::".join(vs_parts)
    
        # for vm_id self.variablemaps["maps"].keys():
        #     if target_time > cfg_time and cfg_time > current:
        #         current = cfg_time

        # if current == "":
        #     return None
       

    async def get_variablemap_by_revision_time(
        self, variablemap: str, target_time: str = ""
    ) -> dict:
        if target_time == "":
            target_time = get_datetime_string()

        current = ""
        for cfg_time, vm in self.variablesets["maps"][variablemap].items():
            if target_time > cfg_time and cfg_time > current:
                current = cfg_time

        if current == "":
            return None

        return self.variablesets["maps"][variablemap][current]

    # async def update_timebase_variableset_by_index(self, timebase_index: dict):
    #     try:
    #         vm_name = timebase_index["variablemap"]
    #         vm_cfg_time = timebase_index["variablemap_revision_time"]
    #         target_vm = self.variablesets["maps"][vm_name][vm_cfg_time]
    #         index_value = timebase_index["index_value"]
    #         index_type = "timebase"
    #         index_value = timebase_index["index_value"]
    #         target_time = timebase_index["index_ready"]

    #         # for vg in self.platform_variablesets["maps"][vm_name]["indices"][index_type][index_value]["variablegroups"]:
    #         for vg in target_vm["indices"][index_type][index_value]["variablegroups"]:
    #             var_set = {
    #                 "attributes": {
    #                     "variablemap": {"type": "string", "data": vm_name},
    #                     "variablemap_revision_time": {
    #                         "type": "string",
    #                         "data": vm_cfg_time,
    #                     },
    #                     "variablegroup": {"type": "string", "data": vg},
    #                     "index_type": {"type": "string", "data": index_type},
    #                     "index_value": {"type": "int", "data": index_value},
    #                 },
    #                 "dimensions": {"time": 1},
    #                 "variables": {},
    #             }

    #             time_var = {
    #                 "type": "str",
    #                 "shape": ["time"],
    #                 "attributes": {
    #                     # how to make sure these are always using proper config?
    #                     "variablemap": {"type": "string", "data": vm_name},
    #                     "variablemap_revision_time": {
    #                         "type": "string",
    #                         "data": vm_cfg_time,
    #                     },
    #                     "variablegroup": {"type": "string", "data": vg},
    #                     "index_type": {"type": "string", "data": index_type},
    #                     "index_value": {"type": "int", "data": index_value},
    #                 },
    #                 "data": target_time,
    #             }
    #             var_set["variables"]["time"] = time_var

    #             # for name, variable in self.platform_variablesets["maps"][vm_name]["indices"][index_type][index_value]["variablegroups"][vg]["variables"].items():
    #             for name, variable in target_vm["indices"][index_type][index_value][
    #                 "variablegroups"
    #             ][vg]["variables"].items():
    #                 map_type = variable["map_type"]
    #                 source = variable["source"]
    #                 index_method = variable["index_method"]
    #                 attributes = variable["attributes"]
    #                 if map_type == "direct":
    #                     source_variable = variable["direct_value"]["source_variable"]

    #                     mapped_var = {
    #                         "type": "float",
    #                         "shape": ["time"],
    #                         "attributes": {
    #                             # how to make sure these are always using proper config?
    #                             "source_type": {
    #                                 "type": "string",
    #                                 "data": source[source_variable]["source_type"],
    #                             },
    #                             "source_id": {
    #                                 "type": "string",
    #                                 "data": source[source_variable]["source_id"],
    #                             },
    #                             "source_variable": {
    #                                 "type": "string",
    #                                 "data": source[source_variable]["source_variable"],
    #                             },
    #                         },
    #                     }

    #                     if len(variable["data"][index_value]) == 0:
    #                         if variable["type"] in ["string", "str", "char"]:
    #                             val = ""
    #                         else:
    #                             val = None
    #                     elif len(variable["data"][index_value]) == 1:
    #                         val = variable["data"][index_value][0]
    #                     else:
    #                         if variable["type"] in ["string", "str", "char"]:
    #                             val = variable["data"][index_value][0]
    #                         else:
    #                             val = round(
    #                                 sum(variable["data"][index_value])
    #                                 / len(variable["data"][index_value]),
    #                                 3,
    #                             )
    #                     mapped_var["data"] = val

    #                     var_set["variables"][name] = mapped_var

    #                     varset_id = f"{vm_name}::{vm_cfg_time}::{vg}"
    #                     source_id = (
    #                         f"envds.{self.config.daq_id}.variableset::{varset_id}"
    #                     )
    #                     source_topic = source_id.replace(".", "/")
    #                     if var_set:
    #                         event = SamplingEvent.create_variableset_update(
    #                             # source="sensor.mockco-mock1-1234", data=record
    #                             source=source_id,
    #                             data=var_set,
    #                         )
    #                         destpath = f"{source_topic}/data/update"
    #                         event["destpath"] = destpath
    #                         self.logger.debug(
    #                             "update_timebase_variableset_by_index",
    #                             extra={"data": event, "destpath": destpath},
    #                         )
    #                         # message = Message(data=event, destpath=destpath)
    #                         # message = event
    #                         # self.logger.debug("default_data_loop", extra={"m": message})
    #                         await self.send_event(event)

    #                 else:
    #                     continue  # TODO fill in for other types

    #     except Exception as e:
    #         self.logger.error("update_timebase_variableset_by_index", extra={"reason": e})

    async def update_direct_variable_by_time_index(self, variablemap:dict, variableset_name:str, variableset_record:dict, variable_name:str, time_index: dict):
        pass
        try:
            # variableset = variablemap["variablesets"][variableset_name]
            indexed_data = variablemap["indexed"]["data"][time_index["index_ready"]][variableset_name]
            
            #TODO handle multi-d data
            if len(indexed_data) == 0:
                if variableset_record["type"] in ["string", "str", "char"]:
                    val = ""
                else:
                    val = None
            elif len(indexed_data) == 1:
                val = indexed_data[0]
            else:
                if variableset_record["type"] in ["string", "str", "char"]:
                    val = indexed_data[0]
                else:
                    val = round(
                        sum(indexed_data)
                        / len(indexed_data),
                        3,
                    )

            variableset_record[variable_name]["data"] = val
        except Exception as e:
            self.logger.error("update_direct_variable_by_time_index", extra={"reason": e})

        return

    async def update_variablesets_by_time_index(self, variablemap:dict, time_index: dict):

        variable_updates = {
            "direct": self.update_direct_variable_by_time_index,
        }

        try:
            # vm_name = time_index["variablemap"]
            # vm_cfg_time = time_index["variablemap_revision_time"]
            # target_vm = self.variablesets["maps"][vm_name][vm_cfg_time]
            # index_value = time_index["index_value"]
            index_type = time_index["index_type"]
            index_value = time_index["index_value"]
            update_type = time_index["update_type"]
            target_time = time_index["index_ready"]

            for vs_name in variablemap["indexed"][index_type][index_value]:
                if target_time not in variablemap["indexed"]["data"] or vs_name not in variablemap["indexed"]["data"][target_time]:
                    continue
                indexed_data = variablemap["indexed"]["data"][target_time][vs_name]
                variableset = variablemap["variablesets"][vs_name].copy()

                for map_type in ["direct", "priority", "aggregate", "calculated"]: #direct, calculated, priority and aggregate
                    for v_name, v in variableset["variables"].items():
                        if v["map_type"] == map_type:
                            variable_updates[map_type](variablemap=variablemap, variableset_name=vs_name, variableset_record=variableset, variable_name=v_name, time_index=time_index)
                            # if map_type == "direct":
                            #     self.update_direct_variable_by_time_index(variableset=variableset, time_index=time_index)
                            # elif map_type == "priority":
                            #     continue
                            # elif map_type == "aggregate":
                            #     continue
                            # elif map_type == "calulated":
                            #     continue


                self.logger.debug("update_variableset_by_time_index", extra={"vs_record": variableset})
                
                varset_id = self.get_variableset_id(variablemap=variablemap, variableset_name=vs_name)
                source_id = (
                    f"envds.{self.config.daq_id}.variableset::{varset_id}"
                )
                source_topic = source_id.replace(".", "/")
                if variableset:
                    event = SamplingEvent.create_variableset_update(
                        # source="sensor.mockco-mock1-1234", data=record
                        source=source_id,
                        data=variableset,
                    )
                    destpath = f"{source_topic}/data/update"
                    event["destpath"] = destpath
                    self.logger.debug(
                        "update_variableset_by_time_index",
                        extra={"data": event, "destpath": destpath},
                    )
                    # message = Message(data=event, destpath=destpath)
                    # message = event
                    # self.logger.debug("default_data_loop", extra={"m": message})
                    
                    
                    # await self.send_event(event)





            #     if update_type == "direct" and update_type in indexed_data:
            #         if v_name not in indexed_data[update_type]:
            #             continue



            #         variablemap["indexed"]["data"][indexed_time][vs_name]["direct"][v_name].append(
            #             source_data.data["variables"][source_v]["data"]

            # # for vg in self.platform_variablesets["maps"][vm_name]["indices"][index_type][index_value]["variablegroups"]:
            # for vg in target_vm["indices"][index_type][index_value]["variablegroups"]:
            #     var_set = {
            #         "attributes": {
            #             "variablemap": {"type": "string", "data": vm_name},
            #             "variablemap_revision_time": {
            #                 "type": "string",
            #                 "data": vm_cfg_time,
            #             },
            #             "variablegroup": {"type": "string", "data": vg},
            #             "index_type": {"type": "string", "data": index_type},
            #             "index_value": {"type": "int", "data": index_value},
            #         },
            #         "dimensions": {"time": 1},
            #         "variables": {},
            #     }

            #     time_var = {
            #         "type": "str",
            #         "shape": ["time"],
            #         "attributes": {
            #             # how to make sure these are always using proper config?
            #             "variablemap": {"type": "string", "data": vm_name},
            #             "variablemap_revision_time": {
            #                 "type": "string",
            #                 "data": vm_cfg_time,
            #             },
            #             "variablegroup": {"type": "string", "data": vg},
            #             "index_type": {"type": "string", "data": index_type},
            #             "index_value": {"type": "int", "data": index_value},
            #         },
            #         "data": target_time,
            #     }
            #     var_set["variables"]["time"] = time_var

            #     # for name, variable in self.platform_variablesets["maps"][vm_name]["indices"][index_type][index_value]["variablegroups"][vg]["variables"].items():
            #     for name, variable in target_vm["indices"][index_type][index_value][
            #         "variablegroups"
            #     ][vg]["variables"].items():
            #         map_type = variable["map_type"]
            #         source = variable["source"]
            #         index_method = variable["index_method"]
            #         attributes = variable["attributes"]
            #         if map_type == "direct":
            #             source_variable = variable["direct_value"]["source_variable"]

            #             mapped_var = {
            #                 "type": "float",
            #                 "shape": ["time"],
            #                 "attributes": {
            #                     # how to make sure these are always using proper config?
            #                     "source_type": {
            #                         "type": "string",
            #                         "data": source[source_variable]["source_type"],
            #                     },
            #                     "source_id": {
            #                         "type": "string",
            #                         "data": source[source_variable]["source_id"],
            #                     },
            #                     "source_variable": {
            #                         "type": "string",
            #                         "data": source[source_variable]["source_variable"],
            #                     },
            #                 },
            #             }

            #             if len(variable["data"][index_value]) == 0:
            #                 if variable["type"] in ["string", "str", "char"]:
            #                     val = ""
            #                 else:
            #                     val = None
            #             elif len(variable["data"][index_value]) == 1:
            #                 val = variable["data"][index_value][0]
            #             else:
            #                 if variable["type"] in ["string", "str", "char"]:
            #                     val = variable["data"][index_value][0]
            #                 else:
            #                     val = round(
            #                         sum(variable["data"][index_value])
            #                         / len(variable["data"][index_value]),
            #                         3,
            #                     )
            #             mapped_var["data"] = val

            #             var_set["variables"][name] = mapped_var

            #             varset_id = f"{vm_name}::{vm_cfg_time}::{vg}"
            #             source_id = (
            #                 f"envds.{self.config.daq_id}.variableset::{varset_id}"
            #             )
            #             source_topic = source_id.replace(".", "/")
            #             if var_set:
            #                 event = SamplingEvent.create_variableset_update(
            #                     # source="sensor.mockco-mock1-1234", data=record
            #                     source=source_id,
            #                     data=var_set,
            #                 )
            #                 destpath = f"{source_topic}/data/update"
            #                 event["destpath"] = destpath
            #                 self.logger.debug(
            #                     "update_timebase_variableset_by_index",
            #                     extra={"data": event, "destpath": destpath},
            #                 )
            #                 # message = Message(data=event, destpath=destpath)
            #                 # message = event
            #                 # self.logger.debug("default_data_loop", extra={"m": message})
            #                 await self.send_event(event)

            #         else:
            #             continue  # TODO fill in for other types

        except Exception as e:
            self.logger.error("update_timebase_variableset_by_index", extra={"reason": e})

    async def index_monitor(self):
        while True: 
            update = await self.index_ready_buffer.get()
            try:

                index_type = update["index_type"]
                index_value = update["index_value"]
                update_type = update["update_type"]
                target_time = update["index_ready"]

                vm_list = await self.get_valid_variablemaps(time=target_time)
                for vm in vm_list:
                    # await self.update_variableset_by_source(variablemap=vm, source_id=source_id, source_data=source_data)


                    index_type = update["index_type"]
                    if index_type == "time":
                        await self.update_variablesets_by_time_index(
                            self, variablemap=vm, timebase_index=update
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

    # async def device_data_get(self, query: DataStoreQuery):
    # async def device_data_get(self, query: DataRequest):

    #     # fill in useful values based on user request

    #     # why do we need to do this?
    #     # # if not make,model,serial_number try to build from device_id
    #     # self.logger.debug("device_data_get", extra={"query": query})
    #     # if not query.make or not query.model or not query.serial_number:
    #     #     if not query.device_id:
    #     #         self.logger.debug("device_data_get:1", extra={"query": query})
    #     #         return {"results": []}
    #     #     parts = query.device_id.split("::")
    #     #     query.make = parts[0]
    #     #     query.model = parts[1]
    #     #     query.serial_number = parts[2]
    #     #     self.logger.debug("device_data_get:2", extra={"query": query})
    #     # else:
    #     #     query.device_id = "::".join([query.make,query.model,query.serial_number])

    #     self.logger.debug("device_data_get:3", extra={"query": query})
    #     if query.start_time:
    #         query.start_timestamp = string_to_timestamp(query.start_time)

    #     if query.end_time:
    #         query.end_timestamp = string_to_timestamp(query.end_time)

    #     if query.last_n_seconds:
    #         # this overrides explicit start,end times
    #         start_dt = get_datetime_with_delta(-(query.last_n_seconds))
    #         # current_time = get_datetime()
    #         # start_dt = current_time - timedelta(seconds=query.last_n_seconds)
    #         query.start_timestamp = start_dt.timestamp()
    #         query.end_timestamp = None

    #     # TODO add in logic to get/sync from erddap if available
    #     if self.db_client:
    #         self.logger.debug("device_data_get:4", extra={"query": query})
    #         return await self.db_client.device_data_get(query)

    #     return {"results": []}

    # async def device_definition_registry_update(self, ce: CloudEvent):

    #     try:
    #         for definition_type, device_def in ce.data.items():
    #             make = device_def["attributes"]["make"]["data"]
    #             model = device_def["attributes"]["model"]["data"]
    #             format_version = device_def["attributes"]["format_version"]["data"]
    #             parts = format_version.split(".")
    #             version = f"v{parts[0]}"
    #             valid_time = device_def.get("valid_time", "2020-01-01T00:00:00Z")

    #             if definition_type == "device-definition":
    #                 database = "registry"
    #                 collection = "device-definition"
    #                 attributes = device_def["attributes"]
    #                 dimensions = device_def["dimensions"]
    #                 variables = device_def["variables"]

    #                 if "device_type" in device_def["attributes"]:
    #                     device_type = device_def["attributes"]["device_type"]["data"]
    #                 else:
    #                     # default for backward compatibility
    #                     device_def["attributes"]["device_type"] = {
    #                         "type": "string",
    #                         "data": "sensor"
    #                     }
    #                     device_type = "sensor"

    #                 device_definition_id = "::".join([make,model,format_version])
    #                 request = DeviceDefinitionUpdate(
    #                     device_definition_id=device_definition_id,
    #                     make=make,
    #                     model=model,
    #                     version=format_version,
    #                     device_type=device_type,
    #                     valid_time=valid_time,
    #                     attributes=attributes,
    #                     dimensions=dimensions,
    #                     variables=variables,
    #                 )

    #                 # request = DatastoreRequest(
    #                 #     database="registry",
    #                 #     collection="device-definition",
    #                 #     request=update
    #                 # )

    #         self.logger.debug(
    #             "device_definition_registry_update", extra={"request": request}
    #         )
    #         if self.db_client:
    #             result = await self.db_client.device_definition_registry_update(
    #                 database=database,
    #                 collection=collection,
    #                 request=request,
    #                 ttl=self.config.db_reg_device_definition_ttl,
    #             )
    #             if result:
    #                 self.logger.debug("configure", extra={"self.config": self.config})
    #                 ack = DAQEvent.create_device_definition_registry_ack(
    #                     source=f"envds.{self.config.daq_id}.datastore",
    #                     data={"device-definition": {"make": make, "model":model, "version": format_version}}

    #                 )
    #                 # f"envds/{self.core_settings.namespace_prefix}/device/registry/ack"
    #                 ack["destpath"] = f"envds/{self.config.daq_id}/device/registry/ack"
    #                 await self.send_event(ack)

    #     except Exception as e:
    #         self.logger.error("device_definition_registry_update", extra={"reason": e})
    #     pass

    # async def device_definition_registry_get(self, query: DeviceDefinitionRequest) -> dict:

    #     # TODO add in logic to get/sync from erddap if available
    #     if self.db_client:
    #         return await self.db_client.device_definition_registry_get(query)

    #     return {"results": []}

    # async def device_instance_registry_update(self, ce: CloudEvent):

    #     try:
    #         self.logger.debug("device_instance_registry_update", extra={"ce": ce})
    #         for instance_type, instance_reg in ce.data.items():
    #             request = None
    #             self.logger.debug("device_instance_registry_update", extra={"instance_type": instance_type, "instance_reg": instance_reg})
    #             try:
    #                 # device_id = instance_reg.get("device_id", None)
    #                 make = instance_reg["make"]
    #                 model = instance_reg["model"]
    #                 serial_number = instance_reg["serial_number"]
    #                 format_version = instance_reg["format_version"]
    #                 parts = format_version.split(".")
    #                 version = f"v{parts[0]}"

    #                 if make is None or model is None or serial_number is None:
    #                     # if "device_id" in instance_reg and instance_reg["device_id"] is not None:
    #                         # parts = instance_reg["device_id"].split("::")
    #                         # make = parts[0]
    #                         # model = parts[1]
    #                         # serial_number = parts[2]
    #                     self.logger.error("couldn't register instance - missing value", extra={"make": make, "model": model, "serial_number": serial_number})
    #                     return

    #                 # if device_id is None:
    #                 device_id = "::".join([make, model, serial_number])

    #                 if instance_type == "device-instance":
    #                     database = "registry"
    #                     collection = "device-instance"
    #                     attributes = instance_reg["attributes"]

    #                     if "device_type" in instance_reg["attributes"]:
    #                         device_type = instance_reg["attributes"]["device_type"]["data"]
    #                     else:
    #                         # default for backward compatibility
    #                         device_type = "sensor"

    #                     request = DeviceInstanceUpdate(
    #                         device_id=device_id,
    #                         make=make,
    #                         model=model,
    #                         serial_number=serial_number,
    #                         version=format_version,
    #                         device_type=device_type,
    #                         attributes=attributes,
    #                     )

    #             except (KeyError, IndexError) as e:
    #                     self.logger.error("datastore:device_instance_registry_update", extra={"reason": e})
    #                     continue

    #                 # request = DatastoreRequest(
    #                 #     database="registry",
    #                 #     collection="device-instance",
    #                 #     request=update,
    #                 # )

    #             self.logger.debug("datastore:device_instance_registry_update", extra={"request": request, "db_client": self.db_client})
    #             if self.db_client and request:
    #                 await self.db_client.device_instance_registry_update(
    #                     database=database,
    #                     collection=collection,
    #                     request=request,
    #                     ttl=self.config.db_reg_device_instance_ttl,
    #                 )

    #     except Exception as e:
    #         self.logger.error("device_instance_registry_update", extra={"reason": e})
    #     pass

    # async def device_instance_registry_get(self, query: DeviceInstanceRequest) -> dict:

    #     # TODO add in logic to get/sync from erddap if available?
    #     if self.db_client:
    #         return await self.db_client.device_instance_registry_get(query)

    #     return {"results": []}

    # async def controller_data_get(self, query: DataRequest):

    #     # fill in useful values based on user request

    #     # why do we need to do this?
    #     # # if not make,model,serial_number try to build from controller_id
    #     # self.logger.debug("controller_data_get", extra={"query": query})
    #     # if not query.make or not query.model or not query.serial_number:
    #     #     if not query.controller_id:
    #     #         self.logger.debug("controller_data_get:1", extra={"query": query})
    #     #         return {"results": []}
    #     #     parts = query.controller_id.split("::")
    #     #     query.make = parts[0]
    #     #     query.model = parts[1]
    #     #     query.serial_number = parts[2]
    #     #     self.logger.debug("controller_data_get:2", extra={"query": query})
    #     # else:
    #     #     query.controller_id = "::".join([query.make,query.model,query.serial_number])

    #     self.logger.debug("controller_data_get:3", extra={"query": query})
    #     if query.start_time:
    #         query.start_timestamp = string_to_timestamp(query.start_time)

    #     if query.end_time:
    #         query.end_timestamp = string_to_timestamp(query.end_time)

    #     if query.last_n_seconds:
    #         # this overrides explicit start,end times
    #         start_dt = get_datetime_with_delta(-(query.last_n_seconds))
    #         # current_time = get_datetime()
    #         # start_dt = current_time - timedelta(seconds=query.last_n_seconds)
    #         query.start_timestamp = start_dt.timestamp()
    #         query.end_timestamp = None

    #     # TODO add in logic to get/sync from erddap if available
    #     if self.db_client:
    #         self.logger.debug("controller_data_get:4", extra={"query": query})
    #         return await self.db_client.controller_data_get(query)

    #     return {"results": []}

    # async def controller_definition_registry_update(self, ce: CloudEvent):

    #     try:
    #         for definition_type, controller_def in ce.data.items():
    #             make = controller_def["attributes"]["make"]["data"]
    #             model = controller_def["attributes"]["model"]["data"]
    #             format_version = controller_def["attributes"]["format_version"]["data"]
    #             parts = format_version.split(".")
    #             version = f"v{parts[0]}"
    #             valid_time = controller_def.get("valid_time", "2020-01-01T00:00:00Z")

    #             if definition_type == "controller-definition":
    #                 database = "registry"
    #                 collection = "controller-definition"
    #                 attributes = controller_def["attributes"]
    #                 dimensions = controller_def["dimensions"]
    #                 variables = controller_def["variables"]

    #                 # if "controller_type" in controller_def["attributes"]:
    #                 #     controller_type = controller_def["attributes"]["controller_type"]["data"]
    #                 # else:
    #                 #     # default for backward compatibility
    #                 #     controller_def["attributes"]["controller_type"] = {
    #                 #         "type": "string",
    #                 #         "data": "sensor"
    #                 #     }
    #                 #     controller_type = "sensor"

    #                 controller_definition_id = "::".join([make,model,format_version])
    #                 request = ControllerDefinitionUpdate(
    #                     controller_definition_id=controller_definition_id,
    #                     make=make,
    #                     model=model,
    #                     version=format_version,
    #                     # controller_type=controller_type,
    #                     valid_time=valid_time,
    #                     attributes=attributes,
    #                     dimensions=dimensions,
    #                     variables=variables,
    #                 )

    #                 # request = DatastoreRequest(
    #                 #     database="registry",
    #                 #     collection="controller-definition",
    #                 #     request=update
    #                 # )

    #         self.logger.debug(
    #             "controller_definition_registry_update", extra={"request": request}
    #         )
    #         if self.db_client:
    #             result = await self.db_client.controller_definition_registry_update(
    #                 database=database,
    #                 collection=collection,
    #                 request=request,
    #                 ttl=self.config.db_reg_controller_definition_ttl,
    #                 # ttl=self.config.db_reg_device_definition_ttl
    #             )
    #             if result:
    #                 self.logger.debug("configure", extra={"self.config": self.config})
    #                 ack = DAQEvent.create_controller_definition_registry_ack(
    #                     source=f"envds.{self.config.daq_id}.datastore",
    #                     data={"controller-definition": {"make": make, "model":model, "version": format_version}}

    #                 )
    #                 # f"envds/{self.core_settings.namespace_prefix}/controller/registry/ack"
    #                 ack["destpath"] = f"envds/{self.config.daq_id}/controller/registry/ack"
    #                 await self.send_event(ack)

    #     except Exception as e:
    #         self.logger.error("controller_definition_registry_update", extra={"reason": e})
    #     pass

    # async def controller_definition_registry_get(self, query: ControllerDefinitionRequest) -> dict:

    #     # TODO add in logic to get/sync from erddap if available
    #     if self.db_client:
    #         return await self.db_client.controller_definition_registry_get(query)

    #     return {"results": []}

    # async def controller_instance_registry_update(self, ce: CloudEvent):

    #     try:
    #         self.logger.debug("controller_instance_registry_update", extra={"ce": ce})
    #         for instance_type, instance_reg in ce.data.items():
    #             request = None
    #             self.logger.debug("controller_instance_registry_update", extra={"instance_type": instance_type, "instance_reg": instance_reg})
    #             try:
    #                 # controller_id = instance_reg.get("controller_id", None)
    #                 make = instance_reg["make"]
    #                 model = instance_reg["model"]
    #                 serial_number = instance_reg["serial_number"]
    #                 format_version = instance_reg["format_version"]
    #                 parts = format_version.split(".")
    #                 version = f"v{parts[0]}"

    #                 if make is None or model is None or serial_number is None:
    #                     # if "controller_id" in instance_reg and instance_reg["controller_id"] is not None:
    #                         # parts = instance_reg["controller_id"].split("::")
    #                         # make = parts[0]
    #                         # model = parts[1]
    #                         # serial_number = parts[2]
    #                     self.logger.error("couldn't register instance - missing value", extra={"make": make, "model": model, "serial_number": serial_number})
    #                     return

    #                 # if controller_id is None:
    #                 controller_id = "::".join([make, model, serial_number])

    #                 if instance_type == "controller-instance":
    #                     database = "registry"
    #                     collection = "controller-instance"
    #                     attributes = instance_reg["attributes"]

    #                     # if "controller_type" in instance_reg["attributes"]:
    #                     #     controller_type = instance_reg["attributes"]["controller_type"]["data"]
    #                     # else:
    #                     #     # default for backward compatibility
    #                     #     controller_type = "sensor"

    #                     request = ControllerInstanceUpdate(
    #                         controller_id=controller_id,
    #                         make=make,
    #                         model=model,
    #                         serial_number=serial_number,
    #                         version=format_version,
    #                         # controller_type=controller_type,
    #                         attributes=attributes,
    #                     )

    #             except (KeyError, IndexError) as e:
    #                     self.logger.error("datastore:controller_instance_registry_update", extra={"reason": e})
    #                     continue

    #                 # request = DatastoreRequest(
    #                 #     database="registry",
    #                 #     collection="controller-instance",
    #                 #     request=update,
    #                 # )

    #             self.logger.debug("datastore:controller_instance_registry_update", extra={"request": request, "db_client": self.db_client})
    #             if self.db_client and request:
    #                 await self.db_client.controller_instance_registry_update(
    #                     database=database,
    #                     collection=collection,
    #                     request=request,
    #                     ttl=self.config.db_reg_controller_instance_ttl,
    #                 )

    #     except Exception as e:
    #         self.logger.error("controller_instance_registry_update", extra={"reason": e})
    #     pass

    # async def controller_instance_registry_get(self, query: ControllerInstanceRequest) -> dict:

    #     # TODO add in logic to get/sync from erddap if available?
    #     if self.db_client:
    #         return await self.db_client.controller_instance_registry_get(query)

    #     return {"results": []}


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
    config = SamplingSystemConfig()
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

    asyncio.run(main(config))
