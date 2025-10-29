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
from cloudevents.conversion import to_structured # , from_http
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
    seconds_elapsed    
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

    mqtt_broker: str = 'mosquitto.default'
    mqtt_port: int = 1883
    # mqtt_topic_filter: str = 'aws-id/acg-daq/+'
    mqtt_topic_subscriptions: str = 'envds/+/+/+/data/#' #['envds/+/+/+/data/#', 'envds/+/+/+/status/#', 'envds/+/+/+/setting/#', 'envds/+/+/+/control/#']
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
        self.platform_variable_maps = dict()

        # this is cache for variable mapping
        self.platform_variable_sets = {
            "sources": dict(),
            "maps": dict()
        }

        self.config = SamplingSystemConfig()
        self.configure()

        self.mqtt_buffer = asyncio.Queue()
        asyncio.create_task(self.get_from_mqtt_loop())
        asyncio.create_task(self.handle_mqtt_buffer())

    def configure(self):
        # set clients

        self.logger.debug("configure", extra={"self.config": self.config})
    
        try:
            # load resource configmaps
            #   load payloads
            with open ("/app/config/platforms.json", "r") as f:
                platforms = json.load(f)

            for platform in platforms:
                if platform["kind"] != "Platform":
                    continue
                if (name:=platform["metadata"]["name"]) not in self.platforms:
                    self.platforms[name] = platform

            # load layout configmaps

            # load variable_map configmaps
            with open ("/app/config/platform_variable_maps.json", "r") as f:
                variable_maps = json.load(f)

            # TODO allow for multiple configs of a given map that are retrieved from datastore or loaded
            for vm in variable_maps:
                if vm["kind"] != "PlatformVariableMap":
                    continue
                vm_name = vm["metadata"]["name"]
                vm_cfg_time = vm["valid-config-time"]
                if vm_name not in self.platform_variable_maps["maps"]:
                    self.platform_variable_maps[name] = dict()
                if vm_cfg_time not in self.platform_variable_maps[vm_name]:
                    self.platform_variable_maps[name][vm_name][vm_cfg_time] = vm

            # TODO retrieve all variable maps from datastore?

            # build variable_sets dict
            for vm_name, vm_rev in self.platform_variable_maps.items():
                for vm_cfg_time, vm in vm_rev.items():

                    if vm_name not in self.platform_variable_sets["maps"]:
                        self.platform_variable_sets["maps"][vm_name] = dict()
                    if vm_cfg_time not in self.platform_variable_sets["maps"][vm_name]:
                        self.platform_variable_sets["maps"][vm_name][vm_cfg_time] = {
                            "variable_groups": dict(),
                            "indices": dict(),
                            # "sources": dict()
                        }
                    
                    # add each variable group
                    for vg_name, vg in vm["variable_groups"].items():
                        if vg_name not in self.platform_variable_sets:
                            self.platform_variable_sets["maps"][vm_name][vm_cfg_time]["variable_groups"][vg_name] = {
                                index: vg["index"],
                                "variables": dict()
                            }

                        index_type = vg["index"]["index_type"]
                        index_value = vg["index"]["index_value"]

                        # create index and add vg_name to index for x-ref
                        if index_type not in self.platform_variable_sets[vm_name][vm_cfg_time]["indices"]:
                            self.platform_variable_sets["maps"][vm_name][vm_cfg_time]["indices"][index_type] = dict()

                        if index_value not in self.platform_variable_sets["maps"][vm_name][vm_cfg_time]["indices"][index_type]:
                            self.platform_variable_sets["maps"][vm_name][vm_cfg_time]["indices"][index_type][index_value] = {
                                "variable_groups": [],
                                "data": []
                            }
                            if vg_name not in self.platform_variable_sets["maps"][vm_name][vm_cfg_time]["indices"][index_type][index_value]["variable_groups"]:
                                self.platform_variable_sets["maps"][vm_name][vm_cfg_time]["indices"][index_type][index_value]["variable_groups"].append(vg_name)
                        
                        # for index_name, index_value in vm["variable_groups"][vg_name].items():
                        #     # if index_name not in self.platform_variable_sets["maps"][vm_name]["variable_groups"][vg_name]["index"]:
                        #     #     self.platform_variable_sets["maps"][vm_name]["variable_groups"][vg_name]["index"][index_name] = index_value
                        #     if index_name not in self.platform_variable_sets[vm_name]["indices"]:
                        #         self.platform_variable_sets["maps"][vm_name]["indices"][index_name] = dict()
                        #     if index_value not in self.platform_variable_sets["maps"][vm_name]["indices"][index_name]:
                        #         self.platform_variable_sets["maps"][vm_name]["indices"][index_name][index_value] = []
                        

                    # for vg_name in vm["variable_groups"]:
                    #     if vg_name not in self.platform_variable_sets[vm_name]["variable_groups"]:
                    #         self.platform_variable_sets[vm_name]["variable_groups"][vg_name] = {
                    #             "timebase": vm["variable_group"]["timebase"],
                    #             "variables": dict()
                    #         }

                    # add variables to each variable group
                    for name, variable in vm["variables"].items():
                        vg_name = variable["variable_group"]
                        if name not in self.platform_variable_sets["maps"][vm_name][vm_cfg_time]["variable_groups"]["variables"][vg_name]:
                            self.platform_variable_sets["maps"][vm_name][vm_cfg_time]["variable_groups"][vg_name]["variables"][name] = {
                                "map": variable, # grab whole thing for now, not sure what we'll need
                                "data": dict(),
                                "value": dict()
                            }

                        # add source and map_id for x-ref
                        for source_name, source in variable["sources"].items():
                            if vm_cfg_time not in self.platform_variable_sets["sources"]:
                                self.platform_variable_sets["sources"][vm_cfg_time] = dict()
                            if source["source_id"] not in  self.platform_variable_sets["sources"][vm_cfg_time]:
                                self.platform_variable_sets["sources"][vm_cfg_time]["source_id"] = []
                            # build map_id
                            map_id = f"{vm_name}::{vg_name}::{name}"
                            if map_id not in self.platform_variable_sets["sources"][vm_cfg_time]["source_id"]:
                                self.platform_variable_sets["sources"][vm_cfg_time]["source_id"].append(map_id)

        except Exception as e:
            self.logger.error("configure error", extra={"reason": e})




    async def send_event(self, ce):
        try:
            self.logger.debug(ce)#, extra=template)
            try:
                timeout = httpx.Timeout(5.0, read=0.1)
                headers, body = to_structured(ce)
                self.logger.debug("send_event", extra={"broker": self.config.knative_broker, "h": headers, "b": body})
                # send to knative broker
                async with httpx.AsyncClient() as client:
                    r = await client.post(
                        self.config.knative_broker,
                        headers=headers,
                        data=body,
                        timeout=timeout
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
                L.debug("listen", extra={"config": self.config})
                client_id=str(ULID())
                async with Client(self.config.mqtt_broker, port=self.config.mqtt_port,identifier=client_id) as self.client:
                    # for topic in self.config.mqtt_topic_subscriptions.split("\n"):
                    for topic in self.config.mqtt_topic_subscriptions.split(","):
                        # print(f"run - topic: {topic.strip()}")
                        # L.debug("run", extra={"topic": topic})
                        if topic.strip():
                            L.debug("subscribe", extra={"topic": topic.strip()})
                            await self.client.subscribe(f"$share/sampling-system/{topic.strip()}")

                        # await client.subscribe(config.mqtt_topic_subscription, qos=2)
                    # async with client.messages() as messages:
                    async for message in self.client.messages: #() as messages:

                        try:
                            ce = from_json(message.payload)
                            topic = message.topic.value
                            ce["sourcepath"] = topic
                            await self.mqtt_buffer.put(ce)
                            L.debug("get_from_mqtt_loop", extra={"cetype": ce["type"], "topic": topic})
                        except Exception as e:
                            L.error("get_from_mqtt_loop", extra={"reason": e})
                        # try:
                        #     L.debug("listen", extra={"payload_type": type(ce), "ce": ce})
                        #     await self.send_to_knbroker(ce)
                        # except Exception as e:
                        #     L.error("Error sending to knbroker", extra={"reason": e})
            except MqttError as error:
                L.error(
                    f'{error}. Trying again in {reconnect} seconds',
                    extra={ k: v for k, v in self.config.dict().items() if k.lower().startswith('mqtt_') }
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
                L.error("handle_mqtt_buffer", extra={"reason": e})
            
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
            if device_id in self.platform_variable_sets["sources"]:
                await self.update_variable_set_by_source(source_id=device_id, source_data=ce)

        except Exception as e:
            L.error("device_data_update", extra={"reason": e})
        pass

    # TODO change this for sampling-system
    async def controller_data_update(self, ce: CloudEvent):

        try:
            attributes = ce.data["attributes"]

            make = attributes["make"]["data"]
            model = attributes["model"]["data"]
            serial_number = attributes["serial_number"]["data"]

            controller_id = "::".join([make, model, serial_number])
            if controller_id in self.platform_variable_sets["sources"]:
                await self.update_variable_set_by_source(source_id=controller_id, source_data=ce)


        except Exception as e:
            L.error("device_data_update", extra={"reason": e})
        pass

    async def update_variable_set_by_source(self, source_id:str, source_data:CloudEvent):
        pass
        if source_id not in self.platform_variable_sets["sources"]:
            return
        for map_id in self.platform_variable_sets["sources"]["source_id"]:
            self.update_variable_by_id(map_id=map_id, source_id=source_id, source_data=source_data)
        # loop through mapped vars in source_id
        #   

    async def update_variable_by_id(self, map_id:str, source_id:str, source_data:CloudEvent):

        try:
            parts = map_id.split(":")
            vm_name = parts[0]
            vg_name = parts[1]
            var_name = parts[3]

            target_time = source_data.data["variables"]["time"]["data"]
            target_vm = self.get_variable_map_by_config_time(variable_map=vm_name, target_time=target_time)

            # variable_group = self.platform_variable_sets["maps"][vm_name]["variable_groups"][vg_name]
            variable_group = target_vm["variable_groups"][vg_name]
            # mapped_var = self.platform_variable_sets["maps"][vm_name]["variable_groups"][vg_name]["variables"][var_name]
            mapped_var = variable_group["variables"][var_name]
            if mapped_var["map_type"] == "direct":
                direct_var = mapped_var["direct_value"]["source_variable"]
                source_var = mapped_var["source"][direct_var]["source_variable"]
                # source_var = source_data.data["variables"][source_var]

                # get index value
                index = variable_group[vg_name]["index"]
                index_value = await self.get_index_value(index=index, source_data=source_data)
                if index_value not in mapped_var["data"]:
                    mapped_var["data"][index_value] = []

                # append data
                mapped_var["data"][index_value].append(source_data.data["variables"][source_var]["data"])

        except Exception as e:
            L.error("update_variable_by_id", extra={"reason": e})

    async def get_index_value(self, index:dict, source_data:CloudEvent):

        if index["index_type"] == "timebase": 
            tb = index["index_value"]

            source_time = source_data.data["variables"]["time"]["data"]
            tb_time = self.round_to_nearest_N_seconds(dt_string=source_time, timebase=tb)

    def get_timebase_period(self, dt_string:str, timebase: int) -> str:
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
    #         L.debug("round_to_nearest_N_seconds", extra={"orig": dt_string, "rounded": result})
    #         # return 
        
    #     except Exception as e:
    #         L.error("round_to_nearest_N_seconds", extra={"reason": e})
    #         result = ""
    #     return result

    async def index_timebase_monitor(self, variable_map:str, timebase:int):
        current_dt_period = round_to_nearest_N_seconds(dt=get_datetime(), Nsec=timebase)

        last_dt_period = None
        if timebase == 1:
            threshhold_direct = 0.75*timebase
            threshhold_other = 0.9*timebase
        else:
            threshhold_direct = 0.6*timebase
            threshhold_other = 0.75*timebase
        
        while True:
            # create timestamp for current interval and save to index values
            dt_period = round_to_nearest_N_seconds(dt=get_datetime(), Nsec=timebase)
            if dt_period != current_dt_period:
                last_dt_period = current_dt_period
                current_dt_period = dt_period

            current_time_period = timestamp_to_string(current_dt_period)
            target_vm = self.get_variable_map_by_config_time(variable_map=variable_map, target_time=current_time_period)
            # if current_time_period not in self.platform_variable_sets["maps"][variable_map]["indices"]["timebase"][timebase]:
            if current_time_period not in target_vm["indices"]["timebase"][timebase]:
                # self.platform_variable_sets["maps"][variable_map]["indices"]["timebase"][timebase].append(current_time_period)
                target_vm["indices"]["timebase"][timebase].append(current_time_period)

            # check if current time is greater than threshold to create previous interval variable_set
            #   e.g., if tb=1, wait for next second, if tb>1, wait for 0.6*tb to pass (tb=10, wait for 6sec to pass)
            if last_dt_period:
                last_time_period = timestamp_to_string(last_time_period)
                if seconds_elapsed(initial_dt=last_dt_period) >= threshhold_direct:
                    update = {
                        "variable_map": variable_map,
                        "variable_map_config_time": target_vm["valid-config-time"],
                        "index_type": "timebase",
                        "index_value": timebase,
                        "threshhold_type": "direct",
                        "index_ready": last_time_period
                    }
                    # await self.direct_timebase_ready_buffer.put(last_time_period)
                    await self.index_ready_buffer.put(update)

            # await asyncio.sleep(time_to_next(timebase))
            await asyncio.sleep(0.1)

    async def get_variable_map_by_config_time(self, variable_map:str, target_time:str="") -> dict:
        if target_time == "":
            target_time = get_datetime_string()
        
        current = ""
        for cfg_time, vm in self.platform_variable_sets["maps"][variable_map].items():
            if target_time > cfg_time and cfg_time > current:
                current = cfg_time
        
        if current == "":
            return None
        
        return self.platform_variable_sets["maps"][variable_map][current]


    async def update_timebase_variable_set_by_index(self, timebase_index:dict):
        try:
            vm_name = timebase_index["variable_map"]
            vm_cfg_time =  timebase_index["variable_map_config_time"]
            target_vm = self.platform_variable_sets["maps"][vm_name][vm_cfg_time]
            index_value = timebase_index["index_value"]
            index_type = "timebase"
            index_value = timebase_index["index_value"]
            target_time = timebase_index["index_ready"]

            # for vg in self.platform_variable_sets["maps"][vm_name]["indices"][index_type][index_value]["variable_groups"]:
            for vg in target_vm["indices"][index_type][index_value]["variable_groups"]:
                var_set = {
                    "attributes": {
                        "variable_map": {"type": "string", "data": vm_name},
                        "variable_map_config_time": {"type": "string", "data": vm_cfg_time},
                        "variable_group": {"type": "string", "data": vg},
                        "index_type": {"type": "string", "data": index_type},
                        "index_value": {"type": "int", "data": index_value}
                    },
                    "dimensions": {"time": 1},
                    "variables": {}
                }

                time_var = {
                    "type": "str",
                    "shape": ["time"],
                    "attributes": {
                        # how to make sure these are always using proper config?
                        "variable_map": {"type": "string", "data": vm_name},
                        "variable_map_config_time": {"type": "string", "data": vm_cfg_time},
                        "variable_group": {"type": "string", "data": vg},
                        "index_type": {"type": "string", "data": index_type},
                        "index_value": {"type": "int", "data": index_value}
                    },
                    "data": target_time
                }
                var_set["variables"]["time"] = time_var

                # for name, variable in self.platform_variable_sets["maps"][vm_name]["indices"][index_type][index_value]["variable_groups"][vg]["variables"].items():
                for name, variable in target_vm["indices"][index_type][index_value]["variable_groups"][vg]["variables"].items():
                    map_type = variable["map_type"]
                    source = variable["source"]
                    index_method = variable["index_method"]
                    attributes = variable["attributes"]
                    if map_type == "direct":
                        source_variable = variable["direct_value"]["source_variable"]
                    
                        mapped_var = {
                            "type": "float",
                            "shape": ["time"],
                            "attributes": {
                                # how to make sure these are always using proper config?
                                "source_type": {"type": "string", "data": source[source_variable]["source_type"]},
                                "source_id": {"type": "string", "data": source[source_variable]["source_id"]},
                                "source_variable": {"type": "string", "data": source[source_variable]["source_variable"]},
                            }
                        }

                        if len(variable["data"][index_value]) == 0:
                            if variable["type"] in ["string", "str", "char"]:
                                val = ""
                            else:
                                val = None
                        elif len(variable["data"][index_value]) == 1:
                            val = variable["data"][index_value][0]
                        else:
                            if variable["type"] in ["string", "str", "char"]:
                                val = variable["data"][index_value][0]
                            else:
                                val = round(sum(variable["data"][index_value]) / len(variable["data"][index_value]),3)
                        mapped_var["data"] = val

                        var_set["variables"][name] = mapped_var

                        varset_id = f"{vm_name}::{vm_cfg_time}::{vg}"
                        source_id = f"envds.{self.config.daq_id}.variableset::{varset_id}"
                        source_topic = source_id.replace(".", "/")
                        if var_set:
                            event = SamplingEvent.create_variableset_update(
                                # source="sensor.mockco-mock1-1234", data=record
                                source=source_id,
                                data=var_set,
                            )
                            destpath = f"{source_topic}/data/update"
                            event["destpath"] = destpath
                            self.logger.debug(
                                "update_timebase_variable_set_by_index",
                                extra={"data": event, "destpath": destpath},
                            )
                            # message = Message(data=event, destpath=destpath)
                            message = event
                            # self.logger.debug("default_data_loop", extra={"m": message})
                            await self.send_message(message)

                    else:
                        continue # TODO fill in for other types


        except Exception as e:
            L.error("update_timebase_variableset_by_index", extra={"reason": e})


    async def index_monitor(self):
        while True:
            update = await self.index_ready_buffer.get()
            try:
                index_type = update["index_type"]
                if index_type == "timebase":
                    self. update_timebase_variable_set_by_index(self, timebase_index=update)
                    # # handle timebase update
                    # vm_name = update["variable_map"]
                    # vm_cfg_time =  update["variable_map_config_time"]
                    # target_vm = self.platform_variable_sets["maps"][vm_name][vm_cfg_time]
                    # index_value = update["index_value"]
                    # # for vg in self.platform_variable_sets["maps"][vm_name]["indices"][index_type][index_value]["variable_groups"]:
                    # for vg in target_vm["indices"][index_type][index_value]["variable_groups"]:
                    #     var_set = {
                    #         "attributes": {
                    #             "variable_map": {"type": "string", "data": vm_name},
                    #             "variable_map_config_time": {"type": "string", "data": vm_cfg_time},
                    #             "variable_group": {"type": "string", "data": vg},
                    #             "index_type": {"type": "string", "data": index_type},
                    #             "index_value": {"type": "int", "data": index_value}
                    #         },
                    #         "dimensions": {"time": 1},
                    #         "variables": {}
                    #     }

                    #     # for name, variable in self.platform_variable_sets["maps"][vm_name]["indices"][index_type][index_value]["variable_groups"][vg]["variables"].items():
                    #     for name, variable in target_vm["indices"][index_type][index_value]["variable_groups"][vg]["variables"].items():
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
                L.error("index_monitor", extra={"reason": e})




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
    #         L.error("device_instance_registry_update", extra={"reason": e})
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
    #         L.error("controller_instance_registry_update", extra={"reason": e})
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
        root_path="/msp/datastore",
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
