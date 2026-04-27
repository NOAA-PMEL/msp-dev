import asyncio
import importlib
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

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.INFO)


class SamplingConditionsManagerConfig(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8080
    debug: bool = True

    # TODO fix ns prefix
    daq_id: str | None = None

    mqtt_broker: str = "mosquitto.default"
    mqtt_port: int = 1883
    # mqtt_topic_filter: str = 'aws-id/acg-daq/+'
    mqtt_topic_subscriptions: str = (
        ""
        # "envds/+/+/+/data/#"  # ['envds/+/+/+/data/#', 'envds/+/+/+/status/#', 'envds/+/+/+/setting/#', 'envds/+/+/+/control/#']
    )
    # mqtt_client_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    mqtt_client_id: str = Field(str(ULID()))

    knative_broker: str | None = None

    class Config:
        env_prefix = "SAMPLING_CONDITIONS_"
        case_sensitive = False


class SamplingCondition:
    """
    Docstring for SamplingCondition
    """

    def __init__(self, config, status_buffer):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug("SamplingCondition instantiated")

        self.config = config
        # self.data_buffer = data_buffer
        self.data_buffer = asyncio.Queue(maxsize=60)
        self.status_buffer = status_buffer
        # self.source_map = {"source_id": dict(), "source_name": dict()}
        self.source_map = dict()
        self.criteria_map = dict()
        # self.source_data = dict()
        self.default_criterion_module: str = (
            # "apps.sampling-operations.sampling-conditions.criteria"
            "criteria"
        )

        self.current_state = False

        self.criterion_tasks = []
        
        self.configure()
        asyncio.create_task(self.condition_monitor())
        # asyncio.create_task(self.update_status_loop())

    def configure(self):

        if not self.config or "sources" not in self.config:
            return

        # for source_name, source in self.config["sources"].items():
        #     vm = source["variablemap_name"]
        #     vs = source["variableset_name"]
        #     v = source["variable"]

        for source_name, _ in self.config["sources"].items():
            if source_name not in self.source_map:
                # self.source_map[source_name] = {"data": dict(), "criteria": []}
                self.source_map[source_name] = {"data": dict()}

        #     if vm not in self.data:
        #         self.data[vm] = dict()
        #     if vs not in self.data[vm]:
        #         self.data[vm][vs] = dict()
        #     if v not in self.data[vm][vs]:
        #         self.data[vm][vs][v] = dict()

        try:
            if "criteria" in self.config:
                for group_type, group in self.config["criteria"].items():
                    if group_type not in self.criteria_map:
                        self.criteria_map[group_type] = {"criteria": []}
                    for criterion_config in group:
                        criterion_module = self.default_criterion_module
                        if "criterion_module" in criterion_config:
                            self.criterion_module = criterion_config["criterion_module"]
                        mod_ = importlib.import_module(criterion_module)
                        criterion_class = criterion_config["criterion_class"]
                        criterion = getattr(mod_, criterion_class)(criterion_config)
                        self.criteria_map[group_type]["criteria"].append(criterion)
        except Exception as e:
            self.logger.error("configure", extra={"reason": e})
            # sys.exit()
        # for source_name, _ in self.config["sources"].items():
        #     if source_name not in self.source_map:
        #         self.source_map[source_name] = {"data": dict(), "criteria": []}

        # # for source_name, source in self.config["sources"].items():
        # #     # TODO: fix this with "source_id"
        # #     source_id = "variableset::variablemap::variable"
        # #     if source_id not in self.source_map["source_id"]:
        # #         self.source_map["source_id"][source_id] = {
        # #             "source_name": source_name,
        # #         }
        # #     if source_name not in self.source_map["source_name"]:
        # #         self.source_map["source_name"][source_name] = {
        # #             "criteria": [],
        # #         }

        # if "criteria" in self.config:
        #     for group_type, group in self.config["criteria"].items():
        #         if group_type not in self.criteria_map:
        #             self.criteria_map[group_type] = dict()
        #         for _, criterion_config in group.items():
        #             criterion_module = self.default_criterion_module
        #             if "criterion_module" in criterion_config:
        #                 self.criterion_module = criterion_config["criterion_module"]
        #             mod_ = importlib.import_module(criterion_module)
        #             criterion_class = criterion_config["criterion_class"]
        #             criterion = getattr(mod_, criterion_class)(criterion_config)

        # for source in criterion_config[group_type]["sources"]:
        #     # if source in self.source_map["source_name"]:
        #     if source in self.source_map:
        #         # self.source_map["source_name"][source]["criteria"].append(
        #         self.source_map[source]["criteria"].append(
        #             {"sources":  criterion
        #         )

    async def update(self, data):
        self.logger.debug("update", extra={"update_data": data})
        # await self.data_buffer.put(data)
        # FIX: Prevent Head-Of-Line blocking. If buffer is full, drop the oldest 
        # evaluation frame instead of freezing the main MQTT ingestion loop.
        try:
            self.data_buffer.put_nowait(data)
        except asyncio.QueueFull:
            self.logger.warning("Condition data_buffer full. Dropping oldest frame.")
            self.data_buffer.get_nowait()
            self.data_buffer.task_done()
            self.data_buffer.put_nowait(data)

    async def condition_monitor(self):

        while True:
            # self.logger.debug("condition_monitor", extra={"data_buffer": self.data_buffer})
            try:
                data = await self.data_buffer.get()
                # self.logger.debug("condition_monitor", extra={"data_buffer": data})
                if "condition_variables" in data:
                    variables = data["condition_variables"]
                    dt = variables["time"]["data"]
                    for varname, var in variables.items():
                        if varname == "time":
                            continue
                        if varname in self.source_map:
                            self.source_map[varname][dt] = var["data"]

                    # self.logger.debug("condition_monitor", extra={"src_map": self.source_map})
                    await self.evaluate_criteria(dt)

            except Exception as e:
                self.logger.error("condition_monitor", extra={"reason": e})

            await asyncio.sleep(0.001)
            self.data_buffer.task_done()

    # async def update_status(self, status):

    #     cond_name = status["condition"]["name"]
    #     cond_ns = status["condition"]["sampling_namespace"]
    #     cond_valid_time = status["condition"]["valid_config_time"]

    #     source_id = (
    #         f"envds.{self.config.daq_id}.condition.{cond_name}"
    #     )
    #     self.logger.debug("evaluate_criteria", extra={"source_id": source_id})
        
    #     source_topic = source_id.replace(".", "/")

    #     event = SamplingEvent.create_condition_status_update(
    #         # source="sensor.mockco-mock1-1234", data=record
    #         source=source_id,
    #         data=status,
    #     )
    #     destpath = f"{source_topic}/status/update"
    #     event["destpath"] = destpath
    #     event["samplingnamespace"] = cond_ns
    #     event["validconfigtime"] = cond_valid_time
    #     self.logger.debug(
    #         "evaluate_criteria",
    #         extra={"data": event, "destpath": destpath},
    #     )
        
    #     # await self.send_event(event)
    #     await self.status_buffer.put(status)



    async def evaluate_criteria(self, timestamp):

        try:
            crit_states = []
            for group_type, group in self.criteria_map.items():
                group_states = []
                for criterion in group["criteria"]:
                    data = {"time": timestamp}
                    for src_name in criterion.get_sources():
                        # self.logger.debug("evaluate_criteria", extra={"src_name": src_name})
                        if (
                            timestamp not in self.source_map[src_name]
                            or self.source_map[src_name][timestamp] is None
                        ):
                            # missing source data, can't evaluate
                            self.logger.info(
                                "evaluate_criteria",
                                extra={
                                    "src_name": src_name,
                                    "ts": timestamp,
                                    "success": False,
                                    "reason": "missing source value",
                                },
                            )
                            return
                        data[src_name] = self.source_map[src_name][timestamp]["data"]
                    self.logger.debug("evaluate_criteria", extra={"criterion": criterion, "data_for_eval": data})
                    try:
                        group_states.append(await criterion.evaluate(sources=data))
                    except Exception as e:
                        # what to do on error?
                        group_states.append(False)
                    # group_states.append(res)
                if group_type == "all":
                    crit_states.append(all(group_states))
                elif group_type == "any":
                    crit_states.append(any(group_states))
                elif group_type == "none":
                    crit_states.append(not any(group_states))
            
            state = all(crit_states)
            self.logger.debug("evaluate_criteria", extra={"current_state": self.current_state, "new_state": state})
            # if state != self.current_state:
                # send event with updated condition state
            
            # Send status whenever data is available
            self.logger.debug("evaluate_criteria - send update with new state")
    
            cond_name = self.config["metadata"]["name"]
            cond_ns = self.config["metadata"]["sampling_namespace"]
            cond_valid_time = self.config["metadata"]["valid_config_time"]

            self.current_state = state

            status = {
                "status": {
                    "kind": "SamplingCondition",
                    "time": timestamp,
                    "name": cond_name,
                    "sampling_namespace": cond_ns,
                    "valid_config_time": cond_valid_time,
                    "status": state
                }
            }
            await self.status_buffer.put(status)

            # await self.update_status(status)


            # for src_name, _ in self.source_map.items():
            #     # self.logger.debug("evaluate_criteria", extra={"src_name": src_name, "src_data": src_data})
            #     # src_data.pop(timestamp)
            #     self.source_map[src_name].pop(timestamp)

        except Exception as e:
            self.logger.error("evaluate_criteria", extra={"reason": e})

        finally:
            # This guarantees cleanup runs even if the function hits a 'return'
            try:
                # Calculate a cutoff time (e.g., 60 seconds ago)
                current_dt = string_to_datetime(timestamp).replace(tzinfo=timezone.utc)
                cutoff_dt = current_dt - timedelta(seconds=60)
                
                for src_name, src_dict in self.source_map.items():
                    # Find all timestamps older than 60 seconds
                    stale_keys = [ts for ts in src_dict.keys() if string_to_datetime(ts).replace(tzinfo=timezone.utc) < cutoff_dt]
                    # Delete them
                    for ts in stale_keys:
                        src_dict.pop(ts, None)
            except Exception as clean_e:
                self.logger.error("evaluate_criteria cleanup error", extra={"reason": clean_e})

    async def update_status_loop(self):
        while True:
            self.logger.debug("update_state_loop - send update")

            cond_name = self.config["metadata"]["name"]
            cond_ns = self.config["metadata"]["sampling_namespace"]
            cond_valid_time = self.config["metadata"]["valid_config_time"]

            status = {
                "status": {
                    "kind": "SamplingCondition",
                    "time": get_datetime_string(),
                    "name": cond_name,
                    "sampling_namespace": cond_ns,
                    "valid_config_time": cond_valid_time,
                    "status": self.current_state
                }
            }
            await self.status_buffer.put(status)

            # await self.update_status(status)

            await asyncio.sleep(10)



class SamplingConditionsManager:
    """docstring for SamplingConditionsManager."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug("SamplingConditionsManager instantiated")

        # self.sampling_mode_control = True
        # self.sampling_modes = dict()
        # self.sampling_states = dict()
        self.sampling_conditions = {"conditions": dict(), "sources": {}}

        # self.status_buffer = asyncio.Queue(maxsize=60)
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

        self.config = SamplingConditionsManagerConfig()

        self.http_client = None

        self.configure()
        # print("here:7")


        # self.mqtt_buffer = asyncio.Queue()
        # asyncio.create_task(self.get_from_mqtt_loop())
        # asyncio.create_task(self.handle_mqtt_buffer())
        # asyncio.create_task(self.condition_status_monitor())
        # asyncio.create_tasks(self.sampling_mode_monitor())
        # asyncio.create_tasks(self.sampling_state_monitor())
        # asyncio.create_task(self.sampling_condition_monitor())
        # asyncio.create_tasks(self.sampling_action_monitor())

        # print("SamplingConditionsManager: init: here:8")

    async def setup(self):
        """Asynchronously initialize buffers, clients, and loops."""
        self.logger.info("Running SamplingConditionsManager async setup...")
        
        self.mqtt_buffer = asyncio.Queue(maxsize=2000)
        # FIX: Only create if it wasn't already created during configure()
        if not getattr(self, "status_buffer", None):
            self.status_buffer = asyncio.Queue(maxsize=2000)
        
        # Initialize connection pool
        self.http_client = httpx.AsyncClient(
            limits=httpx.Limits(max_keepalive_connections=50, max_connections=100)
        )

        asyncio.create_task(self.get_from_mqtt_loop())
        asyncio.create_task(self.handle_mqtt_buffer())
        asyncio.create_task(self.condition_status_monitor())
        asyncio.create_task(self.publish_local_definitions())
        asyncio.create_task(self.sync_sampling_definitions_loop())
        self.logger.info("SamplingConditionsManager background tasks started successfully.")

    def open_http_client(self):
        self.logger.debug("open_http_client")
        self.http_client = httpx.AsyncClient(
            limits=httpx.Limits(max_keepalive_connections=50, max_connections=100)
        )

    async def close_http_client(self):
        if getattr(self, 'http_client', None):
            await self.http_client.aclose()
            self.http_client = None

    # def configure(self):
    #     # set clients

    #     self.logger.debug("configure", extra={"self.config": self.config})

    #     try:

    #         # load sampling conditions
    #         with open("/app/config/sampling_conditions.json", "r") as f:
    #             conditions = json.load(f)

    #             # build dictionary:
    #             for condition in conditions:

    #                 if condition["kind"] != "SamplingCondition":
    #                     continue

    #                 # full condition name with namespace
    #                 # cond_name = f'{condition["metadata"]["name"]}.{condition["metadata"]["sampling_namespace"]}'
    #                 cond_name = f'{condition["metadata"]["name"]}'
    #                 # data_buffer = asyncio.Queue(maxsize=60)
    #                 if cond_name not in self.sampling_conditions["conditions"]:
    #                     self.sampling_conditions["conditions"][cond_name] = {
    #                         "config": None,
    #                         "event_buffer": self.status_buffer,
    #                         "condition": None,
    #                     }
    #                 self.sampling_conditions["conditions"][cond_name]["config"] = condition
    #                 # self.sampling_conditions["conditions"][cond_name]["data_buffer"] = data_buffer

    #                 for source_name, source in condition["sources"].items():
    #                     # TODO get src_id
    #                     # src_id = "111::222::aaa"
    #                     vm_name = source["variablemap_name"]
    #                     vs_name = source["variableset_name"]
    #                     src_id = "::".join([vm_name, vs_name])

    #                     if src_id not in self.sampling_conditions["sources"]:
    #                         self.sampling_conditions["sources"][src_id] = {
    #                             "targets": []
    #                         }
    #                     source_variable = source["variable"]
    #                     self.sampling_conditions["sources"][src_id]["targets"].append(
    #                         {
    #                             "condition": cond_name,
    #                             "source_name": source_name,
    #                             "source_variable": source_variable,
    #                         }
    #                     )

    #                 condition_instance = SamplingCondition(
    #                     config=self.sampling_conditions["conditions"][cond_name][
    #                         "config"
    #                     ],
    #                     # data_buffer=self.sampling_conditions["conditions"][cond_name]["data_buffer"],
    #                     status_buffer=self.status_buffer,
    #                 )
    #                 # self.logger.debug("configure", extra={"condition": condition_instance})
    #                 self.sampling_conditions["conditions"][cond_name][
    #                     "condition"
    #                 ] = condition_instance

    #         self.logger.debug(
    #             "configure", extra={"sampling_conditions": self.sampling_conditions}
    #         )

    #     except Exception as e:
    #         self.logger.error("configure error", extra={"reason": e})

    def configure(self):
        self.logger.debug("configure", extra={"self.config": self.config})
        try:
            # load sampling conditions from file
            conditions_path = "/app/config/sampling_conditions.json"
            if os.path.exists(conditions_path):
                with open(conditions_path, "r") as f:
                    conditions = json.load(f)
                    for condition in conditions:
                        self.load_condition(condition)
                self.logger.debug("configure", extra={"sampling_conditions": self.sampling_conditions})
            else:
                self.logger.info(f"{conditions_path} not found. Skipping local load.")
        except Exception as e:
            self.logger.error("configure error", extra={"reason": e})

    def load_condition(self, condition: dict):
        """Helper to process definitions from either local files or Datastore API."""
        if condition.get("kind") != "SamplingCondition":
            return

        cond_name = condition["metadata"]["name"]
        
        if cond_name not in self.sampling_conditions["conditions"]:
            self.sampling_conditions["conditions"][cond_name] = {
                "config": None,
                "event_buffer": getattr(self, "status_buffer", None),
                "condition": None,
            }
            
        self.sampling_conditions["conditions"][cond_name]["config"] = condition

        # Map sources to targets
        for source_name, source in condition.get("sources", {}).items():
            vm_name = source["variablemap_name"]
            vs_name = source["variableset_name"]
            src_id = "::".join([vm_name, vs_name])

            if src_id not in self.sampling_conditions["sources"]:
                self.sampling_conditions["sources"][src_id] = {"targets": []}
                
            source_variable = source["variable"]
            target_entry = {
                "condition": cond_name,
                "source_name": source_name,
                "source_variable": source_variable,
            }
            
            if target_entry not in self.sampling_conditions["sources"][src_id]["targets"]:
                self.sampling_conditions["sources"][src_id]["targets"].append(target_entry)

        # Ensure status buffer exists if load_condition runs during sync loop
        if not getattr(self, "status_buffer", None):
            self.status_buffer = asyncio.Queue(maxsize=2000)
            self.sampling_conditions["conditions"][cond_name]["event_buffer"] = self.status_buffer

        condition_instance = SamplingCondition(
            config=condition,
            status_buffer=self.status_buffer,
        )
        self.sampling_conditions["conditions"][cond_name]["condition"] = condition_instance

    # def open_http_client(self):
    #     # create a new client for each request
    #     self.http_client = httpx.AsyncClient()

    # async def send_event(self, ce):
    #     try:
    #         self.logger.debug(ce)  # , extra=template)
    #         if not self.http_client:
    #             self.open_http_client()
    #         try:
    #             timeout = httpx.Timeout(5.0, read=0.1)
    #             headers, body = to_structured(ce)
    #             self.logger.debug(
    #                 "send_event",
    #                 extra={
    #                     "broker": self.config.knative_broker,
    #                     "h": headers,
    #                     "b": body,
    #                 },
    #             )
    #             # send to knative broker
    #             # async with httpx.AsyncClient() as client:
    #             #     r = await client.post(
    #             #         self.config.knative_broker,
    #             #         headers=headers,
    #             #         data=body,
    #             #         timeout=timeout,
    #             #     )

    #             r = await self.http_client.post(
    #                 self.config.knative_broker,
    #                 headers=headers,
    #                 data=body,
    #                 timeout=timeout,
    #             )

    #             r.raise_for_status()
    #         except InvalidStructuredJSON:
    #             self.logger.error(f"INVALID MSG: {ce}")
    #         except httpx.TimeoutException:
    #             pass
    #         except httpx.HTTPError as e:
    #             self.logger.error(f"HTTP Error when posting to {e.request.url!r}: {e}")
    #     except Exception as e:
    #         print("error", e)
    #     await asyncio.sleep(0.01)

    async def send_event(self, ce):
        try:
            self.logger.debug(ce)
            if not getattr(self, 'http_client', None):
                self.open_http_client()
            try:
                timeout = httpx.Timeout(5.0, read=10.0)
                headers, body = to_structured(ce)
                
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

    async def submit_get(self, path: str):
        try:
            timeout = httpx.Timeout(10.0, read=10.0)
            if not getattr(self, 'http_client', None):
                self.open_http_client()
            
            datastore_url = f"datastore.{self.config.daq_id}-system.svc.cluster.local"
            results = await self.http_client.get(f"http://{datastore_url}/{path}/", timeout=timeout)
            return results.json()
        except Exception as e:
            self.logger.error("submit_get", extra={"reason": e})
            return {}

    async def submit_request(self, path: str, query: dict):
        try:
            timeout = httpx.Timeout(10.0, read=10.0)
            if not getattr(self, 'http_client', None):
                self.open_http_client()
                
            datastore_url = f"datastore.{self.config.daq_id}-system.svc.cluster.local"
            results = await self.http_client.get(f"http://{datastore_url}/{path}/", params=query, timeout=timeout)
            return results.json()
        except Exception as e:
            self.logger.error("submit_request", extra={"reason": e})
            return {}
        
    # async def submit_request(self, path: str, query: dict):
    #     try:
    #         self.logger.debug("submit_request", extra={"path": path, "query": query})
    #         # results = httpx.get(f"http://{self.datastore_url}/{path}/", params=query)
    #         results = await self.http_client.get(
    #             f"http://{self.datastore_url}/{path}/", params=query
    #         )
    #         self.logger.debug("submit_request", extra={"results": results.json()})
    #         return results.json()
    #     except Exception as e:
    #         self.logger.error("submit_request", extra={"reason": e})
    #         return {}

    async def publish_local_definitions(self):
        """Broadcasts local definitions so Datastore globally registers them."""
        await asyncio.sleep(5)
        while True:
            try:
                for cond_name, cond_data in self.sampling_conditions["conditions"].items():
                    config = cond_data["config"]
                    if not config:
                        continue
                    
                    event = SamplingEvent.create_definition_registry_update(
                        resource="samplingcondition",
                        source=f"envds.{self.config.daq_id}.sampling-conditions",
                        data={"samplingcondition": config}
                    )
                    event["destpath"] = f"envds/{self.config.daq_id}/samplingcondition-definition/registry/update"
                    await self.send_event(event)

            except Exception as e:
                self.logger.error("publish_local_definitions", extra={"reason": e})
            
            await asyncio.sleep(60)

    async def sync_sampling_definitions_loop(self):
        """Concurrently fetches remote definitions to keep local memory updated."""
        while True:
            try:
                # 1. Fetch Condition IDs
                ids_resp = await self.submit_get(path="samplingcondition-definition/registry/ids/get")
                if ids_resp and "results" in ids_resp:
                    
                    # 2. Concurrently fetch all bodies
                    async def fetch_cond(cond_id):
                        return await self.submit_request(
                            path="samplingcondition-definition/registry/get", 
                            query={"name": cond_id}
                        )

                    responses = await asyncio.gather(*(fetch_cond(cid) for cid in ids_resp["results"]))

                    # 3. Load them into memory
                    for resp in responses:
                        if resp and "results" in resp and resp["results"]:
                            cond_db = resp["results"][0]
                            self.load_condition(cond_db)

            except Exception as e:
                self.logger.error("sync_sampling_definitions_loop", extra={"reason": e})
            
            await asyncio.sleep(60)

    async def condition_status_monitor(self):
        while True:
            try:
                status = await self.status_buffer.get()

                cond_name = status["status"]["name"]
                cond_ns = status["status"]["sampling_namespace"]
                cond_valid_time = status["status"]["valid_config_time"]

                source_id = (
                    # f"envds.{self.config.daq_id}.sampling-condition.{cond_name}"
                    f"envds.{self.config.daq_id}.sampling-conditions"
                )
                self.logger.debug("evaluate_criteria", extra={"source_id": source_id})
                
                source_topic = source_id.replace(".", "/")

                event = SamplingEvent.create_sampling_condition_status_update(
                    # source="sensor.mockco-mock1-1234", data=record
                    source=source_id,
                    data=status,
                )
                self.logger.debug("condition_status_monitor", extra={"event-type": event["type"]})
                destpath = f"{source_topic}/status/update"
                event["destpath"] = destpath
                event["samplingnamespace"] = cond_ns
                event["validconfigtime"] = cond_valid_time
                self.logger.debug(
                    "evaluate_criteria",
                    extra={"data": event, "destpath": destpath},
                )

                await self.send_event(event)
            except Exception as e:
                self.logger.error("condition_event_monitor", extra={"reason": e})
            
            await asyncio.sleep(0.001)
            self.status_buffer.task_done()

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
                            self.logger.debug(
                                "subscribe", extra={"topic": topic.strip()}
                            )
                            await self.client.subscribe(
                                f"$share/samplingconditions/{topic.strip()}"
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
                if ce["type"] == sampet.variableset_data_update():
                    self.logger.debug(
                        "handle_mqtt_buffer", extra={"ce-type": ce["type"]}
                    )
                    await self.variableset_data_update(ce)
                # elif ce["type"] == "envds.controller.data.update":
                #     await self.controller_data_update(ce)

            except Exception as e:
                self.logger.error("handle_mqtt_buffer", extra={"reason": e})

            await asyncio.sleep(0.0001)
            self.mqtt_buffer.task_done()

    async def variableset_data_update(self, ce: CloudEvent):

        try:
            
            self.logger.debug("variableset_data_update", extra={"ce": ce})
            # get source_id
            # src_id = "111::222::aaa"
            # get target from dict and send source data to all databuffers
            src_id = ce["source"].split(".")[-1]

            # --- ADD THIS CHECK ---
            if src_id not in self.sampling_conditions["sources"]:
                self.logger.debug("variableset_data_update", extra={"message": f"Source {src_id} not mapped in conditions. Ignoring."})
                return
            # ----------------------
            
            data_map = dict()

            # self.logger.debug("variableset_data_update", extra={"src_id": src_id, "sampling_conditions": self.sampling_conditions})
            for target in self.sampling_conditions["sources"][src_id]["targets"]:
                # self.logger.debug("variableset_data_update", extra={"target": target})
                cond_name = target["condition"]
                # self.logger.debug("variableset_data_update", extra={"cond_name": cond_name})
                if cond_name not in data_map:
                    data_map[cond_name] = {"variables": dict()}

                # self.logger.debug("variableset_data_update", extra={"data_map": data_map})
                condition = self.sampling_conditions["conditions"][cond_name]
                # self.logger.debug("variableset_data_update", extra={"condition": condition})
                dt = ce.data["variables"]["time"]

                # if "time" not in data_map[cond_name]["variables"]:
                #     data_map[cond_name]["variables"]["time"] = {"data": dt["data"]}

                # self.logger.debug("variableset_data_update", extra={"data_map": data_map})

                if target["source_variable"] in ce.data["variables"]:
                    val = ce.data["variables"][target["source_variable"]]
                    if target["source_name"] not in data_map[cond_name]["variables"]:
                        data_map[cond_name]["variables"][target["source_name"]] = {
                            "data": val
                        }
            # self.logger.debug("variableset_data_update", extra={"data_map": data_map})

            # once all condition data compiled, send all to condition for processing
            for cond_name, cond_data in data_map.items():
                cond_data["variables"]["time"] = dt
                payload = {"condition_variables": cond_data["variables"]}
                self.logger.debug("variableset_data_update", extra={"cond_payload": payload})
                # db = self.sampling_conditions["conditions"][cond_name]["data_buffer"]
                # self.logger.debug("variableset_data_update", extra={"data_buffer": db})
                # await db.put(payload)
                # self.logger.debug("variableset_data_update", extra={"db_q": db.qsize()})
                await self.sampling_conditions["conditions"][cond_name]["condition"].update(payload)
                # payload = {
                #     "variables": {
                #         "time": {"data": dt["data"]},
                #         condition["source_name"]: {"data": val["data"]}
                #     }
                # }
                # await condition["data_buffer"].put(payload)

        except Exception as e:
            self.logger.error("variableset_data_update", extra={"reason": e})
        pass            

    async def handle_condition_request(self, ce: CloudEvent):

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
    async def handle_condition_update(self, ce: CloudEvent):
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


        # async def index_monitor(self):
        while True:
            update = await self.index_ready_buffer.get()
            self.index_ready_buffer.task_done()
            try:
                self.logger.debug("index_monitor", extra={"update": update})
                index_type = update["index_type"]
                index_value = update["index_value"]
                update_type = update["update_type"]
                target_time = update["index_ready"]

                vm_list = await self.get_valid_variablemaps(target_time=target_time)
                self.logger.debug(
                    "index_monitor", extra={"len": len(vm_list), "vm_list": vm_list}
                )
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
    config = SamplingConditionsManagerConfig()
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
