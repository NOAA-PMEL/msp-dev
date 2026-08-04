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

    deployment_ref: str = "unknown"
    
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
        self.logger.setLevel(logging.INFO)
        self.logger.debug("SamplingCondition instantiated")

        self.config = config
        self.data_buffer = asyncio.Queue(maxsize=60)
        self.status_buffer = status_buffer
        self.source_map = dict()
        self.criteria_map = dict()
        self.default_criterion_module: str = "criteria.default"

        self.current_state = False
        
        # --- WATCHDOG TRACKING ---
        self.last_eval_time = get_datetime().timestamp()
        # Default to 5 minutes if not specified in json
        self.source_max_age = self.config.get("source_max_age", 300) 
        # -------------------------

        self.criterion_tasks = []
        
        self.configure()
        
        # Safely create background tasks
        if hasattr(self, "condition_monitor"):
            self._monitor_task = asyncio.create_task(self.condition_monitor())
        self._status_task = asyncio.create_task(self.update_status_loop())
        self._cleanup_task = asyncio.create_task(self.cleanup_loop())

    def configure(self):
        """
        Parses the sampling condition configuration to map sources and instantiate 
        criteria classes from specified or default modules.
        """
        if not self.config or "sources" not in self.config:
            return

        for source_name, _ in self.config["sources"].items():
            if source_name not in self.source_map:
                self.source_map[source_name] = {"data": dict()}

        try:
            if "criteria" in self.config:
                for group_type, group in self.config["criteria"].items():
                    if group_type not in self.criteria_map:
                        self.criteria_map[group_type] = {"criteria": []}
                    
                    for criterion_config in group:
                        conditions_module = criterion_config.get("conditions_module", "criteria.default")
                        mod_ = importlib.import_module(conditions_module)
                        criterion_class = criterion_config["criterion_class"]
                        criterion = getattr(mod_, criterion_class)(criterion_config)
                        self.criteria_map[group_type]["criteria"].append(criterion)
                        
        except Exception as e:
            self.logger.error("configure", extra={"reason": e})

    async def update(self, payload: dict):
        """
        Receives new telemetry payloads, updates the local memory buffer (source_map),
        and triggers an immediate evaluation of the condition logic.
        """
        try:
            vars_dict = payload.get("condition_variables", {})
            if "time" not in vars_dict:
                return
                
            # Extract the precise timestamp for this data frame
            # (Handles both nested dicts and raw string formats safely)
            time_block = vars_dict["time"]
            timestamp = time_block.get("data") if isinstance(time_block, dict) else time_block
            
            # 1. Store the incoming variables into the source_map cache
            for var_name, var_payload in vars_dict.items():
                if var_name == "time":
                    continue
                # Ensure the variable is one we actually care about
                if var_name in self.source_map:
                    self.source_map[var_name][timestamp] = var_payload
                    
            # 2. Trigger the math evaluation instantly
            await self.evaluate_criteria(timestamp)
            
        except Exception as e:
            self.logger.error("update method error", extra={"reason": str(e)})

    async def evaluate_criteria(self, timestamp):
        """
        Evaluates sensor data against the condition's criteria logic.
        Pushes an immediate status update ONLY if the resulting state changes.
        """
        try:
            cond_name = self.config["metadata"]["name"]
            is_at_pmel = (cond_name == "at_pmel")

            if is_at_pmel:
                self.logger.warning(f"[DEBUG-EVAL] Triggered evaluation for at_pmel at ts={timestamp}")

            crit_states = []
            for group_type, group in self.criteria_map.items():
                group_states = []
                for criterion in group["criteria"]:
                    data = {"time": timestamp}
                    has_all_sources = True
                    
                    for src_name in criterion.get_sources():
                        # 1. JITTER CHECK
                        if timestamp not in self.source_map.get(src_name, {}):
                            if is_at_pmel:
                                self.logger.warning(f"[DEBUG-EVAL] ABORT! Missing '{src_name}' in buffer for ts={timestamp}. Available: {list(self.source_map.get(src_name, {}).keys())}")
                            return 

                        src_payload = self.source_map[src_name][timestamp]
                        
                        # 2. DEAD-MAN & SCHEMA CHECK
                        if src_payload is None:
                            if is_at_pmel:
                                self.logger.warning(f"[DEBUG-EVAL] payload for '{src_name}' is None!")
                            return 
                            
                        # --- FOOLPROOF PAYLOAD EXTRACTION ---
                        if isinstance(src_payload, dict):
                            if src_payload.get("data") is None:
                                if is_at_pmel:
                                    self.logger.warning(f"[DEBUG-EVAL] payload['data'] for '{src_name}' is None!")
                                return 
                            data[src_name] = src_payload["data"]
                        else:
                            data[src_name] = src_payload
                        # ------------------------------------
                        
                        if is_at_pmel:
                            self.logger.warning(f"[DEBUG-EVAL] Successfully extracted '{src_name}' = {data[src_name]}")

                    if not has_all_sources:
                        group_states.append(False)
                        continue

                    # 3. Evaluate normally
                    try:
                        group_states.append(await criterion.evaluate(sources=data))
                    except Exception as e:
                        if is_at_pmel:
                            self.logger.warning(f"[DEBUG-EVAL] FATAL MATH CRASH! Error: {e}")
                        group_states.append(False)

                if group_type == "all":
                    crit_states.append(all(group_states))
                elif group_type == "any":
                    crit_states.append(any(group_states))
                elif group_type == "none":
                    crit_states.append(not any(group_states))
            
            # --- TICK THE WATCHDOG ---
            now = get_datetime().timestamp()
            self.last_eval_time = now
            # -------------------------

            # 4. Final state determination
            state = all(crit_states) if crit_states else False
            
            if is_at_pmel:
                self.logger.warning(f"[DEBUG-EVAL] Final state for at_pmel calculated as: {state}")
                
            is_changed = (self.current_state != state)

            # 5. ONLY push immediately if the state actually flipped
            if is_changed:
                self.logger.warning(f"condition state change", extra={"new_state": state})
                
                self.current_state = state

                cond_ns = self.config["metadata"].get("sampling_namespace", "")
                cond_valid_time = self.config["metadata"].get("valid_config_time", "")
                status_str = "true" if state else "false"
                
                status = {
                    "id": {
                        "app_group": "condition",
                        "app_uid": cond_name,
                        "sampling_namespace": cond_ns,
                        "valid_config_time": cond_valid_time
                    },
                    "state": {
                        "condition_met": {
                            "requested": "true", 
                            "actual": status_str
                        }
                    },
                    "timestamp": get_datetime_string()
                }
                
                await self.status_buffer.put(status)

        except Exception as e:
            self.logger.error("evaluate_criteria", extra={"reason": str(e)})

    async def cleanup_loop(self):
        """
        Runs in the background every 10 seconds to purge stale data from the source_map.
        Expanded to 300 seconds to allow slow sensors to evaluate safely.
        """
        while True:
            try:
                await asyncio.sleep(10)
                
                now_raw = get_datetime()
                if not now_raw:
                    continue
                    
                cutoff_dt = now_raw.replace(tzinfo=timezone.utc) - timedelta(seconds=300)
                cutoff_str = datetime_to_string(cutoff_dt)
                
                for src_name, src_dict in self.source_map.items():
                    stale_keys = [ts for ts in src_dict if ts < cutoff_str]
                            
                    for ts in stale_keys:
                        src_dict.pop(ts, None)
                        
            except Exception as clean_e:
                self.logger.error("cleanup_loop error", extra={"reason": clean_e})

    async def update_status_loop(self):
        """
        Periodically broadcasts status. Acts as a Watchdog to force a False state 
        if the sensor hasn't updated within the source_max_age.
        """
        while True:
            try:
                await asyncio.sleep(10)

                cond_name = self.config["metadata"]["name"]
                cond_ns = self.config["metadata"]["sampling_namespace"]
                cond_valid_time = self.config["metadata"]["valid_config_time"]

                # --- WATCHDOG CHECK ---
                now = get_datetime().timestamp()
                age = now - self.last_eval_time
                
                if self.current_state is True and age > self.source_max_age:
                    self.logger.warning(f"WATCHDOG TRIGGERED for {cond_name}: Data is {age:.1f}s old (Max {self.source_max_age}s). Forcing False.")
                    self.current_state = False
                # ----------------------

                status_str = "true" if self.current_state else "false"

                status = {
                    "id": {
                        "app_group": "condition",
                        "app_uid": cond_name,
                        "sampling_namespace": cond_ns,
                        "valid_config_time": cond_valid_time
                    },
                    "state": {
                        "condition_met": {
                            "requested": "true", 
                            "actual": status_str
                        }
                    },
                    "timestamp": get_datetime_string()
                }
                
                await self.status_buffer.put(status)

            except Exception as e:
                self.logger.error("update_status_loop error", extra={"reason": str(e)})

    def shutdown(self):
        """Cancels background tasks to prevent task leaks when this condition is replaced."""
        if hasattr(self, '_monitor_task'): self._monitor_task.cancel()
        if hasattr(self, '_status_task'): self._status_task.cancel()
        if hasattr(self, '_cleanup_task'): self._cleanup_task.cancel()
        self.logger.info(f"Condition '{self.config['metadata']['name']}' safely shut down.")
class SamplingConditionsManager:
    """docstring for SamplingConditionsManager."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

        self.sampling_conditions = {"conditions": dict(), "sources": {}}
        self.config = SamplingConditionsManagerConfig()
        self.http_client = None
        self._background_tasks = set()

        # 1. CRITICAL: Initialize ALL buffers BEFORE configure()
        self.status_buffer = asyncio.Queue(maxsize=2000)
        self.mqtt_buffer = asyncio.Queue(maxsize=2000)
        self.publish_queue = asyncio.Queue(maxsize=2000)

        # 2. Safely load the definitions and pass the real queue
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
        """Asynchronously initialize clients and loops."""
        self.logger.info("Running SamplingConditionsManager async setup...")
        
        self.http_client = httpx.AsyncClient(
            limits=httpx.Limits(max_keepalive_connections=50, max_connections=100)
        )

        task1 = asyncio.create_task(self.get_from_mqtt_loop())
        task2 = asyncio.create_task(self.handle_mqtt_buffer())
        task3 = asyncio.create_task(self.condition_status_monitor())
        task4 = asyncio.create_task(self.publish_local_definitions())
        task5 = asyncio.create_task(self.sync_sampling_definitions_loop())
        task6 = asyncio.create_task(self.mqtt_publish_loop())
        
        self._background_tasks.update({task1, task2, task3, task4, task5, task6})

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

    def _load_json_dir(self, dir_path_str: str) -> list:
        """Scans a directory for JSON files, injects env vars, and returns the parsed list."""
        results = []
        dir_path = Path(dir_path_str)
        
        if dir_path.exists() and dir_path.is_dir():
            for file_path in dir_path.glob("*.json"):
                try:
                    with open(file_path, "r") as f:
                        raw_content = f.read()
                        
                        # ---> INJECT VARIABLES BEFORE PARSING <---
                        expanded_content = os.path.expandvars(raw_content)
                        
                        data = json.loads(expanded_content)
                        if isinstance(data, list):
                            results.extend(data)
                        else:
                            results.append(data)
                            
                    self.logger.info(f"Loaded and expanded file: {file_path.name}")
                except Exception as e:
                    self.logger.error(f"Failed to parse {file_path.name}", extra={"reason": str(e)})
        else:
            self.logger.info(f"{dir_path_str} not found or empty. Skipping local load.")
            
        return results
    
    # def configure(self):
    #     self.logger.debug("configure", extra={"self.config": self.config})
    #     try:
    #         conditions_path = "/app/config/sampling_conditions.json"
    #         if os.path.exists(conditions_path):
    #             with open(conditions_path, "r") as f:
    #                 conditions = json.load(f)
                    
    #                 # --- IMMUTABLE IDENTITY BOOTSTRAP ---
    #                 if conditions and (self.config.deployment_ref == "unknown" or not self.config.deployment_ref):
    #                     first_ns = conditions[0].get("metadata", {}).get("sampling_namespace", "")
    #                     if "/" in first_ns:
    #                         self.config.deployment_ref = first_ns.split("/")[-1]
    #                         self.logger.info(f"Immutable boot-strapped deployment_ref: {self.config.deployment_ref}")
    #                 # -------------------------------------
                    
    #                 for condition in conditions:
    #                     self.load_condition(condition)
    #             self.logger.debug("configure", extra={"sampling_conditions": self.sampling_conditions})
    #         else:
    #             self.logger.info(f"{conditions_path} not found. Skipping local load.")
    #     except Exception as e:
    #         self.logger.error("configure error", extra={"reason": e})

    def configure(self):
        self.logger.debug("configure", extra={"self.config": self.config})
        try:
            # ---> LOAD FROM THE DIRECTORY <---
            conditions = self._load_json_dir("/app/config/conditions")
            
            if conditions:
                # --- IMMUTABLE IDENTITY BOOTSTRAP ---
                if self.config.deployment_ref == "unknown" or not self.config.deployment_ref:
                    first_ns = conditions[0].get("metadata", {}).get("sampling_namespace", "")
                    if "/" in first_ns:
                        self.config.deployment_ref = first_ns.split("/")[-1]
                        self.logger.info(f"Immutable boot-strapped deployment_ref: {self.config.deployment_ref}")
                # -------------------------------------
                
                for condition in conditions:
                    self.load_condition(condition)
                    
            self.logger.debug("configure", extra={"sampling_conditions": self.sampling_conditions})
            
        except Exception as e:
            self.logger.error("configure error", extra={"reason": str(e)})

    def load_condition(self, condition: dict):
        """Helper to process definitions from either local files or Datastore API using a composite key."""
        if condition.get("kind") != "SamplingCondition":
            return

        cond_name = condition["metadata"]["name"]
        cond_ns = condition.get("metadata", {}).get("sampling_namespace", "")
        
        # Create the compound tuple key to completely isolate platform domains
        composite_key = (cond_name, cond_ns)
        
        new_time_str = condition.get("metadata", {}).get("valid_config_time", "")
        new_time = string_to_datetime(new_time_str)

        # 1. Check if condition already exists using the composite key lookup
        existing_entry = self.sampling_conditions["conditions"].get(composite_key)

        if existing_entry:
            existing_config = existing_entry.get("config", {})
            existing_time_str = existing_config.get("metadata", {}).get("valid_config_time", "")
            existing_time = string_to_datetime(existing_time_str)

            # --- TIME-GATING FIX: Reject stale configs from Datastore ---
            if new_time and existing_time:
                if new_time < existing_time:
                    self.logger.warning(f"REJECTED STALE CONFIG: {cond_name} ({new_time_str} is older than active {existing_time_str})")
                    return
                if new_time == existing_time:
                    return # Already active
            # ------------------------------------------------------------
            
            old_condition_instance = existing_entry.get("condition")
            if old_condition_instance:
                old_condition_instance.shutdown()
        else:
            # Initialize dictionary for a brand-new condition
            self.sampling_conditions["conditions"][composite_key] = {
                "config": None,
                "event_buffer": getattr(self, "status_buffer", None),
                "condition": None,
            }
            
        self.sampling_conditions["conditions"][composite_key]["config"] = condition

        # Map sources to targets
        for source_name, source in condition.get("sources", {}).items():
            vm_name = source["variablemap_name"]
            vs_name = source["variableset_name"]
            src_id = "::".join([vm_name, vs_name])

            if src_id not in self.sampling_conditions["sources"]:
                self.sampling_conditions["sources"][src_id] = {"targets": []}
                
            source_variable = source["variable"]
            target_entry = {
                "condition": composite_key,
                "source_name": source_name,
                "source_variable": source_variable,
            }
            
            if target_entry not in self.sampling_conditions["sources"][src_id]["targets"]:
                self.sampling_conditions["sources"][src_id]["targets"].append(target_entry)

        if not getattr(self, "status_buffer", None):
            self.status_buffer = asyncio.Queue(maxsize=2000)
            self.sampling_conditions["conditions"][composite_key]["event_buffer"] = self.status_buffer

        condition_instance = SamplingCondition(
            config=condition,
            status_buffer=self.status_buffer,
        )
        self.sampling_conditions["conditions"][composite_key]["condition"] = condition_instance

    async def send_event(self, ce):
        """Routes registry definitions to the Datastore via Knative HTTP Broker."""
        try:
            self.logger.debug("send_event (HTTP)", extra={"ce": ce, "kn-broker": self.config.knative_broker})
            if not getattr(self, 'http_client', None):
                self.open_http_client()
            try:
                timeout = httpx.Timeout(5.0, read=10.0)
                
                # Generates HTTP headers and JSON body for the Knative broker
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
            self.logger.error("send_event failed", extra={"reason": str(e)})

    async def send_to_mqtt(self, topic: str, ce):
        """Routes high-volume telemetry and status updates to the MQTT broker."""
        try:
            self.logger.debug("send_to_mqtt (MQTT)", extra={"topic": topic})
            payload = to_json(ce)
            await self.publish_queue.put((topic, payload))
        except Exception as e:
            self.logger.error("send_to_mqtt failed", extra={"reason": str(e)})

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
                condition_count = len(self.sampling_conditions["conditions"])
                self.logger.debug("publish_local_definitions: STARTING LOOP", extra={"total_conditions": condition_count})
                
                # Unpack the composite tuple key structure from memory
                for composite_key, cond_data in self.sampling_conditions["conditions"].items():
                    cond_name, cond_ns = composite_key
                    
                    # Ignore foreign configurations synchronized into memory
                    if self.config.deployment_ref not in cond_ns and cond_ns != "":
                        continue
                        
                    config = cond_data["config"]
                    self.logger.debug(f"publish_local_definitions: processing condition '{cond_name}'", extra={"has_config": bool(config)})
                    
                    if not config:
                        self.logger.warning(f"publish_local_definitions: Condition '{cond_name}' has no config. Skipping.")
                        continue
                    
                    event = SamplingEvent.create_definition_registry_update(
                        resource="samplingcondition-definition",
                        source=f"envds.{self.config.daq_id}.sampling-conditions",
                        data={"samplingcondition": config}
                    )
                    
                    destpath = f"envds/{self.config.daq_id}/samplingcondition-definition/registry/update"
                    event["destpath"] = destpath
                    
                    self.logger.debug(f"publish_local_definitions: routing event for '{cond_name}' via HTTP", extra={"destpath": destpath, "event_type": event.get("type")})
                    
                    await self.send_event(event)
                    self.logger.debug(f"publish_local_definitions: successfully sent event for '{cond_name}'")

            except Exception as e:
                self.logger.error("publish_local_definitions error", extra={"reason": str(e)})
            
            self.logger.debug("publish_local_definitions: LOOP COMPLETE. Sleeping for 60s.")
            await asyncio.sleep(60)

    async def sync_sampling_definitions_loop(self):
        """Concurrently fetches remote definitions to keep local memory updated."""
        while True:
            try:
                self.logger.debug("sync_sampling_definitions_loop: STARTING LOOP. Requesting IDs.")
                
                # 1. Fetch Condition IDs
                ids_resp = await self.submit_get(path="samplingcondition-definition/registry/ids/get")
                self.logger.debug("sync_sampling_definitions_loop: received IDs response", extra={"response": ids_resp})
                
                if ids_resp and "results" in ids_resp:
                    fetched_ids = ids_resp["results"]
                    self.logger.debug("sync_sampling_definitions_loop: parsed IDs", extra={"id_count": len(fetched_ids), "ids": fetched_ids})
                    
                    if fetched_ids:
                        # 2. Concurrently fetch all bodies
                        async def fetch_cond(cond_id):
                            self.logger.debug(f"sync_sampling_definitions_loop: fetching definition body for '{cond_id}'")
                            return await self.submit_request(
                                path="samplingcondition-definition/registry/get", 
                                query={"name": cond_id}
                            )

                        self.logger.debug("sync_sampling_definitions_loop: gathering definitions...")
                        responses = await asyncio.gather(*(fetch_cond(cid) for cid in fetched_ids))
                        self.logger.debug("sync_sampling_definitions_loop: gather complete", extra={"responses_count": len(responses)})

                        # 3. Load them into memory
                        for idx, resp in enumerate(responses):
                            if resp and "results" in resp and resp["results"]:
                                cond_db = resp["results"][0]
                                cond_name = cond_db.get("metadata", {}).get("name", "unknown")
                                self.logger.debug(f"sync_sampling_definitions_loop: loading condition '{cond_name}' into memory")
                                self.load_condition(cond_db)
                            else:
                                self.logger.warning(f"sync_sampling_definitions_loop: empty or invalid response at index {idx}", extra={"resp": resp})
                    else:
                        self.logger.debug("sync_sampling_definitions_loop: no IDs found to fetch.")
                else:
                    self.logger.warning("sync_sampling_definitions_loop: invalid or missing 'results' in IDs response.")

            except Exception as e:
                self.logger.error("sync_sampling_definitions_loop error", extra={"reason": str(e)})
            
            self.logger.debug("sync_sampling_definitions_loop: LOOP COMPLETE. Sleeping for 60s.")
            await asyncio.sleep(60)

    # async def condition_status_monitor(self):
    #     while True:
    #         try:
    #             status = await self.status_buffer.get()

    #             cond_name = status["status"]["name"]
    #             cond_ns = status["status"]["sampling_namespace"]
    #             cond_valid_time = status["status"]["valid_config_time"]

    #             source_id = (
    #                 # f"envds.{self.config.daq_id}.sampling-condition.{cond_name}"
    #                 f"envds.{self.config.daq_id}.sampling-conditions"
    #             )
    #             self.logger.debug("evaluate_criteria", extra={"source_id": source_id})
                
    #             source_topic = source_id.replace(".", "/")

    #             event = SamplingEvent.create_sampling_condition_status_update(
    #                 # source="sensor.mockco-mock1-1234", data=record
    #                 source=source_id,
    #                 data=status,
    #             )
    #             self.logger.debug("condition_status_monitor", extra={"event-type": event["type"]})
    #             destpath = f"{source_topic}/status/update"
    #             event["destpath"] = destpath
    #             event["samplingnamespace"] = cond_ns
    #             event["validconfigtime"] = cond_valid_time
    #             self.logger.debug(
    #                 "evaluate_criteria",
    #                 extra={"data": event, "destpath": destpath},
    #             )

    #             await self.send_event(event)

    #         except Exception as e:
    #             self.logger.error("condition_event_monitor", extra={"reason": e})
            
    #         await asyncio.sleep(0.001)
    #         self.status_buffer.task_done()

    async def condition_status_monitor(self):
        while True:
            try:
                status_data = await self.status_buffer.get()

                cond_name = status_data["id"]["app_uid"]
                cond_ns = status_data["id"]["sampling_namespace"]
                cond_valid_time = status_data["id"]["valid_config_time"]

                source_id = f"envds.{self.config.daq_id}.sampling-conditions"
                self.logger.debug("evaluate_criteria", extra={"source_id": source_id})
                
                event = SamplingEvent.create_sampling_condition_status_update(
                    source=source_id,
                    data=status_data,
                )
                
                self.logger.debug("condition_status_monitor", extra={"event-type": event["type"]})
                
                destpath = f"envds/{self.config.daq_id}/sampling-conditions/status/update"
                event["destpath"] = destpath
                event["samplingnamespace"] = cond_ns
                event["validconfigtime"] = cond_valid_time
                
                # --- DYNAMIC ROUTING PATCH ---
                if "/" in cond_ns:
                    dep_ref = cond_ns.split("/")[-1]
                else:
                    dep_ref = self.config.deployment_ref if self.config.deployment_ref else "unknown"
                
                event["deploymentref"] = dep_ref
                # -----------------------------
                
                self.logger.debug(
                    "evaluate_criteria",
                    extra={"data": event, "destpath": destpath},
                )

                await self.send_to_mqtt(destpath, event)

            except Exception as e:
                self.logger.error("condition_status_monitor", extra={"reason": str(e)})
            
            finally:
                if 'status_data' in locals():
                    self.status_buffer.task_done()
            
            await asyncio.sleep(0.001)

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

            # await asyncio.sleep(0.0001)
            self.mqtt_buffer.task_done()

    async def mqtt_publish_loop(self):
        """Maintains a persistent MQTT connection strictly for outbound status events."""
        reconnect = 5
        client_id = f"conditions-publisher-{ULID()}"
        while True:
            try:
                async with Client(self.config.mqtt_broker, port=self.config.mqtt_port, identifier=client_id) as client:
                    self.logger.info("Connected to MQTT broker for outbound publishing.")
                    while True:
                        topic, payload = await self.publish_queue.get()
                        # QoS 1 ensures the update makes it to the broker
                        await client.publish(topic, payload, qos=1)
                        self.publish_queue.task_done()
            except MqttError as e:
                self.logger.error(f"MQTT Publish Error: {e}. Reconnecting in {reconnect}s...")
                await asyncio.sleep(reconnect)
            except Exception as e:
                self.logger.error("mqtt_publish_loop unexpected error", extra={"reason": str(e)})
                await asyncio.sleep(reconnect)

    async def variableset_data_update(self, ce: CloudEvent):
        try:
            self.logger.debug("variableset_data_update", extra={"ce": ce})
            
            raw_source = ce.get("source", "UNKNOWN_SOURCE")
            src_id = raw_source.split(".")[-1]

            # --- ADDED DEBUGGING ---
            available_sources = list(self.sampling_conditions["sources"].keys())
            is_match = src_id in self.sampling_conditions["sources"]
            
            self.logger.info(
                "DEBUG variableset_data_update routing", 
                extra={
                    "raw_ce_source": raw_source,
                    "extracted_src_id": src_id,
                    "available_configured_sources": available_sources,
                    "will_it_process": is_match
                }
            )
            # -----------------------

            if not is_match:
                self.logger.warning(f"DROPPING DATA: extracted src_id '{src_id}' not found in configured sources!")
                return

            data_map = dict()

            for target in self.sampling_conditions["sources"][src_id]["targets"]:
                cond_key = target["condition"]
                
                if cond_key not in data_map:
                    data_map[cond_key] = {"variables": dict()}

                condition = self.sampling_conditions["conditions"][cond_key]
                
                # STRICT SCHEMA: Extract time
                dt = ce.data["variables"]["time"]["data"]

                if target["source_variable"] in ce.data["variables"]:
                    # STRICT SCHEMA: Pass the entire CF-compliant dictionary untouched
                    val_block = ce.data["variables"][target["source_variable"]]
                    
                    if target["source_name"] not in data_map[cond_key]["variables"]:
                        data_map[cond_key]["variables"][target["source_name"]] = val_block

            for cond_key, cond_data in data_map.items():
                cond_data["variables"]["time"] = {"data": dt} # Keep time in schema format too
                payload = {"condition_variables": cond_data["variables"]}
                
                # --- NEW DEBUG FOR AT_PMEL INGEST ---
                if isinstance(cond_key, tuple) and cond_key[0] == "at_pmel":
                    self.logger.info(f"DEBUG INGEST [at_pmel]: Preparing to push payload to buffer. Variables present: {list(cond_data['variables'].keys())}")
                elif cond_key == "at_pmel": # Fallback if routing uses string instead of tuple
                    self.logger.info(f"DEBUG INGEST [at_pmel]: Preparing to push payload to buffer. Variables present: {list(cond_data['variables'].keys())}")
                # ------------------------------------

                await self.sampling_conditions["conditions"][cond_key]["condition"].update(payload)

        except Exception as e:
            self.logger.error("variableset_data_update", extra={"reason": str(e)})

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
