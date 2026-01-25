import asyncio
import importlib
import json
import logging
import math
import sys
from time import sleep
from typing import List, Literal

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


class SamplingOperationsManagerConfig(BaseSettings):
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

    system_init_control: str = "auto"
    system_init_mode: str | None = None

    class Config:
        env_prefix = "SAMPLING_OPERATIONS_"
        case_sensitive = False


class SamplingAction:
    """
    Docstring for SamplingAction
    """

    def __init__(self, config, target_buffer):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug("SamplingAction instantiated")

        self.config = config

        # buffers
        self.data_buffer = asyncio.Queue(maxsize=60)
        # self.source_buffer = source_buffer
        self.target_buffer = target_buffer

        self.sources = {"variables": dict(), "data": dict()}
        self.targets = {"variables": dict()}

        self.configure()
        asyncio.create_task(self.update_monitor())
        # asyncio.create_task(self.requirements_monitor())
        # asyncio.create_task(self.update_status_loop())

    def configure(self):
        try:
            if not self.config:
                return

            print("here:1")
            kind = self.config["kind"]
            name = self.config["metadata"]["name"]

            print("here:2")
            self.action_module = self.config["metadata"].get(
                "action_module", "default_actions"
            )
            print("here:3")
            self.action_def = self.config["metadata"].get("action_def", "default_def")
            print("here:4")

            # set default max age of 60s
            self.source_max_age = self.config.get("source_max_age", 60)
            print("here:5")

            if "sources" in self.config:
                for src_name, src_config in self.config["sources"].items():
                    if src_name not in self.sources["variables"]:
                        self.sources["variables"][src_name] = dict()
                    self.sources["variables"][src_name] = src_config
                    print(f"here:6 - {src_config}")

                    vm_name = src_config["variablemap_name"]
                    vs_name = src_config["variableset_name"]
                    src_id = "::".join([vm_name, vs_name])
                    v_name = src_config["variable"]
                    print("here:7")

                    if src_id not in self.sources["data"]:
                        self.sources["data"][src_id] = dict()
                    print("here:8")
                    if v_name not in self.sources["data"][src_id]:
                        self.sources["data"][src_id][v_name] = dict()
                    print("here:9")

            if "targets" in self.config:
                for src_name, src_config in self.config["targets"].items():
                    if src_name not in self.targets["variables"]:
                        self.targets["variables"][src_name] = dict()
                    self.targets["variables"][src_name] = src_config
                    print("here:10")

            mod_ = importlib.import_module(self.action_module)
            print("here:11")
            self.method = getattr(mod_, self.action_def)
            print("here:12")
        except Exception as e:
            self.logger.error("configure-action", extra={"reason": e})

    async def run(self):
        source_vars = dict()

        dt_now = get_datetime().replace(tzinfo=timezone.utc)
        max_time = self.source_max_age
        min_dt = get_datetime_with_delta(delta=(-(max_time)), dt=dt_now)
        for src_name, src in self.sources["variables"].items():
            src_id = "::".join([src["variablemap_name"], src["variableset_name"]])
            last_var = self.sources["data"][src_id][src_name][src["variable"]]
            if (
                string_to_datetime(last_var["latest_update"]).replace(
                    tzinfo=timezone.utc
                )
                < min_dt
            ):
                return None
            source_vars[src_name] = last_var["data"]

        result = await self.method(**source_vars)

        target_vars = dict()
        for trg_name, trg in self.targets["variables"].items():
            if trg_name in result:
                target_vars[trg_name] = {"data": result[trg_name], "metadata": trg}
        await self.target_buffer.put(target_vars)

        self.logger.debug(
            "run - send set settings event", extra={"target_vars": target_vars}
        )

    async def update(self, data: CloudEvent):
        self.logger.debug("update", extra={"update_data": data})
        await self.data_buffer.put(data)

    async def update_monitor(self):

        while True:
            try:
                event = await self.data_buffer.get()
                self.logger.debug(
                    "action.update_monitor", extra={"update_status": event}
                )
                try:
                    src_id = event["source"].split(".")[-1]
                    dt = event.data["variables"]["time"]
                    for var_name, var_data in self.sources["data"][src_id].items():
                        if var_name in event.data["variables"]:
                            var_data["last_update"] = dt["data"]
                            var_data["data"] = event.data["variables"][var_name]["data"]
                except KeyError:
                    continue
            except Exception as e:
                self.logger.error("action.update_monitor", extra={"reason": e})

            self.logger.debug("action.update_monitor", extra={"sources": self.sources})
            await asyncio.sleep(0.001)
            self.data_buffer.task_done()


class SamplingMode:
    """
    Docstring for SamplingMode
    """

    def __init__(self, config, status_buffer, actions_buffer, transitions_buffer):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug("SamplingMode instantiated")

        self.config = config

        # buffers
        self.update_buffer = asyncio.Queue(maxsize=60)
        self.status_buffer = status_buffer
        self.actions_buffer = actions_buffer
        self.transitions_buffer = transitions_buffer

        self.requirements = dict()
        self.actions = {"true": [], "false": []}
        self.transitions = {"true": [], "false": []}

        self.active = False
        self.current_state = False

        self.configure()
        asyncio.create_task(self.update_monitor())
        asyncio.create_task(self.requirements_monitor())
        asyncio.create_task(self.update_status_loop())

    def configure(self):

        try:
            self.logger.debug("configure-samplingmode")
            if not self.config:
                return

            if "requirements" in self.config:
                for req_config in self.config["requirements"]:

                    kind = req_config["kind"]
                    name = req_config["name"]

                    if kind not in self.requirements:
                        self.requirements[kind] = dict()
                    if name not in self.requirements[kind]:
                        self.requirements[kind][name] = {
                            "config": req_config,
                            "data": {},
                            "status": False,
                        }
            print(f"configure-samplingmode - self.requirements {self.requirements}")
            if "actions" in self.config:
                self.logger.debug("configure-samplingmode", extra={"actions": self.config["actions"]})
                for act_test, act_list in self.config["actions"].items():
                    self.logger.debug("configure-samplingmode", extra={"act_test": act_test, "act_list": act_list})
                    if act_test in self.actions:
                        for act in act_list:
                            self.logger.debug("configure-samplingmode", extra={"act": act})
                            if act not in self.actions[act_test]:
                                self.actions[act_test].append(act)
                                self.logger.debug("configure-samplingmode", extra={"act_test": act_test, "actions": self.actions[act_test]})
                    # for act_true in self.config["actions"]["true"]:
                    #     self.actions["true"].append(act_true)
                    # for act_false in self.config["actions"]["false"]:
                    #     self.actions["true"].append(act_false)

            print(f"configure-samplingmode - self.actions {self.actions}")
            if "transitions" in self.config:
                for act_test, act_list in self.config["transitions"].items():
                    if act_test in self.transitions:
                        for act in act_list:
                            if act not in self.transitions[act_test]:
                                self.transitions[act_test].append(act)
            print(f"configure-samplingmode self.transitions {self.transitions}")
        # if "transitions" in self.config:
        #     for act_config in self.config["transitions"]:
        #         for act_true in self.config["transitions"]["true"]:
        #             self.transitions["true"].append(act_true)
        #         for act_false in self.config["transitions"]["false"]:
        #             self.transitions["true"].append(act_false)
        except Exception as e:
            self.logger.error("configure-samplemode", extra={"reason": e})

    def activate(self, active: bool):
        self.active = active

    def is_active(self) -> bool:
        return self.active

    async def update(self, status):
        self.logger.debug("update", extra={"update_data": status})
        await self.update_buffer.put(status)

    async def update_monitor(self):

        while True:
            try:
                status = await self.update_buffer.get()
                self.logger.debug(
                    "mode.update_monitor", extra={"update_status": status}
                )
                try:
                    # self.requirements[status["kind"]][status["name"]]["data"][status["time"]] = status["status"]
                    self.requirements[status["kind"]][status["name"]]["status"] = (
                        status["status"]
                    )
                    if self.active:
                        self.logger.debug(
                            "mode.update_monitor:active",
                            extra={
                                "mode_name": self.config["metadata"]["name"],
                                "active": self.is_active(),
                                "req_kind": status["kind"],
                                "req_name": status["name"],
                                "req_status": status["status"],
                                "reqs": self.requirements,
                            },
                    )
                except KeyError:
                    continue
            except Exception as e:
                self.logger.error("mode.update_monitor", extra={"reason": e})

            await asyncio.sleep(0.001)
            self.update_buffer.task_done()

    async def requirements_monitor(self):

        while True:
            self.logger.debug(
                "SamplingMode.requirements_monitor",
                extra={"data_buffer": self.requirements},
            )

            # all delays are done at state level so current status is directly updated
            try:
                mode_status = []
                current_dt = get_datetime().replace(tzinfo=timezone.utc)
                for req_type, req_kind in self.requirements.items():
                    for req_name, req in req_kind.items():
                        mode_status.append(req["status"])
                        if self.active:
                            self.logger.debug(
                                "SamplingMode.requirements_monitor",
                                extra={"req_name": req_name, "mode_status": mode_status},
                            )

                current_dt = get_datetime().replace(tzinfo=timezone.utc)
                current_secs = current_dt.second
                latest_status = all(mode_status)
                if self.active:
                    self.logger.debug(
                        "SamplingMode.requirements_monitor:last_check",
                        extra={
                            "mode_name": self.config["metadata"]["name"],
                            "active": self.is_active(),
                            "mode_status": mode_status,
                            "current_state": self.current_state,
                            "new_state": latest_status,
                        },
                    )
                if latest_status != self.current_state:
                    # send event with updated condition state
                    self.logger.debug(
                        "SamplingMode.requirements_monitor - send update with new state"
                    )
                    try:
                        self.current_state = latest_status
                        if self.active:
                            mode_kind = self.config["kind"]
                            mode_name = self.config["metadata"]["name"]
                            mode_ns = self.config["metadata"]["sampling_namespace"]
                            mode_valid_time = self.config["metadata"]["valid_config_time"]

                            status = {
                                "status": {
                                    "kind": mode_kind,
                                    "time": get_datetime_string(),
                                    "name": mode_name,
                                    "sampling_namespace": mode_ns,
                                    "valid_config_time": mode_valid_time,
                                    "status": self.current_state,
                                }
                            }
                            self.logger.debug(
                                "active.status", extra={"current_status": status}
                            )
                            await self.status_buffer.put(status)

                            run_type = str(self.current_state).lower()

                            self.logger.debug("active.action.list", extra={"actions": self.actions})
                            for act in self.actions[run_type]:
                                action = {
                                    "action": {"kind": act["kind"], "name": act["name"]}
                                }
                                self.logger.debug("active.action", extra={"action": action})
                                await self.actions_buffer.put(action)

                            for tran in self.transitions[run_type]:
                                transition = {
                                    "transition": {
                                        "kind": tran["kind"],
                                        "name": tran["name"],
                                    }
                                }
                                self.logger.debug(
                                    "active.transition", extra={"transition": transition}
                                )
                                await self.transitions_buffer.put(transition)
                    except Exception as e:
                        self.logger.error("requirments_monitor - update active modes", extra={"reason": e})

                # elif (current_secs % 30) == 0:
                #     self.current_state = latest_status
                #     if self.active:
                #         mode_kind = self.config["metadata"]["kind"]
                #         mode_name = self.config["metadata"]["name"]
                #         mode_ns = self.config["metadata"]["sampling_namespace"]
                #         mode_valid_time = self.config["metadata"]["valid_config_time"]

                #         status = {
                #             "status": {
                #                 "kind": mode_kind,
                #                 "time": get_datetime_string(),
                #                 "name": mode_name,
                #                 "sampling_namespace": mode_ns,
                #                 "valid_config_time": mode_valid_time,
                #                 "status": self.current_state
                #             }
                #         }
                #         await self.status_buffer.put(status)

            except Exception as e:
                self.logger.error("requirements_monitor", extra={"reason": e})

            await asyncio.sleep(time_to_next(1))

    async def update_status_loop(self):
        while True:
            try:
                if self.active:
                    self.logger.debug("SamplingMode.update_state_loop - send update")

                    mode_kind = self.config["kind"]
                    mode_name = self.config["metadata"]["name"]
                    mode_ns = self.config["metadata"]["sampling_namespace"]
                    mode_valid_time = self.config["metadata"]["valid_config_time"]

                    status = {
                        "status": {
                            "kind": mode_kind,
                            "time": get_datetime_string(),
                            "name": mode_name,
                            "sampling_namespace": mode_ns,
                            "valid_config_time": mode_valid_time,
                            "status": self.current_state,
                        }
                    }
                    self.logger.debug(
                        "SamplingMode.update_state_loop - send update",
                        extra={"sampling_status": status},
                    )
                    await self.status_buffer.put(status)

                # await self.update_status(status)
            except Exception as e:
                self.logger.error("SamplingMode.update_state_loop", extra={"reason": e})

            await asyncio.sleep(10)


class SamplingOperationsManager:
    """docstring for SamplingOperationsManager."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug("SamplingOperationsManager instantiated")

        self.sampling_modes = dict()
        # self.sampling_mode_groups = dict()
        self.sampling_actions = dict()

        self.actions_source_map = dict()
        self.mode_requirements_map = dict()
        self.mode_transition_map = dict()
        self.active_modes = {"SystemMode": [], "SamplingMode": []}

        # self.sampling_mode_control = True
        # self.sampling_modes = dict()
        # self.sampling_states = dict()
        self.sampling_conditions = {"conditions": dict(), "sources": {}}

        self.status_buffer = asyncio.Queue(maxsize=60)
        self.actions_buffer = asyncio.Queue(maxsize=60)
        self.transitions_buffer = asyncio.Queue(maxsize=60)

        self.actions_target_buffer = asyncio.Queue(maxsize=60)

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

        self.config = SamplingOperationsManagerConfig()
        self.configure()
        # print("here:7")

        self.http_client = None

        self.mqtt_buffer = asyncio.Queue()
        asyncio.create_task(self.get_from_mqtt_loop())
        asyncio.create_task(self.handle_mqtt_buffer())
        asyncio.create_task(self.mode_status_monitor())
        asyncio.create_task(self.mode_action_monitor())
        asyncio.create_task(self.mode_transition_monitor())
        asyncio.create_task(self.system_mode_loop())
        # asyncio.create_tasks(self.sampling_mode_monitor())
        # asyncio.create_tasks(self.sampling_state_monitor())
        # asyncio.create_task(self.sampling_condition_monitor())
        # asyncio.create_tasks(self.sampling_action_monitor())

        # print("SamplingOperationsManager: init: here:8")

    def configure(self):
        # set clients

        # self.logger.debug("configure", extra={"self.config": self.config})

        try:

            # load actions
            # load modes
            # (in init) start monitors
            # (in init) start handlers for state and mode status information
            print(f"self.config:  {self.config}")
            with open("/app/config/sampling_operations_actions.json", "r") as f:
                actions = json.load(f)

            for action in actions:
                kind = action["kind"]
                name = action["metadata"]["name"]
                if kind not in self.sampling_actions:
                    self.sampling_actions[kind] = dict()
                self.sampling_actions[kind][name] = {
                    "config": action,
                    "action": SamplingAction(action, self.actions_target_buffer),
                }

                if "sources" in action:
                    for src_name, src in action["sources"].items():
                        vm_name = src["variablemap_name"]
                        vs_name = src["variableset_name"]
                        src_id = "::".join([vm_name, vs_name])
                        v_name = src["variable"]
                        if src_id not in self.actions_source_map:
                            self.actions_source_map[src_id] = []
                        # if v_name not in self.actions_source_map[src_id]:
                        #     self.actions_source_map[src_id][v_name] = []
                        self.actions_source_map[src_id].append(
                            {
                                "kind": kind,
                                "name": name,
                                # "variable": src_name
                            }
                        )
            print(f"configure-manager - self.actions {self.sampling_actions}")
            # with open("/app/config/sampling_mode_groups.json", "r") as f:
            #     mode_groups = json.load(f)

            # for group in mode_groups:
            #     kind = group["kind"]
            #     name = group["metadata"]["name"]
            #     if kind not in self.sampling_mode_groups:
            #         self.sampling_mode_groups[kind] = dict()
            #     self.sampling_mode_groups[kind][name] = {
            #         "config": group,
            #         "modes": [],
            #         "active_modes": []
            #     }

            with open("/app/config/sampling_operations_modes.json", "r") as f:
                modes = json.load(f)

            for mode in modes:
                kind = mode["kind"]
                name = mode["metadata"]["name"]
                if kind not in self.sampling_modes:
                    self.sampling_modes[kind] = dict()
                self.sampling_modes[kind][name] = {
                    "config": mode,
                    "mode": SamplingMode(
                        mode,
                        self.status_buffer,
                        self.actions_buffer,
                        self.transitions_buffer,
                    ),
                }

                # map each requirement to the modes listening for them
                if "requirements" in mode:
                    # if "modes" in mode["requirements"]:
                    for req_mode in mode["requirements"]:
                        try:
                            req_kind = req_mode["kind"]
                            req_name = req_mode["name"]
                            if req_kind not in self.mode_requirements_map:
                                self.mode_requirements_map[req_kind] = dict()
                            if req_name not in self.mode_requirements_map[req_kind]:
                                self.mode_requirements_map[req_kind][req_name] = []

                            self.mode_requirements_map[req_kind][req_name].append(
                                {"kind": kind, "name": name, "active": False}
                            )
                        except KeyError:
                            continue
                print(f"configure-manager self.mode {name}: {self.mode_requirements_map}")
                print(f"configure-manager self.mode {self.sampling_modes}")
                # if "states" in mode["requirements"]:
                #     for req_mode in mode["requirements"]["states"]:
                #         try:
                #             req_kind = req_mode["kind"]
                #             req_name = req_mode["metadata"]["name"]
                #             if req_kind not in self.mode_requirements_map:
                #                 self.mode_requirements_map[req_kind] = dict()
                #                 if req_name not in self.mode_requirements_map[req_kind]:
                #                     self.mode_requirements_map[req_kind][req_name] = []

                #                 self.mode_requirements_map[req_kind][req_name].append(
                #                     {
                #                         "kind": kind,
                #                         "name": name,
                #                         "active": False
                #                     }
                #                 )
                #         except KeyError:
                #             continue

        except Exception as e:
            self.logger.error("configure-manager", extra={"reason": e})

    def init_modes(self):
        self.activate_system_mode(self.config.system_init_mode)
        self.set_system_control(self.config.system_init_control)

    def set_system_control(self, control="auto"):
        self.system_control = control

    def activate_required_modes(self, required_mode: dict):

        kind = required_mode["kind"]
        name = required_mode["name"]

        if kind not in self.active_modes:
            return
        self.active_modes[kind].append(name)
        reqs = self.sampling_modes[kind][name]["config"].get("requirements", [])

        for req in reqs:
            self.activate_required_modes(req)

    def activate_system_mode(self, name: str):
        try:
            self.logger.debug("activate_system_mode")
            # self.active_modes = {
            #     "SystemMode": None,

            # }
            if name not in self.sampling_modes["SystemMode"]:
                self.logger.info(
                    "activate_system_mode-can't activate non SystemMode",
                    extra={"req_name": name},
                )
                return
            # if name in self.sampling_modes["SystemMode"]:
            self.active_modes = {"SystemMode": [name], "SamplingMode": []}
            reqs = self.sampling_modes["SystemMode"][name]["config"].get(
                "requirements", []
            )

            for req in reqs:
                self.activate_required_modes(req)

            self.logger.debug(
                "activate_system_mode", extra={"active_modes": self.active_modes}
            )

            # deactivate all modes
            for mod_type, modes in self.sampling_modes.items():
                for mode_name, mode in modes.items():
                    mode["mode"].activate(False)

            # activate modes in active map
            for mode_type, modes in self.active_modes.items():
                for mode_name in modes:
                    mode = self.sampling_modes[mode_type][mode_name]
                    mode["mode"].activate(True)
                    self.logger.debug(
                        "activate_system_mode",
                        extra={mode_name: mode["mode"].is_active()},
                    )
        except Exception as e:
            self.logger.error("activate_system_mode", extra={"reason": e})

    async def system_mode_loop(self):

        while True:
            # if no active mode, set to init value
            if not self.active_modes["SystemMode"]:
                self.activate_system_mode(self.config.system_init_mode)
                pass
            else:
                pass

            await asyncio.sleep(time_to_next(1))

    # async def activate_system_mode(self, name:str):
    #     if name not in self.samping_modes["SystemMode"]:
    #         return
    #     mode_entry = self.samping_modes["SystemMode"][name]
    #     if mode_entry["instance"] is None:
    #         mode = self.create_mode(mode_entry["config"])

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
            results = await self.http_client.get(
                f"http://{self.datastore_url}/{path}/", params=query
            )
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
                    await self.requirement_status_update(ce)
                # elif ce["type"] == "envds.controller.data.update":
                #     await self.controller_data_update(ce)

            except Exception as e:
                self.logger.error("handle_mqtt_buffer", extra={"reason": e})

            await asyncio.sleep(0.0001)
            self.mqtt_buffer.task_done()

    async def mode_action_monitor(self):
        while True:
            try:
                action = await self.actions_buffer.get()

                kind = action["action"]["kind"]
                name = action["action"]["name"]

                action_result = await self.sampling_actions[kind][name]["action"].run()

                self.logger.debug(
                    "mode_action_monitor - build settings request",
                    extra={"action_result": action_result},
                )

            except Exception as e:
                self.logger.error("mode_action_monitor", extra={"reason": e})

            await asyncio.sleep(0.001)
            self.action_buffer.task_done()

    async def mode_transition_monitor(self):
        while True:
            try:
                transition = await self.transitions_buffer.get()
                if self.system_control == "auto":
                    kind = transition["transition"]["kind"]
                    name = transition["transition"]["name"]

                    if kind == "SystemMode":
                        self.activate_system_mode(name)
                        self.logger.debug(
                            "mode_transition_monitor - transition mode",
                            extra={"transition_request": transition},
                        )

            except Exception as e:
                self.logger.error("mode_action_monitor", extra={"reason": e})

            await asyncio.sleep(0.001)
            self.transitions_buffer.task_done()

    async def mode_status_monitor(self):

        while True:
            try:
                status = await self.status_buffer.get()

                mode_name = status["status"]["name"]
                mode_ns = status["status"]["sampling_namespace"]
                mode_valid_time = status["status"]["valid_config_time"]

                source_id = (
                    # f"envds.{self.config.daq_id}.sampling-mode.{mode_name}"
                    f"envds.{self.config.daq_id}.sampling-operations"
                )
                self.logger.debug("mode_status_monitor", extra={"source_id": source_id})

                source_topic = source_id.replace(".", "/")

                event = SamplingEvent.create_sampling_mode_status_update(
                    # source="sensor.mockco-mock1-1234", data=record
                    source=source_id,
                    data=status,
                )
                destpath = f"{source_topic}/status/update"
                event["destpath"] = destpath
                event["samplingnamespace"] = mode_ns
                event["validconfigtime"] = mode_valid_time
                self.logger.debug(
                    "mode_status_monitor",
                    extra={"data": event, "destpath": destpath},
                )

                # update local requirements
                await self.requirement_status_update(event)

                # send event to other listeners and for logging
                await self.send_event(event)

            except Exception as e:
                self.logger.error("mode_status_monitor", extra={"reason": e})

            await asyncio.sleep(0.001)
            self.status_buffer.task_done()

    async def requirement_status_update(self, ce: CloudEvent):

        try:

            self.logger.debug("SOM.requirement_status_update", extra={"ce": ce})

            # for req_type, status in ce.data.items():
            try:
                status = ce.data["status"]
                req_kind = status["kind"]
                req_name = status["name"]
                self.logger.debug(
                    "SOM.requirement_status_update",
                    extra={"sampling_modes": self.sampling_modes},
                )
                print(
                    f"SOM.requirement_status_update - mode_req_map: {self.mode_requirements_map}"
                )
                for req_map in self.mode_requirements_map[req_kind][req_name]:
                    print(f"SOM.requirement_status_update - req_map: {req_map}")
                    mode = self.sampling_modes[req_map["kind"]][req_map["name"]]["mode"]
                    if mode.is_active():
                        self.logger.debug("SOM.requirement_status_update:active",
                            extra={
                                "req_kind": req_kind,
                                "req_name": req_name,
                                "mode": mode,
                                "status": status
                            }             
                        )
                    await mode.update(status)

            except KeyError as e:
                self.logger.error("SOM.requirement_status_update", extra={"reason": e})
                pass

        except Exception as e:
            self.logger.error("SOM.requirement_status_update", extra={"reason": e})
        pass

    async def variableset_data_update(self, ce: CloudEvent):

        try:

            self.logger.debug("variableset_data_update", extra={"ce": ce})
            # get source_id
            # src_id = "111::222::aaa"
            # get target from dict and send source data to all databuffers
            src_id = ce["source"].split(".")[-1]

            if src_id in self.actions_source_map:
                for action_map in self.actions_source_map[src_id]:
                    kind = action_map["kind"]
                    name = action_map["name"]
                    action = self.sampling_actions[kind][name]["action"]
                    self.logger.debug(
                        "variableset_data_update",
                        extra={"action": action, "update_ce": ce},
                    )
                    await action.update(ce)

            # data_map = dict()

            # # self.logger.debug("variableset_data_update", extra={"src_id": src_id, "sampling_conditions": self.sampling_conditions})
            # for target in self.sampling_conditions["sources"][src_id]["targets"]:
            #     # self.logger.debug("variableset_data_update", extra={"target": target})
            #     cond_name = target["condition"]
            #     # self.logger.debug("variableset_data_update", extra={"cond_name": cond_name})
            #     if cond_name not in data_map:
            #         data_map[cond_name] = {"variables": dict()}

            #     # self.logger.debug("variableset_data_update", extra={"data_map": data_map})
            #     condition = self.sampling_conditions["conditions"][cond_name]
            #     # self.logger.debug("variableset_data_update", extra={"condition": condition})
            #     dt = ce.data["variables"]["time"]

            #     # if "time" not in data_map[cond_name]["variables"]:
            #     #     data_map[cond_name]["variables"]["time"] = {"data": dt["data"]}

            #     # self.logger.debug("variableset_data_update", extra={"data_map": data_map})

            #     if target["source_variable"] in ce.data["variables"]:
            #         val = ce.data["variables"][target["source_variable"]]
            #         if target["source_name"] not in data_map[cond_name]["variables"]:
            #             data_map[cond_name]["variables"][target["source_name"]] = {
            #                 "data": val
            #             }
            # # self.logger.debug("variableset_data_update", extra={"data_map": data_map})

            # # once all condition data compiled, send all to condition for processing
            # for cond_name, cond_data in data_map.items():
            #     cond_data["variables"]["time"] = dt
            #     payload = {"condition_variables": cond_data["variables"]}
            #     self.logger.debug("variableset_data_update", extra={"cond_payload": payload})
            #     # db = self.sampling_conditions["conditions"][cond_name]["data_buffer"]
            #     # self.logger.debug("variableset_data_update", extra={"data_buffer": db})
            #     # await db.put(payload)
            #     # self.logger.debug("variableset_data_update", extra={"db_q": db.qsize()})
            #     await self.sampling_conditions["conditions"][cond_name]["condition"].update(payload)
            #     # payload = {
            #     #     "variables": {
            #     #         "time": {"data": dt["data"]},
            #     #         condition["source_name"]: {"data": val["data"]}
            #     #     }
            #     # }
            #     # await condition["data_buffer"].put(payload)

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
    config = SamplingOperationsManagerConfig()
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
