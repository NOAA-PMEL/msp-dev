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


class SamplingStatesManagerConfig(BaseSettings):
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
        env_prefix = "SAMPLING_STATES_"
        case_sensitive = False


class SamplingState:
    """
    Docstring for SamplingState
    """

    def __init__(self, config, status_buffer):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug("SamplingState instantiated")

        self.config = config
        self.requirements = dict()


        # self.data_buffer = data_buffer
        self.update_buffer = asyncio.Queue(maxsize=60)
        self.status_buffer = status_buffer
        # self.source_map = {"source_id": dict(), "source_name": dict()}
        self.source_map = dict()
        self.criteria_map = dict()
        # self.source_data = dict()
        self.default_criterion_module: str = (
            # "apps.sampling-operations.sampling-conditions.criteria"
            "criteria"
        )

        self.current_status = False

        self.criterion_tasks = []
        
        self.configure()
        asyncio.create_task(self.requirement_monitor())
        asyncio.create_task(self.data_gc())

    def configure(self):

        if not self.config or "requirements" not in self.config:
            return

        for req in self.config["requirements"]:
            if req["kind"] not in self.requirements:
                self.requirements[req["kind"]] = dict()
            self.requirements[req["kind"]][req["name"]] = {
                "transition_time": {
                    "to_become_false": req["required_time_to_transition"]["to_become_false"],
                    "to_become_true": req["required_time_to_transition"]["to_become_true"]
                },
                "status": False,
                "data": dict()
            }

    async def update(self, status):
        self.logger.debug("update", extra={"update_status": status})
        await self.update_buffer.put(status)

    async def update_monitor(self):

        while True:
            try:
                status = await self.update_buffer.get()
                self.logger.debug("update_monitor", extra={"update_status": status})
                try:
                    self.requirements[status["kind"]][status["name"]]["data"][status["time"]] = status["status"]
                except KeyError:
                    continue
            except Exception as e:
                self.logger.error("update_monitor", extra={"reason": e})

            await asyncio.sleep(0.001)
            self.update_buffer.task_done()

    async def requirement_monitor(self):

        while True:
            try:
                state_status = []
                for req_type, req_kind in self.requirements.items():
                    for req_name, req in req_kind.items():
                        current_status = req["status"]
                        transition_time = req["transition_time"]["to_become_false"]
                        if current_status is False:
                            transition_time = req["transition_time"]["to_become_true"]
                        # delta = timedelta(seconds=(transition_time * -1))
                        transition_dt = get_datetime_with_delta(-(transition_time))

                        req_status = []
                        self.logger.debug("requirement_monitor", extra={"reqs": self.requirements})
                        for ts, st in req["data"].items():
                            if ts > datetime_to_string(transition_dt):
                                req_status.append(st)
                        req["status"] = all(req_status)
                        state_status.append(req["status"])
                        self.logger.debug("requirement_monitor", extra={"state_status": state_status, "req_status": req_status})
                current_dt = get_datetime()
                current_secs = current_dt.second
                if self.current_status:
                    latest_status = any(state_status)
                else:
                    latest_status = all(state_status)
                # latest_status = all(state_status)
                if self.current_status != latest_status or (current_secs % 30) == 0:
                    self.logger.info("status change", extra={"old_status": self.current_status, "new_status": latest_status})
                    self.current_status = latest_status

                    state_name = self.config["metadata"]["name"]
                    state_ns = self.config["metadata"]["sampling_namespace"]
                    state_valid_time = self.config["metadata"]["valid_config_time"]

                    status = {
                        "condition": {
                            "kind": "SamplingState",
                            "time": get_datetime_string(),
                            "name": state_name,
                            "sampling_namespace": state_ns,
                            "valid_config_time": state_valid_time,
                            "status": self.current_status
                        }
                    }
                    await self.status_buffer.put(status)
            except Exception as e:
                self.logger.error("requirement_monitor", extra={"reason": e})

            await asyncio.sleep(time_to_next(1))

    async def data_gc(self):
        while True:
            try:
                for req_type, req_kind in self.requirements.items():
                    for req_name, req in req_kind.items():
                        gc_time = req["transition_time"]["to_become_false"]
                        if req["transition_time"]["to_become_true"] > gc_time:
                            gc_time = req["transition_time"]["to_become_true"]

                        # delta = timedelta(seconds=(gc_time * -1))
                        # gc_dt = get_datetime_with_delta(delta)
                        gc_dt = get_datetime_with_delta(-(gc_time))
                        keys = list(req["data"].keys())
                        for k in keys:
                            if k < datetime_to_string(gc_dt):
                                req["data"].pop(k)

            except Exception as e:
                self.logger.error("data_gc", extra={"reason": e})
            await asyncio.sleep(time_to_next(30))
            
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
            if state != self.current_state:
                # send event with updated condition state
                self.logger.debug("evaluate_criteria - send update with new state")
        
                cond_name = self.config["metadata"]["name"]
                cond_ns = self.config["metadata"]["sampling_namespace"]
                cond_valid_time = self.config["metadata"]["valid_config_time"]

                self.current_state = state

                status = {
                    "condition": {
                        "kind": "SamplingState",
                        "name": cond_name,
                        "sampling_namespace": cond_ns,
                        "valid_config_time": cond_valid_time,
                        "status": self.current_state
                    }
                }
                await self.status_buffer.put(status)

                # await self.update_status(status)


            for src_name, _ in self.source_map.items():
                # self.logger.debug("evaluate_criteria", extra={"src_name": src_name, "src_data": src_data})
                # src_data.pop(timestamp)
                self.source_map[src_name].pop(timestamp)

        except Exception as e:
            self.logger.error("evaluate_criteria", extra={"reason": e})

    async def update_status_loop(self):
        while True:
            self.logger.debug("update_state_loop - send update")

            cond_name = self.config["metadata"]["name"]
            cond_ns = self.config["metadata"]["sampling_namespace"]
            cond_valid_time = self.config["metadata"]["valid_config_time"]

            status = {
                "condition": {
                    "kind": "SamplingState",
                    "name": cond_name,
                    "sampling_namespace": cond_ns,
                    "valid_config_time": cond_valid_time,
                    "status": self.current_state
                }
            }
            await self.status_buffer.put(status)

            # await self.update_status(status)

            await asyncio.sleep(10)



class SamplingStatesManager:
    """docstring for SamplingStatesManager."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug("SamplingStatesManager instantiated")

        # self.sampling_mode_control = True
        # self.sampling_modes = dict()
        # self.sampling_states = dict()
        self.sampling_states = {"states": dict(), "requirement_map": {}}

        self.status_buffer = asyncio.Queue(maxsize=60)
        # self.sampling_actions = dict()


        self.config = SamplingStatesManagerConfig()
        self.configure()
        # print("here:7")

        self.http_client = None

        self.mqtt_buffer = asyncio.Queue()
        asyncio.create_task(self.get_from_mqtt_loop())
        asyncio.create_task(self.handle_mqtt_buffer())
        asyncio.create_task(self.state_status_monitor())

        # print("SamplingStatesManager: init: here:8")

    def configure(self):
        # set clients

        self.logger.debug("configure", extra={"self.config": self.config})

        try:

            # load sampling conditions
            with open("/app/config/sampling_states.json", "r") as f:
                states = json.load(f)

                for state in states:

                    if state["kind"] != "SamplingState":
                        continue
                   
                    state_name = f'{state["metadata"]["name"]}'
                    # data_buffer = asyncio.Queue(maxsize=60)
                    if state_name not in self.sampling_states["states"]:
                        self.sampling_states["states"][state_name] = {
                            "config": None,
                            "state": None,
                        }
                    self.sampling_states["states"][state_name]["config"] = state
                    self.sampling_states["states"][state_name]["state"] = SamplingState(state, self.status_buffer)
                    # build list to send status updates for each req to all affected states
                    for req in state["requirements"]:
                        if req["kind"] not in self.sampling_states["requirement_map"]:
                           self.sampling_states["requirement_map"][req["kind"]] = dict()
                        if req["name"] not in self.sampling_states["requirement_map"][req["kind"]]:
                            self.sampling_states["requirement_map"][req["kind"]][req["name"]] = []
                        self.sampling_states["requirement_map"][req["kind"]][req["name"]].append(state_name)

            self.logger.debug(
                "configure", extra={"sampling_states": self.sampling_states}
            )

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

    async def state_status_monitor(self):
        while True:
            try:
                status = await self.status_buffer.get()

                state_name = status["condition"]["name"]
                state_ns = status["condition"]["sampling_namespace"]
                state_valid_time = status["condition"]["valid_config_time"]

                source_id = (
                    f"envds.{self.config.daq_id}.sampling-state.{state_name}"
                )
                self.logger.debug("state_status_monitor", extra={"source_id": source_id})
                
                source_topic = source_id.replace(".", "/")

                event = SamplingEvent.create_sampling_state_status_update(
                    # source="sensor.mockco-mock1-1234", data=record
                    source=source_id,
                    data=status,
                )
                destpath = f"{source_topic}/status/update"
                event["destpath"] = destpath
                event["samplingnamespace"] = state_ns
                event["validconfigtime"] = state_valid_time
                self.logger.debug(
                    "state_status_monitor",
                    extra={"data": event, "destpath": destpath},
                )

                await self.send_event(event)
            except Exception as e:
                self.logger.error("state_status_monitor", extra={"reason": e})
            
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
                                f"$share/sampling-states/{topic.strip()}"
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
                    # await self.condition_status_update(ce)
                    await self.requirement_status_update(ce)
                # elif ce["type"] == "envds.controller.data.update":
                #     await self.controller_data_update(ce)

            except Exception as e:
                self.logger.error("handle_mqtt_buffer", extra={"reason": e})

            await asyncio.sleep(0.0001)
            self.mqtt_buffer.task_done()

    async def requirement_status_update(self, ce: CloudEvent):

        try:
            
            self.logger.debug("requirement_status_update", extra={"ce": ce})

            for req_type, status in ce.data.items():
                # req_type = ce.data["condition"]
                if "kind" in status and "name" in status:
                    req_kind = status["kind"]
                    req_name = status["name"]

                try:
                    self.logger.debug("requirement_status_update", extra={"sampling_states": self.sampling_states})
                    for state_name in self.sampling_states["requirement_map"][req_kind][req_name]:
                        state = self.sampling_states["states"][state_name]["state"]
                        await state.update(status)
                except KeyError:
                    self.logger.error("requirement_status_update", extra={"reason": e})
                    continue

        except Exception as e:
            self.logger.error("condition_status_update", extra={"reason": e})
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
    config = SamplingStatesManagerConfig()
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
