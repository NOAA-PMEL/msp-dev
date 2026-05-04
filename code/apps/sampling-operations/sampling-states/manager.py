import asyncio
import importlib
import json
import logging
import sys
from pathlib import Path
import os

import httpx
from logfmter import Logfmter
from pydantic import BaseSettings, Field
from ulid import ULID

from cloudevents.http import CloudEvent, from_json, to_structured
from aiomqtt import Client, MqttError

from datetime import datetime, timezone
from envds.util.util import (
    get_datetime_string,
    get_datetime,
    string_to_datetime,
    get_datetime_with_delta,
    time_to_next,
)

from envds.sampling.event import SamplingEvent

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
    daq_id: str | None = None
    mqtt_broker: str = "mosquitto.default"
    mqtt_port: int = 1883
    mqtt_topic_subscriptions: str = ""
    mqtt_client_id: str = Field(str(ULID()))
    knative_broker: str | None = None
    system_init_control: str = "auto"
    system_init_mode: str | None = None
    is_primary_controller: bool = True

    class Config:
        env_prefix = "SAMPLING_OPERATIONS_"
        case_sensitive = False


class SamplingAction:
    def __init__(self, config, target_buffer):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.config = config
        self.data_buffer = asyncio.Queue(maxsize=500)
        self.target_buffer = target_buffer
        self.sources = {"variables": dict(), "data": dict()}
        self.targets = {"variables": dict()}

        self.configure()
        asyncio.create_task(self.update_monitor())

    def configure(self):
        try:
            if not self.config:
                return
            self.action_module = self.config["metadata"].get("action_module", "default_actions")
            self.action_def = self.config["metadata"].get("action_def", "default_def")
            self.source_max_age = self.config.get("source_max_age", 60)

            if "sources" in self.config:
                for src_name, src_config in self.config["sources"].items():
                    self.sources["variables"][src_name] = src_config
                    vm_name = src_config["variablemap_name"]
                    vs_name = src_config["variableset_name"]
                    src_id = "::".join([vm_name, vs_name])
                    v_name = src_config["variable"]
                    if src_id not in self.sources["data"]:
                        self.sources["data"][src_id] = dict()
                    if v_name not in self.sources["data"][src_id]:
                        self.sources["data"][src_id][v_name] = dict()

            if "targets" in self.config:
                for src_name, src_config in self.config["targets"].items():
                    self.targets["variables"][src_name] = src_config

            mod_ = importlib.import_module(self.action_module)
            self.method = getattr(mod_, self.action_def)
        except Exception as e:
            self.logger.error("configure-action", extra={"reason": e})

    async def run(self):
        source_vars = dict()
        dt_now = get_datetime().replace(tzinfo=timezone.utc)
        min_dt = get_datetime_with_delta(delta=(-(self.source_max_age)), dt=dt_now)
        
        for src_name, src in self.sources["variables"].items():
            src_id = "::".join([src["variablemap_name"], src["variableset_name"]])
            v_name = src["variable"]
            
            last_var = self.sources["data"].get(src_id, {}).get(v_name, {})
            last_update_str = last_var.get("last_update")
            
            if not last_update_str:
                return None
                
            last_update_dt = string_to_datetime(last_update_str).replace(tzinfo=timezone.utc)
            if last_update_dt < min_dt:
                return None
                
            source_vars[src_name] = last_var.get("data")

        try:
            if asyncio.iscoroutinefunction(self.method):
                result = await self.method(**source_vars)
            else:
                result = self.method(**source_vars)
        except Exception as e:
            self.logger.error("action method execution failed", extra={"reason": str(e)})
            return None

        if not result:
            return None

        target_vars = dict()
        for trg_name, trg in self.targets["variables"].items():
            if trg_name in result:
                target_vars[trg_name] = {"data": result[trg_name], "metadata": trg}
                
        if target_vars:
            await self.target_buffer.put(target_vars)

    async def update(self, data: CloudEvent):
        try:
            self.data_buffer.put_nowait(data)
        except asyncio.QueueFull:
            self.data_buffer.get_nowait()
            self.data_buffer.task_done()
            self.data_buffer.put_nowait(data)

    async def update_monitor(self):
        while True:
            try:
                event = await self.data_buffer.get()
                try:
                    src_id = event["source"].split(".")[-1]
                    dt = event.data["variables"]["time"]
                    for var_name, var_data in self.sources["data"][src_id].items():
                        if var_name in event.data["variables"]:
                            var_data["last_update"] = dt["data"]
                            var_data["data"] = event.data["variables"][var_name]["data"]
                except KeyError:
                    pass
            except Exception as e:
                self.logger.error("action.update_monitor", extra={"reason": e})
            await asyncio.sleep(0.001)
            self.data_buffer.task_done()


class SamplingMode:
    def __init__(self, config, status_buffer, actions_buffer, transitions_buffer):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.config = config
        self.update_buffer = asyncio.Queue(maxsize=500)
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
            if not self.config: return
            if "requirements" in self.config:
                for req_config in self.config["requirements"]:
                    kind = req_config["kind"]
                    name = req_config["name"]
                    if kind not in self.requirements: self.requirements[kind] = dict()
                    if name not in self.requirements[kind]:
                        self.requirements[kind][name] = {"config": req_config, "data": {}, "status": False}

            if "actions" in self.config:
                for act_test, act_list in self.config["actions"].items():
                    if act_test in self.actions:
                        for act in act_list:
                            if act not in self.actions[act_test]:
                                self.actions[act_test].append(act)

            if "transitions" in self.config:
                for act_test, act_list in self.config["transitions"].items():
                    if act_test in self.transitions:
                        for act in act_list:
                            if act not in self.transitions[act_test]:
                                self.transitions[act_test].append(act)
        except Exception as e:
            self.logger.error("configure-samplemode", extra={"reason": e})

    def activate(self, active: bool):
        self.active = active

    def is_active(self) -> bool:
        return self.active

    async def update(self, status):
        try:
            self.update_buffer.put_nowait(status)
        except asyncio.QueueFull:
            self.update_buffer.get_nowait()
            self.update_buffer.task_done()
            self.update_buffer.put_nowait(status)

    async def update_monitor(self):
        while True:
            try:
                status = await self.update_buffer.get()
                try:
                    self.requirements[status["kind"]][status["name"]]["status"] = status["status"]
                except KeyError:
                    pass
            except Exception as e:
                self.logger.error("mode.update_monitor", extra={"reason": e})
            await asyncio.sleep(0.001)
            self.update_buffer.task_done()

    async def requirements_monitor(self):
        while True:
            try:
                mode_status = []
                for req_type, req_kind in self.requirements.items():
                    for req_name, req in req_kind.items():
                        mode_status.append(req["status"])

                latest_status = all(mode_status) if mode_status else False
                
                if latest_status != self.current_state:
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
                        await self.status_buffer.put(status)

                        run_type = str(self.current_state).lower()
                        for act in self.actions[run_type]:
                            await self.actions_buffer.put({"action": {"kind": act["kind"], "name": act["name"]}})

                        for tran in self.transitions[run_type]:
                            await self.transitions_buffer.put({"transition": {"kind": tran["kind"], "name": tran["name"]}})
            except Exception as e:
                self.logger.error("requirements_monitor", extra={"reason": e})
            await asyncio.sleep(time_to_next(1))

    async def update_status_loop(self):
        while True:
            try:
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
                    await self.status_buffer.put(status)
            except Exception as e:
                self.logger.error("SamplingMode.update_state_loop", extra={"reason": e})
            await asyncio.sleep(10)


class SamplingOperationsManager:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)

        self.sampling_modes = dict()
        self.sampling_actions = dict()
        self.actions_source_map = dict()
        self.mode_requirements_map = dict()
        self.active_modes = {"SystemMode": [], "SamplingMode": []}

        self.config = SamplingOperationsManagerConfig()
        self.http_client = None
        self._background_tasks = set()

        self.status_buffer = None
        self.actions_buffer = None
        self.transitions_buffer = None
        self.actions_target_buffer = None
        self.mqtt_buffer = None
        self.publish_queue = None

        self.configure()

    async def setup(self):
        self.logger.info("Running SamplingOperationsManager async setup...")
        
        self.status_buffer = asyncio.Queue(maxsize=2000)
        self.actions_buffer = asyncio.Queue(maxsize=2000)
        self.transitions_buffer = asyncio.Queue(maxsize=2000)
        self.actions_target_buffer = asyncio.Queue(maxsize=2000)
        self.mqtt_buffer = asyncio.Queue(maxsize=2000)
        self.publish_queue = asyncio.Queue(maxsize=2000)

        self.http_client = httpx.AsyncClient(
            limits=httpx.Limits(max_keepalive_connections=50, max_connections=100)
        )

        self.init_modes()

        task1 = asyncio.create_task(self.get_from_mqtt_loop())
        task2 = asyncio.create_task(self.handle_mqtt_buffer())
        task3 = asyncio.create_task(self.mode_status_monitor())
        task4 = asyncio.create_task(self.mode_action_monitor())
        task5 = asyncio.create_task(self.mode_transition_monitor())
        task6 = asyncio.create_task(self.system_mode_loop())
        task7 = asyncio.create_task(self.publish_local_definitions())
        task8 = asyncio.create_task(self.sync_sampling_definitions_loop())
        task9 = asyncio.create_task(self.action_target_monitor())
        task10 = asyncio.create_task(self.mqtt_publish_loop())

        self._background_tasks.update({task1, task2, task3, task4, task5, task6, task7, task8, task9, task10})

    def open_http_client(self):
        self.http_client = httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=50, max_connections=100))

    async def close_http_client(self):
        if getattr(self, 'http_client', None):
            await self.http_client.aclose()
            self.http_client = None

    def configure(self):
        try:
            actions_path = "/app/config/sampling_operations_actions.json"
            if os.path.exists(actions_path):
                with open(actions_path, "r") as f:
                    actions = json.load(f)
                for action in actions:
                    self.load_action(action)

            modes_path = "/app/config/sampling_operations_modes.json"
            if os.path.exists(modes_path):
                with open(modes_path, "r") as f:
                    modes = json.load(f)
                for mode in modes:
                    self.load_mode(mode)
        except Exception as e:
            self.logger.error("configure-manager", extra={"reason": e})

    def load_action(self, action_config: dict):
        kind = action_config.get("kind")
        name = action_config.get("metadata", {}).get("name")
        if not kind or not name: return
        
        if kind not in self.sampling_actions:
            self.sampling_actions[kind] = dict()
            
        self.sampling_actions[kind][name] = {
            "config": action_config,
            "action": SamplingAction(action_config, self.actions_target_buffer),
        }

        if "sources" in action_config:
            for src_name, src in action_config["sources"].items():
                src_id = f"{src['variablemap_name']}::{src['variableset_name']}"
                if src_id not in self.actions_source_map:
                    self.actions_source_map[src_id] = []
                
                entry = {"kind": kind, "name": name}
                if entry not in self.actions_source_map[src_id]:
                    self.actions_source_map[src_id].append(entry)

    def load_mode(self, mode_config: dict):
        kind = mode_config.get("kind")
        name = mode_config.get("metadata", {}).get("name")
        if not kind or not name: return
        
        if kind not in self.sampling_modes:
            self.sampling_modes[kind] = dict()
            
        self.sampling_modes[kind][name] = {
            "config": mode_config,
            "mode": SamplingMode(mode_config, self.status_buffer, self.actions_buffer, self.transitions_buffer),
        }

        if "requirements" in mode_config:
            for req_mode in mode_config["requirements"]:
                req_kind = req_mode.get("kind")
                req_name = req_mode.get("name")
                if not req_kind or not req_name: continue
                
                if req_kind not in self.mode_requirements_map:
                    self.mode_requirements_map[req_kind] = dict()
                if req_name not in self.mode_requirements_map[req_kind]:
                    self.mode_requirements_map[req_kind][req_name] = []
                
                entry = {"kind": kind, "name": name, "active": False}
                if entry not in self.mode_requirements_map[req_kind][req_name]:
                    self.mode_requirements_map[req_kind][req_name].append(entry)

    def init_modes(self):
        self.activate_system_mode(self.config.system_init_mode)
        self.set_system_control(self.config.system_init_control)

    def set_system_control(self, control="auto"):
        self.system_control = control

    def activate_required_modes(self, required_mode: dict):
        kind = required_mode["kind"]
        name = required_mode["name"]

        if kind not in self.active_modes: return
        self.active_modes[kind].append(name)
        reqs = self.sampling_modes[kind][name]["config"].get("requirements", [])

        for req in reqs:
            self.activate_required_modes(req)

    def activate_system_mode(self, name: str):
        try:
            if "SystemMode" not in self.sampling_modes:
                self.logger.warning("No SystemMode definitions loaded yet. Waiting for sync.")
                return

            if name not in self.sampling_modes["SystemMode"]:
                return

            self.active_modes = {"SystemMode": [name], "SamplingMode": []}
            reqs = self.sampling_modes["SystemMode"][name]["config"].get("requirements", [])

            for req in reqs:
                self.activate_required_modes(req)

            for mod_type, modes in self.sampling_modes.items():
                for mode_name, mode in modes.items():
                    mode["mode"].activate(False)

            for mode_type, modes in self.active_modes.items():
                for mode_name in modes:
                    mode = self.sampling_modes[mode_type][mode_name]
                    mode["mode"].activate(True)
        except Exception as e:
            self.logger.error("activate_system_mode", extra={"reason": e})

    async def system_mode_loop(self):
        while True:
            if "SystemMode" in self.sampling_modes and not self.active_modes.get("SystemMode"):
                self.activate_system_mode(self.config.system_init_mode)
            await asyncio.sleep(time_to_next(1))

    async def submit_get(self, path: str):
        try:
            timeout = httpx.Timeout(10.0, read=10.0)
            if not getattr(self, 'http_client', None): self.open_http_client()
            datastore_url = f"datastore.{self.config.daq_id}-system.svc.cluster.local:80"
            results = await self.http_client.get(f"http://{datastore_url}/{path}/", timeout=timeout)
            return results.json()
        except Exception as e:
            self.logger.error("submit_get failed", extra={"reason": str(e)})
            return {}

    async def submit_request(self, path: str, query: dict):
        try:
            timeout = httpx.Timeout(10.0, read=10.0)
            if not getattr(self, 'http_client', None): self.open_http_client()
            datastore_url = f"datastore.{self.config.daq_id}-system.svc.cluster.local:80"
            results = await self.http_client.get(f"http://{datastore_url}/{path}/", params=query, timeout=timeout)
            return results.json()
        except Exception as e:
            self.logger.error("submit_request failed", extra={"reason": str(e)})
            return {}

    async def send_event(self, ce):
        try:
            topic = ce.get("destpath", "")
            if not topic: return
            headers, body = to_structured(ce)
            await self.publish_queue.put((topic, body))
        except Exception as e:
            self.logger.error("send_event failed", extra={"reason": str(e)})

    async def publish_local_definitions(self):
        await asyncio.sleep(5)
        while True:
            try:
                definitions = [(self.sampling_actions, "action"), (self.sampling_modes, "samplingmode")]
                for registry_dict, resource_name in definitions:
                    for kind, item_dict in registry_dict.items():
                        res_type = "systemmode" if resource_name == "samplingmode" and kind == "SystemMode" else resource_name
                        for name, data_obj in item_dict.items():
                            config = data_obj["config"]
                            event = SamplingEvent.create_definition_registry_update(
                                resource=f"{res_type}-definition",
                                source=f"envds.{self.config.daq_id}.sampling-operations",
                                data={res_type: config}
                            )
                            event["destpath"] = f"envds/{self.config.daq_id}/{res_type}-definition/registry/update"
                            await self.send_event(event)
            except Exception as e:
                self.logger.error("publish_local_definitions", extra={"reason": e})
            await asyncio.sleep(60)

    async def sync_sampling_definitions_loop(self):
        resources_to_sync = ["action", "systemmode", "samplingmode"]
        await asyncio.sleep(10)
        while True:
            try:
                for res in resources_to_sync:
                    ids_resp = await self.submit_get(path=f"{res}-definition/registry/ids/get")
                    if ids_resp and "results" in ids_resp:
                        async def fetch_def(def_id):
                            return await self.submit_request(path=f"{res}-definition/registry/get", query={"name": def_id})
                        responses = await asyncio.gather(*(fetch_def(did) for did in ids_resp["results"]))
                        for resp in responses:
                            if resp and "results" in resp and resp["results"]:
                                config = resp["results"][0]
                                kind = config.get("kind", "")
                                if "Action" in kind: self.load_action(config)
                                elif "Mode" in kind: self.load_mode(config)
                                
                if "SystemMode" in self.sampling_modes and not self.active_modes.get("SystemMode"):
                    self.activate_system_mode(self.config.system_init_mode)
            except Exception as e:
                self.logger.error("sync_sampling_definitions_loop", extra={"reason": str(e)})
            await asyncio.sleep(60)

    async def get_from_mqtt_loop(self):
        reconnect = 10
        while True:
            try:
                client_id = str(ULID())
                async with Client(self.config.mqtt_broker, port=self.config.mqtt_port, identifier=client_id) as self.client:
                    for topic in self.config.mqtt_topic_subscriptions.split(","):
                        if topic.strip():
                            await self.client.subscribe(f"$share/samplingoperations/{topic.strip()}")

                    async for message in self.client.messages:
                        topic = message.topic.value
                        if "sampling-operations" in topic: continue
                        try:
                            ce = from_json(message.payload)
                            ce["sourcepath"] = topic
                            await self.mqtt_buffer.put(ce)
                        except Exception as e:
                            self.logger.error("get_from_mqtt_loop JSON error", extra={"reason": e})
            except MqttError:
                await asyncio.sleep(reconnect)

    async def handle_mqtt_buffer(self):
        while True:
            try:
                ce = await self.mqtt_buffer.get()
                ce_type = ce.get("type", "")
                
                if "status.update" in ce_type or "status" in getattr(ce, "data", {}):
                    await self.requirement_status_update(ce)
                elif ce_type == "envds.sampling-operations.transition.request":
                    await self.transitions_buffer.put(ce.data)
            except Exception as e:
                self.logger.error("handle_mqtt_buffer", extra={"reason": e})
            finally:
                self.mqtt_buffer.task_done()

    async def mqtt_publish_loop(self):
        reconnect = 5
        client_id = f"operations-publisher-{ULID()}"
        while True:
            try:
                async with Client(self.config.mqtt_broker, port=self.config.mqtt_port, identifier=client_id) as client:
                    while True:
                        topic, payload = await self.publish_queue.get()
                        await client.publish(topic, payload, qos=1)
                        self.publish_queue.task_done()
            except Exception:
                await asyncio.sleep(reconnect)

    async def handle_manual_action(self, kind: str, name: str):
        action = {"action": {"kind": kind, "name": name}, "is_manual_override": True}
        await self.actions_buffer.put(action)

    async def mode_action_monitor(self):
        while True:
            try:
                action = await self.actions_buffer.get()
                kind = action["action"]["kind"]
                name = action["action"]["name"]
                is_manual = action.get("is_manual_override", False)

                if not is_manual:
                    if self.system_control == "manual": continue
                    if not self.config.is_primary_controller: continue

                await self.sampling_actions[kind][name]["action"].run()
            except Exception as e:
                self.logger.error("mode_action_monitor", extra={"reason": e})
            finally:
                self.actions_buffer.task_done()

    async def mode_transition_monitor(self):
        while True:
            try:
                transition_data = await self.transitions_buffer.get()
                is_remote = transition_data.get("is_remote_command", False)
                
                if self.system_control == "auto" or is_remote:
                    kind = transition_data["transition"]["kind"]
                    name = transition_data["transition"]["name"]
                    if kind == "SystemMode":
                        self.activate_system_mode(name)
            except Exception as e:
                self.logger.error("mode_transition_monitor", extra={"reason": e})
            finally:
                self.transitions_buffer.task_done()

    async def mode_status_monitor(self):
        while True:
            try:
                status = await self.status_buffer.get()
                mode_name = status["status"]["name"]
                mode_ns = status["status"]["sampling_namespace"]
                mode_valid_time = status["status"]["valid_config_time"]
                source_id = f"envds.{self.config.daq_id}.sampling-operations"
                source_topic = source_id.replace(".", "/")

                event = SamplingEvent.create_sampling_mode_status_update(source=source_id, data=status)
                event["destpath"] = f"{source_topic}/status/update"
                event["samplingnamespace"] = mode_ns
                event["validconfigtime"] = mode_valid_time

                await self.requirement_status_update(event)
                await self.send_event(event)
            except Exception as e:
                self.logger.error("mode_status_monitor", extra={"reason": e})
            await asyncio.sleep(0.001)
            self.status_buffer.task_done()

    async def requirement_status_update(self, ce: CloudEvent):
        try:
            status = ce.data["status"]
            req_kind = status["kind"]
            req_name = status["name"]
            
            if req_kind in self.mode_requirements_map and req_name in self.mode_requirements_map[req_kind]:
                for req_map in self.mode_requirements_map[req_kind][req_name]:
                    mode = self.sampling_modes[req_map["kind"]][req_map["name"]]["mode"]
                    await mode.update(status)
        except KeyError:
            pass
        except Exception as e:
            self.logger.error("SOM.requirement_status_update", extra={"reason": e})

    async def variableset_data_update(self, ce: CloudEvent):
        try:
            src_id = ce["source"].split(".")[-1]
            if src_id in self.actions_source_map:
                for action_map in self.actions_source_map[src_id]:
                    kind = action_map["kind"]
                    name = action_map["name"]
                    action = self.sampling_actions[kind][name]["action"]
                    await action.update(ce)
        except Exception as e:
            self.logger.error("variableset_data_update", extra={"reason": e})

    async def action_target_monitor(self):
        while True:
            try:
                targets = await self.actions_target_buffer.get()
                for trg_name, trg_data in targets.items():
                    val = trg_data["data"]
                    meta = trg_data["metadata"]
                    
                    target_type = meta.get("target_type", "controller").lower()
                    target_id = meta.get("target_id", meta.get("variablemap_name", "unknown"))
                    v_name = meta.get("variable", trg_name)

                    source_id = f"envds.{self.config.daq_id}.sampling-operations"
                    topic = f"envds/{self.config.daq_id}/{target_type}/{target_id}/settings/update"
                    ce_type = f"envds.{target_type}.settings.update"
                    
                    event = CloudEvent(
                        attributes={"type": ce_type, "source": source_id, "datacontenttype": "application/json"},
                        data={"variables": {v_name: {"data": val}}}
                    )
                    event["destpath"] = topic
                    await self.send_event(event)
            except Exception as e:
                self.logger.error("action_target_monitor", extra={"reason": e})
            finally:
                self.actions_target_buffer.task_done()

    async def send_remote_transition_request(self, target_daq_id: str, kind: str, name: str):
        source_id = f"envds.{self.config.daq_id}.sampling-operations"
        topic = f"envds/{target_daq_id}/sampling-operations/transition/request"
        event = CloudEvent(
            attributes={"type": "envds.sampling-operations.transition.request", "source": source_id, "datacontenttype": "application/json"},
            data={"transition": {"kind": kind, "name": name}, "is_remote_command": True}
        )
        event["destpath"] = topic
        await self.send_event(event)


async def shutdown():
    print("shutting down")

async def main(config):
    config = uvicorn.Config(
        "main:app",
        host=config.host,
        port=config.port,
        root_path="/msp/sampling-system",
    )
    server = uvicorn.Server(config)
    L.info(f"server: {server}")
    await server.serve()
    print("starting shutdown...")
    await shutdown()
    print("done.")

if __name__ == "__main__":
    config = SamplingOperationsManagerConfig()
    try:
        index = sys.argv.index("--host")
        config.host = sys.argv[index + 1]
    except (ValueError, IndexError): pass
    try:
        index = sys.argv.index("--port")
        config.port = int(sys.argv[index + 1])
    except (ValueError, IndexError): pass
    
    asyncio.run(main(config))