import asyncio
import importlib
import logging
import httpx
import os
import json
import sys
from ulid import ULID
from datetime import timezone
from pydantic import BaseSettings, Field
from logfmter import Logfmter 
from cloudevents.http import CloudEvent, from_json
from cloudevents.conversion import to_structured, to_json
from cloudevents.exceptions import InvalidStructuredJSON
from aiomqtt import Client, MqttError
import uvicorn
from envds.util.util import (
    get_datetime_string, 
    get_datetime, 
    time_to_next, 
    string_to_datetime
)
from envds.sampling.event import SamplingEvent

# Configure structured logging consistent with sampling-operations
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger("SamplingModesManager")
L.setLevel(logging.DEBUG)

class SamplingModesConfig(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8080
    daq_id: str = "default_daq"
    mqtt_broker: str = "mosquitto.default"
    mqtt_port: int = 1883
    # Subscriptions include telemetry, local state evaluations, and local mode status
    mqtt_topic_subscriptions: str = "envds/+/+/+/data/#,envds/+/sampling-states/#,envds/+/sampling-modes/#"
    is_primary_controller: bool = True
    
    class Config:
        env_prefix = "SAMPLING_MODES_"

class SamplingMode:
    """Evaluates environmental requirements and triggers assigned SamplingActions."""
    def __init__(self, config, status_buffer, actions_buffer):
        self.config = config
        self.status_buffer = status_buffer
        self.actions_buffer = actions_buffer
        self.requirements = {}
        self.actions = {"true": [], "false": []}
        self.current_state = False
        self.active = True 
        self._configure_requirements()

    def _configure_requirements(self):
        """Initialize requirement tracking and action mapping from config."""
        for req in self.config.get("requirements", []):
            kind, name = req.get("kind"), req.get("name")
            if kind not in self.requirements:
                self.requirements[kind] = {}
            self.requirements[kind][name] = {"status": False}

        for act_test, act_list in self.config.get("actions", {}).items():
            for act in act_list:
                if act not in self.actions[act_test]:
                    self.actions[act_test].append(act)

    # async def update(self, status_update: dict):
    #     """Updates internal requirement cache from external status updates."""
    #     kind, name, status = status_update.get("kind"), status_update.get("name"), status_update.get("status")
    #     if kind in self.requirements and name in self.requirements[kind]:
    #         self.requirements[kind][name]["status"] = status

    async def update(self, payload: dict):
        """Updates the status of requirements (e.g., SamplingStates or SamplingConditions)."""
        id_block = payload.get("id", {})
        state_block = payload.get("state", {})
        
        app_group = id_block.get("app_group", "")
        
        # Map app_group to the strict CamelCase kind used in your requirements JSON
        req_kind = "SamplingCondition" if app_group == "condition" else "SamplingState" if app_group == "state" else "SamplingMode" if app_group == "mode" else app_group
        name = id_block.get("app_uid")
        
        # DYNAMICALLY grab the actual status depending on the app_group!
        if app_group == "condition":
            actual_status = state_block.get("condition_met", {}).get("actual", "false")
        elif app_group == "state":
            actual_status = state_block.get("state_active", {}).get("actual", "false")
        elif app_group == "mode":
            actual_status = state_block.get("mode_active", {}).get("actual", "false")
        else:
            actual_status = "false"
            
        is_met = (str(actual_status).lower() == "true")
        
        if req_kind in self.requirements and name in self.requirements[req_kind]:
            self.requirements[req_kind][name]["status"] = is_met

    async def evaluate(self):
        """Checks requirements and dispatches actions on state changes."""
        if not self.active: return
        mode_status = [req["status"] for kind in self.requirements.values() for req in kind.values()]
        latest_status = all(mode_status) if mode_status else False
        is_changed = (latest_status != self.current_state)

        # Heartbeat every 30s or broadcast immediately on state change
        if is_changed or (get_datetime().replace(tzinfo=timezone.utc).second % 30 == 0):
            self.current_state = latest_status
            await self.status_buffer.put({
                "status": {
                    "kind": "SamplingMode",
                    "name": self.config["metadata"]["name"],
                    "status": self.current_state,
                    "time": get_datetime_string()
                }
            })
            if is_changed:
                run_type = str(self.current_state).lower()
                for act in self.actions.get(run_type, []):
                    await self.actions_buffer.put({"action": act})

class SamplingAction:
    """Executes python modules to compute physical system settings."""
    def __init__(self, config, actions_target_buffer):
        self.config = config
        self.actions_target_buffer = actions_target_buffer
        self.sources = {"data": {}}
        mod_name = config["metadata"].get("action_module", "default_actions")
        def_name = config["metadata"].get("action_def", "default_def")
        mod_ = importlib.import_module(mod_name)
        self.method = getattr(mod_, def_name)

    async def run(self):
        """Assembles variables from cache and runs the action method."""
        source_vars = {}
        for src_name, src in self.config.get("sources", {}).items():
            src_id = f"{src['variablemap_name']}::{src['variableset_name']}"
            source_vars[src_name] = self.sources["data"].get(src_id, {}).get(src["variable"], {}).get("data")
        try:
            res = await self.method(**source_vars) if asyncio.iscoroutinefunction(self.method) else self.method(**source_vars)
            if res:
                for t_name, t in self.config.get("targets", {}).items():
                    if t_name in res:
                        await self.actions_target_buffer.put({t_name: {"data": res[t_name], "metadata": t}})
        except Exception as e:
            L.error("action_execution_failed", extra={"reason": str(e)})

class SamplingModesManager:
    def __init__(self):
        # ---> ADD THESE TWO LINES <---
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        
        self.logger.debug("SamplingModesManager instantiated")
        self.config = SamplingModesConfig()
        self.modes, self.actions = {}, {}
        self.status_buffer = None
        self.actions_buffer = None
        self.actions_target_buffer = None
        self.publish_queue = None
        self.http_client = None
        
        # Consistent load pattern from sampling-states
        self.configure()

    def configure(self):
        """Loads definitions from local mounted files."""
        try:
            modes_path = "/app/config/sampling_modes_modes.json"
            if os.path.exists(modes_path):
                with open(modes_path, "r") as f:
                    for cfg in json.load(f): self.load_mode(cfg)
            
            actions_path = "/app/config/sampling_modes_actions.json"
            if os.path.exists(actions_path):
                with open(actions_path, "r") as f:
                    for cfg in json.load(f): self.load_action(cfg)
        except Exception as e:
            L.error("configure_failed", extra={"reason": str(e)})

    def load_mode(self, cfg):
        if self.status_buffer is None: self.status_buffer = asyncio.Queue(maxsize=2000)
        if self.actions_buffer is None: self.actions_buffer = asyncio.Queue(maxsize=2000)
        name = cfg["metadata"]["name"]
        self.modes[name] = SamplingMode(cfg, self.status_buffer, self.actions_buffer)
        # Rename "name" to "res_name"
        L.info("loaded_mode", extra={"res_name": name})

    def load_action(self, cfg):
        if self.actions_target_buffer is None: self.actions_target_buffer = asyncio.Queue(maxsize=2000)
        name = cfg["metadata"]["name"]
        try:
            self.actions[name] = SamplingAction(cfg, self.actions_target_buffer)
            # Rename "name" to "res_name"
            L.info("loaded_action", extra={"res_name": name})
        except Exception as e:
            # Rename "name" to "res_name"
            L.error("action_load_failed", extra={"res_name": name, "reason": str(e)})

    async def setup(self):
        """Infrastructure and background task initialization."""
        if self.status_buffer is None: self.status_buffer = asyncio.Queue(maxsize=2000)
        if self.actions_buffer is None: self.actions_buffer = asyncio.Queue(maxsize=2000)
        if self.actions_target_buffer is None: self.actions_target_buffer = asyncio.Queue(maxsize=2000)
        if self.publish_queue is None: self.publish_queue = asyncio.Queue(maxsize=2000)
        
        self.http_client = httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=50))
        
        asyncio.create_task(self.publish_local_definitions())
        asyncio.create_task(self.sync_sampling_definitions_loop())
        asyncio.create_task(self.mqtt_listen_loop())
        asyncio.create_task(self.mqtt_publish_loop())
        asyncio.create_task(self.mode_evaluation_loop())
        asyncio.create_task(self.action_execution_monitor())
        asyncio.create_task(self.action_target_monitor())
        asyncio.create_task(self.status_publish_monitor())

    async def publish_local_definitions(self):
        await asyncio.sleep(5)
        while True:
            try:
                for registry, res_type in [(self.actions, "action"), (self.modes, "samplingmode")]:
                    for obj in registry.values():
                        event = SamplingEvent.create_definition_registry_update(
                            resource=f"{res_type}-definition",
                            source=f"envds.{self.config.daq_id}.sampling-modes",
                            data={res_type: obj.config}
                        )
                        event["destpath"] = f"envds/{self.config.daq_id}/{res_type}-definition/registry/update"
                        await self.send_event(event)
            except Exception as e:
                L.error("publish_failed", extra={"reason": str(e)})
            await asyncio.sleep(60)

    async def sync_sampling_definitions_loop(self):
        while True:
            try:
                ds_url = f"datastore.{self.config.daq_id}-system.svc.cluster.local:80"
                for res in ["action", "samplingmode"]:
                    resp = await self.http_client.get(f"http://{ds_url}/{res}-definition/registry/ids/get/")
                    if resp.status_code == 200:
                        for did in resp.json().get("results", []):
                            d_resp = await self.http_client.get(f"http://{ds_url}/{res}-definition/registry/get/", params={"name": did})
                            if d_resp.status_code == 200 and d_resp.json().get("results"):
                                cfg = d_resp.json()["results"][0]
                                if res == "action": self.load_action(cfg)
                                else: self.load_mode(cfg)
            except Exception as e:
                L.error("sync_failed", extra={"reason": str(e)})
            await asyncio.sleep(60)

    async def send_event(self, ce):
        """Routes registry definitions to the Datastore via Knative HTTP Broker."""
        try:
            self.logger.debug("send_event (HTTP)", extra={"ce": ce})
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

    async def mqtt_listen_loop(self):
        my_id = f"envds.{self.config.daq_id}.sampling-modes"
        while True:
            try:
                async with Client(self.config.mqtt_broker, port=self.config.mqtt_port) as client:
                    for topic in self.config.mqtt_topic_subscriptions.split(","):
                        await client.subscribe(topic.strip())
                    async for msg in client.messages:
                        try:
                            ce = from_json(msg.payload)
                            if ce.get("source") == my_id: continue
                            
                            if "data.update" in ce.get("type", ""):
                                src_id = ce.get("source", "").split(".")[-1]
                                ts = ce.data.get("variables", {}).get("time", {}).get("data")
                                for action_obj in self.actions.values():
                                    for req_src in action_obj.config.get("sources", {}).values():
                                        rid = f"{req_src['variablemap_name']}::{req_src['variableset_name']}"
                                        if rid == src_id:
                                            if rid not in action_obj.sources["data"]: action_obj.sources["data"][rid] = {}
                                            for k, v in ce.data.get("variables", {}).items():
                                                action_obj.sources["data"][rid][k] = {"data": v.get("data"), "last_update": ts}
                            
                            elif "status.update" in ce.get("type", ""):
                                # for mode in self.modes.values(): 
                                #     await mode.update(ce.data.get("status", {}))
                                # Update evaluation map for current active mode
                                for mode in self.modes.values(): 
                                    await mode.update(ce.data)
                        except Exception: pass
            except MqttError:
                await asyncio.sleep(5)

    async def mqtt_publish_loop(self):
        while True:
            try:
                async with Client(self.config.mqtt_broker, port=self.config.mqtt_port) as client:
                    while True:
                        t, p = await self.publish_queue.get()
                        await client.publish(t, p, qos=1)
                        self.publish_queue.task_done()
            except MqttError:
                await asyncio.sleep(5)

    async def mode_evaluation_loop(self):
        while True:
            for mode in list(self.modes.values()): 
                await mode.evaluate()
            await asyncio.sleep(time_to_next(1))

    # async def status_publish_monitor(self):
    #     while True:
    #         data = await self.status_buffer.get()
    #         event = CloudEvent(
    #             attributes={"type": "envds.samplingmode.status.update", "source": f"envds.{self.config.daq_id}.sampling-modes"},
    #             data=data
    #         )
    #         event["destpath"] = f"envds/{self.config.daq_id}/sampling-modes/status/update"
    #         await self.send_event(event)
    #         self.status_buffer.task_done()

    async def status_publish_monitor(self):
        while True:
            try:
                # 1. Pull the raw update from the Mode evaluation
                data = await self.status_buffer.get()
                
                # 2. Extract the flat payload
                status_obj = data.get("status", {})
                mode_name = status_obj.get("name", "unknown")
                is_active = status_obj.get("status", False)
                status_str = "true" if is_active else "false"

                # 3. Translate to the NEW envds-COMPLIANT STATUS BLOCK
                status_data = {
                    "id": {
                        "app_group": "mode",
                        "app_uid": mode_name
                    },
                    "state": {
                        "mode_active": {
                            "requested": "true", 
                            "actual": status_str
                        }
                    },
                    "timestamp": get_datetime_string()
                }

                # 4. Publish via standard SamplingEvent factory
                event = SamplingEvent.create_sampling_mode_status_update(
                    source=f"envds.{self.config.daq_id}.sampling-modes",
                    data=status_data
                )

                # ---> ADD THESE TWO LINES <---
                destpath = f"envds/{self.config.daq_id}/sampling-modes/status/update"
                event["destpath"] = destpath
                await self.send_to_mqtt(destpath, event)

            except Exception as e:
                L.error("status_publish_monitor error", extra={"reason": str(e)})

            finally:
                # Mark task done so the queue doesn't lock up
                if 'data' in locals():
                    self.status_buffer.task_done()

    async def action_execution_monitor(self):
        while True:
            req = await self.actions_buffer.get()
            if not self.config.is_primary_controller:
                self.actions_buffer.task_done()
                continue
            name = req.get("action", {}).get("name")
            if name in self.actions:
                # Rename "name" to "res_name"
                L.info("executing_action", extra={"res_name": name})
                await self.actions[name].run()
            self.actions_buffer.task_done()
            
    async def action_target_monitor(self):
        while True:
            try:
                targets = await self.actions_target_buffer.get()
                for name, data in targets.items():
                    val, meta = data["data"], data["metadata"]
                    t_type = meta.get("target_type", "controller").lower()

                    # Get the target ID from the target's metadata block
                    t_id = meta.get("target_id", meta.get("variablemap_name", "unknown"))
                    target_var_name = meta.get("variable", name)

                    # Send a formatted settings request using the base factory
                    event = SamplingEvent.create(
                        type=f"envds.{t_type}.settings.request", 
                        source=f"envds.{self.config.daq_id}.sampling-modes",
                        data={
                            "settings": target_var_name,
                            "requested": val
                        },
                        extra_header={"deviceid": t_id}
                    )
                    
                    # Devices listen to the generic settings/request topic for incoming commands
                    event["destpath"] = f"envds/sensor/settings/request"
                    await self.send_event(event)

            except Exception as e:
                L.error("action_target_monitor error", extra={"reason": str(e)})

            finally:
                if 'targets' in locals():
                    self.actions_target_buffer.task_done()

# Consistent entry point logic from sampling-states
async def shutdown():
    print("shutting down")

async def main(config):
    # uvicorn.Config points to main.py app instance
    config_uv = uvicorn.Config(
        "main:app", 
        host=config.host, 
        port=config.port, 
        root_path="/msp/sampling-modes"
    )

    server = uvicorn.Server(config_uv)
    L.info(f"server: {server}")
    await server.serve()

    print("starting shutdown...")
    await shutdown()
    print("done.")

if __name__ == "__main__":
    mgr_config = SamplingModesConfig()
    # Parsing sys.argv for simple command line override support
    try:
        idx = sys.argv.index("--host")
        mgr_config.host = sys.argv[idx + 1]
    except (ValueError, IndexError): pass
    try:
        idx = sys.argv.index("--port")
        mgr_config.port = int(sys.argv[idx + 1])
    except (ValueError, IndexError): pass
    
    print("going to run(main)")
    asyncio.run(main(mgr_config))