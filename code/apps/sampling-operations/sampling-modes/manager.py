import asyncio
import importlib
import logging
import httpx
from ulid import ULID
from datetime import timezone
from pydantic import BaseSettings
from logfmter import Logfmter  #
from cloudevents.http import CloudEvent, from_json
from cloudevents.conversion import to_structured
from aiomqtt import Client, MqttError
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

        self.configure()

    def configure(self):
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

    async def update(self, status_update: dict):
        """Updates internal requirement cache from external status updates."""
        kind = status_update.get("kind")
        name = status_update.get("name")
        status = status_update.get("status")
        if kind in self.requirements and name in self.requirements[kind]:
            self.requirements[kind][name]["status"] = status

    async def evaluate(self):
        """Checks requirements and dispatches actions on state changes."""
        if not self.active: return

        mode_status = [
            req["status"] 
            for kind in self.requirements.values() 
            for req in kind.values()
        ]
        
        # Default to False if no requirements are present
        latest_status = all(mode_status) if mode_status else False
        current_secs = get_datetime().replace(tzinfo=timezone.utc).second
        is_changed = (latest_status != self.current_state)

        # Heartbeat every 30s or broadcast immediately on state change
        if is_changed or (current_secs % 30 == 0):
            self.current_state = latest_status
            
            # 1. Dispatch own status update
            await self.status_buffer.put({
                "status": {
                    "kind": "SamplingMode",
                    "name": self.config.get("metadata", {}).get("name", "unknown"),
                    "status": self.current_state,
                    "time": get_datetime_string()
                }
            })

            # 2. Trigger configured actions ONLY on state change
            if is_changed:
                run_type = str(self.current_state).lower()
                for act in self.actions.get(run_type, []):
                    await self.actions_buffer.put({
                        "action": {"kind": act.get("kind"), "name": act.get("name")}
                    })

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
            last_var = self.sources["data"].get(src_id, {}).get(src["variable"], {})
            source_vars[src_name] = last_var.get("data")
            
        try:
            result = await self.method(**source_vars) if asyncio.iscoroutinefunction(self.method) else self.method(**source_vars)
            if not result: return

            for trg_name, trg in self.config.get("targets", {}).items():
                if trg_name in result:
                    await self.actions_target_buffer.put({
                        trg_name: {"data": result[trg_name], "metadata": trg}
                    })
        except Exception as e:
            L.error("action_execution_failed", extra={"reason": str(e)})

class SamplingModesManager:
    def __init__(self):
        self.config = SamplingModesConfig()
        self.modes = {}
        self.actions = {}
        
        self.status_buffer = asyncio.Queue()
        self.actions_buffer = asyncio.Queue()
        self.actions_target_buffer = asyncio.Queue()
        self.publish_queue = asyncio.Queue()
        self.http_client = None

    async def setup(self):
        self.http_client = httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=50))
        
        # Patterns for synchronization and definition broadcasting
        asyncio.create_task(self.publish_local_definitions())
        asyncio.create_task(self.sync_sampling_definitions_loop())
        
        # Communication loops
        asyncio.create_task(self.mqtt_listen_loop())
        asyncio.create_task(self.mqtt_publish_loop())
        
        # Operational monitors
        asyncio.create_task(self.mode_evaluation_loop())
        asyncio.create_task(self.action_execution_monitor())
        asyncio.create_task(self.action_target_monitor())
        asyncio.create_task(self.status_publish_monitor())

    async def publish_local_definitions(self):
        """Broadcasts locally mounted definitions to the global registry."""
        await asyncio.sleep(5)
        while True:
            try:
                definitions = [(self.actions, "action"), (self.modes, "samplingmode")]
                for registry_dict, res_type in definitions:
                    for name, data_obj in registry_dict.items():
                        config = data_obj.config
                        event = SamplingEvent.create_definition_registry_update(
                            resource=f"{res_type}-definition",
                            source=f"envds.{self.config.daq_id}.sampling-modes",
                            data={res_type: config}
                        )
                        event["destpath"] = f"envds/{self.config.daq_id}/{res_type}-definition/registry/update"
                        await self.send_event(event)
            except Exception as e:
                L.error("publish_local_definitions_failed", extra={"reason": str(e)})
            await asyncio.sleep(60)

    async def sync_sampling_definitions_loop(self):
        """Keeps local instances in sync with the central Datastore."""
        await asyncio.sleep(10)
        while True:
            try:
                ds_url = f"datastore.{self.config.daq_id}-system.svc.cluster.local:80"
                for res in ["action", "samplingmode"]:
                    ids_resp = await self.http_client.get(f"http://{ds_url}/{res}-definition/registry/ids/get/")
                    if ids_resp.status_code == 200:
                        for did in ids_resp.json().get("results", []):
                            resp = await self.http_client.get(f"http://{ds_url}/{res}-definition/registry/get/", params={"name": did})
                            if resp.status_code == 200 and resp.json().get("results"):
                                cfg = resp.json()["results"][0]
                                name = cfg["metadata"]["name"]
                                if res == "action" and name not in self.actions:
                                    self.actions[name] = SamplingAction(cfg, self.actions_target_buffer)
                                    L.info("loaded_action", extra={"name": name})
                                elif res == "samplingmode" and name not in self.modes:
                                    self.modes[name] = SamplingMode(cfg, self.status_buffer, self.actions_buffer)
                                    L.info("loaded_mode", extra={"name": name})
            except Exception as e:
                L.error("sync_definitions_failed", extra={"reason": str(e)})
            await asyncio.sleep(60)

    async def send_event(self, ce: CloudEvent):
        """Encapsulates CloudEvents for MQTT publishing."""
        topic = ce.get("destpath", "")
        if topic:
            headers, body = to_structured(ce)
            await self.publish_queue.put((topic, body))

    async def mqtt_listen_loop(self):
        """Listens for telemetry and requirements while filtering own echoes."""
        my_id = f"envds.{self.config.daq_id}.sampling-modes"
        while True:
            try:
                async with Client(self.config.mqtt_broker, port=self.config.mqtt_port) as client:
                    for topic in self.config.mqtt_topic_subscriptions.split(","):
                        await client.subscribe(topic.strip())
                    async for msg in client.messages:
                        try:
                            ce = from_json(msg.payload)
                            # Anti-loop Filter to prevent processing internal broadcasts
                            if ce.get("source") == my_id: continue
                            
                            if "data.update" in ce.get("type", ""):
                                src_id = ce.get("source", "").split(".")[-1]
                                ts = ce.data.get("variables", {}).get("time", {}).get("data")
                                for evaluator in self.actions.values():
                                    for req_src in evaluator.config.get("sources", {}).values():
                                        rid = f"{req_src['variablemap_name']}::{req_src['variableset_name']}"
                                        if rid == src_id:
                                            if rid not in evaluator.sources["data"]: evaluator.sources["data"][rid] = {}
                                            for k, v in ce.data.get("variables", {}).items():
                                                evaluator.sources["data"][rid][k] = {"data": v.get("data"), "last_update": ts}
                            elif "status.update" in ce.get("type", ""):
                                for m in self.modes.values(): 
                                    await m.update(ce.data.get("status", {}))
                        except Exception: pass
            except MqttError: await asyncio.sleep(5)

    async def mqtt_publish_loop(self):
        """Handles outbound MQTT communications."""
        while True:
            try:
                async with Client(self.config.mqtt_broker, port=self.config.mqtt_port) as client:
                    while True:
                        t, p = await self.publish_queue.get()
                        await client.publish(t, p, qos=1)
                        self.publish_queue.task_done()
            except MqttError: await asyncio.sleep(5)

    async def mode_evaluation_loop(self):
        """Periodically triggers evaluation for all loaded modes."""
        while True:
            for mode in self.modes.values():
                await mode.evaluate()
            await asyncio.sleep(time_to_next(1))

    async def status_publish_monitor(self):
        """Broadcasts current mode statuses to the cluster."""
        while True:
            data = await self.status_buffer.get()
            event = CloudEvent(
                attributes={
                    "type": "envds.samplingmode.status.update", 
                    "source": f"envds.{self.config.daq_id}.sampling-modes"
                },
                data=data
            )
            event["destpath"] = f"envds/{self.config.daq_id}/sampling-modes/status/update"
            await self.send_event(event)
            self.status_buffer.task_done()

    async def action_execution_monitor(self):
        """Executes actions if this node is the primary controller."""
        while True:
            req = await self.actions_buffer.get()
            if not self.config.is_primary_controller:
                self.actions_buffer.task_done()
                continue
            name = req.get("action", {}).get("name")
            if name in self.actions:
                L.info("executing_action", extra={"name": name})
                await self.actions[name].run()
            self.actions_buffer.task_done()

    async def action_target_monitor(self):
        """Translates action results into controller settings updates."""
        while True:
            targets = await self.actions_target_buffer.get()
            for name, data in targets.items():
                val, meta = data["data"], data["metadata"]
                t_type = meta.get("target_type", "controller").lower()
                t_id = meta.get("target_id", meta.get("variablemap_name", "unknown"))
                event = CloudEvent(
                    attributes={
                        "type": f"envds.{t_type}.settings.update", 
                        "source": f"envds.{self.config.daq_id}.sampling-modes"
                    },
                    data={"variables": {meta.get("variable", name): {"data": val}}}
                )
                event["destpath"] = f"envds/{self.config.daq_id}/{t_type}/{t_id}/settings/update"
                await self.send_event(event)
            self.actions_target_buffer.task_done()