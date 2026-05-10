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
from cloudevents.conversion import to_structured
from aiomqtt import Client, MqttError
import uvicorn
from envds.util.util import (
    get_datetime_string, 
    get_datetime, 
    time_to_next, 
    string_to_datetime
)
from envds.sampling.event import SamplingEvent

# Configure structured logging consistent with the system architecture
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger("SystemModesManager")
L.setLevel(logging.DEBUG)

class SystemModesConfig(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8080
    daq_id: str = "default_daq"
    mqtt_broker: str = "mosquitto.default"
    mqtt_port: int = 1883
    # Subscribes to telemetry and mode statuses for orchestration
    mqtt_topic_subscriptions: str = "envds/+/+/+/data/#,envds/+/system-modes/#,envds/+/sampling-modes/#"
    system_init_mode: str = "startup"
    is_primary_controller: bool = True
    
    class Config:
        env_prefix = "SYSTEM_MODES_"

class SystemMode:
    """Orchestrates system-level transitions based on SamplingMode statuses."""
    def __init__(self, config, status_buffer, transitions_buffer):
        self.config = config
        self.status_buffer = status_buffer
        self.transitions_buffer = transitions_buffer
        self.requirements = {}
        self.transitions = {"true": [], "false": []}
        self.current_state = False
        self.active = False 
        self._configure_logic()

    def _configure_logic(self):
        """Initializes internal requirement tracking from config."""
        for req in self.config.get("requirements", []):
            kind, name = req.get("kind"), req.get("name")
            if kind not in self.requirements:
                self.requirements[kind] = {}
            self.requirements[kind][name] = {"status": False}

        for test, trans_list in self.config.get("transitions", {}).items():
            self.transitions[test] = trans_list

    # async def update(self, status_update: dict):
    #     """Updates the status of requirements (e.g., SamplingModes)."""
    #     kind, name, status = status_update.get("kind"), status_update.get("name"), status_update.get("status")
    #     if kind in self.requirements and name in self.requirements[kind]:
    #         self.requirements[kind][name]["status"] = status

    async def update(self, payload: dict):
        """Updates the status of requirements (e.g., SamplingModes)."""
        # Consume the envds-compliant status format
        id_block = payload.get("id", {})
        state_block = payload.get("state", {})
        
        # Map app_group back to kind for internal routing
        app_group = id_block.get("app_group", "")
        kind = "SamplingMode" if app_group == "mode" else app_group
        name = id_block.get("app_uid")
        
        # Extract boolean actual status (defaults to false if missing)
        mode_state = state_block.get("mode_active", {}).get("actual", "false")
        is_met = (str(mode_state).lower() == "true")
        
        if kind in self.requirements and name in self.requirements[kind]:
            self.requirements[kind][name]["status"] = is_met

    async def evaluate(self):
        """Determines if the current mode logic triggers a state transition."""
        if not self.active: return
        mode_status = [req["status"] for kind in self.requirements.values() for req in kind.values()]
        latest_status = all(mode_status) if mode_status else False
        
        # Trigger transitions if defined in the logic
        run_type = str(latest_status).lower()
        if self.transitions.get(run_type):
            for trans in self.transitions[run_type]:
                await self.transitions_buffer.put({"transition": trans})

class SystemModesManager:
    def __init__(self):
        self.config = SystemModesConfig()
        self.modes = {}
        self.active_mode = None
        self.requirement_status_map = {}
        
        self.status_buffer = None
        self.transitions_buffer = None
        self.publish_queue = None
        self.http_client = None
        
        # Load local definitions following consistent pattern
        self.configure()

    def configure(self):
        """Loads system mode definitions from mounted files."""
        try:
            path = "/app/config/system_modes_modes.json"
            if os.path.exists(path):
                with open(path, "r") as f:
                    for cfg in json.load(f): self.load_mode(cfg)
        except Exception as e:
            L.error("configure_failed", extra={"reason": str(e)})

    def load_mode(self, cfg):
        if self.status_buffer is None: self.status_buffer = asyncio.Queue(maxsize=2000)
        if self.transitions_buffer is None: self.transitions_buffer = asyncio.Queue(maxsize=2000)
        name = cfg["metadata"]["name"]
        self.modes[name] = SystemMode(cfg, self.status_buffer, self.transitions_buffer)
        L.info("loaded_mode", extra={"res-name": name})

    async def setup(self):
        """Starts background task orchestration."""
        if self.status_buffer is None: self.status_buffer = asyncio.Queue(maxsize=2000)
        if self.transitions_buffer is None: self.transitions_buffer = asyncio.Queue(maxsize=2000)
        if self.publish_queue is None: self.publish_queue = asyncio.Queue(maxsize=2000)
        
        self.http_client = httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=50))
        
        asyncio.create_task(self.publish_local_definitions())
        asyncio.create_task(self.sync_system_definitions_loop())
        asyncio.create_task(self.mqtt_listen_loop())
        asyncio.create_task(self.mqtt_publish_loop())
        asyncio.create_task(self.evaluation_loop())
        asyncio.create_task(self.transition_monitor())
        asyncio.create_task(self.status_publish_monitor())

    async def evaluation_loop(self):
        """Primary controller logic for driving the state machine."""
        while True:
            if self.config.is_primary_controller:
                if not self.active_mode and self.modes:
                    self.activate_system_mode(self.config.system_init_mode)
                
                if self.active_mode and self.active_mode in self.modes:
                    await self.modes[self.active_mode].evaluate()
            await asyncio.sleep(time_to_next(1))

    def activate_system_mode(self, name):
        if name not in self.modes or name == self.active_mode: return
        
        if self.active_mode: self.modes[self.active_mode].active = False
        self.active_mode = name
        self.modes[name].active = True
        L.info("system_mode_activated", extra={"mode": name})

    async def transition_monitor(self):
        """Listens for state changes from the evaluation loop or remote nodes."""
        while True:
            data = await self.transitions_buffer.get()
            # Only primary nodes auto-transition; monitor nodes follow transition requests
            is_remote = data.get("is_remote_command", False)
            if self.config.is_primary_controller or is_remote:
                name = data.get("transition", {}).get("name")
                self.activate_system_mode(name)
            self.transitions_buffer.task_done()

    async def publish_local_definitions(self):
        await asyncio.sleep(5)
        while True:
            try:
                for obj in self.modes.values():
                    event = SamplingEvent.create_definition_registry_update(
                        resource="systemmode-definition",
                        source=f"envds.{self.config.daq_id}.system-modes",
                        data={"systemmode": obj.config}
                    )
                    event["destpath"] = f"envds/{self.config.daq_id}/systemmode-definition/registry/update"
                    await self.send_event(event)
            except Exception as e: L.error("publish_failed", extra={"reason": str(e)})
            await asyncio.sleep(60)

    async def sync_system_definitions_loop(self):
        while True:
            try:
                ds_url = f"datastore.{self.config.daq_id}-system.svc.cluster.local:80"
                resp = await self.http_client.get(f"http://{ds_url}/systemmode-definition/registry/ids/get/")
                if resp.status_code == 200:
                    for did in resp.json().get("results", []):
                        d_resp = await self.http_client.get(f"http://{ds_url}/systemmode-definition/registry/get/", params={"name": did})
                        if d_resp.status_code == 200 and d_resp.json().get("results"):
                            self.load_mode(d_resp.json()["results"][0])
            except Exception as e: L.error("sync_failed", extra={"reason": str(e)})
            await asyncio.sleep(60)

    async def mqtt_listen_loop(self):
        my_id = f"envds.{self.config.daq_id}.system-modes"
        while True:
            try:
                async with Client(self.config.mqtt_broker, port=self.config.mqtt_port) as client:
                    for t in self.config.mqtt_topic_subscriptions.split(","): await client.subscribe(t.strip())
                    async for msg in client.messages:
                        try:
                            ce = from_json(msg.payload)
                            if ce.get("source") == my_id: continue
                            
                            if "status.update" in ce.get("type", ""):
                                # Update evaluation map for current active mode
                                # for mode in self.modes.values(): 
                                #     await mode.update(ce.data.get("status", {}))
                                # Update evaluation map for current active mode
                                for mode in self.modes.values(): 
                                    await mode.update(ce.data)
                            elif "transition.request" in ce.get("type", ""):
                                await self.transitions_buffer.put({"transition": ce.data, "is_remote_command": True})
                        except Exception: pass
            except MqttError: await asyncio.sleep(5)

    async def mqtt_publish_loop(self):
        while True:
            try:
                async with Client(self.config.mqtt_broker, port=self.config.mqtt_port) as client:
                    while True:
                        t, p = await self.publish_queue.get()
                        await client.publish(t, p, qos=1)
                        self.publish_queue.task_done()
            except MqttError: await asyncio.sleep(5)

    async def send_event(self, ce):
        topic = ce.get("destpath", "")
        if topic: await self.publish_queue.put((topic, to_structured(ce)[1]))

    # async def status_publish_monitor(self):
    #     while True:
    #         event = CloudEvent(
    #             attributes={"type": "envds.systemmode.status.update", "source": f"envds.{self.config.daq_id}.system-modes"},
    #             data={"status": {"kind": "SystemMode", "name": self.active_mode, "status": True, "time": get_datetime_string()}}
    #         )
    #         event["destpath"] = f"envds/{self.config.daq_id}/system-modes/status/update"
    #         await self.send_event(event)
    #         await asyncio.sleep(30)
    
    async def status_publish_monitor(self):
        while True:
            try:
                # 1. Pull the raw update from the System evaluation
                data = await self.status_buffer.get()
                
                # 2. Extract the flat payload
                status_obj = data.get("status", {})
                sys_name = status_obj.get("name", "unknown")
                is_active = status_obj.get("status", False)
                status_str = "true" if is_active else "false"

                # 3. Translate to the NEW envds-COMPLIANT STATUS BLOCK
                status_data = {
                    "id": {
                        "app_group": "system",
                        "app_uid": sys_name
                    },
                    "state": {
                        "system_active": {
                            "requested": "true", 
                            "actual": status_str
                        }
                    },
                    "timestamp": get_datetime_string()
                }

                # 4. Publish via standard CloudEvent
                event = CloudEvent(
                    attributes={
                        "type": "envds.systemmode.status.update", 
                        "source": f"envds.{self.config.daq_id}.system-modes"
                    },
                    data=status_data
                )
                
                event["destpath"] = f"envds/{self.config.daq_id}/system-modes/status/update"
                await self.send_event(event)

            except Exception as e:
                self.logger.error("status_publish_monitor error", extra={"reason": str(e)})

            finally:
                # Mark task done so the queue doesn't lock up
                if 'data' in locals():
                    self.status_buffer.task_done()

async def shutdown():
    print("shutting down")

async def main(config):
    config_uv = uvicorn.Config("main:app", host=config.host, port=config.port, root_path="/msp/system-modes")
    server = uvicorn.Server(config_uv)
    await server.serve()
    await shutdown()

if __name__ == "__main__":
    mgr_config = SystemModesConfig()
    try:
        idx = sys.argv.index("--host"); mgr_config.host = sys.argv[idx + 1]
    except (ValueError, IndexError): pass
    try:
        idx = sys.argv.index("--port"); mgr_config.port = int(sys.argv[idx + 1])
    except (ValueError, IndexError): pass
    
    print("going to run(main)")
    asyncio.run(main(mgr_config))