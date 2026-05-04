import asyncio
import logging
import httpx
from ulid import ULID
from datetime import timezone
from pydantic import BaseSettings, Field
from logfmter import Logfmter
from cloudevents.http import CloudEvent, from_json
from cloudevents.conversion import to_structured
from aiomqtt import Client, MqttError
from envds.util.util import (
    get_datetime_string, 
    get_datetime, 
    time_to_next
)
from envds.sampling.event import SamplingEvent

# Configure structured logging consistent with sampling-operations
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger("SystemModesManager")
L.setLevel(logging.DEBUG)

class SystemModesConfig(BaseSettings):
    daq_id: str = "default_daq"
    mqtt_broker: str = "mosquitto.default"
    mqtt_port: int = 1883
    # Subscriptions for status tracking and remote commands
    mqtt_topic_subscriptions: str = "envds/+/+/+/data/#,envds/+/system-modes/#,envds/+/sampling-modes/#"
    system_init_control: str = "auto"
    system_init_mode: str = "startup"
    is_primary_controller: bool = True
    
    class Config:
        env_prefix = "SYSTEM_MODES_"

class SystemModeState:
    """Manages the lifecycle and requirement evaluation for a single SystemMode."""
    def __init__(self, config, status_buffer, transitions_buffer):
        self.config = config
        self.status_buffer = status_buffer
        self.transitions_buffer = transitions_buffer
        self.requirements = config.get("requirements", [])
        self.active = False
        self.current_state = False
        
    def activate(self, active: bool):
        """Sets whether this mode is currently being targeted for evaluation."""
        self.active = active

    async def evaluate(self, requirement_status_map):
        """Checks if requirements (SamplingModes or other SystemModes) are satisfied."""
        if not self.active: return
        
        mode_status = []
        for req in self.requirements:
            kind, name = req.get("kind"), req.get("name")
            # Pull current state from the manager's global status cache
            mode_status.append(requirement_status_map.get(kind, {}).get(name, False))

        latest_status = all(mode_status) if mode_status else False
        
        current_dt = get_datetime().replace(tzinfo=timezone.utc)
        is_changed = (latest_status != self.current_state)
        
        # Heartbeat every 30s or immediate broadcast on state change
        if is_changed or (current_dt.second % 30 == 0):
            self.current_state = latest_status
            status_event = {
                "status": {
                    "kind": "SystemMode",
                    "time": get_datetime_string(),
                    "name": self.config.get("metadata", {}).get("name", "unknown"),
                    "status": self.current_state,
                }
            }
            await self.status_buffer.put(status_event)
            
            # If the mode is now satisfied, trigger transitions to the next phase
            if is_changed and self.current_state:
                run_type = str(self.current_state).lower()
                for tran in self.config.get("transitions", {}).get(run_type, []):
                    await self.transitions_buffer.put({"transition": tran})

class SystemModesManager:
    """Orchestrates high-level system states and interacts with the central registry."""
    def __init__(self):
        self.config = SystemModesConfig()
        self.modes = {}
        self.active_mode = None
        self.system_control = self.config.system_init_control
        
        # Cache for all incoming Mode statuses (Sampling and System)
        self.requirement_status_map = {} 
        
        self.status_buffer = asyncio.Queue()
        self.transitions_buffer = asyncio.Queue()
        self.publish_queue = asyncio.Queue()
        self.http_client = None

    async def setup(self):
        self.http_client = httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=50))
        
        # Background tasks for registry sync
        asyncio.create_task(self.publish_local_definitions())
        asyncio.create_task(self.sync_definitions_loop())
        
        # Communication tasks
        asyncio.create_task(self.mqtt_listen_loop())
        asyncio.create_task(self.mqtt_publish_loop())
        
        # Operational monitors
        asyncio.create_task(self.transition_monitor())
        asyncio.create_task(self.evaluation_loop())
        asyncio.create_task(self.status_publish_monitor())

    async def publish_local_definitions(self):
        """Registers local definitions with the Datastore/Registrar."""
        await asyncio.sleep(5)
        while True:
            try:
                for name, mode_obj in self.modes.items():
                    event = SamplingEvent.create_definition_registry_update(
                        resource="systemmode-definition", 
                        source=f"envds.{self.config.daq_id}.system-modes", 
                        data={"systemmode": mode_obj.config}
                    )
                    event["destpath"] = f"envds/{self.config.daq_id}/systemmode-definition/registry/update"
                    await self.send_event(event)
            except Exception as e: 
                L.error("publish_defs_failed", extra={"reason": str(e)})
            await asyncio.sleep(60)

    async def sync_definitions_loop(self):
        """Reconciles local modes with the central Datastore."""
        await asyncio.sleep(10)
        while True:
            try:
                ds_url = f"datastore.{self.config.daq_id}-system.svc.cluster.local:80"
                ids_resp = await self.http_client.get(f"http://{ds_url}/systemmode-definition/registry/ids/get/")
                
                if ids_resp.status_code == 200:
                    for def_id in ids_resp.json().get("results", []):
                        resp = await self.http_client.get(f"http://{ds_url}/systemmode-definition/registry/get/", params={"name": def_id})
                        if resp.status_code == 200 and resp.json().get("results"):
                            config = resp.json()["results"][0]
                            name = config["metadata"]["name"]
                            if name not in self.modes:
                                self.modes[name] = SystemModeState(config, self.status_buffer, self.transitions_buffer)
                                L.info("loaded_system_mode", extra={"name": name})
            except Exception as e: 
                L.error("sync_failed", extra={"reason": str(e)})
            await asyncio.sleep(60)

    async def send_event(self, ce: CloudEvent):
        """Queues CloudEvents for MQTT publication."""
        topic = ce.get("destpath", "")
        if topic:
            headers, body = to_structured(ce)
            await self.publish_queue.put((topic, body))

    def activate_system_mode(self, name: str):
        """Sets a specific SystemMode as active for evaluation."""
        if name not in self.modes:
            L.warning("unknown_system_mode", extra={"req_name": name})
            return
            
        for mode_name, mode_obj in self.modes.items():
            mode_obj.activate(mode_name == name)
        
        self.active_mode = name
        L.info("system_mode_activated", extra={"mode": name})

    async def evaluation_loop(self):
        """Continuous loop to evaluate requirements for the active mode."""
        while True:
            # ONLY the primary controller can boot the system into the init mode
            if self.config.is_primary_controller:
                if not self.active_mode and self.modes:
                    self.activate_system_mode(self.config.system_init_mode)
                
                if self.active_mode and self.active_mode in self.modes:
                    await self.modes[self.active_mode].evaluate(self.requirement_status_map)
            
            # If NOT primary, we don't evaluate; we just wait for transition requests
            await asyncio.sleep(time_to_next(1))

    async def status_publish_monitor(self):
        """Broadcasts current system mode status to the MQTT bus."""
        while True:
            status_data = await self.status_buffer.get()
            event = CloudEvent(
                attributes={
                    "type": "envds.systemmode.status.update", 
                    "source": f"envds.{self.config.daq_id}.system-modes"
                },
                data=status_data
            )
            event["destpath"] = f"envds/{self.config.daq_id}/system-modes/status/update"
            await self.send_event(event)
            self.status_buffer.task_done()

    async def transition_monitor(self):
        """Listens for transitions triggered internally or requested remotely."""
        while True:
            transition_data = await self.transitions_buffer.get()
            
            # 1. Internal transitions (from evaluation_loop) 
            # 2. Remote transitions (from MQTT/CloudEvents)
            is_remote = transition_data.get("is_remote_command", False)
            
            # A Monitor node ONLY processes transitions if they are remote commands
            if self.config.is_primary_controller or is_remote:
                name = transition_data.get("transition", {}).get("name")
                if name: 
                    self.activate_system_mode(name)
                    
            self.transitions_buffer.task_done()

    async def mqtt_listen_loop(self):
        """Main ingress loop for status updates and remote commands."""
        my_id = f"envds.{self.config.daq_id}.system-modes"
        while True:
            try:
                async with Client(self.config.mqtt_broker, port=self.config.mqtt_port) as client:
                    for topic in self.config.mqtt_topic_subscriptions.split(","):
                        await client.subscribe(topic.strip())
                        
                    async for message in client.messages:
                        try:
                            ce = from_json(message.payload)
                            
                            # Filter out our own published updates
                            if ce.get("source") == my_id: continue
                            
                            # Cache incoming statuses to evaluate requirements
                            if "status.update" in ce.get("type", ""):
                                status = ce.data.get("status", {})
                                kind, name, state = status.get("kind"), status.get("name"), status.get("status")
                                if kind not in self.requirement_status_map:
                                    self.requirement_status_map[kind] = {}
                                self.requirement_status_map[kind][name] = state
                                
                            # Handle Remote Transition Requests
                            elif ce.get("type") == "envds.system-modes.transition.request":
                                await self.transitions_buffer.put(ce.data)
                                
                        except Exception: pass
            except MqttError:
                await asyncio.sleep(5)

    async def mqtt_publish_loop(self):
        """Main egress loop for outbound MQTT events."""
        while True:
            try:
                async with Client(self.config.mqtt_broker, port=self.config.mqtt_port) as client:
                    while True:
                        topic, payload = await self.publish_queue.get()
                        await client.publish(topic, payload, qos=1)
                        self.publish_queue.task_done()
            except MqttError:
                await asyncio.sleep(5)