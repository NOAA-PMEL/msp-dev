import asyncio
import importlib
import logging
from pathlib import Path
import httpx
import os
import json
import sys
from ulid import ULID
from datetime import timezone
from pydantic import BaseSettings, Field
from logfmter import Logfmter 
from cloudevents.http import CloudEvent, from_json
from cloudevents.conversion import to_json, to_structured
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

# Configure structured logging consistent with the system architecture
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger("SystemModesManager")
L.setLevel(logging.DEBUG)

class SystemModesConfig(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8080
    debug: bool = True

    daq_id: str | None = None

    deployment_ref: str = "unknown"

    mqtt_broker: str = "mosquitto.default"
    mqtt_port: int = 1883
    mqtt_topic_subscriptions: str = ""
    mqtt_client_id: str = Field(str(ULID()))

    # FIX: Allow this to be parsed correctly from the environment or default to None
    knative_broker: str | None = None

    system_init_control: str = "auto"
    system_init_mode: str = "startup"
    is_primary_controller: bool = False
    
    class Config:
        env_prefix = "SYSTEM_MODES_"
        case_sensitive = False

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
        self.last_status_time = 0
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

    def stop(self):
        """Placeholder for stopping internal mode tasks if any are added in the future."""
        # Current SystemMode implementation uses Manager loops, but we include this 
        # for architectural consistency with SamplingState/SamplingAction.
        pass

    async def evaluate(self):
        """
        Determines if the current mode logic triggers a state transition and sends heartbeats.
        """
        # 1. Calculate status of underlying requirements
        mode_status = [req["status"] for kind in self.requirements.values() for req in kind.values()]
        latest_status = all(mode_status) if mode_status else False
        
        # 2. HEARTBEAT & CHANGE DETECTION
        # ---------------------------------------------------------
        now = get_datetime().timestamp()
        # SystemMode reports its 'active' flag status to the registry
        is_changed = (self.active != self.current_state)
        # Force a refresh every 30 seconds for dashboard persistence
        is_heartbeat = (now - self.last_status_time >= 30) and self.active

        if is_changed or is_heartbeat:
            self.current_state = self.active
            self.last_status_time = now # Update heartbeat timer

            status_str = "true" if self.active else "false"
            
            # 3. CONSTRUCT envds-compliant status update
            # ---------------------------------------------------------
            status_update = {
                "id": {
                    "app_group": "system", # Group must be 'system' for SystemModes
                    "app_uid": self.config["metadata"]["name"]
                },
                "state": {
                    "system_active": { # State key matches SystemMode expectations
                        "requested": status_str,
                        "actual": status_str
                    }
                },
                "timestamp": get_datetime_string()
            }
            # Push directly to the status buffer for MQTT broadcast
            await self.status_buffer.put({"status": status_update})

        # 4. TRANSITION LOGIC
        # ---------------------------------------------------------
        # Only evaluate transitions if this is the currently active mode
        if not self.active: return 
        
        run_type = str(latest_status).lower()
        if self.transitions.get(run_type):
            for trans in self.transitions[run_type]:
                await self.transitions_buffer.put({"transition": trans})

class SystemModesManager:
    def __init__(self):
        # ---> ADD THESE TWO LINES <---
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)

        self.config = SystemModesConfig()
        self.modes = {}
        self.active_mode = None
        self.control_mode = self.config.system_init_control

        self.requirement_status_map = {}
        
        self.http_client = None

        # 1. CRITICAL: Initialize buffers BEFORE configure()
        self.status_buffer = asyncio.Queue(maxsize=2000)
        self.transitions_buffer = asyncio.Queue(maxsize=2000)
        self.publish_queue = asyncio.Queue(maxsize=2000)
        
        # Load local definitions following consistent pattern
        self.configure()

    # def _load_json_dir(self, dir_path_str: str) -> list:
    #     """Scans a directory for JSON files, injects env vars, and returns the parsed list."""
    #     results = []
    #     dir_path = Path(dir_path_str)
        
    #     if dir_path.exists() and dir_path.is_dir():
    #         for file_path in dir_path.glob("*.json"):
    #             try:
    #                 with open(file_path, "r") as f:
    #                     raw_content = f.read()
                        
    #                     # ---> INJECT VARIABLES BEFORE PARSING <---
    #                     expanded_content = os.path.expandvars(raw_content)
                        
    #                     data = json.loads(expanded_content)
    #                     if isinstance(data, list):
    #                         results.extend(data)
    #                     else:
    #                         results.append(data)
                            
    #                 self.logger.info(f"Loaded and expanded file: {file_path.name}")
    #             except Exception as e:
    #                 self.logger.error(f"Failed to parse {file_path.name}", extra={"reason": str(e)})
    #     else:
    #         self.logger.info(f"{dir_path_str} not found or empty. Skipping local load.")
            
    #     return results
    
    # def configure(self):
    #     """Loads system mode definitions from mounted files and bootstraps identity."""
    #     try:
    #         path = "/app/config/system_modes_modes.json"
    #         if os.path.exists(path):
    #             with open(path, "r") as f:
    #                 modes = json.load(f)
                    
    #                 if modes:
    #                     # --- IMMUTABLE IDENTITY BOOTSTRAP ---
    #                     if self.config.deployment_ref == "unknown" or not self.config.deployment_ref:
    #                         first_ns = modes[0].get("metadata", {}).get("sampling_namespace", "")
    #                         if "/" in first_ns:
    #                             self.config.deployment_ref = first_ns.split("/")[-1]
    #                             self.logger.info(f"Immutable boot-strapped deployment_ref: {self.config.deployment_ref}")
    #                     # -------------------------------------
                        
    #                     for cfg in modes:
    #                         self.load_mode(cfg)
    #         else:
    #             self.logger.info("No local system modes found at /app/config/system_modes_modes.json. Skipping.")
    #     except Exception as e:
    #         self.logger.error("configure_failed", extra={"reason": str(e)})

    def _load_json_dir(self, dir_path_str: str) -> list:
        """Scans a directory for JSON files, injects env vars, and returns the parsed list."""
        results = []
        dir_path = Path(dir_path_str)
        
        self.logger.info(f"[DEBUG] _load_json_dir called. Path: '{dir_path_str}' | Exists: {dir_path.exists()} | Is Dir: {dir_path.is_dir()}")
        
        if dir_path.exists() and dir_path.is_dir():
            for file_path in dir_path.glob("*.json"):
                self.logger.info(f"[DEBUG] Found file: {file_path.name}")
                try:
                    with open(file_path, "r") as f:
                        raw_content = f.read()
                        
                        # ---> INJECT VARIABLES BEFORE PARSING <---
                        expanded_content = os.path.expandvars(raw_content)
                        
                        data = json.loads(expanded_content)
                        if isinstance(data, list):
                            self.logger.info(f"[DEBUG] Parsed list of {len(data)} items from {file_path.name}")
                            results.extend(data)
                        else:
                            self.logger.info(f"[DEBUG] Parsed single dict from {file_path.name}")
                            results.append(data)
                            
                    self.logger.info(f"Loaded and expanded file: {file_path.name}")
                except Exception as e:
                    self.logger.error(f"Failed to parse {file_path.name}", extra={"reason": str(e)})
        else:
            self.logger.info(f"{dir_path_str} not found or empty. Skipping local load.")
            
        return results
    
    def configure(self):
        """Loads system mode definitions from mounted files and bootstraps identity."""
        try:
            # ---> THE FIX: Target the exact, deterministic mount path <---
            modes = self._load_json_dir("/app/config/modes")
            
            if modes:
                # --- IMMUTABLE IDENTITY BOOTSTRAP ---
                if self.config.deployment_ref == "unknown" or not self.config.deployment_ref:
                    first_ns = modes[0].get("metadata", {}).get("sampling_namespace", "")
                    if "/" in first_ns:
                        self.config.deployment_ref = first_ns.split("/")[-1]
                        self.logger.info(f"Immutable boot-strapped deployment_ref: {self.config.deployment_ref}")
                # -------------------------------------
                
                for cfg in modes:
                    self.load_mode(cfg)
            else:
                self.logger.info("No local system modes found in /app/config/modes. Skipping.")
        except Exception as e:
            self.logger.error("configure_failed", extra={"reason": str(e)})

    async def submit_get(self, path: str):
        """Standard helper to fetch from local datastore with logging."""
        try:
            timeout = httpx.Timeout(10.0, read=10.0)
            datastore_url = f"datastore.{self.config.daq_id}-system.svc.cluster.local:80"
            url = f"http://{datastore_url}/{path}/"
            
            resp = await self.http_client.get(url, timeout=timeout)
            
            if resp.status_code == 200:
                return resp.json()
            else:
                self.logger.warning("datastore_get_failed", extra={"path": path, "status": resp.status_code})
                return {}
        except Exception as e:
            self.logger.error("datastore_get_error", extra={"path": path, "reason": str(e)})
            return {}

    async def submit_request(self, path: str, query: dict):
        """Standard helper to fetch specific definitions with logging."""
        try:
            timeout = httpx.Timeout(10.0, read=10.0)
            datastore_url = f"datastore.{self.config.daq_id}-system.svc.cluster.local:80"
            url = f"http://{datastore_url}/{path}/"
            
            resp = await self.http_client.get(url, params=query, timeout=timeout)
            
            if resp.status_code == 200:
                return resp.json()
            else:
                self.logger.warning("datastore_request_failed", extra={"path": path, "status": resp.status_code})
                return {}
        except Exception as e:
            self.logger.error("datastore_request_error", extra={"path": path, "reason": str(e)})
            return {}

    def load_mode(self, cfg):
        """Processes a definition and instantiates a SystemMode object using a composite key."""
        if self.status_buffer is None: self.status_buffer = asyncio.Queue(maxsize=2000)
        if self.transitions_buffer is None: self.transitions_buffer = asyncio.Queue(maxsize=2000)
        
        try:
            name = cfg["metadata"]["name"]
            ns = cfg.get("metadata", {}).get("sampling_namespace", "")
            
            # Create the compound tuple key to prevent namespace clashes
            composite_key = (name, ns)
            
            new_time_str = cfg.get("metadata", {}).get("valid_config_time", "")
            new_time = string_to_datetime(new_time_str)

            existing_entry = self.modes.get(composite_key)
            if existing_entry:
                existing_config = existing_entry.config
                existing_time_str = existing_config.get("metadata", {}).get("valid_config_time", "")
                existing_time = string_to_datetime(existing_time_str)

                # --- TIME-GATING FIX: Reject stale configs from Datastore ---
                if new_time and existing_time:
                    if new_time < existing_time:
                        self.logger.warning(f"REJECTED STALE CONFIG: {name} ({new_time_str} is older than active {existing_time_str})")
                        return
                    if new_time == existing_time:
                        if existing_config == cfg:
                            return # Config hasn't changed and timestamp is the same
                # ------------------------------------------------------------
                
                # PREVENT TASK LEAKS: Stop the old instance if it exists
                existing_entry.stop()

            self.modes[composite_key] = SystemMode(cfg, self.status_buffer, self.transitions_buffer)
            
            # Restore active state if this specific node mode was already running
            if self.active_mode == name and (self.config.deployment_ref in ns or not self.config.deployment_ref):
                self.modes[composite_key].active = True

            # SUCCESS LOG: Explicitly confirms the definition is now working in memory
            self.logger.info("mode_instance_created", extra={
                "res_name": name, 
                "namespace": ns,
                "req_count": len(cfg.get("requirements", [])),
                "trans_count": len(cfg.get("transitions", {}))
            })
        except Exception as e:
            self.logger.error("mode_load_failed", extra={"reason": str(e), "config_name": cfg.get("metadata", {}).get("name")})
            
    async def setup(self):
        """Starts background task orchestration."""
        # if self.status_buffer is None: self.status_buffer = asyncio.Queue(maxsize=2000)
        # if self.transitions_buffer is None: self.transitions_buffer = asyncio.Queue(maxsize=2000)
        # if self.publish_queue is None: self.publish_queue = asyncio.Queue(maxsize=2000)
        
        self.http_client = httpx.AsyncClient(limits=httpx.Limits(max_keepalive_connections=50))
        
        asyncio.create_task(self.publish_local_definitions())
        asyncio.create_task(self.sync_system_definitions_loop())
        asyncio.create_task(self.mqtt_listen_loop())
        asyncio.create_task(self.mqtt_publish_loop())
        asyncio.create_task(self.evaluation_loop())
        asyncio.create_task(self.transition_monitor())
        asyncio.create_task(self.status_publish_monitor())

    # async def evaluation_loop(self):
    #     """Primary controller logic for driving the state machine."""
    #     while True:
    #         try:
    #             if self.config.is_primary_controller:
    #                 if not self.active_mode and self.modes:
    #                     await self.activate_system_mode(self.config.system_init_mode)

    #                     # 🟢 === ADD THIS BROADCAST BLOCK === 🟢
    #                     event = SamplingEvent.create_system_control_update(
    #                         source=f"envds.{self.config.daq_id}.system-modes", 
    #                         data={"mode": self.control_mode}
    #                     )
    #                     await self.send_to_mqtt(f"envds/{self.config.daq_id}/system-modes/control/update", event)
    #                     # 🟢 ================================ 🟢
                        
    #             # Evaluate ALL modes so they all broadcast their heartbeat
    #             for mode in list(self.modes.values()):
    #                 await mode.evaluate()
                    
    #         except Exception as e:
    #             self.logger.error("evaluation_loop error", extra={"reason": str(e)})
                
    #         await asyncio.sleep(time_to_next(1))

    async def evaluation_loop(self):
        """Primary controller logic for driving the state machine."""
        while True:
            try:
                if self.config.is_primary_controller:
                    if not self.active_mode and self.modes:
                        await self.activate_system_mode(self.config.system_init_mode)
                                          
                # Evaluate ALL modes so they all broadcast their heartbeat
                for mode in list(self.modes.values()):
                    await mode.evaluate()
                
            except Exception as e:
                self.logger.error("evaluation_loop error", extra={"reason": str(e)})
            
            await asyncio.sleep(time_to_next(1))

    # def activate_system_mode(self, name):
    #     if name not in self.modes or name == self.active_mode: return
        
    #     if self.active_mode: self.modes[self.active_mode].active = False
    #     self.active_mode = name
    #     self.modes[name].active = True
    #     L.info("system_mode_activated", extra={"mode": name})

    async def activate_system_mode(self, name):
        """Switches the active macro-mode and commands subordinate SamplingModes."""
        # Helper to find a composite key tuple by its simple string name, prioritizing local node
        def find_mode_by_string(target_name):
            for (m_name, m_ns), m_obj in self.modes.items():
                if m_name == target_name and self.config.deployment_ref in m_ns:
                    return (m_name, m_ns), m_obj
            for (m_name, m_ns), m_obj in self.modes.items():
                if m_name == target_name:
                    return (m_name, m_ns), m_obj
            return None, None

        target_key, target_mode = find_mode_by_string(name)
        if not target_mode or name == self.active_mode: return
        
        # 1. Deactivate current mode and command its SamplingModes to stop
        if self.active_mode: 
            _, current_mode = find_mode_by_string(self.active_mode)
            if current_mode:
                current_mode.active = False
                for req in current_mode.config.get("requirements", []):
                    if req.get("kind") == "SamplingMode":
                        await self.send_activation_request(req.get("name"), False)
                    
        # 2. Activate new mode
        self.active_mode = name
        target_mode.active = True
        self.logger.info("system_mode_activated", extra={"mode": name})
        
        # 3. Command new subordinate SamplingModes to start
        for req in target_mode.config.get("requirements", []):
            if req.get("kind") == "SamplingMode":
                await self.send_activation_request(req.get("name"), True)

    async def send_activation_request(self, mode_name: str, active: bool):
        """Broadcasts an explicit command for a SamplingMode to toggle its active state."""
        source_id = f"envds.{self.config.daq_id}.system-modes"
        topic = f"envds/{self.config.daq_id}/sampling-modes/{mode_name}/activation/request"
        ce_type = "envds.samplingmode.activation.request"
        
        event = CloudEvent(
            attributes={
                "type": ce_type,
                "source": source_id,
                "datacontenttype": "application/json"
            },
            data={
                "mode_name": mode_name,
                "active": active
            }
        )
        event["destpath"] = topic
        self.logger.info(f"Commanding SamplingMode '{mode_name}' -> Active: {active}")
        await self.send_to_mqtt(topic, event)
        
    async def transition_monitor(self):
        """Listens for state changes from the evaluation loop or remote nodes."""
        while True:
            data = await self.transitions_buffer.get()
            # Only primary nodes auto-transition; monitor nodes follow transition requests
            is_remote = data.get("is_remote_command", False)
            name = data.get("transition", {}).get("name")
            
            if is_remote:
                # Always honor remote transition commands (overrides)
                await self.activate_system_mode(name)
            elif self.config.is_primary_controller and self.control_mode == "auto":
                # Only follow automated internal transitions if in AUTO mode
                await self.activate_system_mode(name)
            else:
                self.logger.debug(f"Automated transition to '{name}' ignored (control_mode='{self.control_mode}')")
                
            self.transitions_buffer.task_done()

    async def publish_local_definitions(self):
        """Publishes only local overlay files to the database, leaving synchronized profiles intact."""
        await asyncio.sleep(5)
        while True:
            try:
                self.logger.info(f"[DEBUG] publish_local_definitions loop starting. Modes in memory: {len(self.modes)}")
                
                for (name, ns), obj in self.modes.items():
                    self.logger.info(f"[DEBUG] Evaluating mode '{name}' | Config NS: '{ns}' | Node Deployment Ref: '{self.config.deployment_ref}'")
                    
                    # Only re-publish profiles that genuinely belong to our node namespace
                    if self.config.deployment_ref in ns or not self.config.deployment_ref:
                        self.logger.info(f"[DEBUG] Mode '{name}' matched namespace filter. Preparing CloudEvent.")
                        
                        event = SamplingEvent.create_definition_registry_update(
                            resource="systemmode-definition",
                            source=f"envds.{self.config.daq_id}.system-modes",
                            data={"systemmode": obj.config}
                        )
                        
                        destpath = f"envds/{self.config.daq_id}/systemmode-definition/registry/update"
                        event["destpath"] = destpath
                        
                        self.logger.info(f"[DEBUG] Sending event for '{name}' to Knative Broker. Target destpath: {destpath}")
                        await self.send_event(event)
                        self.logger.info(f"[DEBUG] Successfully posted event to Knative for '{name}'")
                    else:
                        self.logger.info(f"[DEBUG] Mode '{name}' SKIPPED. Config NS '{ns}' did not match Node Ref '{self.config.deployment_ref}'")
                    
            except Exception as e: 
                self.logger.error("publish_failed", extra={"reason": str(e)})
                
            await asyncio.sleep(60)

    async def sync_system_definitions_loop(self):
        """Syncs SystemMode definitions dynamically from the local Datastore."""
        resource = "systemmode"
        while True:
            try:
                # 1. Fetch available SystemMode IDs
                ids_resp = await self.submit_get(path=f"{resource}-definition/registry/ids/get")
                
                if ids_resp and "results" in ids_resp:
                    ids = ids_resp["results"]
                    self.logger.debug("definitions_ids_received", extra={"resource": resource, "count": len(ids), "ids": ids})
                    
                    for did in ids:
                        # 2. Fetch the actual definition body for each ID
                        d_resp = await self.submit_request(path=f"{resource}-definition/registry/get", query={"name": did})
                        
                        if d_resp and "results" in d_resp and d_resp["results"]:
                            config = d_resp["results"][0]
                            self.logger.info("definition_received", extra={"resource": resource, "res_name": did})
                            
                            # 3. Load into memory
                            self.load_mode(config)
                        else:
                            self.logger.warning("definition_body_missing", extra={"resource": resource, "res_name": did})
                else:
                    self.logger.debug("no_definitions_found", extra={"resource": resource})

            except Exception as e:
                self.logger.error("sync_loop_failed", extra={"reason": str(e)})
                
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

                                # ---> ADD THIS BLOCK: Replicas obey the primary <---
                                if ce.get("isprimarycontroller") == "true" and not self.config.is_primary_controller:
                                    # Ensure it's a SystemMode update
                                    if ce.data.get("id", {}).get("app_group") == "system":
                                        remote_mode = ce.data.get("id", {}).get("app_uid")
                                        is_active = str(ce.data.get("state", {}).get("system_active", {}).get("actual", "false")).lower() == "true"
                                        
                                        if is_active and self.active_mode != remote_mode:
                                            self.logger.info(f"Aligning replica state to primary: {remote_mode}")
                                            await self.activate_system_mode(remote_mode)
                                # ---------------------------------------------------

                                # Update evaluation map for current active mode
                                # for mode in self.modes.values(): 
                                #     await mode.update(ce.data.get("status", {}))
                                # Update evaluation map for current active mode
                                for mode in self.modes.values(): 
                                    await mode.update(ce.data)
                            elif "transition.request" in ce.get("type", ""):
                                await self.transitions_buffer.put({"transition": ce.data, "is_remote_command": True})
                            elif "control.request" in ce.get("type", ""):
                                req = ce.data.get("system_mode", {}).get("requested")
                                if req in ["auto", "manual"]:
                                    self.control_mode = req
                                    self.logger.info(f"System control mode switched to {req}")
                                    
                                    # Instantly echo the state change back to the UI
                                    event = SamplingEvent.create_system_control_update(
                                        source=f"envds.{self.config.daq_id}.system-modes", 
                                        data={"mode": self.control_mode}
                                    )
                                    await self.send_to_mqtt(f"envds/{self.config.daq_id}/system-modes/control/update", event)
                                elif req: 
                                    # Catch manual mode dropdown selections
                                    await self.activate_system_mode(req)
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
        """Routes registry definitions to the Knative HTTP Broker."""
        try:
            self.logger.debug("send_event (HTTP)", extra={"ce": ce})
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
            self.logger.error("send_event failed", extra={"reason": str(e)})


    async def send_to_mqtt(self, topic: str, ce):
        """Routes high-volume telemetry and status updates to the MQTT broker."""
        try:
            self.logger.debug("send_to_mqtt (MQTT)", extra={"topic": topic})
            payload = to_json(ce)
            self.logger.debug("send_to_mqtt (MQTT)", extra={"payload": payload})
            await self.publish_queue.put((topic, payload))
        except Exception as e:
            self.logger.error("send_to_mqtt failed", extra={"reason": str(e)})

    # async def status_publish_monitor(self):
    #     while True:
    #         event = CloudEvent(
    #             attributes={"type": "envds.systemmode.status.update", "source": f"envds.{self.config.daq_id}.system-modes"},
    #             data={"status": {"kind": "SystemMode", "name": self.active_mode, "status": True, "time": get_datetime_string()}}
    #         )
    #         event["destpath"] = f"envds/{self.config.daq_id}/system-modes/status/update"
    #         await self.send_event(event)
    #         await asyncio.sleep(30)
    
    # async def status_publish_monitor(self):
    #     while True:
    #         try:
    #             # 1. Safely check if a mode is active
    #             active = self.active_mode if self.active_mode else "none"
    #             status_str = "true" if self.active_mode else "false"

    #             # 2. Build the NEW envds-COMPLIANT STATUS BLOCK
    #             status_data = {
    #                 "id": {
    #                     "app_group": "system",
    #                     "app_uid": active
    #                 },
    #                 "state": {
    #                     "system_active": {
    #                         "requested": "true", 
    #                         "actual": status_str
    #                     }
    #                 },
    #                 "timestamp": get_datetime_string()
    #             }

    #             # 3. Publish via standard SamplingEvent factory (Base create method)
    #             event = SamplingEvent.create(
    #                 type="envds.systemmode.status.update",
    #                 source=f"envds.{self.config.daq_id}.system-modes",
    #                 data=status_data
    #             )
                
    #             destpath = f"envds/{self.config.daq_id}/system-modes/status/update"
    #             event["destpath"] = destpath
    #             # event["samplingnamespace"] = state_ns
    #             # event["validconfigtime"] = state_valid_time

    #             self.logger.debug("state_status_monitor", extra={"event-type": event["type"], "destpath": destpath})

    #             # event["destpath"] = f"envds/{self.config.daq_id}/system-modes/status/update"
    #             await self.send_to_mqtt(destpath, event)

    #         except Exception as e:
    #             # Use L.error since self.logger isn't defined in this manager
    #             L.error("status_publish_monitor error", extra={"reason": str(e)})

    #         # 4. Wait 30 seconds before sending the next heartbeat
    #         await asyncio.sleep(30)

    # async def status_publish_monitor(self):
    #     """Standardized monitor: Immediate update on change, reliable 30s heartbeat."""
    #     while True:
    #         try:
    #             # 1. Retrieve the standardized status payload from the evaluation loop
    #             data = await self.status_buffer.get()
    #             status_data = data.get("status", {})

    #             # 2. STRICT envds STANDARD: Use the System Mode factory
    #             # This ensures the CloudEvent 'type' is set correctly for dashboard filtering
    #             event = SamplingEvent.create_system_mode_status_update(
    #                 source=f"envds.{self.config.daq_id}.system-modes", 
    #                 data=status_data
    #             )

    #             # 3. Dynamic Topic Routing
    #             # Uses the daq_id from ConfigMap to allow raz1, crk8s, etc., to coexist
    #             destpath = f"envds/{self.config.daq_id}/system-modes/status/update"
    #             event["destpath"] = destpath
    #             event["deploymentref"] = self.config.deployment_ref

    #             # ---> ADD THIS LINE: Broadcast our primary authority <---
    #             event["isprimarycontroller"] = str(self.config.is_primary_controller).lower()

    #             # 4. Broadcast via the manager's MQTT publish queue
    #             await self.send_to_mqtt(destpath, event)

    #         except Exception as e:
    #             self.logger.error("status_publish_monitor error", extra={"reason": str(e)})
    #         finally:
    #             # 5. Mark the task as done to prevent buffer lockup
    #             if 'data' in locals():
    #                 self.status_buffer.task_done()

    # async def status_publish_monitor(self):
    #     """Standardized monitor: Immediate update on change, reliable 30s heartbeat."""
    #     last_control_broadcast = 0
    #     while True:
    #         try:
    #             # 1. Retrieve the standardized status payload from the evaluation loop
    #             data = await self.status_buffer.get()
    #             status_data = data.get("status", {})
    #             # 2. STRICT envds STANDARD: Use the System Mode factory
    #             # This ensures the CloudEvent 'type' is set correctly for dashboard filtering
    #             event = SamplingEvent.create_system_mode_status_update(
    #                 source=f"envds.{self.config.daq_id}.system-modes", 
    #                 data=status_data
    #             )
    #             # 3. Dynamic Topic Routing
    #             # Uses the daq_id from ConfigMap to allow raz1, crk8s, etc., to coexist
    #             destpath = f"envds/{self.config.daq_id}/system-modes/status/update"
    #             event["destpath"] = destpath
    #             event["deploymentref"] = self.config.deployment_ref
    #             # ---> ADD THIS LINE: Broadcast our primary authority <---
    #             event["isprimarycontroller"] = str(self.config.is_primary_controller).lower()
    #             # 4. Broadcast via the manager's MQTT publish queue
    #             await self.send_to_mqtt(destpath, event)

    #             # --- NEW: Piggyback the System Control Heartbeat ---
    #             now = get_datetime().timestamp()
    #             if now - last_control_broadcast >= 30:
    #                 ctrl_event = SamplingEvent.create_system_control_update(
    #                     source=f"envds.{self.config.daq_id}.system-modes", 
    #                     data={"mode": self.control_mode}
    #                 )
    #                 await self.send_to_mqtt(f"envds/{self.config.daq_id}/system-modes/control/update", ctrl_event)
    #                 last_control_broadcast = now
    #             # ---------------------------------------------------

    #         except Exception as e:
    #             self.logger.error("status_publish_monitor error", extra={"reason": str(e)})
    #         finally:
    #             # 5. Mark the task as done to prevent buffer lockup
    #             if 'data' in locals():
    #                 self.status_buffer.task_done()

    async def status_publish_monitor(self):
        """Standardized monitor: Immediate update on change, reliable 30s heartbeat."""
        last_control_broadcast = 0
        while True:
            try:
                # --- NEW: Unblocked System Control Heartbeat ---
                now = get_datetime().timestamp()
                if now - last_control_broadcast >= 30:
                    ctrl_event = SamplingEvent.create_system_control_update(
                        source=f"envds.{self.config.daq_id}.system-modes", 
                        data={"mode": self.control_mode}
                    )
                    await self.send_to_mqtt(f"envds/{self.config.daq_id}/system-modes/control/update", ctrl_event)
                    last_control_broadcast = now
                # ---------------------------------------------------

                # 1. Retrieve the standardized status payload from the evaluation loop WITHOUT blocking forever
                try:
                    data = await asyncio.wait_for(self.status_buffer.get(), timeout=5.0)
                    status_data = data.get("status", {})
                    
                    # 2. STRICT envds STANDARD: Use the System Mode factory
                    event = SamplingEvent.create_system_mode_status_update(
                        source=f"envds.{self.config.daq_id}.system-modes", 
                        data=status_data
                    )
                    
                    # 3. Dynamic Topic Routing
                    destpath = f"envds/{self.config.daq_id}/system-modes/status/update"
                    event["destpath"] = destpath
                    event["deploymentref"] = self.config.deployment_ref
                    
                    # Broadcast our primary authority
                    event["isprimarycontroller"] = str(self.config.is_primary_controller).lower()
                    
                    # 4. Broadcast via the manager's MQTT publish queue
                    await self.send_to_mqtt(destpath, event)
                    
                    self.status_buffer.task_done()
                    
                except asyncio.TimeoutError:
                    # Expected timeout when no status changes occur; loop continues to service heartbeat
                    pass

            except Exception as e:
                self.logger.error("status_publish_monitor error", extra={"reason": str(e)})

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