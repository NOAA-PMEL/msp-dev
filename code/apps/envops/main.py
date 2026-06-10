import asyncio
import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.middleware.wsgi import WSGIMiddleware
from pydantic import BaseSettings, Field
from ulid import ULID
from aiomqtt import Client, MqttError

from cloudevents.http import from_json
from logfmter import Logfmter

# Import the initialized Dash app from app.py
from app import app as dash_app
from envds.daq.event import DAQEvent

# --- CONFIG ---
class Settings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8080
    debug: bool = False
    daq_id: str = "default"
    log_level: str = "INFO"

    mqtt_broker: str = "mosquitto.default"
    mqtt_port: int = 1883
    mqtt_topic_subscriptions: str = "envds/+/+/+/data/#,envds/+/+/+/status/#"
    mqtt_client_id: str = Field(default_factory=lambda: f"envops-dash-{str(ULID())}")

    class Config:
        env_prefix = "ENVOPS_"
        case_sensitive = False

config = Settings()

# --- LOGGING ---
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger("EnvOps-Main")
numeric_level = getattr(logging, config.log_level.upper(), logging.INFO)
L.setLevel(numeric_level)

# --- CONNECTION MANAGER ---
class ConnectionManager:
    """Manages granular WebSocket connections with bounded queues and load shedding."""
    def __init__(self):
        self.active_connections = {
            "fleet": {}, "fleet_telemetry": {}, "deployment_c2": {},
            "deployment_telemetry": {}, "variableset": {}, "sensor": {}, "registry": {},
            "chat": {} # Included for the Comms Widget
        }

    async def connect(self, websocket: WebSocket, client_type: str, client_id: str):
        await websocket.accept()
        
        ws_queue = asyncio.Queue(maxsize=50) 
        worker_task = asyncio.create_task(self._ws_sender_worker(websocket, ws_queue, client_type, client_id))
        
        if client_type not in self.active_connections:
            self.active_connections[client_type] = {}
            
        if client_id not in self.active_connections[client_type]:
            self.active_connections[client_type][client_id] = {}
            
        self.active_connections[client_type][client_id][websocket] = (ws_queue, worker_task)
        L.debug(f"WS Connected: {client_type}/{client_id}")

    async def _ws_sender_worker(self, websocket: WebSocket, queue: asyncio.Queue, client_type: str, client_id: str):
        try:
            while True:
                message = await queue.get()
                await websocket.send_text(message)
                queue.task_done()
        except Exception:
            pass 

    def disconnect(self, websocket: WebSocket, client_type: str, client_id: str):
        if client_type in self.active_connections and client_id in self.active_connections[client_type]:
            if websocket in self.active_connections[client_type][client_id]:
                queue, worker_task = self.active_connections[client_type][client_id][websocket]
                worker_task.cancel() 
                del self.active_connections[client_type][client_id][websocket]
                
            if not self.active_connections[client_type][client_id]:
                del self.active_connections[client_type][client_id]
            L.debug(f"WS Disconnected: {client_type}/{client_id}")

    async def broadcast(self, message: str, client_type: str, client_id: str):
        if client_id in self.active_connections.get(client_type, {}):
            for websocket, (queue, worker_task) in list(self.active_connections[client_type][client_id].items()):
                try:
                    queue.put_nowait(message)
                except asyncio.QueueFull:
                    try:
                        queue.get_nowait()
                        queue.task_done()
                    except asyncio.QueueEmpty:
                        pass
                    try:
                        queue.put_nowait(message)
                    except asyncio.QueueFull:
                        pass

manager = ConnectionManager()

# THE FIX: Properly instantiated the MQTT publishing queue
mqtt_publish_queue = asyncio.Queue()

# --- MQTT BACKGROUND TASKS ---
async def mqtt_listen_task():
    """Listens to the broker and routes incoming CloudEvents to the correct WebSockets."""
    reconnect_delay = 5
    while True:
        try:
            L.info(f"Connecting to MQTT Broker: {config.mqtt_broker}:{config.mqtt_port}")
            async with Client(config.mqtt_broker, port=config.mqtt_port, identifier=config.mqtt_client_id) as client:
                
                for topic in config.mqtt_topic_subscriptions.split(","):
                    if topic.strip():
                        await client.subscribe(topic.strip())
                        L.info(f"Subscribed to MQTT topic: {topic.strip()}")

                async for message in client.messages:
                    try:
                        ce = from_json(message.payload)
                        topic = message.topic.value
                        ce_type = ce.get("type", "")
                        source = ce.get("source", "")
                        
                        ce_str = message.payload.decode()  
                        payload_str = json.dumps(ce.data)     

                        # 1. Route Operations Health (Status Updates)
                        if any(x in ce_type for x in ["systemmode", "samplingmode", "samplingstate", "samplingcondition"]):
                            for dep_id in manager.active_connections.get("deployment_c2", {}).keys():
                                await manager.broadcast(ce_str, "deployment_c2", dep_id)
                            
                            for fleet_id in manager.active_connections.get("fleet", {}).keys():
                                await manager.broadcast(payload_str, "fleet", fleet_id)

                        # 2. Route Variableset Telemetry to Variableset WebSockets
                        elif ce_type in ["envds.variableset.data.update"]:
                            vs_id = source.split(".")[-1] 
                            await manager.broadcast(payload_str, "variableset", vs_id)
                            
                            variables = ce.data.get("variables", {})
                            if "latitude" in variables and "longitude" in variables:
                                try:
                                    target_id = None
                                    try:
                                        target_id = ce["deploymentref"]
                                    except Exception:
                                        target_id = ce.data.get("attributes", {}).get("deployment_ref", {}).get("data", "unknown")
                                    
                                    loc_payload = json.dumps({"target_id": target_id, "data": ce.data})
                                    
                                    for fleet_id in manager.active_connections.get("fleet_telemetry", {}).keys():
                                        await manager.broadcast(loc_payload, "fleet_telemetry", fleet_id)
                                except Exception as e:
                                    L.error(f"Error parsing nav source for map: {e}")

                        # 3. Route Raw Sensor & Controller Telemetry
                        elif ce_type in ["envds.data.update", "envds.controller.data.update"]:
                            attrs = ce.data.get("attributes", {})
                            make = attrs.get("make", {}).get("data", "unknown")
                            model = attrs.get("model", {}).get("data", "unknown")
                            sn = attrs.get("serial_number", {}).get("data", "unknown")
                            
                            fully_qualified_id = f"{make}::{model}::{sn}"
                            
                            if "controller" in ce_type:
                                await manager.broadcast(payload_str, "controller", fully_qualified_id)
                            else:
                                await manager.broadcast(payload_str, "sensor", fully_qualified_id)

                        # 4. Route Sensor Settings
                        elif ce_type == "envds.sensor.settings.update":
                            attrs = ce.data.get("attributes", {})
                            make = attrs.get("make", {}).get("data", "unknown")
                            model = attrs.get("model", {}).get("data", "unknown")
                            sn = attrs.get("serial_number", {}).get("data", "unknown")
                            
                            device_id = f"{make}::{model}::{sn}"
                            await manager.broadcast(payload_str, "sensor", device_id)
                            
                        # 5. Route Controller Settings
                        elif ce_type == "envds.controller.settings.update":
                            attrs = ce.data.get("attributes", {})
                            make = attrs.get("make", {}).get("data", "unknown")
                            model = attrs.get("model", {}).get("data", "unknown")
                            sn = attrs.get("serial_number", {}).get("data", "unknown")
                            
                            controller_id = f"{make}::{model}::{sn}"
                            await manager.broadcast(payload_str, "controller", controller_id)
                            
                    except Exception as e:
                        L.error(f"Error processing MQTT message on topic {topic}: {e}")
                        
        except MqttError as e:
            L.error(f"MQTT Connection dropped: {e}. Reconnecting in {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
        except Exception as e:
            L.error(f"Unexpected MQTT listener error: {e}")
            await asyncio.sleep(reconnect_delay)

async def mqtt_publish_task():
    """Takes validated CloudEvent objects from the queue and publishes them to MQTT."""
    reconnect_delay = 5
    client_id = f"envops-pub-{str(ULID())}"
    
    L.info("--- [MQTT_PUB] Background publish task started. ---")
    
    while True:
        try:
            async with Client(config.mqtt_broker, port=config.mqtt_port, identifier=client_id) as client:
                L.info("--- [MQTT_PUB] Publisher successfully connected! Waiting for queue... ---")
                
                while True:
                    topic, ce_obj = await mqtt_publish_queue.get()
                    L.info(f"\n--- [MQTT_PUB] DEQUEUED CLOUDEVENT --- \nTopic: {topic} | Type: {ce_obj.get('type')}")
                    
                    try:
                        # STRICT COMPLIANCE: Serialize the CloudEvent object right before publish
                        payload_bytes = to_json(ce_obj)
                        await client.publish(topic, payload_bytes, qos=1)
                        L.info("--- [MQTT_PUB] Successfully published CloudEvent to broker! ---")
                    except Exception as pub_err:
                        L.error(f"--- [MQTT_PUB] FAILED TO PUBLISH: {pub_err} ---")
                    finally:
                        mqtt_publish_queue.task_done()
                        
        except MqttError as e:
            L.error(f"--- [MQTT_PUB] MQTT Connection dropped: {e}. Reconnecting in {reconnect_delay}s... ---")
            await asyncio.sleep(reconnect_delay)
        except Exception as e:
            L.error(f"--- [MQTT_PUB] CRITICAL TASK CRASH: {e} ---")
            await asyncio.sleep(reconnect_delay)
            
# --- APP LIFECYCLE ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    task_listen = asyncio.create_task(mqtt_listen_task())
    task_publish = asyncio.create_task(mqtt_publish_task())
    yield
    task_listen.cancel()
    task_publish.cancel()

app = FastAPI(lifespan=lifespan)

# --- WEBSOCKET ENDPOINTS ---
@app.websocket("/ws/deployment/{deployment_id}/c2")
async def ws_deployment_c2(websocket: WebSocket, deployment_id: str):
    await manager.connect(websocket, "deployment_c2", deployment_id)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                # Parse the frontend string directly into a CloudEvent object
                ce_obj = from_json(data.encode('utf-8'))
                topic = ce_obj.get("destpath")
                if topic:
                    await mqtt_publish_queue.put((topic, ce_obj))
            except Exception as e:
                L.error(f"Deployment C2 Bridge Error: {e}")
    except WebSocketDisconnect:
        manager.disconnect(websocket, "deployment_c2", deployment_id)


@app.websocket("/ws/variableset/{variableset_id}")
async def ws_variableset(websocket: WebSocket, variableset_id: str):
    await manager.connect(websocket, "variableset", variableset_id)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                payload = json.loads(data)
                destpath = payload.get("destpath")
                target_type = payload.get("target_type")
                target_id = payload.get("target_id")

                if destpath and target_type:
                    # --- THE FIX: Gateway takes ownership of building the CloudEvent ---
                    if target_type == "controller":
                        ce_obj = DAQEvent.create_controller_settings_request(
                            source=payload.get("source", f"envds.{config.daq_id}.dashboard"),
                            data=payload.get("data", {}),
                            extra_header={"controllerid": target_id, "destpath": destpath}
                        )
                    else:
                        ce_obj = DAQEvent.create_sensor_settings_request(
                            source=payload.get("source", f"envds.{config.daq_id}.dashboard"),
                            data=payload.get("data", {}),
                            extra_header={"deviceid": target_id, "destpath": destpath}
                        )
                        
                    # Drop the validated object directly onto the publisher queue
                    await mqtt_publish_queue.put((destpath, ce_obj))
            except Exception as e:
                L.error(f"VariableSet Command Bridge Error: {e}")
    except WebSocketDisconnect:
        manager.disconnect(websocket, "variableset", variableset_id)


@app.websocket("/ws/sensor/{device_id}")
async def ws_sensor(websocket: WebSocket, device_id: str):
    await manager.connect(websocket, "sensor", device_id)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                payload = json.loads(data)
                destpath = payload.get("destpath")
                if destpath:
                    # Build the CloudEvent object using the DAQEvent helper
                    ce_obj = DAQEvent.create_sensor_settings_request(
                        source=payload.get("source", f"envds.{config.daq_id}.dashboard"),
                        data=payload.get("data", {}),
                        extra_header={"deviceid": payload.get("deviceid", ""), "destpath": destpath}
                    )
                    await mqtt_publish_queue.put((destpath, ce_obj))
            except Exception as e:
                L.error(f"Sensor Bridge Error: {e}")
    except WebSocketDisconnect:
        manager.disconnect(websocket, "sensor", device_id)


@app.websocket("/ws/controller/{controller_id}")
async def ws_controller(websocket: WebSocket, controller_id: str):
    await manager.connect(websocket, "controller", controller_id)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                payload = json.loads(data)
                destpath = payload.get("destpath")
                if destpath:
                    # Build the CloudEvent object using the DAQEvent helper
                    ce_obj = DAQEvent.create_controller_settings_request(
                        source=payload.get("source", f"envds.{config.daq_id}.dashboard"),
                        data=payload.get("data", {}),
                        extra_header={"controllerid": payload.get("controllerid", ""), "destpath": destpath}
                    )
                    await mqtt_publish_queue.put((destpath, ce_obj))
            except Exception as e:
                L.error(f"Controller Bridge Error: {e}")
    except WebSocketDisconnect:
        manager.disconnect(websocket, "controller", controller_id)

@app.websocket("/ws/fleet/telemetry")
async def ws_fleet_telemetry(websocket: WebSocket):
    await manager.connect(websocket, "fleet_telemetry", "global") 
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket, "fleet_telemetry", "global")

@app.websocket("/ws/fleet/status")
async def ws_fleet_status(websocket: WebSocket):
    await manager.connect(websocket, "fleet", "global") 
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket, "fleet", "global")

@app.websocket("/ws/chat")
async def ws_chat(websocket: WebSocket):
    await manager.connect(websocket, "chat", "global")
    try:
        while True:
            data = await websocket.receive_text()
            await manager.broadcast(data, "chat", "global")
    except WebSocketDisconnect:
        manager.disconnect(websocket, "chat", "global")

# --- MOUNT DASH FRONTEND ---
app.mount("/", WSGIMiddleware(dash_app.server))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host=config.host, port=config.port, log_level="info")