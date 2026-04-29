import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
import logging
import traceback
import socket
from fastapi import (
    FastAPI,
    APIRouter,
    HTTPException,
    Request,
    WebSocket,
    WebSocketDisconnect,
    status,
    Response
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.wsgi import WSGIMiddleware

from cloudevents.http import CloudEvent, from_http, from_json, to_json
from cloudevents.conversion import to_structured, to_json
from cloudevents.exceptions import InvalidStructuredJSON
from aiomqtt import Client, MqttError

import httpx
from logfmter import Logfmter
from pydantic import BaseModel, BaseSettings, Field
from ulid import ULID

from dashapp import app as dash_app

from envds.daq.types import DAQEventType as det
from envds.daq.event import DAQEvent
from envds.message.message import Message
from envds.core import envdsBase, envdsAppID, envdsStatus

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.DEBUG)

class Settings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8787
    debug: bool = False
    daq_id: str = "default"

    external_hostname: str = "localhost"
    http_use_tls: bool = False
    http_port: int = 80
    https_port: int = 443
    ws_use_tls: bool = False
    ws_port: int = 80
    wss_port: int = 443

    knative_broker: str = (
        "http://kafka-broker-ingress.knative-eventing.svc.cluster.local/default/default"
    )
    mongodb_data_connection: str = (
        "mongodb://uasdaq:password@uasdaq-mongodb-0.uasdaq-mongodb-svc.mongodb.svc.cluster.local:27017/data"
    )
    mqtt_broker: str = 'mosquitto.default'
    mqtt_port: int = 1883
    mqtt_topic_subscriptions: str = 'envds/+/+/+/data/#'
    mqtt_client_id: str = Field(default_factory=lambda: str(ULID()))

    class Config:
        env_prefix = "DASHBOARD_"
        case_sensitive = False

config = Settings()

# --- FIX 1: Definition Caching ---
# Store the latest schema/settings so new clients get them immediately
definition_cache = {
    "sensor": {},      # key: sensor_id
    "controller": {},  # key: controller_id
    "variableset": {}  # key: variableset_id
}

class ConnectionManager:
    def __init__(self):
        self.active_connections = {}

    async def connect(self, websocket: WebSocket, client_type: str, client_id: str):
        await websocket.accept()
        if client_type not in self.active_connections:
            self.active_connections[client_type] = {}
        if client_id not in self.active_connections[client_type]:
            self.active_connections[client_type][client_id] = []
        self.active_connections[client_type][client_id].append(websocket)
        
        # FIX 1: Immediate Sync from Cache on connection
        if client_id in definition_cache.get(client_type, {}):
            cached_msg = definition_cache[client_type][client_id]
            L.debug(f"Syncing {client_type} {client_id} from cache")
            await websocket.send_text(json.dumps(cached_msg))

    async def disconnect(self, websocket: WebSocket):
        for client_type, types in self.active_connections.items():
            for client_id, ws_list in types.items():
                if websocket in ws_list:
                    ws_list.remove(websocket)
                    return

    async def broadcast(self, message: str, client_type: str, client_id: str):
        try:
            if client_type in self.active_connections and client_id in self.active_connections[client_type]:
                for connection in self.active_connections[client_type][client_id]:
                    await connection.send_text(message)
        except Exception as e:
            L.error(f"broadcast error: {e}")

manager = ConnectionManager()

# --- FIX 2: MQTT Pipeline & Backpressure ---
# Limit buffer size to prevent memory exhaustion
mqtt_buffer = asyncio.Queue(maxsize=1000)

async def send_event(ce: CloudEvent):
    try:
        timeout = httpx.Timeout(5.0, read=0.1)
        headers, body = to_structured(ce)
        async with httpx.AsyncClient() as client:
            r = await client.post(config.knative_broker, headers=headers, data=body, timeout=timeout)
            r.raise_for_status()
    except Exception as e:
        L.error("send_event error", extra={"reason": e})

async def get_from_mqtt_loop():
    reconnect = 10
    while True:
        try:
            async with Client(config.mqtt_broker, port=config.mqtt_port, identifier=str(ULID())) as client:
                for topic in config.mqtt_topic_subscriptions.split(","):
                    if topic.strip():
                        await client.subscribe(topic.strip())

                async for message in client.messages:
                    try:
                        ce = from_json(message.payload)
                        # FIX 2: Handling Queue Backpressure
                        try:
                            mqtt_buffer.put_nowait(ce)
                        except asyncio.QueueFull:
                            # Drop oldest and add newest to maintain real-time relevance
                            mqtt_buffer.get_nowait()
                            mqtt_buffer.put_nowait(ce)
                            L.warning("MQTT Buffer full: dropping oldest record")
                    except Exception as e:
                        L.error("MQTT parsing error", extra={"reason": e})
        except MqttError as error:
            L.error(f"MQTT Error: {error}. Retrying in {reconnect}s")
            await asyncio.sleep(reconnect)

async def handle_mqtt_buffer():
    while True:
        try:
            ce = await mqtt_buffer.get()
            data_type = ce.get("type")
            
            if data_type in ["envds.data.update", "envds.controller.data.update"]:
                attr = ce.data["attributes"]
                device_id = "::".join([attr["make"]["data"], attr["model"]["data"], attr["serial_number"]["data"]])
                client_type = "sensor" if "controller" not in data_type else "controller"
                
                msg = {"data-update": ce.data}
                await manager.broadcast(json.dumps(msg), client_type, device_id)
                
            elif data_type == "envds.variableset.data.update":
                variableset_id = "raz1::main" # Simplified per current requirements
                msg = {"data-update": ce.data}
                await manager.broadcast(json.dumps(msg), "variableset", variableset_id)
        
        except Exception as e:
            L.error("handle_mqtt_buffer error", extra={"reason": e})

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(get_from_mqtt_loop())
    asyncio.create_task(handle_mqtt_buffer())
    yield

app = FastAPI(lifespan=lifespan)
app.mount("/dash", WSGIMiddleware(dash_app.server))

# --- Definition Update Endpoints (Caching Integrated) ---

@app.post("/sensor/settings/update/")
async def sensor_settings_update(request: Request):
    try:
        ce = from_http(headers=request.headers, data=await request.body())
        attr = ce.data["attributes"]
        sensor_id = "::".join([attr["make"]["data"], attr["model"]["data"], attr["serial_number"]["data"]])
        
        msg = {"settings-update": ce.data}
        # FIX 1: Populate cache
        definition_cache["sensor"][sensor_id] = msg
        
        await manager.broadcast(json.dumps(msg), "sensor", sensor_id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as e:
        L.error("sensor_settings_update error", extra={"reason": e})
        return Response(status_code=status.HTTP_400_BAD_REQUEST)

@app.post("/controller/settings/update/")
async def controller_settings_update(request: Request):
    try:
        ce = from_http(headers=request.headers, data=await request.body())
        attr = ce.data["attributes"]
        controller_id = "::".join([attr["make"]["data"], attr["model"]["data"], attr["serial_number"]["data"]])
        
        msg = {"settings-update": ce.data}
        # FIX 1: Populate cache
        definition_cache["controller"][controller_id] = msg
        
        await manager.broadcast(json.dumps(msg), "controller", controller_id)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as e:
        L.error("controller_settings_update error", extra={"reason": e})
        return Response(status_code=status.HTTP_400_BAD_REQUEST)

# --- WebSocket Endpoints ---

@app.websocket("/ws/sensor/{client_id}")
async def sensor_ws_endpoint(websocket: WebSocket, client_id: str):
    await manager.connect(websocket, client_type="sensor", client_id=client_id)
    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            if 'sensor/settings/request' in message.get('destpath', ''):
                event = DAQEvent.create_sensor_settings_request(source=message['source'], data=message['data'])
                event["deviceid"] = message["deviceid"]
                await send_event(event)
    except WebSocketDisconnect:
        await manager.disconnect(websocket)

@app.websocket("/ws/controller/{client_id}")
async def controller_ws_endpoint(websocket: WebSocket, client_id: str):
    await manager.connect(websocket, client_type="controller", client_id=client_id)
    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            if 'controller/settings/request' in message.get('destpath', ''):
                event = DAQEvent.create_controller_settings_request(source=message['source'], data=message['data'])
                event["controllerid"] = message["controllerid"]
                await send_event(event)
    except WebSocketDisconnect:
        await manager.disconnect(websocket)

@app.get("/")
async def root():
    return {"message": "Hello World from Test Dashboard"}
