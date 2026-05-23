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

# from dashapp import app as dash_app
from envops_app import app as dash_app
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

    knative_broker: str = "http://kafka-broker-ingress.knative-eventing.svc.cluster.local/default/default"
    
    dry_run: bool = False

    mqtt_broker: str = 'mosquitto.default'
    mqtt_port: int = 1883
    mqtt_topic_subscriptions: str = 'envds/+/+/+/data/#', 'envds/+/+/status/#' 
    mqtt_client_id: str = Field(str(ULID()))

    class Config:
        env_prefix = "ENVOPS_"
        case_sensitive = False

config = Settings()

class ConnectionManager:
    def __init__(self):
        self.active_connections = {}

    async def connect(self, websocket: WebSocket, client_type: str, client_id: str):
        print(f"{client_type}: {client_id}")
        await websocket.accept()
        if client_type not in self.active_connections:
            self.active_connections[client_type] = dict()
        if client_id not in self.active_connections[client_type]:
            self.active_connections[client_type][client_id] = []
        
        self.active_connections[client_type][client_id].append(websocket)

    async def disconnect(self, websocket: WebSocket):
        for client_type, types in self.active_connections.items():
            for client_id, ws_list in types.items():
                if websocket in ws_list:
                    ws_list.remove(websocket)
                    if websocket:
                        await websocket.close()
                    return

    async def send_personal_message(self, message: str, websocket: WebSocket, client_type: str, client_id: str):
        await websocket.send_text(message)

    async def broadcast(self, message: str, client_type: str, client_id: str):
        try:
            if client_type in self.active_connections and client_id in self.active_connections[client_type]:
                for connection in self.active_connections[client_type][client_id]:
                    await connection.send_text(message)
        except Exception as e:
            L.error(f"broadcast error: {e}")

    async def broadcast_exclude_self(self, message: str, websocket: WebSocket, client_type: str, client_id: str):
        try:
            if client_type in self.active_connections and client_id in self.active_connections[client_type]:
                for connection in self.active_connections[client_type][client_id]:
                    if connection != websocket:
                        await connection.send_text(message)
        except Exception as e:
            L.error(f"broadcast_exclude_self error: {e}")

manager = ConnectionManager()
host_name = socket.gethostname()
host_ip = socket.gethostbyname(host_name)
L.info(f"name: {host_name}, ip: {host_ip}")

async def send_event(ce: CloudEvent):
    try:
        timeout = httpx.Timeout(5.0, read=0.1)
        headers, body = to_structured(ce)
        async with httpx.AsyncClient() as client:
            r = await client.post(
                config.knative_broker,
                headers=headers,
                data=body,
                timeout=timeout,
            )
            r.raise_for_status()
    except InvalidStructuredJSON:
        L.error(f"INVALID MSG: {ce}")
    except httpx.TimeoutException:
        pass
    except httpx.HTTPError as e:
        L.error(f"HTTP Error when posting to {e.request.url!r}: {e}")
    except Exception as e:
        L.error("send_event", extra={"reason": str(e)})

mqtt_buffer = asyncio.Queue()

async def get_from_mqtt_loop():
    reconnect = 10
    while True:
        try:
            client_id = str(ULID())
            async with Client(config.mqtt_broker, port=config.mqtt_port, identifier=client_id) as client:
                for topic in config.mqtt_topic_subscriptions.split(","):
                    if topic.strip():
                        await client.subscribe(f"{topic.strip()}")

                async for message in client.messages: 
                    try:
                        ce = from_json(message.payload)
                        topic = message.topic.value
                        ce["sourcepath"] = topic
                        await mqtt_buffer.put(ce)
                    except Exception as e:
                        L.error("get_from_mqtt_loop inner", extra={"reason": str(e)})
        except MqttError as error:
            L.error(f'{error}. Trying again in {reconnect} seconds')
            await asyncio.sleep(reconnect)
        except Exception as e:
            L.error("get_from_mqtt_loop outer", extra={"reason": str(e)})
        finally:
            await asyncio.sleep(0.0001)

async def handle_mqtt_buffer():
    while True:
        try:
            ce = await mqtt_buffer.get()
            ce_type = ce.get("type", "")

            # 1. SENSOR TELEMETRY ROUTING
            if ce_type in ["envds.data.update", "envds.sensor.data.update", "sensor.data.update"]:
                attributes = ce.data.get("attributes", {})
                make = str(attributes.get("make", {}).get("data", "unknown") if "make" in attributes else "unknown")
                model = str(attributes.get("model", {}).get("data", "unknown") if "model" in attributes else "unknown")
                sn = str(attributes.get("serial_number", {}).get("data", "unknown") if "serial_number" in attributes else "unknown")
                
                sensor_id = f"{make}::{model}::{sn}"
                msg = {"data-update": ce.data}
                await manager.broadcast(json.dumps(msg), "sensor", sensor_id)

            # 2. CONTROLLER TELEMETRY ROUTING
            elif ce_type in ["envds.controller.data.update", "controller.data.update"]:
                attributes = ce.data.get("attributes", {})
                make = str(attributes.get("make", {}).get("data", "unknown") if "make" in attributes else "unknown")
                model = str(attributes.get("model", {}).get("data", "unknown") if "model" in attributes else "unknown")
                sn = str(attributes.get("serial_number", {}).get("data", "unknown") if "serial_number" in attributes else "unknown")
                
                controller_id = f"{make}::{model}::{sn}"
                msg = {"data-update": ce.data}
                await manager.broadcast(json.dumps(msg), "controller", controller_id)

            # 3. VARIABLESET ROUTING (Smart Routing enabled!)
            elif ce_type == "envds.variableset.data.update":
                variableset_id = ce.get("variablesetid", "unknown")
                if "variablesetid" not in ce and "variablesetfullid" in ce:
                    variableset_id = ce["variablesetfullid"]

                msg = {
                    "data-update": ce.data, 
                    "variablesetfullid": ce.get("variablesetfullid")
                }
                
                # Send the heavy payload to specific variableset subscribers (system_data.py)
                await manager.broadcast(json.dumps(msg), "variableset", variableset_id)
                
                # ---> ADD THIS: 2. Platform Pub/Sub (Groups all variablesets for a specific instrument platform)
                platform_id = ce.data.get("attributes", {}).get("platform", {}).get("data", variableset_id)
                await manager.broadcast(json.dumps(msg), "platform", platform_id)
                # <---

                # --- SMART ROUTING: Extract lightweight GPS data for the fleet map ---
                variables = ce.data.get("variables", {})
                if "latitude" in variables and "longitude" in variables:
                    platform_id = ce.data.get("attributes", {}).get("platform", {}).get("data", variableset_id)
                    
                    mini_msg = {
                        "type": "fleet.location.update",
                        "platform": platform_id,
                        "lat": variables["latitude"].get("data"),
                        "lon": variables["longitude"].get("data"),
                        "time": variables.get("time", {}).get("data")
                    }
                    # Push just the coordinates to the global system-ops channel
                    await manager.broadcast(json.dumps(mini_msg), "system-ops", "main")
            
            # 4. SYSTEM OPS ROUTING (Modes, States, Logs)
            elif any(x in ce_type for x in ["systemmode", "samplingmode", "samplingstate", "samplingcondition", "operations.log"]):
                msg = {
                    "type": ce_type,
                    "data": ce.data
                }
                await manager.broadcast(json.dumps(msg), "system-ops", "main")

        except Exception as e:
            L.error("handle_mqtt_buffer", extra={"reason": str(e)})
        
        await asyncio.sleep(0.0001)

@asynccontextmanager
async def lifespan(app: FastAPI):
    L.debug("lifespan: Application starting up...")
    asyncio.create_task(get_from_mqtt_loop())
    asyncio.create_task(handle_mqtt_buffer())
    yield
    L.debug("lifespan: Application shutting down...")

app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# app.mount("/dash", WSGIMiddleware(dash_app.server))
app.mount("/envds/envops", WSGIMiddleware(dash_app.server))

@app.get("/")
async def root():
    return {"message": "EnvOps Middleware Online"}

# --- WEBSOCKET ENDPOINTS ---

@app.websocket("/ws/sensor/{client_id}")
async def sensor_ws_endpoint(websocket: WebSocket, client_id: str):
    await manager.connect(websocket, client_type="sensor", client_id=client_id)
    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)

            if 'sensor/settings/request' in message.get('destpath', ''):
                event = DAQEvent.create_sensor_settings_request(
                    source=message['source'],
                    data=message['data']
                )
                event['destpath'] = message['destpath']
                event["deviceid"] = message["deviceid"]
                await send_event(event)

            if message.get("client-request") == "start-updates":
                msg_type = "sensor.registry.request"
                attributes = {
                        "type": msg_type,
                        "source": "uasdaq.dashboard",
                        "id": str(ULID()),
                        "datacontenttype": "application/json; charset=utf-8",
                }
                reg_request = {"register-sensor-request": "update-sensor-definition-all"}
                ce = CloudEvent(attributes=attributes, data=reg_request)
                try:
                    headers, body = to_structured(ce)
                    async with httpx.AsyncClient() as client:
                        r = await client.post(config.knative_broker, headers=headers, data=body)
                except Exception as e:
                    L.error(f"Error requesting registry update: {e}")
                    
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
        await asyncio.sleep(.1)

@app.websocket("/ws/controller/{client_id}")
async def controller_ws_endpoint(websocket: WebSocket, client_id: str):
    await manager.connect(websocket, client_type="controller", client_id=client_id)
    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)

            if 'controller/settings/request' in message.get('destpath', ''):
                event = DAQEvent.create_controller_settings_request(
                    source=message['source'],
                    data=message['data']
                )
                event['destpath'] = message['destpath']
                event["controllerid"] = message["controllerid"]
                await send_event(event)

            if message.get("client-request") == "start-updates":
                msg_type = "controller.registry.request"
                attributes = {
                        "type": msg_type,
                        "source": "uasdaq.dashboard",
                        "id": str(ULID()),
                        "datacontenttype": "application/json; charset=utf-8",
                }
                reg_request = {"register-sensor-request": "update-sensor-definition-all"}
                ce = CloudEvent(attributes=attributes, data=reg_request)
                try:
                    headers, body = to_structured(ce)
                    async with httpx.AsyncClient() as client:
                        r = await client.post(config.knative_broker, headers=headers, data=body)
                except Exception as e:
                    L.error(f"Error requesting registry update: {e}")
                    
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
        await asyncio.sleep(.1)

@app.websocket("/ws/variableset/{client_id}")
async def variableset_ws_endpoint(websocket: WebSocket, client_id: str):
    await manager.connect(websocket, client_type="variableset", client_id=client_id)
    try:
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)

            if message.get("client-request") == "start-updates":
                msg_type = "sensor.registry.request"
                attributes = {
                        "type": msg_type,
                        "source": "uasdaq.dashboard",
                        "id": str(ULID()),
                        "datacontenttype": "application/json; charset=utf-8",
                }
                reg_request = {"register-sensor-request": "update-sensor-definition-all"}
                ce = CloudEvent(attributes=attributes, data=reg_request)
                try:
                    headers, body = to_structured(ce)
                    async with httpx.AsyncClient() as client:
                        r = await client.post(config.knative_broker, headers=headers, data=body)
                except Exception as e:
                    L.error(f"Error requesting registry update: {e}")
                    
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
        await asyncio.sleep(.1)

@app.websocket("/ws/system-ops/{client_id}")
async def system_ops_ws_endpoint(websocket: WebSocket, client_id: str):
    await manager.connect(websocket, client_type="system-ops", client_id=client_id)
    try:
        while True:
            data = await websocket.receive_text()
            # Loopback broadcast if the UI sends an acknowledgment or command
            await manager.broadcast(f"received: {data}", "system-ops", client_id)
    except WebSocketDisconnect:
        await manager.disconnect(websocket)

# --- HTTP POST ENDPOINTS (Fallback for Datastore/External pushes) ---

@app.post("/sensor/data/update/")
async def sensor_data_update(request: Request):
    data = await request.body()
    try:
        ce = from_http(headers=request.headers, data=data)
        if isinstance(ce.data, str):
            ce.data = json.loads(ce.data)
            
        attributes = ce.data["attributes"]
        make = attributes["make"]["data"]
        model = attributes["model"]["data"]
        serial_number = attributes["serial_number"]["data"]
        sensor_id = "::".join([make, model, serial_number])
        
        msg = {"data-update": ce.data}
        await manager.broadcast(json.dumps(msg), "sensor", sensor_id)
    except Exception:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@app.post("/sensor/settings/update/")
async def sensor_settings_update(request: Request):
    data = await request.body()
    try:
        ce = from_http(headers=request.headers, data=data)
        if isinstance(ce.data, str):
            ce.data = json.loads(ce.data)
            
        attributes = ce.data["attributes"]
        make = attributes["make"]["data"]
        model = attributes["model"]["data"]
        serial_number = attributes["serial_number"]["data"]
        sensor_id = "::".join([make, model, serial_number])

        msg = {"settings-update": ce.data}
        await manager.broadcast(json.dumps(msg), "sensor", sensor_id)
    except Exception:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@app.post("/controller/data/update/")
async def controller_data_update(request: Request):
    data = await request.body()
    try:
        ce = from_http(headers=request.headers, data=data)
        if isinstance(ce.data, str):
            ce.data = json.loads(ce.data)
            
        attributes = ce.data["attributes"]
        make = attributes["make"]["data"]
        model = attributes["model"]["data"]
        serial_number = attributes["serial_number"]["data"]
        controller_id = "::".join([make, model, serial_number])

        msg = {"data-update": ce.data}
        await manager.broadcast(json.dumps(msg), "controller", controller_id)
    except Exception:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@app.post("/controller/settings/update/")
async def controller_settings_update(request: Request):
    data = await request.body()
    try:
        ce = from_http(headers=request.headers, data=data)
        if isinstance(ce.data, str):
            ce.data = json.loads(ce.data)
            
        attributes = ce.data["attributes"]
        make = attributes["make"]["data"]
        model = attributes["model"]["data"]
        serial_number = attributes["serial_number"]["data"]
        controller_id = "::".join([make, model, serial_number])

        msg = {"settings-update": ce.data}
        await manager.broadcast(json.dumps(msg), "controller", controller_id)
    except Exception:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@app.post("/variableset/data/update/")
async def variableset_data_update(request: Request):
    data = await request.body()
    try:
        ce = from_http(headers=request.headers, data=data)
        if isinstance(ce.data, str):
            ce.data = json.loads(ce.data)
            
        variableset_id = ce.data["variableset_id"]
        msg = {"data-update": ce.data}
        await manager.broadcast(json.dumps(msg), "variableset", variableset_id)
    except Exception:
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@app.websocket("/ws/platform/{client_id}")
async def platform_ws_endpoint(websocket: WebSocket, client_id: str):
    await manager.connect(websocket, client_type="platform", client_id=client_id)
    try:
        while True:
            await websocket.receive_text() # Just keep the pipe open
    except WebSocketDisconnect:
        await manager.disconnect(websocket)
