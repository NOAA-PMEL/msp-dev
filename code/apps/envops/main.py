import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, status, Response
from fastapi.middleware.wsgi import WSGIMiddleware
import uvicorn

from aiomqtt import Client, MqttError
from ulid import ULID

# --- ENVDS-BASE IMPORTS ---
from cloudevents.http import from_json, from_http
from cloudevents.conversion import to_json
from logfmter import Logfmter

# Import native envds event factories
from envds.event.event import envdsEvent
from envds.daq.event import DAQEvent
from envds.sampling.event import SamplingEvent

# Import the EnvOps Dash app
from envops_app import dash_app

# --- LOGGING SETUP ---
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.DEBUG)

# --- CONFIGURATION ---
DAQ_ID = os.environ.get("DASHBOARD_DAQ_ID", "default")
MQTT_BROKER = os.environ.get("DASHBOARD_MQTT_BROKER", "localhost")
MQTT_PORT = int(os.environ.get("DASHBOARD_MQTT_PORT", 1883))
MQTT_TOPICS_ENV = os.environ.get("DASHBOARD_MQTT_TOPIC_SUBSCRIPTIONS", "envds/#")
MQTT_TOPICS = [t.strip() for t in MQTT_TOPICS_ENV.split(",") if t.strip()]

# Outbound buffer for UI -> System commands
outgoing_mqtt_queue = asyncio.Queue(maxsize=1000)

# --- WEBSOCKET MANAGER ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in list(self.active_connections):
            try:
                await connection.send_text(message)
            except Exception as e:
                L.error(f"WebSocket broadcast error: {e}")
                self.disconnect(connection)

ws_manager = ConnectionManager()

# --- MQTT LISTENER & PUBLISHER ---
async def mqtt_runner():
    """Maintains the MQTT connection, listens for telemetry, and publishes UI commands."""
    reconnect_interval = 5
    client_id = f"envops-dash-{str(ULID())}"
    
    while True:
        try:
            L.info(f"Connecting to MQTT Broker at {MQTT_BROKER}:{MQTT_PORT}...")
            async with Client(hostname=MQTT_BROKER, port=MQTT_PORT, identifier=client_id) as client:
                L.info("Successfully connected to MQTT Broker.")
                
                # Subscribe to topics defined in ConfigMap
                for topic in MQTT_TOPICS:
                    await client.subscribe(f"$share/envops-dashboard/{topic}")
                    L.info(f"Subscribed to topic: {topic}")

                listen_task = asyncio.create_task(listen_to_mqtt(client))
                publish_task = asyncio.create_task(publish_to_mqtt(client))
                
                done, pending = await asyncio.wait(
                    [listen_task, publish_task], 
                    return_when=asyncio.FIRST_EXCEPTION
                )
                
                for task in pending:
                    task.cancel()
                    
        except MqttError as error:
            L.warning(f"MQTT connection lost: {error}. Reconnecting in {reconnect_interval}s...")
            await asyncio.sleep(reconnect_interval)

async def listen_to_mqtt(client: Client):
    """Parses envds MQTT messages and routes them to the Dash UI."""
    async for message in client.messages:
        try:
            topic = message.topic.value
            
            # Use cloudevents from_json parser (as seen in envds architecture)
            ce = from_json(message.payload) 
            
            # Repackage the payload cleanly for the Dash UI
            payload_to_ui = {
                "topic": topic,
                "type": ce.get("type", "unknown"),
                "source": ce.get("source", "unknown"),
                "data": ce.data
            }
            
            await ws_manager.broadcast(json.dumps(payload_to_ui))
            
        except Exception as e:
            L.error(f"Error processing incoming MQTT: {e}")

async def publish_to_mqtt(client: Client):
    """Waits for messages from the UI and publishes them via MQTT using native envds factories."""
    while True:
        try:
            msg_dict = await outgoing_mqtt_queue.get()
            
            destpath = msg_dict.get("destpath", f"envds/{DAQ_ID}/system/default")
            event_type = msg_dict.get("type", "envds.control.request")
            event_data = msg_dict.get("data", {})
            
            # Automatically assign the source if the UI didn't provide one
            source = msg_dict.get("source", f"envds.{DAQ_ID}.envops.dashboard")
            
            # --- USE NATIVE ENVDS FACTORY ---
            # Instead of manually assembling a CloudEvent, use the underlying 
            # envdsEvent.create() which injects the ULID and formats attributes correctly.
            ce = envdsEvent.create(
                type=event_type,
                source=source,
                data=event_data
            )
            
            if ce:
                # Convert using cloudevents helper
                payload_bytes = to_json(ce)
                
                L.debug(f"Publishing UI Command to {destpath}", extra={"type": event_type})
                await client.publish(destpath, payload=payload_bytes, qos=1)
            else:
                L.error("Failed to create envdsEvent payload")
                
            outgoing_mqtt_queue.task_done()
            
        except Exception as e:
            L.error(f"Error publishing outgoing MQTT: {e}")

# --- FASTAPI LIFESPAN & APP ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    mqtt_task = asyncio.create_task(mqtt_runner())
    yield
    mqtt_task.cancel()

app = FastAPI(title="EnvOps Hub API", lifespan=lifespan)

# --- WEBSOCKET ENDPOINT ---
@app.websocket("/ws/telemetry")
async def telemetry_ws(websocket: WebSocket):
    """Global WebSocket connection for the Dash UI."""
    await ws_manager.connect(websocket)
    try:
        while True:
            # The Dash UI sends commands here (e.g. power toggles, mode changes)
            data = await websocket.receive_text()
            try:
                command_payload = json.loads(data)
                await outgoing_mqtt_queue.put(command_payload)
            except json.JSONDecodeError:
                L.error("Invalid JSON command received from UI")
                
    except WebSocketDisconnect:
        ws_manager.disconnect(websocket)

# --- KNATIVE HTTP FALLBACKS ---
@app.post("/sensor/data/update/", status_code=status.HTTP_202_ACCEPTED)
@app.post("/controller/data/update/", status_code=status.HTTP_202_ACCEPTED)
async def http_data_update(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        payload_to_ui = {
            "topic": ce.get("destpath", "http/direct"),
            "type": ce.get("type", "unknown"),
            "source": ce.get("source", "unknown"),
            "data": ce.data
        }
        await ws_manager.broadcast(json.dumps(payload_to_ui))
    except Exception as e:
        L.error("HTTP cloud event parse error", extra={"reason": e})
    return Response(status_code=status.HTTP_204_NO_CONTENT)

# --- MOUNT DASH FRONTEND ---
app.mount("/", WSGIMiddleware(dash_app.server))

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("main:app", host="0.0.0.0", port=port)