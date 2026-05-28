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

# --- LOGGING ---
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger("EnvOps-Main")
L.setLevel(logging.DEBUG)

# --- CONFIG ---
class Settings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8080
    debug: bool = False
    daq_id: str = "default"

    mqtt_broker: str = "mosquitto.default"
    mqtt_port: int = 1883
    # Subscribe to relevant telemetry, status, and variableset updates
    mqtt_topic_subscriptions: str = "envds/+/+/+/data/#,envds/+/+/+/status/#"
    mqtt_client_id: str = Field(default_factory=lambda: f"envops-dash-{str(ULID())}")

    class Config:
        env_prefix = "ENVOPS_"
        case_sensitive = False

config = Settings()

# --- CONNECTION MANAGER ---
class ConnectionManager:
    """Manages granular WebSocket connections for Dash drill-down pages."""
    def __init__(self):
        # We store connections grouped by type (e.g., 'deployment', 'variableset', 'sensor')
        # and then by their specific ID.
        self.active_connections: dict[str, dict[str, list[WebSocket]]] = {
            "deployment_c2": {},
            "deployment_telemetry": {},
            "variableset": {},
            "sensor": {},
            "registry": {}
        }

    async def connect(self, websocket: WebSocket, client_type: str, client_id: str):
        await websocket.accept()
        if client_id not in self.active_connections[client_type]:
            self.active_connections[client_type][client_id] = []
        self.active_connections[client_type][client_id].append(websocket)
        L.debug(f"WS Connected: {client_type}/{client_id}. Total: {len(self.active_connections[client_type][client_id])}")

    def disconnect(self, websocket: WebSocket, client_type: str, client_id: str):
        if client_id in self.active_connections[client_type]:
            self.active_connections[client_type][client_id].remove(websocket)
            if not self.active_connections[client_type][client_id]:
                del self.active_connections[client_type][client_id]
            L.debug(f"WS Disconnected: {client_type}/{client_id}")

    async def broadcast(self, message: str, client_type: str, client_id: str):
        """Send a message strictly to the WebSockets listening to this specific client_id."""
        if client_id in self.active_connections.get(client_type, {}):
            for connection in self.active_connections[client_type][client_id]:
                try:
                    await connection.send_text(message)
                except Exception as e:
                    L.error(f"WS Broadcast error on {client_type}/{client_id}: {e}")

manager = ConnectionManager()
mqtt_publish_queue = asyncio.Queue()

# --- MQTT BACKGROUND TASKS ---
async def mqtt_listen_task():
    """Listens to the broker and routes incoming CloudEvents to the correct WebSockets."""
    reconnect_delay = 5
    while True:
        try:
            L.info(f"Connecting to MQTT Broker: {config.mqtt_broker}:{config.mqtt_port}")
            async with Client(config.mqtt_broker, port=config.mqtt_port, identifier=config.mqtt_client_id) as client:
                
                # Subscribe to required topics
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
                        
                        payload_str = json.dumps({"data": message.payload.decode()})

                        # 1. Route Operations Health (Status Updates) to Deployment C2 WebSockets
                        if "status.update" in ce_type:
                            # You can extract deployment mapping here if needed. 
                            # For now, broadcasting to a general deployment scope or mapping it via source.
                            # Example: broadcast to ALL active deployment C2 dashboards
                            for dep_id in manager.active_connections.get("deployment_c2", {}).keys():
                                await manager.broadcast(payload_str, "deployment_c2", dep_id)

                        # 2. Route Variableset Telemetry to Variableset WebSockets
                        elif "variableset" in ce_type and "data.update" in ce_type:
                            # Extract variableset ID (e.g., 'main', 'met', etc.)
                            vs_id = source.split(".")[-1] 
                            await manager.broadcast(payload_str, "variableset", vs_id)

                        # 3. Route Raw Sensor Telemetry
                        elif "sensor" in ce_type and "data.update" in ce_type:
                            # Depending on exact source string format (e.g., 'envds.default.sensor.make::model::sn')
                            sensor_id = source.split(".")[-1]
                            await manager.broadcast(payload_str, "sensor", sensor_id)

                    except Exception as e:
                        L.error(f"Error processing MQTT message on topic {topic}: {e}")
                        
        except MqttError as e:
            L.error(f"MQTT Connection dropped: {e}. Reconnecting in {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
        except Exception as e:
            L.error(f"Unexpected MQTT listener error: {e}")
            await asyncio.sleep(reconnect_delay)

async def mqtt_publish_task():
    """Takes outbound messages (like C2 requests) from Dash and pushes them to MQTT."""
    reconnect_delay = 5
    client_id = f"envops-pub-{str(ULID())}"
    while True:
        try:
            async with Client(config.mqtt_broker, port=config.mqtt_port, identifier=client_id) as client:
                while True:
                    topic, payload = await mqtt_publish_queue.get()
                    await client.publish(topic, payload, qos=1)
                    mqtt_publish_queue.task_done()
        except MqttError as e:
            L.error(f"MQTT Publisher dropped: {e}. Reconnecting...")
            await asyncio.sleep(reconnect_delay)

# --- APP LIFECYCLE ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Launch background tasks
    task_listen = asyncio.create_task(mqtt_listen_task())
    task_publish = asyncio.create_task(mqtt_publish_task())
    yield
    # Shutdown: Clean up tasks
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
            # Incoming data from the dashboard C2 panel (Auto/Manual request)
            try:
                event = json.loads(data)
                topic = event.get("destpath")
                if topic:
                    # Drop it onto the MQTT publisher queue
                    await mqtt_publish_queue.put((topic, data))
            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        manager.disconnect(websocket, "deployment_c2", deployment_id)

@app.websocket("/ws/variableset/{variableset_id}")
async def ws_variableset(websocket: WebSocket, variableset_id: str):
    await manager.connect(websocket, "variableset", variableset_id)
    try:
        while True:
            await websocket.receive_text() # Mostly listening, but keep socket alive
    except WebSocketDisconnect:
        manager.disconnect(websocket, "variableset", variableset_id)

@app.websocket("/ws/sensor/{sensor_id}")
async def ws_sensor(websocket: WebSocket, sensor_id: str):
    await manager.connect(websocket, "sensor", sensor_id)
    try:
        while True:
            data = await websocket.receive_text()
            # Handle settings/config updates from the raw sensor page
            try:
                event = json.loads(data)
                topic = event.get("destpath")
                if topic:
                    await mqtt_publish_queue.put((topic, data))
            except json.JSONDecodeError:
                pass
    except WebSocketDisconnect:
        manager.disconnect(websocket, "sensor", sensor_id)

# --- MOUNT DASH FRONTEND ---
# Traefik strips `/envds/envops`, so FastAPI mounts this at the root.
app.mount("/", WSGIMiddleware(dash_app.server))

if __name__ == "__main__":
    import uvicorn
    # When running locally without Docker
    uvicorn.run("main:app", host=config.host, port=config.port, log_level="info")