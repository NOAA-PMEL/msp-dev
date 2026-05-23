import os
import asyncio
import json
import logging
from logfmter import Logfmter
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.wsgi import WSGIMiddleware
from pydantic import BaseSettings
from aiomqtt import Client, MqttError
from ulid import ULID

# Import the Dash app instance
from envops_app import app as dash_app

# 1. Read environmental visibility configurations
LOG_LEVEL = os.getenv("ENVOPS_LOG_LEVEL", "INFO").upper()

# 2. Bind the logfmt handler to standard out
handler = logging.StreamHandler()
handler.setFormatter(Logfmter(
    keys=["at", "logger", "msg"], 
    mapping={"at": "levelname", "logger": "name"}
))

logging.basicConfig(
    level=LOG_LEVEL,
    handlers=[handler]
)
L = logging.getLogger(__name__)
L.setLevel(LOG_LEVEL)

class EnvOpsSettings(BaseSettings):
    daq_id: str = "default"
    external_hostname: str = "localhost"
    port: int = 8080
    ws_port: int = 8080
    ws_use_tls: bool = False

    mqtt_broker: str = "mosquitto.default"
    mqtt_port: int = 1883
    # Subscribe to status events emitted by the sampling-system managers
    mqtt_topics: str = "envds/+/+/status/#" 
    
    class Config:
        env_prefix = "ENVOPS_"
        case_sensitive = False

config = EnvOpsSettings()

# -----------------------------------------------------------------------------
# WebSocket Connection Manager
# -----------------------------------------------------------------------------
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception as e:
                L.error(f"Failed to send message to websocket: {e}")
                self.disconnect(connection)

manager = ConnectionManager()

# -----------------------------------------------------------------------------
# MQTT Background Task
# -----------------------------------------------------------------------------
async def mqtt_to_websocket_bridge():
    """Listens to MQTT status topics and broadcasts payloads to all connected WS clients."""
    reconnect_delay = 5
    client_id = f"envops-dashboard-{ULID()}"
    
    while True:
        try:
            L.info(f"Connecting to MQTT Broker at {config.mqtt_broker}:{config.mqtt_port}")
            async with Client(config.mqtt_broker, port=config.mqtt_port, identifier=client_id) as client:
                
                # Subscribe to the topics our sampling managers are publishing to
                topics = config.mqtt_topics.split(",")
                for topic in topics:
                    await client.subscribe(topic.strip())
                    L.info(f"Subscribed to {topic.strip()}")

                async for message in client.messages:
                    try:
                        payload = message.payload.decode("utf-8")
                        # You can inject the topic if your frontend needs it for routing
                        L.debug(
                            "Processing incoming telemetry packet", 
                            extra={
                                "mqtt_topic": message.topic.value,
                                "packet_len": len(payload)
                            }
                        )
                        ws_payload = json.dumps({
                            "topic": message.topic.value,
                            "data": json.loads(payload)
                        })
                        await manager.broadcast(ws_payload)
                    except Exception as e:
                        L.error(f"Error processing MQTT message: {e}")
                        
        except MqttError as e:
            L.error(f"MQTT Connection lost: {e}. Reconnecting in {reconnect_delay}s...")
            await asyncio.sleep(reconnect_delay)
        except asyncio.CancelledError:
            L.info("MQTT bridge task cancelled.")
            break
        except Exception as e:
            L.error(f"Unexpected MQTT bridge error: {e}")
            await asyncio.sleep(reconnect_delay)

# -----------------------------------------------------------------------------
# FastAPI Application & Lifespan
# -----------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Start the MQTT bridge task
    mqtt_task = asyncio.create_task(mqtt_to_websocket_bridge())
    yield
    # Shutdown: Cancel the task
    mqtt_task.cancel()
    try:
        await mqtt_task
    except asyncio.CancelledError:
        pass

app = FastAPI(title="EnvOps API", lifespan=lifespan)

# WebSocket Endpoint (Matches the URL requested by the Dash frontend)
@app.websocket("/ws/system-ops/{client_id}")
async def websocket_endpoint(websocket: WebSocket, client_id: str):
    await manager.connect(websocket)
    try:
        while True:
            # We are primarily broadcasting TO the client, but we must keep the connection open
            data = await websocket.receive_text()
            L.debug(f"Received from client {client_id}: {data}")
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        L.info(f"Client #{client_id} disconnected")

# Mount the Dash app inside FastAPI (Catch-all for UI routes)
app.mount("/", WSGIMiddleware(dash_app.server))

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)