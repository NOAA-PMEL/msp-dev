import os
import asyncio
import zlib
import logging
import paho.mqtt.client as mqtt
from logfmter import Logfmter
from pydantic import BaseSettings, Field
from ulid import ULID
import time
from aiomqtt import Client, MqttError
import paho.mqtt.client as mqtt
import uvicorn
from fastapi import FastAPI, Request, Response, status

# High-performance software cipher for ARM/Raspberry Pi (RFC 8439)
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305

# CloudEvents
from cloudevents.http import from_http, from_json
from cloudevents.conversion import to_json
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes

# --- CONFIGURATION ---
class ProxySettings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8081
    daq_id: str = "default"
    log_level: str = "INFO"  # Use "DEBUG" to see compression metrics!
    
    mqtt_broker: str = "mosquitto.default"
    mqtt_port: int = 1883
    mqtt_client_id: str = Field(default_factory=lambda: f"proxy-{str(ULID())}")
    
    # Comma-separated list of local topics to intercept and compress
    local_intercept_topics: str = "envds/+/+/+/data/update,envds/+/+/+/status/update,envds/+/+/+/settings/update"
    
    # The dedicated bridge topics communicating with AWS IoT Core
    bridge_topic_out: str = "envds/transport/compressed/edge-to-cloud"
    bridge_topic_in: str = "envds/transport/compressed/cloud-to-edge"
    
    # Must be 32-bytes. Deployed via SealedSecrets
    aes_encryption_key: str = "" 

    class Config:
        env_prefix = "PROXY_"
        case_sensitive = False

config = ProxySettings()

# --- LOGGING ---
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger("TelemetryProxy")
L.setLevel(getattr(logging, config.log_level.upper(), logging.INFO))


# --- FASTAPI SETUP ---
app = FastAPI()


# --- PROXY SERVICE ---
class TelemetryProxyClient:
    def __init__(self, cfg: ProxySettings):
        self.config = cfg
        
        # Initialize ChaCha20-Poly1305 (Requires exact 32-byte key)
        key_bytes = self.config.aes_encryption_key.encode('utf-8')[:32].ljust(32, b'\0')
        self.cipher = ChaCha20Poly1305(key_bytes)
        
        # Async worker queues for non-blocking I/O
        self.outbound_queue = asyncio.Queue(maxsize=2000)
        self.inbound_queue = asyncio.Queue(maxsize=2000)

        self.start_time = time.time()
        self.total_outbound_bytes = 0
        self.total_raw_bytes = 0

    async def setup(self):
        """Starts background task orchestration."""
        asyncio.create_task(self.mqtt_loop())

    async def mqtt_loop(self):
        """Maintains the core Mosquitto connection and worker threads."""
        reconnect = 5
        while True:
            try:
                # Use explicit paho-mqtt constant for v5 protocol
                async with Client(
                    self.config.mqtt_broker, 
                    port=self.config.mqtt_port, 
                    identifier=self.config.mqtt_client_id, 
                    protocol=mqtt.MQTTv5
                ) as client:
                    L.info("MQTT Connected. Starting Proxy pipelines.")
                    
                    # Start async workers
                    publish_task = asyncio.create_task(self.publisher_worker(client))
                    subscribe_task = asyncio.create_task(self.subscriber_worker(client))
                    inbound_processor_task = asyncio.create_task(self.inbound_processor_worker(client))
                    
                    await asyncio.gather(publish_task, subscribe_task, inbound_processor_task)
                    
            except MqttError as e:
                L.error(f"MQTT Error: {e}. Reconnecting in {reconnect}s...")
                await asyncio.sleep(reconnect)
            except Exception as e:
                L.error(f"Fatal error in mqtt_loop: {e}")
                await asyncio.sleep(reconnect)

    async def subscriber_worker(self, client):
        """Listens for raw local messages AND incoming encrypted bridge messages."""
        # Subscribe to local topics via Shared Subscriptions for horizontal scaling
        for topic in self.config.local_intercept_topics.split(","):
            if topic.strip(): 
                await client.subscribe(f"$share/proxy_group/{topic.strip()}", qos=0)
            
        # Subscribe to incoming bridge topic from AWS IoT Core
        if self.config.bridge_topic_in:
            await client.subscribe(self.config.bridge_topic_in, qos=0)

        L.info("Proxy Subscriber listening on configured topics.")

        async for msg in client.messages:
            topic = msg.topic.value
            
            # Route to respective pipelines
            if topic == self.config.bridge_topic_in:
                await self.inbound_queue.put(msg)
            else:
                await self.process_outbound(original_topic=topic, raw_payload=msg.payload)

    async def process_outbound(self, original_topic: str, raw_payload: bytes):
        """Compresses, encrypts, and wraps outgoing telemetry into a Binary CE."""
        try:
            size_in = len(raw_payload)
            if size_in == 0: return

            # 1. Compress
            compressed_bytes = zlib.compress(raw_payload, level=9)
            
            # 2. Encrypt (ChaCha20 requires a 12-byte Nonce per message)
            nonce = os.urandom(12)
            ciphertext = self.cipher.encrypt(nonce, compressed_bytes, associated_data=None)
            
            # 3. Prepend the nonce to the ciphertext for the receiving proxy
            final_payload = nonce + ciphertext
            size_out = len(final_payload)
            
            # ---> ADD METRICS CALCULATION HERE <---
            self.total_outbound_bytes += size_out
            self.total_raw_bytes += size_in
            elapsed_hours = (time.time() - self.start_time) / 3600.0
            
            # Prevent divide-by-zero on the very first packet
            if elapsed_hours > 0:
                # Convert bytes to MB, then divide by hours
                mb_per_hour = (self.total_outbound_bytes / (1024 * 1024)) / elapsed_hours
                raw_mb_per_hour = (self.total_raw_bytes / (1024 * 1024)) / elapsed_hours
            else:
                mb_per_hour = 0.0
                raw_mb_per_hour = 0.0
            # --------------------------------------

            # 4. Metrics Logging (Only visible if PROXY_LOG_LEVEL=DEBUG)
            reduction = (1 - (size_out / size_in)) * 100
            L.debug("compression_stats", extra={
                "direction": "outbound",
                "topic": original_topic,
                "bytes_in": size_in, 
                "bytes_out": size_out, 
                "reduction_pct": round(reduction, 1),
                "est_mb_per_hr": round(mb_per_hour, 4),
                "est_raw_mb_per_hr": round(raw_mb_per_hour, 4)  # <--- NEW LOG OUTPUT
            })

            # 5. Build Binary CloudEvent MQTT v5 Headers
            props = Properties(PacketTypes.PUBLISH)
            props.UserProperty = [
                ("ce-specversion", "1.0"),
                ("ce-id", str(ULID())),
                ("ce-type", "envds.transport.compressed"),
                ("ce-source", f"envds.{self.config.daq_id}.proxy"),
                ("ce-originaltopic", original_topic)
            ]
            props.ContentType = "application/octet-stream"

            # 6. Queue for the publisher worker
            await self.outbound_queue.put((self.config.bridge_topic_out, final_payload, props))

        except Exception as e:
            L.error(f"Outbound proxy error: {e}")

    async def inbound_processor_worker(self, client):
        """Unpacks and decrypts incoming bridge messages and routes them locally."""
        while True:
            msg = await self.inbound_queue.get()
            try:
                # 1. Read Original Topic from MQTT v5 Headers
                props = msg.properties
                user_props = getattr(props, "UserProperty", [])
                original_topic = next((v for k, v in user_props if k == "ce-originaltopic"), None)
                
                if not original_topic:
                    L.warning("Incoming compressed packet missing 'ce-originaltopic' header. Dropping.")
                    continue

                # 2. Extract Nonce and Ciphertext
                incoming_payload = msg.payload
                size_in = len(incoming_payload)
                nonce = incoming_payload[:12]
                ciphertext = incoming_payload[12:]
                
                # 3. Decrypt & Decompress
                compressed_bytes = self.cipher.decrypt(nonce, ciphertext, associated_data=None)
                original_json_bytes = zlib.decompress(compressed_bytes)
                size_out = len(original_json_bytes)
                
                # 4. Metrics Logging
                reduction = (1 - (size_in / size_out)) * 100 if size_out > 0 else 0
                L.debug("compression_stats", extra={
                    "direction": "inbound",
                    "topic": original_topic,
                    "bytes_in": size_in, 
                    "bytes_out": size_out, 
                    "original_reduction_pct": round(reduction, 1)
                })

                # 5. Publish back to the local broker at QoS 0
                await client.publish(original_topic, payload=original_json_bytes, qos=0)

            except Exception as e:
                L.error(f"Inbound unpack error: {e}")
            finally:
                self.inbound_queue.task_done()

    async def publisher_worker(self, client):
        """Publishes processed messages to the Mosquitto bridge."""
        while True:
            bridge_topic, payload, props = await self.outbound_queue.get()
            try:
                # Enforce QoS 0: Fire and forget to avoid staleness/backlogs on Starlink
                await client.publish(bridge_topic, payload=payload, qos=0, properties=props)
            except MqttError as e:
                L.error(f"Publish failed: {e}")
                raise e
            except Exception as e:
                L.error(f"Unexpected publisher error: {e}")
            finally:
                self.outbound_queue.task_done()


# --- HTTP API ---
proxy = None

@app.on_event("startup")
async def startup_event():
    global proxy
    proxy = TelemetryProxyClient(config)
    await proxy.setup()
    L.info("Secure Telemetry Proxy initialized.")

@app.get("/health")
def health_check():
    return Response(status_code=status.HTTP_200_OK)

@app.post("/events/inbound")
async def http_event_ingest(request: Request):
    """
    Allows Knative Triggers or external systems to POST CloudEvents via HTTP.
    The Proxy will intercept them, compress them, and ship them over the bridge.
    """
    try:
        # Parse CloudEvent from HTTP request
        ce = from_http(request.headers, await request.body())
        
        # Determine the routing destination 
        destpath = ce.get("destpath")
        if not destpath:
            L.warning("HTTP Ingest dropped: CloudEvent missing 'destpath' attribute.")
            return Response(status_code=status.HTTP_204_NO_CONTENT)

        # Convert back to raw JSON bytes and inject into the outbound pipeline
        raw_payload_bytes = to_json(ce)
        await proxy.process_outbound(original_topic=destpath, raw_payload=raw_payload_bytes)
        
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    except Exception as e:
        L.error(f"HTTP Ingest error: {e}")
        return Response(status_code=status.HTTP_400_BAD_REQUEST)


if __name__ == "__main__":
    uvicorn.run("telemetry_proxy:app", host=config.host, port=config.port)