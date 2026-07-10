import os
import asyncio
import zlib
import logging
import concurrent.futures
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
    log_level: str = "INFO"  # Configured to INFO for max performance on CM4
    
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
        
        # Bounded async worker queues for non-blocking I/O load shedding
        self.outbound_queue = asyncio.Queue(maxsize=100)
        self.inbound_queue = asyncio.Queue(maxsize=100)

        self.start_time = time.time()
        self.total_outbound_bytes = 0
        self.total_raw_bytes = 0

    async def setup(self):
        """Starts background task orchestration."""
        
        # --- THE FIX: Constrain the default thread pool for the CM4 ---
        # 3 workers leaves 1 core entirely free for the asyncio network loop
        loop = asyncio.get_running_loop()
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)
        loop.set_default_executor(executor)
        # --------------------------------------------------------------
        
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
                try:
                    # Non-blocking injection
                    self.inbound_queue.put_nowait(msg)
                except asyncio.QueueFull:
                    # Drop oldest inbound message to prevent lag build-up
                    try:
                        self.inbound_queue.get_nowait()
                        self.inbound_queue.task_done()
                    except asyncio.QueueEmpty:
                        pass
                    await self.inbound_queue.put(msg)
            else:
                # --- THE ARCHITECTURE FIX: Task-offload the outbound processor ---
                # Do NOT 'await' this inside the loop; fire it as a concurrent background task
                # so the MQTT reader loop never stops pulling packets off the network.
                asyncio.create_task(self.process_outbound(original_topic=topic, raw_payload=msg.payload))

    def synchronous_compress_and_encrypt(self, raw_payload: bytes):
        """Helper to run CPU-heavy compression/crypto inside a thread pool."""
        # Level 6 provides maximum efficiency without the Level 9 CPU penalty
        compressed_bytes = zlib.compress(raw_payload, level=6)
        nonce = os.urandom(12)
        ciphertext = self.cipher.encrypt(nonce, compressed_bytes, associated_data=None)
        return nonce + ciphertext

    async def process_outbound(self, original_topic: str, raw_payload: bytes):
        """Processes and queues outgoing telemetry concurrently."""
        try:
            size_in = len(raw_payload)
            if size_in == 0: return

            t_start = time.perf_counter()

            # --- THE ARCHITECTURE FIX: Offload CPU-heavy work to an isolated thread pool ---
            final_payload = await asyncio.to_thread(self.synchronous_compress_and_encrypt, raw_payload)
            
            t_elapsed_ms = (time.perf_counter() - t_start) * 1000.0
            
            # Useful warning if the CM4 is under severe thermal throttling or load
            if t_elapsed_ms > 50.0:
                L.warning(f"Heavy compression task took {t_elapsed_ms:.2f}ms for '{original_topic}'. (Safely offloaded from main loop)")

            size_out = len(final_payload)
            self.total_outbound_bytes += size_out
            self.total_raw_bytes += size_in
            elapsed_hours = (time.time() - self.start_time) / 3600.0
            
            if elapsed_hours > 0:
                mb_per_hour = (self.total_outbound_bytes / (1024 * 1024)) / elapsed_hours
                raw_mb_per_hour = (self.total_raw_bytes / (1024 * 1024)) / elapsed_hours
            else:
                mb_per_hour = 0.0
                raw_mb_per_hour = 0.0

            reduction = (1 - (size_out / size_in)) * 100 if size_in > 0 else 0
            
            # This is perfectly safe: Python bypasses string formatting entirely when LogLevel is INFO
            L.debug("compression_stats", extra={
                "direction": "outbound", "topic": original_topic,
                "bytes_in": size_in, "bytes_out": size_out, "reduction_pct": round(reduction, 1),
                "est_mb_per_hr": round(mb_per_hour, 4), "est_raw_mb_per_hr": round(raw_mb_per_hour, 4)
            })

            props = Properties(PacketTypes.PUBLISH)
            props.UserProperty = [
                ("ce-specversion", "1.0"), ("ce-id", str(ULID())),
                ("ce-type", "envds.transport.compressed"), ("ce-source", f"envds.{self.config.daq_id}.proxy"),
                ("ce-originaltopic", original_topic)
            ]
            props.ContentType = "application/octet-stream"

            # --- THE ARCHITECTURE FIX: Non-Blocking Load Shedding ---
            try:
                self.outbound_queue.put_nowait((self.config.bridge_topic_out, final_payload, props))
            except asyncio.QueueFull:
                # Shed load: Discard the oldest stale item in the queue to write the newest real-time location
                try:
                    self.outbound_queue.get_nowait()
                    self.outbound_queue.task_done()
                    L.warning("Outbound queue full. Shedding oldest telemetry packet to avoid network lag.")
                except asyncio.QueueEmpty:
                    pass
                # Inject the fresh frame
                self.outbound_queue.put_nowait((self.config.bridge_topic_out, final_payload, props))

        except Exception as e:
            L.error(f"Outbound proxy error: {e}")

    async def inbound_processor_worker(self, client):
        """Unpacks and decrypts incoming bridge messages and routes them locally."""
        while True:
            msg = await self.inbound_queue.get()
            try:
                # 1. Read Headers from MQTT v5 User Properties
                props = msg.properties
                user_props = getattr(props, "UserProperty", [])
                
                original_topic = next((v for k, v in user_props if k == "ce-originaltopic"), None)
                ce_type = next((v for k, v in user_props if k == "ce-type"), "unknown")
                ce_source = next((v for k, v in user_props if k == "ce-source"), "unknown")
                
                if not original_topic:
                    L.warning("Incoming compressed packet missing 'ce-originaltopic' header. Dropping.", extra={
                        "ce_type": ce_type,
                        "ce_source": ce_source
                    })
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
                
                # 4. Metrics & Routing Debug Logging
                reduction = (1 - (size_in / size_out)) * 100 if size_out > 0 else 0
                
                # Use L.info so it shows up even if you aren't in DEBUG mode
                L.info("Routing inbound telemetry", extra={
                    "original_topic": original_topic,
                    "ce_type": ce_type,
                    "ce_source": ce_source,
                    "bytes_in": size_in,
                    "bytes_out": size_out
                })
                
                # If you want the full compression stats only on DEBUG:
                L.debug("compression_stats", extra={
                    "direction": "inbound",
                    "topic": original_topic,
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