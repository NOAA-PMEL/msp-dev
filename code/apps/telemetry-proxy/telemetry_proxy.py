import os
import asyncio
import zlib
import json
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
    log_level: str = "INFO"  
    
    # ---> THE NEW SETTING (Defaults to 2Hz) <---
    transmission_rate_hz: float = 5.0

    mqtt_broker: str = "mosquitto.default"
    mqtt_port: int = 1883
    mqtt_client_id: str = Field(default_factory=lambda: f"proxy-{str(ULID())}")
    
    local_intercept_topics: str = "envds/+/+/+/data/update,envds/+/+/+/status/update,envds/+/+/+/settings/update"
    
    bridge_topic_out: str = "envds/transport/compressed/edge-to-cloud"
    bridge_topic_in: str = "envds/transport/compressed/cloud-to-edge"
    
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
        
        # Initialize ChaCha20-Poly1305
        key_bytes = self.config.aes_encryption_key.encode('utf-8')[:32].ljust(32, b'\0')
        self.cipher = ChaCha20Poly1305(key_bytes)
        
        self.outbound_queue = asyncio.Queue(maxsize=10)
        self.inbound_queue = asyncio.Queue(maxsize=100)
        self.local_batch_queue = asyncio.Queue(maxsize=5000)

        # NEW: Massive secondary queue for preserving delayed data (10,000 batches = ~20MB RAM)
        self.backfill_queue = asyncio.Queue(maxsize=10000)

        self.start_time = time.time()
        self.total_outbound_bytes = 0
        self.total_raw_bytes = 0

    async def setup(self):
        """Starts background task orchestration."""
        loop = asyncio.get_running_loop()
        executor = concurrent.futures.ThreadPoolExecutor(max_workers=3)
        loop.set_default_executor(executor)
        
        asyncio.create_task(self.mqtt_loop())
        asyncio.create_task(self.batch_aggregator_worker())

    async def mqtt_loop(self):
        """Maintains the core Mosquitto connection and worker threads."""
        reconnect = 5
        while True:
            try:
                async with Client(
                    self.config.mqtt_broker, 
                    port=self.config.mqtt_port, 
                    identifier=self.config.mqtt_client_id, 
                    protocol=mqtt.MQTTv5
                ) as client:
                    L.info("MQTT Connected. Starting Proxy pipelines.")
                    
                    publish_tasks = [asyncio.create_task(self.publisher_worker(client, i)) for i in range(3)]
                    subscribe_task = asyncio.create_task(self.subscriber_worker(client))
                    inbound_processor_task = asyncio.create_task(self.inbound_processor_worker(client))
                    backfill_task = asyncio.create_task(self.backfill_publisher_worker(client))
                    
                    await asyncio.gather(*publish_tasks, subscribe_task, inbound_processor_task, backfill_task)
                    
            except MqttError as e:
                L.error(f"MQTT Error: {e}. Reconnecting in {reconnect}s...")
                await asyncio.sleep(reconnect)
            except Exception as e:
                L.error(f"Fatal error in mqtt_loop: {e}")
                await asyncio.sleep(reconnect)

    async def subscriber_worker(self, client):
        """Listens for raw local messages AND incoming encrypted bridge messages."""
        for topic in self.config.local_intercept_topics.split(","):
            if topic.strip(): 
                await client.subscribe(f"$share/proxy_group/{topic.strip()}", qos=0)
            
        if self.config.bridge_topic_in:
            await client.subscribe(self.config.bridge_topic_in, qos=0)

        L.info("Proxy Subscriber listening on configured topics.")

        async for msg in client.messages:
            topic = msg.topic.value
            
            if topic == self.config.bridge_topic_in:
                try:
                    self.inbound_queue.put_nowait(msg)
                except asyncio.QueueFull:
                    try:
                        self.inbound_queue.get_nowait()
                        self.inbound_queue.task_done()
                    except asyncio.QueueEmpty:
                        pass
                    await self.inbound_queue.put(msg)
            else:
                # Instantly pipe to aggregator to prevent blocking the network loop
                # ---> THE FIX: Shed oldest telemetry if queue is full <---
                try:
                    self.local_batch_queue.put_nowait((topic, msg.payload))
                except asyncio.QueueFull:
                    try:
                        self.local_batch_queue.get_nowait()
                        self.local_batch_queue.task_done()
                    except asyncio.QueueEmpty:
                        pass
                    try:
                        self.local_batch_queue.put_nowait((topic, msg.payload))
                    except asyncio.QueueFull:
                        pass

    async def batch_aggregator_worker(self):
        """Aggregates local MQTT messages into a time-windowed batch."""
        MAX_BATCH_BYTES = 128 * 1024 
        FLUSH_INTERVAL = 1.0 / self.config.transmission_rate_hz 
        
        while True:
            await asyncio.sleep(FLUSH_INTERVAL)
            
            batch_items = []
            current_size = 0
            
            while not self.local_batch_queue.empty():
                try:
                    topic, raw_payload = self.local_batch_queue.get_nowait()
                    
                    item_str = f'{{"t":"{topic}","d":{raw_payload.decode("utf-8")}}}'
                    item_size = len(item_str)
                    
                    if batch_items and (current_size + item_size >= MAX_BATCH_BYTES):
                        asyncio.create_task(self.flush_batch(batch_items))
                        batch_items = []
                        current_size = 0
                        
                    batch_items.append(item_str)
                    current_size += item_size
                    self.local_batch_queue.task_done()
                except asyncio.QueueEmpty:
                    break
                    
            if batch_items:
                asyncio.create_task(self.flush_batch(batch_items))

    def synchronous_compress_and_encrypt(self, raw_payload: bytes):
        """Helper to run CPU-heavy compression/crypto inside a thread pool."""
        compressed_bytes = zlib.compress(raw_payload, level=6)
        nonce = os.urandom(12)
        ciphertext = self.cipher.encrypt(nonce, compressed_bytes, associated_data=None)
        return nonce + ciphertext

    async def flush_batch(self, batch_items: list):
        """Compresses and queues outgoing batched telemetry directly on the event loop."""
        try:
            # --- THE FIX: Route shed data to backfill ---
            if self.outbound_queue.full():
                try:
                    stale_topic, stale_payload, stale_props = self.outbound_queue.get_nowait()
                    self.outbound_queue.task_done()
                    L.warning("Outbound queue full. Shifting oldest batch to backfill queue.")
                    try:
                        self.backfill_queue.put_nowait((stale_topic, stale_payload, stale_props))
                    except asyncio.QueueFull:
                        L.error("Backfill queue full. Dropping oldest telemetry permanently.")
                except asyncio.QueueEmpty:
                    pass

            final_json = "[" + ",".join(batch_items) + "]"
            raw_payload = final_json.encode("utf-8")
            
            size_in = len(raw_payload)
            if size_in == 0: return

            t_start = time.perf_counter()
            final_payload = self.synchronous_compress_and_encrypt(raw_payload)
            t_elapsed_ms = (time.perf_counter() - t_start) * 1000.0

            if t_elapsed_ms > 50.0:
                L.warning(f"Heavy batch compression ({len(batch_items)} items) took {t_elapsed_ms:.2f}ms.")

            size_out = len(final_payload)
            self.total_outbound_bytes += size_out
            self.total_raw_bytes += size_in
            
            reduction = (1 - (size_out / size_in)) * 100 if size_in > 0 else 0
            
            L.info("compression_stats", extra={
                "direction": "outbound", "batch_items": len(batch_items),
                "bytes_in": size_in, "bytes_out": size_out, "reduction_pct": round(reduction, 1)
            })

            props = Properties(PacketTypes.PUBLISH)
            props.UserProperty = [
                ("ce-specversion", "1.0"), ("ce-id", str(ULID())),
                ("ce-type", "envds.transport.batch"), ("ce-source", f"envds.{self.config.daq_id}.proxy"),
                ("txratehz", str(self.config.transmission_rate_hz))
            ]
            props.ContentType = "application/octet-stream"

            try:
                self.outbound_queue.put_nowait((self.config.bridge_topic_out, final_payload, props))
            except asyncio.QueueFull:
                pass 

        except Exception as e:
            L.error(f"Outbound proxy error: {e}")

    async def inbound_processor_worker(self, client):
        """Unpacks and decrypts incoming bridge messages and routes them locally without artificial delays."""
        while True:
            msg = await self.inbound_queue.get()
            try:
                props = msg.properties
                user_props = getattr(props, "UserProperty", [])
                
                original_topic = next((v for k, v in user_props if k == "ce-originaltopic"), None)
                
                incoming_payload = msg.payload
                size_in = len(incoming_payload)
                nonce = incoming_payload[:12]
                ciphertext = incoming_payload[12:]
                
                compressed_bytes = self.cipher.decrypt(nonce, ciphertext, associated_data=None)
                original_json_bytes = zlib.decompress(compressed_bytes)
                size_out = len(original_json_bytes)
                
                reduction = (1 - (size_in / size_out)) * 100 if size_out > 0 else 0
                
                batch = json.loads(original_json_bytes)
                
                if isinstance(batch, list):
                    batch_size = len(batch)
                    L.info(f"Routing inbound batch of {batch_size} items.", extra={"reduction_pct": round(reduction, 1)})
                    
                    # Burst instantly. Do not sleep between items.
                    for item in batch:
                        await client.publish(item["t"], payload=json.dumps(item["d"]).encode('utf-8'), qos=0)
                else:
                    if original_topic:
                        await client.publish(original_topic, payload=original_json_bytes, qos=0)

            except Exception as e:
                L.error(f"Inbound unpack error: {e}")
            finally:
                self.inbound_queue.task_done()

    async def publisher_worker(self, client, worker_id: int = 0):
        """Publishes processed messages to the Mosquitto bridge."""
        while True:
            bridge_topic, payload, props = await self.outbound_queue.get()
            try:
                # Enforce QoS 0: Wrapped in a timeout to aggressively drop delayed packets
                await asyncio.wait_for(
                    client.publish(bridge_topic, payload=payload, qos=0, properties=props),
                    timeout=1.0
                )
            except asyncio.TimeoutError:
                L.warning(f"Publisher {worker_id} timed out. Shedding packet due to network backpressure.")
            except MqttError as e:
                L.error(f"Publish failed: {e}")
            except Exception as e:
                L.error(f"Unexpected publisher error: {e}")
            finally:
                self.outbound_queue.task_done()

    async def backfill_publisher_worker(self, client):
        """Publishes delayed historical telemetry only when the real-time queue is empty."""
        while True:
            bridge_topic, payload, props = await self.backfill_queue.get()
            try:
                # Yield to live data: Block execution as long as there is real-time traffic waiting
                while not self.outbound_queue.empty():
                    await asyncio.sleep(0.2)

                # Attempt to transmit the historical batch
                await asyncio.wait_for(
                    client.publish(bridge_topic, payload=payload, qos=0, properties=props),
                    timeout=1.0
                )
            except asyncio.TimeoutError:
                L.warning("Backfill publish timed out due to backpressure. Packet dropped.")
            except MqttError as e:
                L.error(f"Backfill publish failed: {e}")
            except Exception as e:
                L.error(f"Unexpected backfill publisher error: {e}")
            finally:
                self.backfill_queue.task_done()

    async def flush_batch(self, batch_items: list):
        """Compresses and queues outgoing batched telemetry directly on the event loop."""
        try:
            # --- THE FIX: Route shed data to backfill ---
            if self.outbound_queue.full():
                try:
                    stale_topic, stale_payload, stale_props = self.outbound_queue.get_nowait()
                    self.outbound_queue.task_done()
                    L.warning("Outbound queue full. Shifting oldest batch to backfill queue.")
                    try:
                        self.backfill_queue.put_nowait((stale_topic, stale_payload, stale_props))
                    except asyncio.QueueFull:
                        L.error("Backfill queue full. Dropping oldest telemetry permanently.")
                except asyncio.QueueEmpty:
                    pass

            final_json = "[" + ",".join(batch_items) + "]"
            raw_payload = final_json.encode("utf-8")
            
            size_in = len(raw_payload)
            if size_in == 0: return

            t_start = time.perf_counter()
            # Run inline to avoid thread pool exhaustion
            final_payload = self.synchronous_compress_and_encrypt(raw_payload)
            t_elapsed_ms = (time.perf_counter() - t_start) * 1000.0

            if t_elapsed_ms > 50.0:
                L.warning(f"Heavy batch compression ({len(batch_items)} items) took {t_elapsed_ms:.2f}ms.")

            size_out = len(final_payload)
            self.total_outbound_bytes += size_out
            self.total_raw_bytes += size_in
            
            reduction = (1 - (size_out / size_in)) * 100 if size_in > 0 else 0
            
            L.info("compression_stats", extra={
                "direction": "outbound", "batch_items": len(batch_items),
                "bytes_in": size_in, "bytes_out": size_out, "reduction_pct": round(reduction, 1)
            })

            props = Properties(PacketTypes.PUBLISH)
            props.UserProperty = [
                ("ce-specversion", "1.0"), ("ce-id", str(ULID())),
                ("ce-type", "envds.transport.batch"), ("ce-source", f"envds.{self.config.daq_id}.proxy"),
                ("txratehz", str(self.config.transmission_rate_hz))
            ]
            props.ContentType = "application/octet-stream"

            try:
                self.outbound_queue.put_nowait((self.config.bridge_topic_out, final_payload, props))
            except asyncio.QueueFull:
                pass 

        except Exception as e:
            L.error(f"Outbound proxy error: {e}")
                
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
    """
    try:
        ce = from_http(request.headers, await request.body())
        destpath = ce.get("destpath")
        if not destpath:
            return Response(status_code=status.HTTP_204_NO_CONTENT)

        # Inject straight into the batch queue so HTTP events get aggregated natively
        raw_payload_bytes = to_json(ce)
        await proxy.local_batch_queue.put((destpath, raw_payload_bytes))
        
        return Response(status_code=status.HTTP_204_NO_CONTENT)

    except Exception as e:
        L.error(f"HTTP Ingest error: {e}")
        return Response(status_code=status.HTTP_400_BAD_REQUEST)

if __name__ == "__main__":
    uvicorn.run("telemetry_proxy:app", host=config.host, port=config.port)