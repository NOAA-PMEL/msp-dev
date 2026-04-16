import asyncio
import base64
import json
import logging
import os
import sys
import ssl
from typing import Dict, Optional, Tuple, Any
from ulid import ULID
from pathlib import Path

from logfmter import Logfmter
from pydantic import BaseSettings, BaseModel, Field
from cloudevents import ValidationError
from cloudevents.http import CloudEvent, from_json
from aiomqtt import Client, MqttError
from envds.core import envdsBase
import uvicorn

# Set up logging using the established format
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.INFO)

# --- 1. Data Models for Chunking ---

class ChunkPayload(BaseModel):
    """File metadata sent inside the CloudEvent 'data' field."""
    file_id: str
    file_name: str
    chunk_number: int
    total_chunks: int
    data: str  # Base64-encoded chunk data

class FileTransferState:
    """Internal state tracker for a single file assembly."""
    def __init__(self, total_chunks: int, output_path: str):
        self.total_chunks = total_chunks
        self.output_path = output_path
        self.chunks: Dict[int, bytes] = {}
        self.last_activity = asyncio.get_event_loop().time()

# --- 2. Async File Assembler ---

class AsyncFileAssembler:
    """Manages chunk storage and reassembly using asyncio locks."""
    def __init__(self, stale_timeout: int = 300):
        self.stale_timeout = stale_timeout
        self.transfers: Dict[str, FileTransferState] = {}
        self._lock = asyncio.Lock()

    async def cleanup_stale_transfers(self):
        """Asynchronously purges transfers with no recent activity."""
        while True:
            await asyncio.sleep(60)
            now = asyncio.get_event_loop().time()
            async with self._lock:
                stale_ids = [
                    fid for fid, state in self.transfers.items() 
                    if now - state.last_activity > self.stale_timeout
                ]
                for fid in stale_ids:
                    L.warning("Removing stale transfer", extra={"file_id": fid})
                    del self.transfers[fid]

    async def process_chunk(self, ce: CloudEvent, output_dir: str) -> Tuple[Optional[ChunkPayload], bool, Optional[str]]:
        """Parses the CloudEvent and attempts to assemble the file."""
        try:
            data = ce.data
            chunk_info = ChunkPayload(**data) if isinstance(data, dict) else ChunkPayload.parse_raw(data)
            chunk_bytes = base64.b64decode(chunk_info.data)
        except (ValidationError, Exception) as e:
            L.error("Failed to parse chunk CloudEvent", extra={"reason": e})
            return None, False, None

        async with self._lock:
            state = self.transfers.get(chunk_info.file_id)
            if not state:
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir, exist_ok=True)
                
                output_path = os.path.join(output_dir, chunk_info.file_name)
                state = FileTransferState(chunk_info.total_chunks, output_path)
                self.transfers[chunk_info.file_id] = state

            if chunk_info.chunk_number not in state.chunks:
                state.chunks[chunk_info.chunk_number] = chunk_bytes
                state.last_activity = asyncio.get_event_loop().time()
                L.debug("Received chunk", extra={"file_id": chunk_info.file_id, "chunk": chunk_info.chunk_number})

            if len(state.chunks) == state.total_chunks:
                try:
                    with open(state.output_path, "wb") as f:
                        for i in range(1, state.total_chunks + 1):
                            f.write(state.chunks[i])
                    del self.transfers[chunk_info.file_id]
                    L.info("File reassembled", extra={"path": state.output_path})
                    return chunk_info, True, state.output_path
                except IOError as e:
                    L.error("File write failed", extra={"reason": e})
            
            return chunk_info, False, None

# --- 3. Main Filemanager Logic ---

class FilemanagerConfig(BaseSettings):
    """Configuration settings for the chunked file manager."""
    host: str = "0.0.0.0"
    port: int = 8080
    debug: bool = True
    data_base_path: str = "/data/files"
    
    # AWS IoT Connection Details
    mqtt_broker: str = "iot.pmel-dev.oarcloud.noaa.gov"
    mqtt_port: int = 443  # ALPN x-amzn-mqtt-ca is standard on port 443
    mqtt_topic_subscriptions: str = "envds/+/+/+/data/files"
    mqtt_client_id: str = Field(default_factory=lambda: str(ULID()))
    
    # TLS Certificate Paths
    ca_path: str = "./certs/AmazonRootCA1.pem"
    cert_path: str = "./certs/a-certificate.pem.crt"
    key_path: str = "./certs/b-private.pem.key"

    class Config:
        env_prefix = "CHUNKED_FILEMANAGER_"
        case_sensitive = False

class ChunkedFilemanager:
    """Manages file reassembly from MQTT CloudEvent streams."""
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.config = FilemanagerConfig()
        self.assembler = AsyncFileAssembler()
        self.save_buffer = asyncio.Queue()
        self.client: Optional[Client] = None
        self.ID_DELIM = envdsBase.ID_DELIM

        asyncio.create_task(self.get_from_mqtt_loop())
        asyncio.create_task(self.handle_save_buffer())
        asyncio.create_task(self.assembler.cleanup_stale_transfers())

    async def get_from_mqtt_loop(self):
        """Native asyncio MQTT loop using aiomqtt with TLS ALPN authentication."""
        reconnect = 10
        
        # Configure TLS Context for AWS IoT Core with ALPN
        try:
            tls_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=self.config.ca_path)
            tls_context.load_cert_chain(certfile=self.config.cert_path, keyfile=self.config.key_path)
            tls_context.verify_mode = ssl.CERT_REQUIRED
            # Set ALPN protocols for AWS IoT Core
            tls_context.set_alpn_protocols(["x-amzn-mqtt-ca"])
        except Exception as e:
            L.error("Failed to initialize TLS context with ALPN", extra={"reason": e})
            return

        while True:
            try:
                async with Client(
                    hostname=self.config.mqtt_broker,
                    port=self.config.mqtt_port,
                    identifier=str(ULID()),
                    tls_context=tls_context
                ) as self.client:
                    for topic in self.config.mqtt_topic_subscriptions.split(","):
                        if topic.strip():
                            await self.client.subscribe(f"$share/datastore/{topic.strip()}")

                    async for message in self.client.messages:
                        try:
                            ce = from_json(message.payload)
                            ce["mqtt_topic"] = message.topic.value
                            await self.save_buffer.put(ce)
                        except Exception as e:
                            L.error("MQTT message error", extra={"reason": e})
            except MqttError as error:
                L.error(f"MQTT Error: {error}. Retrying in {reconnect}s")
                await asyncio.sleep(reconnect)
            await asyncio.sleep(0.1)

    async def handle_save_buffer(self):
        """Processes events from the internal queue."""
        while True:
            ce = await self.save_buffer.get()
            try:
                if ce["type"] == "sensor.file.chunk":
                    await self.handle_file_chunk(ce)
            except Exception as e:
                L.error("Save buffer error", extra={"reason": e})
            finally:
                self.save_buffer.task_done()

    async def handle_file_chunk(self, ce: CloudEvent):
        """Routes chunks to correct subdirectories based on CloudEvent source."""
        try:
            src = ce["source"]
            parts = src.split(".")
            device_id = parts[-1]
            path_parts = device_id.split(self.ID_DELIM)
            
            output_dir = os.path.join(self.config.data_base_path, *path_parts)
            chunk_info, complete, _ = await self.assembler.process_chunk(ce, output_dir)
            
            if complete and chunk_info and self.client:
                # Send ACK back to sender on sub-topic: <topic>/ack/<file_id>
                ack_topic = f"{ce['mqtt_topic']}/ack/{chunk_info.file_id}"
                ack_payload = json.dumps({"status": "SUCCESS", "file": chunk_info.file_name})
                await self.client.publish(ack_topic, payload=ack_payload, qos=1)
                L.info("ACK sent", extra={"topic": ack_topic})

        except Exception as e:
            self.logger.error("Chunk handling error", extra={"reason": e})

async def main(config):
    """Starts the web server and file manager logic."""
    server_config = uvicorn.Config(
        "main:app",
        host=config.host,
        port=config.port,
        root_path="/msp/chunked-filemanager",
    )
    server = uvicorn.Server(server_config)
    await server.serve()

if __name__ == "__main__":
    config = FilemanagerConfig()
    if "--host" in sys.argv:
        config.host = sys.argv[sys.argv.index("--host") + 1]
    if "--port" in sys.argv:
        config.port = int(sys.argv[sys.argv.index("--port") + 1])

    # Instantiate renamed file manager logic
    filemanager = ChunkedFilemanager()
    asyncio.run(main(config))