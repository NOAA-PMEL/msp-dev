import asyncio
import base64
import json
import logging
import os
import ssl
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple

import aiomqtt
from fastapi import FastAPI, Response, status
from logfmter import Logfmter
from pydantic import BaseModel, BaseSettings

# --- Configuration (Similar to Settings in filemanager/main.py) ---
class Settings(BaseSettings):
    mqtt_broker: str = "iot.pmel-dev.oarcloud.noaa.gov"
    mqtt_port: int = 8883
    mqtt_client_id: str = "pmel-pco2-data-testserver"
    mqtt_sub_topic: str = "pmel/pco2/+/file/update"
    
    # Paths for certificates (to be mounted via K8s Secret)
    ca_cert_path: str = "/app/certs/AmazonRootCA1.pem"
    client_cert_path: str = "/app/certs/device-certificate"
    client_key_path: str = "/app/certs/private-key"
    
    storage_base_path: str = "/data/files"

    class Config:
        env_prefix = "CHUNKED_FILEMANAGER_"
        case_sensitive = False

settings = Settings()

# --- Logging Setup ---
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.INFO)

# --- Models ---
class FileChunkData(BaseModel):
    file_id: str
    file_name: str
    chunk_number: int
    total_chunks: int
    data_b64: str

class FileChunkEvent(BaseModel):
    specversion: str
    type: str
    source: str
    id: str
    time: str
    data: FileChunkData

# --- Assembler Logic ---
@dataclass
class _FileTransfer:
    total_chunks: int
    chunks: Dict[int, bytes] = field(default_factory=dict)

class AsyncFileAssembler:
    def __init__(self, base_path: str):
        self.base_path = base_path
        self.transfers: Dict[str, _FileTransfer] = {}

    def _get_save_path(self, topic: str, filename: str) -> str:
        segments = topic.split("/")
        try:
            make, model, serial = segments[2].split("::")
            return os.path.join(self.base_path, "pmel", "pco2", make, model, serial, filename)
        except Exception:
            return os.path.join(self.base_path, "incoming", filename)

    async def add_chunk(self, topic: str, payload: bytes) -> Tuple[bool, Optional[str]]:
        try:
            event = FileChunkEvent.parse_raw(payload)
            msg = event.data
            if msg.file_id not in self.transfers:
                self.transfers[msg.file_id] = _FileTransfer(total_chunks=msg.total_chunks)
            
            transfer = self.transfers[msg.file_id]
            transfer.chunks[msg.chunk_number] = base64.b64decode(msg.data_b64)

            if len(transfer.chunks) < transfer.total_chunks:
                return False, msg.file_id

            # Save file
            full_data = b''.join(transfer.chunks[i] for i in range(1, transfer.total_chunks + 1))
            path = self._get_save_path(topic, msg.file_name)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            
            with open(path, "wb") as f:
                f.write(full_data)
            
            del self.transfers[msg.file_id]
            return True, msg.file_id
        except Exception as e:
            L.error("assembler_error", extra={"error": str(e)})
            return False, None

# --- FastAPI and Background Task ---
app = FastAPI()
assembler = AsyncFileAssembler(settings.storage_base_path)

@app.get("/health")
async def health():
    return {"status": "healthy"}

async def mqtt_loop():
    tls_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=settings.ca_cert_path)
    tls_context.load_cert_chain(certfile=settings.client_cert_path, keyfile=settings.client_key_path)

    while True:
        try:
            async with aiomqtt.Client(
                hostname=settings.mqtt_broker,
                port=settings.mqtt_port,
                tls_context=tls_context,
                identifier=settings.mqtt_client_id
            ) as client:
                await client.subscribe(settings.mqtt_sub_topic)
                L.info("mqtt_connected", extra={"topic": settings.mqtt_sub_topic})
                
                async for message in client.messages:
                    topic = str(message.topic)
                    complete, file_id = await assembler.add_chunk(topic, message.payload)
                    
                    if complete:
                        ack_topic = topic.replace("/update", "/ack")
                        ack = {"data": {"file_id": file_id, "status": "success"}}
                        await client.publish(ack_topic, payload=json.dumps(ack), qos=1)
                        L.info("file_saved", extra={"file_id": file_id, "topic": ack_topic})
        except Exception as e:
            L.error("mqtt_loop_error", extra={"error": str(e)})
            await asyncio.sleep(5)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(mqtt_loop())