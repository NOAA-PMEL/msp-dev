import asyncio
import json
import logging
import sys
import os

from ulid import ULID
from logfmter import Logfmter
from pydantic import BaseSettings, Field
from cloudevents.http import CloudEvent, from_json
from aiomqtt import Client, MqttError

from envds.daq.types import DAQEventType as det
from envds.sampling.types import SamplingEventType as sampet
from envds.util.util import time_to_next
import uvicorn

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.INFO)


class FilemanagerConfig(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8080
    debug: bool = True

    daq_id: str | None = None
    
    # Base configuration for file saves
    base_path: str = "/data"
    save_interval: int = 60
    file_interval: str = "day" # options: 'day', 'hour'

    # Filter toggles for what data types to save
    save_device_data: bool = True
    save_controller_data: bool = True
    save_sampling_status: bool = True

    mqtt_broker: str = "mosquitto.default"
    mqtt_port: int = 1883
    mqtt_topic_subscriptions: str = "envds/+/+/+/data/#,envds/+/+/+/status/#"
    mqtt_client_id: str = Field(default_factory=lambda: str(ULID()))

    class Config:
        env_prefix = "FILEMANAGER_"
        case_sensitive = False


class GenericRecordFile:
    """Consolidated file handler that appends robustly formatted JSON lines."""
    def __init__(self, base_path: str, save_interval: int = 60, file_interval: str = "day"):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.base_path = base_path
        self.save_interval = save_interval
        self.file_interval = file_interval
        
        self.save_now = True
        self.data_buffer = asyncio.Queue()
        self.task_list = []
        self.file = None
        
        self.open()

    async def write(self, record: dict, ymd: str, hour: str = "00"):
        await self.data_buffer.put((record, ymd, hour))

    # async def __write(self):
    #     while True:
    #         record, ymd, hour = await self.data_buffer.get()
    #         try:
    #             self.__open(ymd, hour)
    #             if not self.file:
    #                 self.data_buffer.task_done()
    #                 continue

    #             json.dump(record, self.file)
    #             self.file.write("\n")

    #             if self.save_now:
    #                 self.file.flush()
    #                 if self.save_interval > 0:
    #                     self.save_now = False

    #         except Exception as e:
    #             self.logger.error("__write loop error", extra={"reason": str(e)})

    #         self.data_buffer.task_done()

    # 1. Create a synchronous helper for the blocking I/O
    def _sync_write(self, record):
        json.dump(record, self.file)
        self.file.write("\n")

        if self.save_now:
            self.file.flush()
            if self.save_interval > 0:
                self.save_now = False

    # 2. Call it in your async loop using to_thread
    async def __write(self):
        while True:
            record, ymd, hour = await self.data_buffer.get()
            try:
                self.__open(ymd, hour)
                if not self.file:
                    self.data_buffer.task_done()
                    continue

                # Offload the blocking disk write to a background thread
                await asyncio.to_thread(self._sync_write, record)

            except Exception as e:
                self.logger.error("__write loop error", extra={"reason": str(e)})

            self.data_buffer.task_done()

    # def __open(self, ymd: str, hour: str = "00"):
    #     fname = ymd
    #     if self.file_interval == "hour":
    #         fname += f"_{hour}"
    #     fname += ".jsonl"

    #     if self.file is not None and not self.file.closed and os.path.basename(self.file.name) == fname:
    #         return

    #     try:
    #         if not os.path.exists(self.base_path):
    #             os.makedirs(self.base_path, exist_ok=True)
    #         self.file = open(os.path.join(self.base_path, fname), mode="a")
    #     except OSError as e:
    #         self.logger.error("OSError creating path", extra={"error": str(e), "path": self.base_path})
    #         self.file = None

    def __open(self, ymd: str, hour: str = "00"):
        # 1. Construct the target filename
        fname = ymd
        if self.file_interval == "hour":
            fname += f"_{hour}"
        fname += ".jsonl"

        # 2. Check if we already have a file open
        if self.file is not None and not self.file.closed:
            # If the currently open file is the one we want, do nothing and return
            if os.path.basename(self.file.name) == fname:
                return
            # If the name doesn't match (e.g., the day or hour rolled over), 
            # safely flush and close the old file descriptor to prevent leaks!
            else:
                try:
                    self.file.flush()
                    self.file.close()
                except Exception as e:
                    self.logger.error("error closing old file during rollover", extra={"reason": str(e)})

        # 3. Open the new file
        try:
            if not os.path.exists(self.base_path):
                os.makedirs(self.base_path, exist_ok=True)
            self.file = open(os.path.join(self.base_path, fname), mode="a")
        except OSError as e:
            self.logger.error("OSError creating path", extra={"error": str(e), "path": self.base_path})
            self.file = None

    def open(self):
        self.task_list.append(asyncio.create_task(self.save_file_loop()))
        self.task_list.append(asyncio.create_task(self.__write()))

    def close(self):
        for t in self.task_list:
            t.cancel()
        if self.file:
            try:
                self.file.flush()
                self.file.close()
                self.file = None
            except Exception:
                self.logger.info("file already closed")

    async def save_file_loop(self):
        while True:
            if self.save_interval > 0:
                await asyncio.sleep(time_to_next(self.save_interval))
                self.save_now = True
            else:
                self.save_now = True
                await asyncio.sleep(1)


class Filemanager:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.DEBUG)
        self.config = FilemanagerConfig()

        self.file_map = {}
        self._background_tasks = set()

    async def setup(self):
        self.logger.info("Running Filemanager async setup...")
        # Bound queue to prevent memory leaks from massive throughput
        self.save_buffer = asyncio.Queue(maxsize=2000)
        
        t1 = asyncio.create_task(self.get_from_mqtt_loop())
        t2 = asyncio.create_task(self.handle_save_buffer())
        self._background_tasks.update({t1, t2})

    def _extract_val(self, obj: dict, key: str, default: any):
        """Safely extracts a flat value from varied nested dict shapes."""
        if not isinstance(obj, dict): return default
        val = obj.get(key)
        if val is None: return default
        if isinstance(val, dict) and "data" in val:
            return val.get("data", default)
        return val

    def _extract_time(self, data: dict) -> str:
        """Find the timestamp regardless of it being device telemetry or status event."""
        # 1. Variables block
        ts_str = self._extract_val(data.get("variables", {}), "time", None)
        # 2. Status block
        if not ts_str: ts_str = data.get("status", {}).get("time")
        # 3. Root/Metadata fallback
        if not ts_str: ts_str = data.get("timestamp") or data.get("metadata", {}).get("time")
            
        if isinstance(ts_str, list) and len(ts_str) > 0:
            ts_str = ts_str[-1]
            
        return ts_str

    def _extract_identity(self, data: dict, source: str):
        """Find the Make, Model, and SN/ID structure."""
        attributes = data.get("attributes", {})
        metadata = data.get("metadata", {})
        
        make = self._extract_val(attributes, "make", self._extract_val(metadata, "make", "unknown"))
        model = self._extract_val(attributes, "model", self._extract_val(metadata, "model", "unknown"))
        sn = self._extract_val(attributes, "serial_number", self._extract_val(metadata, "serial_number", "unknown"))
        
        if make == "unknown" and model == "unknown" and sn == "unknown":
            parts = source.split(".")
            sn = parts[-1] if len(parts) > 0 else "unknown"
            
        return str(make), str(model), str(sn)

    async def get_from_mqtt_loop(self):
        reconnect = 10
        while True:
            try:
                L.debug("MQTT Listen", extra={"broker": self.config.mqtt_broker})
                async with Client(
                    self.config.mqtt_broker, port=self.config.mqtt_port, identifier=str(ULID())
                ) as self.client:
                    for topic in self.config.mqtt_topic_subscriptions.split(","):
                        if topic.strip():
                            await self.client.subscribe(f"$share/filemanager/{topic.strip()}")

                    async for message in self.client.messages:
                        try:
                            ce = from_json(message.payload)
                            ce["sourcepath"] = message.topic.value
                            # Drop message via wait_for timeout to prevent blocking if queue is backpressured
                            try:
                                await asyncio.wait_for(self.save_buffer.put(ce), timeout=1.0)
                            except asyncio.TimeoutError:
                                L.warning("Filemanager queue full! Dropping message to protect memory.")
                        except Exception as e:
                            L.error("MQTT loop payload error", extra={"reason": str(e)})

            except MqttError as error:
                L.error(f"{error}. Reconnecting in {reconnect} seconds.")
                await asyncio.sleep(reconnect)
            finally:
                await asyncio.sleep(0.0001)

    async def save(self, ce: CloudEvent):
        """Entrypoint for API-forwarded CloudEvents."""
        try:
            await asyncio.wait_for(self.save_buffer.put(ce), timeout=1.0)
        except asyncio.TimeoutError:
            L.warning("API push rejected. Filemanager buffer full.")

    async def handle_save_buffer(self):
        status_map = {
            sampet.sampling_condition_status_update(): "samplingcondition",
            sampet.sampling_state_status_update(): "samplingstate",
            sampet.sampling_mode_status_update(): "samplingmode"
        }

        while True:
            try:
                ce = await self.save_buffer.get()
                ctype = ce["type"]

                # if ctype in [det.data_update(), det.sensor_data_update(), "envds.data.update"]:
                if ctype in [det.data_update(), "envds.data.update"]:
                    if self.config.save_device_data:
                        await self.process_data_record(ce, component="device")
                
                elif ctype in [det.controller_data_update(), "envds.controller.data.update"]:
                    if self.config.save_controller_data:
                        await self.process_data_record(ce, component="controller")

                elif ctype in status_map:
                    if self.config.save_sampling_status:
                        status_type = status_map[ctype]
                        await self.process_status_record(ce, status_type=status_type)

            except Exception as e:
                L.error("handle_save_buffer loop", extra={"reason": str(e)})

            self.save_buffer.task_done()

    async def process_data_record(self, ce: CloudEvent, component: str):
        try:
            data = ce.data
            
            # 1. Extract Routing info
            make, model, sn = self._extract_identity(data, ce.get("source", ""))
            
            # 2. Extract Time info
            ts_str = self._extract_time(data)
            if not ts_str:
                self.logger.warning("Dropped data file write: No timestamp found.", extra={"source": ce.get("source")})
                return
                
            d_and_t = str(ts_str).split("T")
            ymd = d_and_t[0]
            hour = d_and_t[1].split(":")[0] if len(d_and_t) > 1 else "00"

            # 3. Path construction (e.g. /data/data/device/Make/Model/SN/)
            file_path = os.path.join(self.config.base_path, "data", component, make, model, sn)

            # 4. Map & Write
            if file_path not in self.file_map:
                self.file_map[file_path] = GenericRecordFile(
                    base_path=file_path, 
                    save_interval=self.config.save_interval, 
                    file_interval=self.config.file_interval
                )
            
            await self.file_map[file_path].write(data, ymd, hour)

        except Exception as e:
            self.logger.error("process_data_record error", extra={"reason": str(e)})

    async def process_status_record(self, ce: CloudEvent, status_type: str):
        try:
            data = ce.data
            
            ts_str = self._extract_time(data)
            if not ts_str:
                self.logger.warning("Dropped status file write: No timestamp found.", extra={"source": ce.get("source")})
                return
                
            d_and_t = str(ts_str).split("T")
            ymd = d_and_t[0]
            hour = d_and_t[1].split(":")[0] if len(d_and_t) > 1 else "00"

            # Flat Path construction (e.g. /data/status/samplingcondition/)
            file_path = os.path.join(self.config.base_path, "status", status_type)

            if file_path not in self.file_map:
                self.file_map[file_path] = GenericRecordFile(
                    base_path=file_path, 
                    save_interval=self.config.save_interval, 
                    file_interval=self.config.file_interval
                )
            
            await self.file_map[file_path].write(data, ymd, hour)

        except Exception as e:
            self.logger.error("process_status_record error", extra={"reason": str(e)})


async def shutdown():
    print("shutting down")

async def main(config):
    config = uvicorn.Config(
        "main:app",
        host=config.host,
        port=config.port,
        root_path="/msp/filemanager",
    )

    server = uvicorn.Server(config)
    L.info(f"server: {server}")
    await server.serve()

    print("starting shutdown...")
    await shutdown()
    print("done.")


if __name__ == "__main__":
    config = FilemanagerConfig()

    try:
        index = sys.argv.index("--host")
        config.host = sys.argv[index + 1]
    except (ValueError, IndexError):
        pass

    try:
        index = sys.argv.index("--port")
        config.port = int(sys.argv[index + 1])
    except (ValueError, IndexError):
        pass

    asyncio.run(main(config))