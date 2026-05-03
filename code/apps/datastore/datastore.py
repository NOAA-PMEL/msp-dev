import asyncio
import json
import logging
import math
import sys
from time import sleep
from typing import List

from ulid import ULID
from pathlib import Path
import os

import httpx
from logfmter import Logfmter

from pydantic import BaseSettings, BaseModel, Field
from cloudevents.http import CloudEvent, from_http, from_json, to_json
from cloudevents.conversion import to_structured 
from cloudevents.exceptions import InvalidStructuredJSON
from aiomqtt import Client, MqttError

from datetime import datetime, timedelta, timezone
from envds.util.util import (
    get_datetime_string,
    get_datetime,
    datetime_to_string,
    get_datetime_with_delta,
    string_to_timestamp,
    timestamp_to_string
)

from envds.daq.event import DAQEvent
from envds.daq.types import DAQEventType as det
from envds.sampling.types import SamplingEventType as sampet

import uvicorn

from datastore_requests import (
    DataStoreQuery,
    DataUpdate,
    DataRequest,
    DeviceDefinitionUpdate,
    DeviceDefinitionRequest,
    DeviceInstanceUpdate,
    DeviceInstanceRequest,
    DatastoreRequest,
    ControllerInstanceUpdate,
    ControllerInstanceRequest,
    ControllerDataRequest,
    ControllerDataUpdate,
    ControllerDefinitionRequest,
    ControllerDefinitionUpdate,
    VariableSetDataUpdate,
    VariableSetDataRequest,
    VariableSetDefinitionUpdate,
    VariableSetDefinitionRequest,
    VariableMapDefinitionRequest,
    VariableMapDefinitionUpdate,
    VariableSetInstanceUpdate,
    VariableSetInstanceRequest
)
from db_client import DBClientManager, DBClientConfig

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.INFO)


class DatastoreConfig(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8080
    debug: bool = True

    # TODO fix ns prefix
    daq_id: str | None = None

    db_client_type: str | None = None
    db_client_connection: str | None = None
    db_client_hostname: str | None = None
    db_client_port: str | None = None
    db_client_username: str | None = None
    db_client_password: str | None = None
    db_clear_db: bool | None = False

    db_data_ttl: int = 600  # seconds
    db_reg_device_definition_ttl: int = 0  # permanent
    db_reg_device_instance_ttl: int = 600  # seconds
    db_reg_controller_definition_ttl: int = 0  # permanent
    db_reg_controller_instance_ttl: int = 600  # seconds

    db_reg_variablemap_definition_ttl: int = 0  # permanent
    db_reg_variableset_definition_ttl: int = 0  # permanent
    db_reg_variableset_definition_ttl: int = 0  # permanent
    db_reg_variableset_instance_ttl: int = 600  # Added: 10 minute active timeout
    db_reg_platform_definition_ttl: int = 0  # permanent

    erddap_enable: bool = False
    erddap_http_connection: str | None = None
    erddap_author: str = "fake_author"

    mqtt_broker: str = 'mosquitto.default'
    mqtt_port: int = 1883
    mqtt_topic_subscriptions: str = 'envds/+/+/+/data/#' 
    mqtt_client_id: str = Field(str(ULID()))

    knative_broker: str | None = None
    class Config:
        env_prefix = "DATASTORE_"
        case_sensitive = False


class Datastore:
    """docstring for TestClass."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("TestClass instantiated")
        self.logger.setLevel(logging.DEBUG)
        self.db_client = None
        self.erddap_client = None
        self.config = DatastoreConfig()

        self.http_client = None

        self._background_tasks = set()

        self.configure()

    async def setup(self):
        self.logger.info("Running Datastore async setup...")
        
        # Start background tasks safely inside the event loop
        # FIX: Add maxsize to prevent infinite memory growth (backpressure)
        self.mqtt_buffer = asyncio.Queue(maxsize=2000)
        
        # FIX: Store tasks in the set
        task1 = asyncio.create_task(self.get_from_mqtt_loop())
        task2 = asyncio.create_task(self.handle_mqtt_buffer())
        self._background_tasks.update({task1, task2})

        # Build Redis indexes if the client supports it
        if hasattr(self.db_client, "build_indexes"):
            await self.db_client.build_indexes()
            self.logger.info("Redis indexes built successfully.")
            
    def configure(self):
        self.logger.debug("configure", extra={"self.config": self.config})
        db_client_config = DBClientConfig(
            type=self.config.db_client_type,
            config={
                "connection": self.config.db_client_connection,
                "hostname": self.config.db_client_hostname,
                "port": self.config.db_client_port,
                "username": self.config.db_client_username,
                "password": self.config.db_client_password,
                "clear_db": self.config.db_clear_db
            },
        )
        self.logger.debug("configure", extra={"db_client_config": db_client_config})
        self.db_client = DBClientManager.create(db_client_config)

        if self.config.erddap_enable:
            pass

    def open_http_client(self):
        self.logger.debug("open_http_client")
        self.http_client = httpx.AsyncClient()

    async def close_http_client(self):
        if self.http_client:
            await self.http_client.aclose()
            self.http_client = None

    async def send_event(self, ce):
        try:
            self.logger.debug(ce)

            # Lazy initialization mirroring registrar.py
            if not self.http_client:
                self.open_http_client()
            try:
                timeout = httpx.Timeout(5.0, read=0.1)
                headers, body = to_structured(ce)
                self.logger.debug("send_event", extra={"broker": self.config.knative_broker, "h": headers, "b": body})
                # send to knative broker
                r = await self.http_client.post(
                    self.config.knative_broker,
                    headers=headers,
                    data=body,
                    timeout=timeout
                )
                r.raise_for_status()
            except InvalidStructuredJSON:
                self.logger.error(f"INVALID MSG: {ce}")
            except httpx.TimeoutException:
                pass
            except httpx.HTTPError as e:
                self.logger.error(f"HTTP Error when posting to {e.request.url!r}: {e}")
        except Exception as e:
            print("error", e)
        await asyncio.sleep(0.01)

    async def get_from_mqtt_loop(self):
        reconnect = 10
        while True:
            try:
                L.debug("listen", extra={"config": self.config})
                client_id=str(ULID())
                async with Client(self.config.mqtt_broker, port=self.config.mqtt_port,identifier=client_id) as self.client:
                    for topic in self.config.mqtt_topic_subscriptions.split(","):
                        if topic.strip():
                            L.debug("subscribe", extra={"topic": topic.strip()})
                            await self.client.subscribe(f"$share/datastore/{topic.strip()}")

                    async for message in self.client.messages: 

                        try:
                            ce = from_json(message.payload)
                            topic = message.topic.value
                            ce["sourcepath"] = topic

                            # FIX: Use wait_for to drop messages if the buffer is full, 
                            # preventing the async for loop (and ping responses) from blocking indefinitely.
                            try:
                                await asyncio.wait_for(self.mqtt_buffer.put(ce), timeout=1.0)
                                L.debug("get_from_mqtt_loop", extra={"cetype": ce["type"], "topic": topic})
                            except asyncio.TimeoutError:
                                L.warning("MQTT buffer full! Dropping message to prevent backpressure.", extra={"topic": topic})

                            L.debug("get_from_mqtt_loop", extra={"cetype": ce["type"], "topic": topic})
                        except Exception as e:
                            L.error("get_from_mqtt_loop", extra={"reason": e})
            except MqttError as error:
                L.error(
                    f'{error}. Trying again in {reconnect} seconds',
                    extra={ k: v for k, v in self.config.dict().items() if k.lower().startswith('mqtt_') }
                )
                await asyncio.sleep(reconnect)
            finally:
                await asyncio.sleep(0.0001)

    async def handle_mqtt_buffer(self):
        while True:
            try:
                ce = await self.mqtt_buffer.get()
                
                if "variable" in ce["type"]:
                    self.logger.debug("handle_mqtt_buffer", extra={"ce-type": ce["type"]})
                if ce["type"] == "envds.data.update":
                    await self.device_data_update(ce) 
                elif ce["type"] == "envds.controller.data.update":
                    await self.controller_data_update(ce)
                elif ce["type"] == sampet.variableset_data_update():
                    self.logger.debug("handle_mqtt_buffer", extra={"ce": ce})
                    await self.variableset_data_update(ce)           

            except Exception as e:
                L.error("handle_mqtt_buffer", extra={"reason": e})
            
    def find_one(self):  
        return None

    def insert_one(self): 
        return None

    def update_one(self):
        return None

    # -------------------------------------------------------------------------------------
    # DEVICES
    # -------------------------------------------------------------------------------------
    # async def device_data_update(self, ce: CloudEvent):
    #     try:
    #         database = "data"
    #         collection = "device"
    #         attributes = ce.data["attributes"]
    #         dimensions = ce.data["dimensions"]
    #         variables = ce.data["variables"]

    #         make = attributes["make"]["data"]
    #         model = attributes["model"]["data"]
    #         serial_number = attributes["serial_number"]["data"]

    #         format_version = attributes["format_version"]["data"]
    #         parts = format_version.split(".")
    #         self.logger.debug(f"parts: {parts}, {format_version}")
    #         erddap_version = f"v{parts[0]}"
    #         device_id = "::".join([make, model, serial_number])
    #         self.logger.debug("device_data_update", extra={"device_id": device_id})

    #         # timestamp = string_to_timestamp(ce.data["timestamp"]) 
    #         record_time_str = variables.get("time", {}).get("data")
    #         if not record_time_str:
    #             record_time_str = ce.data.get("timestamp")
                
    #         timestamp = string_to_timestamp(record_time_str)
    #         self.logger.debug("device_data_update", extra={"timestamp": timestamp, "ce-timestamp": ce.data["timestamp"]})

    #         request = DataUpdate(
    #             make=make,
    #             model=model,
    #             serial_number=serial_number,
    #             version=erddap_version,
    #             timestamp=timestamp,
    #             attributes=attributes,
    #             dimensions=dimensions,
    #             variables=variables,
    #         )

    #         self.logger.debug("device_data_update", extra={"request": request})
    #         await self.db_client.device_data_update(
    #             database=database,
    #             collection=collection,
    #             request=request,
    #             ttl=self.config.db_data_ttl,
    #         )

    #         # If the device is sending data, it is currently active.
    #         device_type = attributes.get("device_type", {}).get("data", "sensor")
    #         instance_request = DeviceInstanceUpdate(
    #             device_id=device_id,
    #             make=make,
    #             model=model,
    #             serial_number=serial_number,
    #             version=erddap_version,
    #             device_type=device_type,
    #             attributes=attributes,
    #         )
            
    #         await self.db_client.device_instance_registry_update(
    #             database="registry",
    #             collection="device-instance",
    #             request=instance_request,
    #             ttl=self.config.db_reg_device_instance_ttl,
    #         )

    #     except Exception as e:
    #         L.error("device_data_update", extra={"reason": e})
    #     pass

    async def device_data_update(self, ce: CloudEvent):
        try:
            database = "data"
            collection = "device"
            data = ce.data
            
            # Extract main blocks
            attributes = data.get("attributes", {})
            variables = data.get("variables", {})
            dimensions = data.get("dimensions", {}) # FIX: Guaranteed extraction

            # Metadata Extraction with Hardcoded Defaults to satisfy Pydantic
            make = attributes.get("make", {}).get("data", "unknown")
            model = attributes.get("model", {}).get("data", "unknown")
            sn = attributes.get("serial_number", {}).get("data", "unknown")
            
            # Version Extraction (Sample: "2.0.0" -> "v2")
            format_ver = attributes.get("format_version", {}).get("data", "1.0.0")
            version = f"v{str(format_ver).split('.')[0]}"

            # ID Reconstruction
            device_id = attributes.get("device_id", {}).get("data")
            if not device_id:
                device_id = f"{make}::{model}::{sn}"

            # Timestamp Logic
            ts_str = variables.get("time", {}).get("data") or data.get("time", {}).get("data") or data.get("timestamp")
            timestamp = string_to_timestamp(ts_str)

            request = DataUpdate(
                device_id=device_id,
                make=make,
                model=model,
                serial_number=sn,
                version=version,     # Guaranteed string
                timestamp=timestamp, # Guaranteed float
                attributes=attributes,
                dimensions=dimensions, # Included to fix missing field error
                variables=variables,
            )

            if self.db_client:
                await self.db_client.device_data_update(
                    database=database, collection=collection, request=request, ttl=self.config.db_data_ttl
                )
        except Exception as e:
            L.error("device_data_update error", extra={"reason": str(e)})

    async def device_data_get(self, query: DataRequest):
        self.logger.debug("device_data_get:3", extra={"query": query})
        if query.start_time:
            query.start_timestamp = string_to_timestamp(query.start_time)

        if query.end_time:
            query.end_timestamp = string_to_timestamp(query.end_time)

        if query.last_n_seconds:
            start_dt = get_datetime_with_delta(-(query.last_n_seconds))
            query.start_timestamp = start_dt.timestamp()
            query.end_timestamp = None

        if self.db_client:
            self.logger.debug("device_data_get:4", extra={"query": query})
            return await self.db_client.device_data_get(query)

        return {"results": []}

    async def device_definition_registry_update(self, ce: CloudEvent):
        try:
            for definition_type, device_def in ce.data.items():
                if definition_type not in ["device-definition", "device-definition-update"]:
                    continue

                # ROBUST EXTRACTION: Works for both flat sync payloads AND nested sensor payloads
                make = device_def.get("make") or device_def.get("attributes", {}).get("make", {}).get("data", "unknown")
                model = device_def.get("model") or device_def.get("attributes", {}).get("model", {}).get("data", "unknown")
                
                # Handle version extraction cleanly
                version = device_def.get("version")
                if not version:
                    format_version = device_def.get("attributes", {}).get("format_version", {}).get("data", "1.0.0")
                    version = f"v{format_version.split('.')[0]}"

                valid_time = device_def.get("valid_time", "2020-01-01T00:00:00Z")
                device_definition_id = device_def.get("device_definition_id", f"{make}::{model}::{version}")

                # Ensure these top-level keys exist so RediSearch can index them
                device_def["device_definition_id"] = device_definition_id
                device_def["make"] = make
                device_def["model"] = model
                device_def["version"] = version

                # Build a simple Pydantic model ONLY for structured insertion, passing the whole dict as **kwargs
                request = DeviceDefinitionUpdate(
                    device_definition_id=device_definition_id,
                    make=make,
                    model=model,
                    version=version,
                    device_type=device_def.get("device_type") or device_def.get("attributes", {}).get("device_type", {}).get("data", "sensor"),
                    valid_time=valid_time,
                    attributes=device_def.get("attributes", {}),
                    dimensions=device_def.get("dimensions", {}),
                    variables=device_def.get("variables", {})
                )

                self.logger.debug("device_definition_registry_update", extra={"request": request.device_definition_id})
                if self.db_client:
                    await self.db_client.device_definition_registry_update(
                        database="registry",
                        collection="device-definition",
                        request=request,
                        ttl=self.config.db_reg_device_definition_ttl,
                    )
        except Exception as e:
            self.logger.error("device_definition_registry_update", extra={"reason": str(e)})


    async def device_definition_registry_get_ids(self) -> dict:
        if self.db_client:
            self.logger.debug("device_definition_registry_get_ids")
            return await self.db_client.device_definition_registry_get_ids()
        
        return {"results": []}

    async def device_definition_registry_get(self, query: DeviceDefinitionRequest) -> dict:
        if self.db_client:
            return await self.db_client.device_definition_registry_get(query)
        
        return {"results": []}

    async def device_instance_registry_update(self, ce: CloudEvent):
        try:
            self.logger.debug("device_instance_registry_update", extra={"ce": ce})
            for instance_type, instance_reg in ce.data.items():
                request = None
                self.logger.debug("device_instance_registry_update", extra={"instance_type": instance_type, "instance_reg": instance_reg})
                try:
                    make = instance_reg["make"]
                    model = instance_reg["model"]
                    serial_number = instance_reg["serial_number"]
                    format_version = instance_reg["format_version"]
                    parts = format_version.split(".")
                    version = f"v{parts[0]}"

                    if make is None or model is None or serial_number is None:
                        self.logger.error("couldn't register instance - missing value", extra={"make": make, "model": model, "serial_number": serial_number})
                        return
                    
                    device_id = "::".join([make, model, serial_number])

                    if instance_type == "device-instance":
                        database = "registry"
                        collection = "device-instance"
                        attributes = instance_reg["attributes"]

                        if "device_type" in instance_reg["attributes"]:
                            device_type = instance_reg["attributes"]["device_type"]["data"]
                        else:
                            device_type = "sensor"

                        request = DeviceInstanceUpdate(
                            device_id=device_id,
                            make=make,
                            model=model,
                            serial_number=serial_number,
                            version=format_version,
                            device_type=device_type,
                            attributes=attributes,
                        )

                except (KeyError, IndexError) as e:
                        self.logger.error("datastore:device_instance_registry_update", extra={"reason": e})
                        continue

                self.logger.debug("datastore:device_instance_registry_update", extra={"request": request, "db_client": self.db_client})
                if self.db_client and request:
                    await self.db_client.device_instance_registry_update(
                        database=database,
                        collection=collection,
                        request=request,
                        ttl=self.config.db_reg_device_instance_ttl,
                    )

        except Exception as e:
            L.error("device_instance_registry_update", extra={"reason": e})
        pass

    async def device_instance_registry_get_ids(self) -> dict:
        if self.db_client:
            return await self.db_client.device_instance_registry_get_ids()
        return {"results": []}

    async def device_instance_registry_get(self, query: DeviceInstanceRequest) -> dict:
        if self.db_client:
            return await self.db_client.device_instance_registry_get(query)
        
        return {"results": []}

    # -------------------------------------------------------------------------------------
    # CONTROLLERS
    # -------------------------------------------------------------------------------------
    # async def controller_data_update(self, ce: CloudEvent):
    #     try:
    #         database = "data"
    #         collection = "controller"
    #         attributes = ce.data["attributes"]
    #         dimensions = ce.data["dimensions"]
    #         variables = ce.data["variables"]

    #         make = attributes["make"]["data"]
    #         model = attributes["model"]["data"]
    #         serial_number = attributes["serial_number"]["data"]

    #         format_version = attributes["format_version"]["data"]
    #         parts = format_version.split(".")
    #         self.logger.debug(f"parts: {parts}, {format_version}")
    #         erddap_version = f"v{parts[0]}"
    #         controller_id = "::".join([make, model, serial_number])
    #         self.logger.debug("controller_data_update", extra={"device_id": controller_id})

    #         # timestamp = string_to_timestamp(ce.data["timestamp"])

    #         record_time_str = variables.get("time", {}).get("data")
    #         if not record_time_str:
    #             record_time_str = ce.data.get("timestamp")
                
    #         timestamp = string_to_timestamp(record_time_str)

    #         self.logger.debug("device_data_update", extra={"timestamp": timestamp, "ce-timestamp": ce.data["timestamp"]})

    #         request = ControllerDataUpdate(
    #             make=make,
    #             model=model,
    #             serial_number=serial_number,
    #             version=erddap_version,
    #             timestamp=timestamp,
    #             attributes=attributes,
    #             dimensions=dimensions,
    #             variables=variables,
    #         )

    #         self.logger.debug("controller_data_update", extra={"request": request})
    #         await self.db_client.controller_data_update(
    #             database=database,
    #             collection=collection,
    #             request=request,
    #             ttl=self.config.db_data_ttl,
    #         )

    #         # If the controller is sending data, it is currently active.
    #         instance_request = ControllerInstanceUpdate(
    #             controller_id=controller_id,
    #             make=make,
    #             model=model,
    #             serial_number=serial_number,
    #             version=erddap_version,
    #             attributes=attributes,
    #         )
            
    #         await self.db_client.controller_instance_registry_update(
    #             database="registry",
    #             collection="controller-instance",
    #             request=instance_request,
    #             ttl=self.config.db_reg_controller_instance_ttl,
    #         )

    #     except Exception as e:
    #         L.error("device_data_update", extra={"reason": e})
    #     pass

    async def controller_data_update(self, ce: CloudEvent):
        try:
            database = "data"
            collection = "controller"
            data = ce.data
            
            attributes = data.get("attributes", {})
            variables = data.get("variables", {})
            dimensions = data.get("dimensions", {})

            make = attributes.get("make", {}).get("data", "unknown")
            model = attributes.get("model", {}).get("data", "unknown")
            sn = attributes.get("serial_number", {}).get("data", "unknown")
            
            format_ver = attributes.get("format_version", {}).get("data", "1.0.0")
            version = f"v{str(format_ver).split('.')[0]}"

            controller_id = attributes.get("controller_id", {}).get("data")
            if not controller_id:
                controller_id = f"{make}::{model}::{sn}"

            ts_str = variables.get("time", {}).get("data") or data.get("time", {}).get("data") or data.get("timestamp")
            timestamp = string_to_timestamp(ts_str)

            request = ControllerDataUpdate(
                controller_id=controller_id,
                make=make,
                model=model,
                serial_number=sn,
                version=version,
                timestamp=timestamp,
                attributes=attributes,
                dimensions=dimensions,
                variables=variables,
            )

            if self.db_client:
                await self.db_client.controller_data_update(
                    database=database, collection=collection, request=request, ttl=self.config.db_data_ttl
                )
        except Exception as e:
            L.error("controller_data_update error", extra={"reason": str(e)})

    async def controller_data_get(self, query: DataRequest):
        self.logger.debug("controller_data_get:3", extra={"query": query})
        if query.start_time:
            query.start_timestamp = string_to_timestamp(query.start_time)

        if query.end_time:
            query.end_timestamp = string_to_timestamp(query.end_time)

        if query.last_n_seconds:
            start_dt = get_datetime_with_delta(-(query.last_n_seconds))
            query.start_timestamp = start_dt.timestamp()
            query.end_timestamp = None

        if self.db_client:
            self.logger.debug("controller_data_get:4", extra={"query": query})
            return await self.db_client.controller_data_get(query)

        return {"results": []}

    async def controller_definition_registry_update(self, ce: CloudEvent):
        try:
            for definition_type, controller_def in ce.data.items():
                if definition_type not in ["controller-definition", "controller-definition-update"]:
                    continue

                # ROBUST EXTRACTION: Works for both flat sync payloads AND nested local payloads
                make = controller_def.get("make") or controller_def.get("attributes", {}).get("make", {}).get("data", "unknown")
                model = controller_def.get("model") or controller_def.get("attributes", {}).get("model", {}).get("data", "unknown")
                
                # Handle version extraction cleanly
                version = controller_def.get("version")
                if not version:
                    format_version = controller_def.get("attributes", {}).get("format_version", {}).get("data", "1.0.0")
                    version = f"v{format_version.split('.')[0]}"

                valid_time = controller_def.get("valid_time", "2020-01-01T00:00:00Z")
                controller_definition_id = controller_def.get("controller_definition_id", f"{make}::{model}::{version}")

                # Ensure these top-level keys exist so RediSearch can index them
                controller_def["controller_definition_id"] = controller_definition_id
                controller_def["make"] = make
                controller_def["model"] = model
                controller_def["version"] = version
                controller_def["valid_time"] = valid_time

                # Build a simple Pydantic model ONLY for structured insertion
                request = ControllerDefinitionUpdate(
                    controller_definition_id=controller_definition_id,
                    make=make,
                    model=model,
                    version=version,
                    valid_time=valid_time,
                    attributes=controller_def.get("attributes", {}),
                    dimensions=controller_def.get("dimensions", {}),
                    variables=controller_def.get("variables", {})
                )

                self.logger.debug("controller_definition_registry_update", extra={"request": request.controller_definition_id})
                
                if self.db_client:
                    await self.db_client.controller_definition_registry_update(
                        database="registry",
                        collection="controller-definition",
                        request=request,
                        ttl=self.config.db_reg_controller_definition_ttl,
                    )
        except Exception as e:
            self.logger.error("controller_definition_registry_update", extra={"reason": str(e)})

    async def controller_definition_registry_get_ids(self) -> dict:
        if self.db_client:
            self.logger.debug("controller_definition_registry_get_ids")
            return await self.db_client.controller_definition_registry_get_ids()
        
        return {"results": []}

    async def controller_definition_registry_get(self, query: ControllerDefinitionRequest) -> dict:
        if self.db_client:
            return await self.db_client.controller_definition_registry_get(query)
        
        return {"results": []}

    async def controller_instance_registry_update(self, ce: CloudEvent):
        try:
            self.logger.debug("controller_instance_registry_update", extra={"ce": ce})
            for instance_type, instance_reg in ce.data.items():
                request = None
                self.logger.debug("controller_instance_registry_update", extra={"instance_type": instance_type, "instance_reg": instance_reg})
                try:
                    make = instance_reg["make"]
                    model = instance_reg["model"]
                    serial_number = instance_reg["serial_number"]
                    format_version = instance_reg["format_version"]
                    parts = format_version.split(".")
                    version = f"v{parts[0]}"

                    if make is None or model is None or serial_number is None:
                        self.logger.error("couldn't register instance - missing value", extra={"make": make, "model": model, "serial_number": serial_number})
                        return
                    
                    controller_id = "::".join([make, model, serial_number])

                    if instance_type == "controller-instance":
                        database = "registry"
                        collection = "controller-instance"
                        attributes = instance_reg["attributes"]

                        request = ControllerInstanceUpdate(
                            controller_id=controller_id,
                            make=make,
                            model=model,
                            serial_number=serial_number,
                            version=format_version,
                            attributes=attributes,
                        )

                except (KeyError, IndexError) as e:
                        self.logger.error("datastore:controller_instance_registry_update", extra={"reason": e})
                        continue

                self.logger.debug("datastore:controller_instance_registry_update", extra={"request": request, "db_client": self.db_client})
                if self.db_client and request:
                    await self.db_client.controller_instance_registry_update(
                        database=database,
                        collection=collection,
                        request=request,
                        ttl=self.config.db_reg_controller_instance_ttl,
                    )

        except Exception as e:
            L.error("controller_instance_registry_update", extra={"reason": e})
        pass

    async def controller_instance_registry_get_ids(self) -> dict:
        if self.db_client:
            return await self.db_client.controller_instance_registry_get_ids()
        return {"results": []}

    async def controller_instance_registry_get(self, query: ControllerInstanceRequest) -> dict:
        if self.db_client:
            return await self.db_client.controller_instance_registry_get(query)
        
        return {"results": []}

    # -------------------------------------------------------------------------------------
    # VARIABLE MAPS & SETS
    # -------------------------------------------------------------------------------------
    async def variablemap_definition_registry_get_ids(self) -> dict:
        if self.db_client:
            return await self.db_client.variablemap_definition_registry_get_ids()
        return {"results": []}

    # -------------------------------------------------------------------------------------
    # UTILITY HELPER (Add this above your update methods)
    # -------------------------------------------------------------------------------------
    def _extract_val(self, obj: dict, key: str, default: any):
        """Safely extracts a value whether it is a flat string or a nested {"data": ...} dict."""
        if not isinstance(obj, dict):
            return default
        val = obj.get(key)
        if val is None:
            return default
        if isinstance(val, dict) and "data" in val:
            return val.get("data", default)
        return val
    
    async def variablemap_definition_registry_update(self, ce: CloudEvent):
        try:
            for definition_type, vm_def in ce.data.items():
                if "variablemap_definition_id" in vm_def:
                    request = VariableMapDefinitionUpdate(**vm_def)
                else:
                    # FIX: Read primary IDs from the 'metadata' block as defined in sampling_system.py
                    metadata = vm_def.get("metadata", {})
                    variablemap = metadata.get("name", "unknown")
                    variablemap_type_id = metadata.get("platform", "unknown")
                    valid_config_time = metadata.get("valid_config_time", "2020-01-01T00:00:00Z")

                    data = vm_def.get("data", {})
                    attributes = data.get("attributes", {})
                    
                    # Safely extract flat or nested attributes
                    variablemap_type = self._extract_val(attributes, "variablemap_type", "Platform")

                    variablemap_definition_id = "::".join([variablemap_type_id, variablemap, valid_config_time])
                    
                    request = VariableMapDefinitionUpdate(
                        variablemap_definition_id=variablemap_definition_id,
                        variablemap_type=variablemap_type,
                        variablemap_type_id=variablemap_type_id,
                        variablemap=variablemap,
                        valid_config_time=valid_config_time,
                        attributes=attributes,
                        variablesets=data.get("variablesets", {}),
                        variables=data.get("variables", {}),
                    )

                self.logger.debug("variablemap_definition_registry_update", extra={"request": request})
                if self.db_client:
                    await self.db_client.variablemap_definition_registry_update(
                        database="registry",
                        collection="variablemap-definition",
                        request=request,
                        ttl=self.config.db_reg_variablemap_definition_ttl,
                    )
        except Exception as e:
            self.logger.error("variablemap_definition_registry_update", extra={"reason": repr(e)})


    async def variablemap_definition_registry_get(self, query: VariableMapDefinitionRequest) -> dict:
        if self.db_client:
            return await self.db_client.variablemap_definition_registry_get(query)
        
        return {"results": []}

    async def variableset_definition_registry_get_ids(self) -> dict:
        if self.db_client:
            return await self.db_client.variableset_definition_registry_get_ids()
        return {"results": []}

    async def variableset_definition_registry_update(self, ce: CloudEvent):
        try:
            for definition_type, vs_payload in ce.data.items():
                if definition_type != "variableset-definition":
                    continue

                if "variableset_definition_id" in vs_payload:
                    request = VariableSetDefinitionUpdate(**vs_payload)
                else:
                    # Look exactly where publish_local_definitions() puts the data
                    vs_name = vs_payload.get("metadata", {}).get("name", "unknown")
                    data = vs_payload.get("data", {})
                    attributes = data.get("attributes", {})
                    
                    # Safely extract flat or nested attributes
                    variablemap_name = self._extract_val(attributes, "variablemap_id", "unknown")
                    platform = self._extract_val(attributes, "platform", "unknown")
                    valid_config_time = self._extract_val(attributes, "valid_config_time", "2020-01-01T00:00:00Z")
                    
                    # Safely extract index info (sampling_system puts this in 'data', not 'attributes')
                    index_type = self._extract_val(data, "index_type", self._extract_val(attributes, "index_type", "unknown"))
                    index_value = self._extract_val(data, "index_value", self._extract_val(attributes, "index_value", 0))

                    variablemap_definition_id = "::".join([platform, variablemap_name, valid_config_time])
                    
                    request = VariableSetDefinitionUpdate(
                        variableset_definition_id=f"{variablemap_definition_id}::{vs_name}",
                        variablemap_definition_id=variablemap_definition_id,
                        variableset=vs_name,
                        index_type=index_type,
                        index_value=index_value,
                        attributes=attributes,
                        dimensions=data.get("dimensions", {}),
                        variables=data.get("variables", {})
                    )

                self.logger.debug("variableset_definition_registry_update", extra={"request": request})
                if self.db_client:
                    await self.db_client.variableset_definition_registry_update(
                        database="registry",
                        collection="variableset-definition", 
                        request=request,
                        ttl=self.config.db_reg_variableset_definition_ttl,
                    )
        except Exception as e:
            self.logger.error("variableset_definition_registry_update", extra={"reason": str(e)})

    async def variableset_definition_registry_get(self, query: VariableSetDefinitionRequest) -> dict:
        if self.db_client:
            return await self.db_client.variableset_definition_registry_get(query)
        
        return {"results": []}

    # -------------------------------------------------------------------------------------
    # SAMPLING DEFINITIONS (DYNAMIC)
    # -------------------------------------------------------------------------------------
    async def sampling_definition_registry_update(self, ce: CloudEvent, resource: str):
        try:   
            self.logger.debug("sampling_definition_registry_update", extra={"cd-data": ce.data})
            for definition_type, resource_def in ce.data.items():
                if definition_type not in [f"{resource}", f"{resource}-definition", f"{resource}-definition-update"]:
                    continue
                
                # Setup safe references to all possible data locations
                metadata = resource_def.get("metadata", {})
                data_block = resource_def.get("data", {})
                attributes = data_block.get("attributes", {})

                # Ensure a metadata block exists in the object we save to Redis
                if "metadata" not in resource_def:
                    resource_def["metadata"] = {}

                # FIX: Safely extract 'name' using the helper, cascading through all possible locations
                name = (
                    self._extract_val(resource_def, "name", None) or 
                    self._extract_val(metadata, "name", None) or 
                    self._extract_val(attributes, "name", "unknown")
                )

                # FIX: Safely extract 'valid_config_time' using the helper, cascading through all possible locations
                valid_time = (
                    self._extract_val(resource_def, "valid_config_time", None) or 
                    self._extract_val(metadata, "valid_config_time", None) or 
                    self._extract_val(attributes, "valid_config_time", "2020-01-01T00:00:00Z")
                )

                # Re-inject the perfectly extracted flat strings back into the metadata block
                # so redis_client.py can reliably build the ID: f"{name}::{valid_time}"
                resource_def["metadata"]["name"] = name
                resource_def["metadata"]["valid_config_time"] = valid_time

                if self.db_client:
                    await self.db_client.sampling_definition_registry_update(
                        resource=resource,
                        database="registry",
                        collection=f"{resource}-definition",
                        request=resource_def,
                        ttl=0
                    )
        except Exception as e:
            self.logger.error(f"sampling_definition_registry_update:{resource}", extra={"reason": str(e)})

    async def sampling_definition_registry_get_ids(self, resource: str) -> dict:
        if self.db_client:
            return await self.db_client.sampling_definition_registry_get_ids(resource)
        return {"results": []}

    async def sampling_definition_registry_get(self, resource: str, query: dict) -> dict:
        if self.db_client:
            return await self.db_client.sampling_definition_registry_get(resource, query)
        
        return {"results": []}

    # -------------------------------------------------------------------------------------
    # VARIABLE SET TELEMETRY DATA
    # -------------------------------------------------------------------------------------
    async def variableset_data_update(self, ce: CloudEvent):
        try:
            database = "data"
            collection = "variableset"
            
            self.logger.debug("variableset_data_update")
            data = ce.data
            attributes = data.get("attributes", {})
            dimensions = data.get("dimensions", {})
            variables = data.get("variables", {})
            
            # ---------------------------------------------------------
            # FIX: Safely extract the nested ["data"] telemetry fields
            # ---------------------------------------------------------
            platform = attributes.get("platform", {}).get("data", "unknown")
            vmap_name = attributes.get("variablemap", {}).get("data", "unknown")
            vmap_time = attributes.get("valid_config_time", {}).get("data", "2020-01-01T00:00:00Z")

            # Reconstruct the parent VariableMap ID
            variablemap_id = f"{platform}::{vmap_name}::{vmap_time}"
            
            # The time is attached as a variable object in the variablesets loop
            timestamp_str = variables.get("time", {}).get("data")
            timestamp = string_to_timestamp(timestamp_str) if timestamp_str else 0.0

            # Reconstruct ID from the cloud event source 
            source_parts = ce.get("source", "").split(".")
            variableset_id = source_parts[-1] if len(source_parts) > 0 else "unknown"
            
            request = VariableSetDataUpdate(
                variableset_id=variableset_id,
                variablemap_id=variablemap_id, # <--- Successfully populated!
                variableset=variableset_id.split("::")[-1] if "::" in variableset_id else "unknown",
                timestamp=timestamp,
                attributes=attributes,
                dimensions=dimensions,
                variables=variables,
            )
            self.logger.debug("variableset_data_update", extra={"req": request})
            if self.db_client:
                await self.db_client.variableset_data_update(
                    database=database,
                    collection=collection,
                    request=request,
                    ttl=self.config.db_data_ttl,
                )
            
                # If the variableset is sending data, it is currently active.
                instance_request = VariableSetInstanceUpdate(
                    variableset_id=request.variableset_id,
                    variablemap_id=request.variablemap_id,
                    variableset=request.variableset,
                    attributes=attributes,
                )
                
                await self.db_client.variableset_instance_registry_update(
                    database="registry",
                    collection="variableset-instance",
                    request=instance_request,
                    ttl=self.config.db_reg_variableset_instance_ttl,
                )
                
        except Exception as e:
            self.logger.error("variableset_data_update", extra={"reason": e})

    async def variableset_data_get(self, query: VariableSetDataRequest):
        if query.start_time:
            query.start_timestamp = string_to_timestamp(query.start_time)
        if query.end_time:
            query.end_timestamp = string_to_timestamp(query.end_time)
        if query.last_n_seconds:
            start_dt = get_datetime_with_delta(-(query.last_n_seconds))
            query.start_timestamp = start_dt.timestamp()
            query.end_timestamp = None

        if self.db_client:
            return await self.db_client.variableset_data_get(query)

        return {"results": []}
    
    async def variableset_instance_registry_get_ids(self) -> dict:
        if self.db_client:
            return await self.db_client.variableset_instance_registry_get_ids()
        return {"results": []}

    async def variableset_instance_registry_get(self, query: VariableSetInstanceRequest) -> dict:
        if self.db_client:
            return await self.db_client.variableset_instance_registry_get(query)
        return {"results": []}

async def shutdown():
    print("shutting down")

async def main(config):
    config = uvicorn.Config(
        "main:app",
        host=config.host,
        port=config.port,
        root_path="/msp/datastore",
    )

    server = uvicorn.Server(config)
    L.info(f"server: {server}")
    await server.serve()

    print("starting shutdown...")
    await shutdown()
    print("done.")


if __name__ == "__main__":
    config = DatastoreConfig()

    try:
        index = sys.argv.index("--host")
        host = sys.argv[index + 1]
        config.host = host
    except (ValueError, IndexError):
        pass

    try:
        index = sys.argv.index("--port")
        port = sys.argv[index + 1]
        config.port = int(port)
    except (ValueError, IndexError):
        pass

    try:
        index = sys.argv.index("--log_level")
        ll = sys.argv[index + 1]
        config.log_level = ll
    except (ValueError, IndexError):
        pass

    asyncio.run(main(config))