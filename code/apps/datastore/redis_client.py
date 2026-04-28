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
    VariableMapDefinitionUpdate
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
    async def device_data_update(self, ce: CloudEvent):
        try:
            database = "data"
            collection = "device"
            attributes = ce.data["attributes"]
            dimensions = ce.data["dimensions"]
            variables = ce.data["variables"]

            make = attributes["make"]["data"]
            model = attributes["model"]["data"]
            serial_number = attributes["serial_number"]["data"]

            format_version = attributes["format_version"]["data"]
            parts = format_version.split(".")
            self.logger.debug(f"parts: {parts}, {format_version}")
            erddap_version = f"v{parts[0]}"
            device_id = "::".join([make, model, serial_number])
            self.logger.debug("device_data_update", extra={"device_id": device_id})
            timestamp = string_to_timestamp(ce.data["timestamp"]) 

            self.logger.debug("device_data_update", extra={"timestamp": timestamp, "ce-timestamp": ce.data["timestamp"]})

            request = DataUpdate(
                make=make,
                model=model,
                serial_number=serial_number,
                version=erddap_version,
                timestamp=timestamp,
                attributes=attributes,
                dimensions=dimensions,
                variables=variables,
            )

            self.logger.debug("device_data_update", extra={"request": request})
            await self.db_client.device_data_update(
                database=database,
                collection=collection,
                request=request,
                ttl=self.config.db_data_ttl,
            )
        except Exception as e:
            L.error("device_data_update", extra={"reason": e})
        pass

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
                make = device_def["attributes"]["make"]["data"]
                model = device_def["attributes"]["model"]["data"]
                format_version = device_def["attributes"]["format_version"]["data"]
                parts = format_version.split(".")
                version = f"v{parts[0]}"
                valid_time = device_def.get("valid_time", "2020-01-01T00:00:00Z")

                if definition_type == "device-definition":
                    database = "registry"
                    collection = "device-definition"
                    attributes = device_def["attributes"]
                    dimensions = device_def["dimensions"]
                    variables = device_def["variables"]

                    if "device_type" in device_def["attributes"]:
                        device_type = device_def["attributes"]["device_type"]["data"]
                    else:
                        device_def["attributes"]["device_type"] = {
                            "type": "string",
                            "data": "sensor"
                        }
                        device_type = "sensor"

                    device_definition_id = "::".join([make,model,format_version])
                    request = DeviceDefinitionUpdate(
                        device_definition_id=device_definition_id,
                        make=make,
                        model=model,
                        version=format_version,
                        device_type=device_type,
                        valid_time=valid_time,
                        attributes=attributes,
                        dimensions=dimensions,
                        variables=variables,
                    )

            self.logger.debug(
                "device_definition_registry_update", extra={"request": request}
            )
            if self.db_client:
                result = await self.db_client.device_definition_registry_update(
                    database=database,
                    collection=collection,
                    request=request,
                    ttl=self.config.db_reg_device_definition_ttl,
                )
        except Exception as e:
            self.logger.error("device_definition_registry_update", extra={"reason": e})
        pass

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
    async def controller_data_update(self, ce: CloudEvent):
        try:
            database = "data"
            collection = "controller"
            attributes = ce.data["attributes"]
            dimensions = ce.data["dimensions"]
            variables = ce.data["variables"]

            make = attributes["make"]["data"]
            model = attributes["model"]["data"]
            serial_number = attributes["serial_number"]["data"]

            format_version = attributes["format_version"]["data"]
            parts = format_version.split(".")
            self.logger.debug(f"parts: {parts}, {format_version}")
            erddap_version = f"v{parts[0]}"
            controller_id = "::".join([make, model, serial_number])
            self.logger.debug("controller_data_update", extra={"device_id": controller_id})
            timestamp = string_to_timestamp(ce.data["timestamp"])

            self.logger.debug("device_data_update", extra={"timestamp": timestamp, "ce-timestamp": ce.data["timestamp"]})

            request = ControllerDataUpdate(
                make=make,
                model=model,
                serial_number=serial_number,
                version=erddap_version,
                timestamp=timestamp,
                attributes=attributes,
                dimensions=dimensions,
                variables=variables,
            )

            self.logger.debug("controller_data_update", extra={"request": request})
            await self.db_client.controller_data_update(
                database=database,
                collection=collection,
                request=request,
                ttl=self.config.db_data_ttl,
            )
        except Exception as e:
            L.error("device_data_update", extra={"reason": e})
        pass

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
                make = controller_def["attributes"]["make"]["data"]
                model = controller_def["attributes"]["model"]["data"]
                format_version = controller_def["attributes"]["format_version"]["data"]
                parts = format_version.split(".")
                version = f"v{parts[0]}"
                valid_time = controller_def.get("valid_time", "2020-01-01T00:00:00Z")

                if definition_type == "controller-definition":
                    database = "registry"
                    collection = "controller-definition"
                    attributes = controller_def["attributes"]
                    dimensions = controller_def["dimensions"]
                    variables = controller_def["variables"]

                    controller_definition_id = "::".join([make,model,format_version])
                    request = ControllerDefinitionUpdate(
                        controller_definition_id=controller_definition_id,
                        make=make,
                        model=model,
                        version=format_version,
                        valid_time=valid_time,
                        attributes=attributes,
                        dimensions=dimensions,
                        variables=variables,
                    )

            self.logger.debug(
                "controller_definition_registry_update", extra={"request": request}
            )
            if self.db_client:
                result = await self.db_client.controller_definition_registry_update(
                    database=database,
                    collection=collection,
                    request=request,
                    ttl=self.config.db_reg_controller_definition_ttl,
                )

        except Exception as e:
            self.logger.error("controller_definition_registry_update", extra={"reason": e})
        pass

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

    async def variablemap_definition_registry_update(self, ce: CloudEvent):
        try:
            for definition_type, vm_def in ce.data.items():
                
                # FIX: Check if it's already a flattened database record (from a registrar sync)
                if "variablemap_definition_id" in vm_def:
                    request = VariableMapDefinitionUpdate(**vm_def)
                else:
                    # It's an original payload from sampling-system
                    variablemap = vm_def.get("metadata", {}).get("name")
                    data = vm_def.get("data", {})
                    attributes = data.get("attributes", {})
                    
                    variablemap_type = attributes.get("variablemap_type", "Platform")
                    if variablemap_type == "Platform":
                        variablemap_type_id = attributes.get("platform")
                    else:
                        self.logger.error("variablemap_definition_registry_update", extra={"reason": f"unknown variablemap_type-{variablemap_type}"})
                        continue
                    
                    valid_config_time = attributes.get("valid_config_time", "2020-01-01T00:00:00Z")

                    variablesets = data.get("variablesets", {})
                    variables = data.get("variables", {})

                    variablemap_definition_id = "::".join([variablemap_type_id, variablemap, valid_config_time])
                    request = VariableMapDefinitionUpdate(
                        variablemap_definition_id=variablemap_definition_id,
                        variablemap_type=variablemap_type,
                        variablemap_type_id=variablemap_type_id,
                        variablemap=variablemap,
                        valid_config_time=valid_config_time,
                        attributes=attributes,
                        variablesets=variablesets,
                        variables=variables,
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
                
                # FIX 1: Ensure we only process the correct definition type
                if definition_type != "variableset-definition":
                    continue

                if "variableset_definition_id" in vs_payload:
                    request = VariableSetDefinitionUpdate(**vs_payload)
                else:
                    vs_name = vs_payload.get("metadata", {}).get("name")
                    data = vs_payload.get("data", {})
                    attributes = data.get("attributes", {})
                    
                    variablemap_name = attributes.get("variablemap_id", "")
                    variablemap_type_id = attributes.get("platform", "")
                    valid_config_time = attributes.get("valid_config_time", "2020-01-01T00:00:00Z")
                    
                    variablemap_definition_id = "::".join([variablemap_type_id, variablemap_name, valid_config_time])
                    
                    request = VariableSetDefinitionUpdate(
                        variableset_definition_id=f"{variablemap_definition_id}::{vs_name}",
                        variablemap_definition_id=variablemap_definition_id,
                        variableset=vs_name,
                        index_type=data.get("index_type"),
                        index_value=data.get("index_value"),
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
    async def sampling_definition_registry_update(self, resource: str, ce: CloudEvent):
        try:
            for definition_type, def_data in ce.data.items():
                name = def_data["metadata"]["name"]
                
                # Use a default time if valid_config_time is missing
                valid_config_time = def_data["metadata"].get("valid_config_time", "2020-01-01T00:00:00Z")

                database = "registry"
                collection = f"{resource}-definition"

                # Since these share a generic structure, we can pass the dict directly 
                # instead of creating a strict Pydantic model for each of the 5 types.
                request = def_data 

                self.logger.debug(
                    f"{resource}_definition_registry_update", extra={"request": request}
                )

                if self.db_client:
                    result = await self.db_client.sampling_definition_registry_update(
                        resource=resource,
                        database=database,
                        collection=collection,
                        request=request,
                        ttl=0, # Permanent
                    )

        except Exception as e:
            self.logger.error(f"{resource}_definition_registry_update", extra={"reason": e})

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
            self.logger.debug("variableset_data_update", extra={"ce-data": data})
            attributes = data.get("attributes", {})
            dimensions = data.get("dimensions", {})
            variables = data.get("variables", {})
            self.logger.debug("variableset_data_update", extra={"atts": attributes})
            
            # The time is attached as a variable object in the variablesets loop
            timestamp_str = variables.get("time", {}).get("data")
            timestamp = string_to_timestamp(timestamp_str) if timestamp_str else 0.0

            # Reconstruct ID from the cloud event source (e.g., envds.mspbase01.variableset.MSPPayload03::main)
            source_parts = ce.get("source", "").split(".")
            self.logger.debug("variableset_data_update", extra={"source_parts": source_parts})
            variableset_id = source_parts[-1] if len(source_parts) > 0 else "unknown"
            self.logger.debug("variableset_data_update", extra={"vset_id": data})
            request = VariableSetDataUpdate(
                variableset_id=variableset_id,
                variablemap_id=attributes.get("variablemap_id", ""),
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
    