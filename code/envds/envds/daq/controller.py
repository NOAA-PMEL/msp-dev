import abc
import importlib
from ulid import ULID
import asyncio

import logging
from logfmter import Logfmter
import traceback

from pydantic import BaseModel, validator
from typing import Any

from envds.core import envdsBase, envdsStatus
from envds.message.message import Message
from envds.daq.types import DAQEventType as det
from envds.daq.event import DAQEvent
from envds.daq.client import DAQClientConfig

from envds.util.util import get_datetime, seconds_elapsed
from cloudevents.http import CloudEvent

from envds.util.util import (
    get_datetime_format,
    time_to_next,
    get_datetime,
    get_datetime_string,
)

class ControllerClientConfig(BaseModel):
    controller: dict

class ControllerPathConfig(BaseModel):
    name: str
    path: str
    modes: list | None = ["default"]


class ControllerPath(object):
    """docstring for ControllerPath."""

    def __init__(self, config=None):
        super(ControllerPath, self).__init__()

        # message buffers and loops
        self.send_buffer = asyncio.Queue()
        self.recv_buffer = asyncio.Queue()
        self.message_tasks = [
            asyncio.create_task(self.send_loop()),
            asyncio.create_task(self.recv_loop()),
        ]

        self.client = None

    async def send(self, data: str):
        await self.send_buffer.put(data)

    async def send_loop(self):
        while True:
            data = await self.send_buffer.get()
            await self.send_data(data)
            await asyncio.sleep(0.1)

    async def send_data(self, data: str):
        if self.client:
            await self.client.send(data)
        pass

    async def recv(self) -> str:
        return await self.recv_buffer.get()

    async def recv_loop(self) -> str:
        while True:
            data = await self.recv_data()
            await self.recv_buffer.put(data)
            await asyncio.sleep(0.1)

    async def recv_data(self):
        return await self.client.recv()

    # async def _data(self, data: str):
    #     pass


class ControllerConfig_old(BaseModel):
    """docstring for SensorConfig."""

    type: str
    name: str
    uid: str
    paths: dict | None = {}

class ControllerAttribute(BaseModel):
    # name: str
    type: str | None = "str"
    data: Any

    @validator("data")
    def data_check(cls, v, values):
        if "type" in values:
            data_type = values["type"]
            if data_type == "char" or data_type == "string":
                data_type = "str"

            if "type" in values and not isinstance(v, eval(data_type)):
                raise ValueError("attribute data is wrong type")
        return v


class ControllerVariable(BaseModel):
    """docstring for DeviceVariable."""

    # name: str
    type: str | None = "str"
    shape: list[str] | None = ["time"]
    attributes: dict[str, ControllerAttribute]
    # attributes: dict | None = dict()
    # modes: list[str] | None = ["default"]


class ControllerSetting(BaseModel):
    """docstring for DeviceSetting."""

    # name: str
    type: str | None = "str"
    shape: list[str] | None = ["time"]
    attributes: dict[str, ControllerAttribute]
    # attributes: dict | None = dict()
    # modes: list[str] | None = ["default"]

class ControllerMetadata(BaseModel):
    """docstring for DeviceMetadata."""

    attributes: dict[str, ControllerAttribute]
    dimensions: dict[str, int | None]
    variables: dict[str, ControllerVariable]
    settings: dict[str, ControllerSetting]


class ControllerConfig(BaseModel):
    """docstring for DeviceConfig."""

    make: str
    model: str
    serial_number: str
    format_version: str
    metadata: ControllerMetadata
    host: str
    port: int
    # # variables: list | None = []
    # attributes: dict[str, DeviceAttribute]
    # variables: dict[str, DeviceVariable]
    # # # variables: dict | None = {}
    # settings: dict[str, DeviceSetting]
    # interfaces: dict | None = {}
    daq_id: str | None = "default"


class RuntimeSettings(object):
    """docstring for RuntimeSettings."""

    def __init__(self, settings: dict = None):

        if settings is None:
            self.settings = dict()

    def add_setting(self, name: str, requested, actual=None):
        if name not in self.settings:
            self.settings[name] = dict()
        
        self.settings[name]["requested"] = requested
        self.settings[name]["actual"] = actual

    def set_requested(self, name: str, requested) -> bool:
        return self.update_setting(name=name, requested=requested)
    
    def set_actual(self, name: str, actual) -> bool:
        return self.update_setting(name=name, actual=actual)

    def update_setting(self, name: str, requested=None, actual=None) -> bool:
        # print(f"update_setting1: {name}, {requested}, {actual}, {self.settings}")
        if name not in self.settings:
            return False
        
        # print(f"update_setting2: {name}, {requested}, {actual}, {self.settings}")
        if requested is not None:
            # print(f"update_setting3: {name}, {requested}, {actual}, {self.settings}")
            self.settings[name]["requested"] = requested
            # print(f"update_setting4: {name}, {requested}, {actual}, {self.settings}")
        if actual is not None:
            # print(f"update_setting5: {name}, {requested}, {actual}, {self.settings}")
            self.settings[name]["actual"] = actual
            # print(f"update_setting6: {name}, {requested}, {actual}, {self.settings}")
        # print(f"update_setting7: {name}, {requested}, {actual}, {self.settings}")
        return True
    
    def get_setting(self, name: str):
        if name in self.settings:
            return self.settings[name]
        return None

    def get_settings(self):
        return self.settings

    def get_health_setting(self, name: str) -> bool:
        if (setting := self.get_setting(name)) is not None:
            return setting["requested"] == setting["actual"]
        return False

    def get_health(self) -> bool:
        for name in self.settings.keys():
            if not self.get_health_setting(name):
                return False
        return True

class Controller(envdsBase):
    """docstring for Controller."""
    CONNECTED = "connected"    

    def __init__(self, config=None, **kwargs):
        super(Controller, self).__init__(config=config, **kwargs)

        self.default_client_module = "unknown"
        self.default_client_class = "unknown"

        self.min_recv_delay = 0.1

        self.update_id("app_group", "controller")
        self.update_id("app_ns", "envds")
        # self.update_id("app_uid", f"controller-id-{ULID()}")
        self.update_id("app_uid", f"make-model-{ULID()}")
        self.logger.debug("controller id", extra={"self.id": self.id})

        self.status.set_id_AppID(self.id)

        self.settings = RuntimeSettings()

        self.client_registry = {}
        self.client_map = {}
        self.multistep_data = []

        self.client_recv_buffer = asyncio.Queue()
        self.client_send_buffer = asyncio.Queue()
        self.client = None
        self.client_config = dict()

        # self.run_task_list.append(self.client_monitor())
        # self.run_task_list.append(self.client_registry_monitor())
        self.run_task_list.append(self.register_controller_definition())
        self.run_task_list.append(self.register_controller_instance())
        self.run_task_list.append(self.settings_monitor())
        # self.run_task_list.append(self.client_recv_loop())
        self.run_task_list.append(self.client_monitor())
        self.run_task_list.append(self.client_recv_loop())

        self.controller_definition_registered = False
        self.controller_definition_send_time = 5 # start with every 5 seconds and change once ack
        self.controller_registered = False

    def configure(self):
        super(Controller, self).configure()
        self.logger.debug("configure()")

    # can be overridden if metadata in another place
    def get_metadata(self):
        return self.metadata

    def run_setup(self):
        super().run_setup()

        self.logger = logging.getLogger(self.build_app_uid())
        self.update_id("app_uid", self.build_app_uid())


        # for name, path in self.config.paths.items():
        #     if name not in self.client_map:
        #         self.client_map[name] = {
        #             "client_id": name,
        #             "client": None,
        #             "recv_handler": self.config.paths[name]["recv_handler"],
        #             "recv_task": None,
        #         }

        self.logger.debug("run_setup", extra={"client_map": self.client_map})
        # self.update_id("app_uid", self.build_app_uid())

    async def client_recv_loop(self):
        while True:
            if self.client:
                # data = await self.client.recv_from_client()
                data = await self.client.recv()
                self.logger.debug("client_recv_loop", extra={"recv": data})
                await self.client_recv_buffer.put(data)
            await asyncio.sleep(.01)

    # async def client_send_loop(self):
    #     while True:
    #         if self.client:
    #             data = await self.client_send_buffer.get()
    #             self.client.send_to_client(data)
    #         await asyncio.sleep(.01)
    # async def client_recv_loop(self):
    #     while True:

    async def register_controller_definition(self):
        while True:
            if not self.controller_definition_registered:
                try:
                    event = DAQEvent.create_controller_definition_registry_update(
                        # source="device.mockco-mock1-1234", data=record
                        source=self.get_id_as_source(),
                        # data={"device-definition": self.metadata},
                        data = {"controller-definition": self.metadata},
                    )
                    # destpath = f"{self.get_id_as_topic()}/registry/update"
                    destpath = f"envds/{self.core_settings.namespace_prefix}/controller-definition/registry/update"
                    self.logger.debug(
                        "register_controller_definition", extra={"data": event, "destpath": destpath}
                    )
                    event["destpath"] = destpath
                    # message = Message(data=event, destpath=destpath)
                    message = event
                    # self.logger.debug("default_data_loop", extra={"m": message})
                    await self.send_message(message)
                except Exception as e:
                    self.logger.error("register_controller_definition", extra={"reason": e})
                    print(traceback.format_exc())
            await asyncio.sleep(self.controller_definition_send_time)

    async def register_controller_instance(self):
        while True:
        
            # if self.enabled and not self.device_registered:
            # if not self.device_registered:
            if not self.controller_registered:
                
                instance_reg = {
                    "make": self.config.make,
                    "model": self.config.model,
                    "serial_number": self.config.serial_number,
                    "format_version": self.metadata["attributes"]["format_version"]["data"],
                    "attributes": self.metadata["attributes"]
                }

                event = DAQEvent.create_controller_registry_update(
                    # source="device.mockco-mock1-1234", data=record
                    source=self.get_id_as_source(),
                    data={"controller-instance": instance_reg},
                )
                # destpath = f"{self.get_id_as_topic()}/registry/update"
                destpath = f"envds/{self.core_settings.namespace_prefix}/controller-instance/registry/update"
                self.logger.debug(
                    "register_controller_instance", extra={"data": event, "destpath": destpath}
                )
                event["destpath"] = destpath
                # message = Message(data=event, destpath=destpath)
                message = event
                # self.logger.debug("default_data_loop", extra={"m": message})
                await self.send_message(message)
        
            await asyncio.sleep(5)

    def disable(self):

        # TODO set all settings to default?

        # remove all subscribers to each client to force disable
        # for id, client in self.client_registry.items():
        #     if self.client_map[id]["client"]:
        #         self.client_map[id]["client"].disable()
        #         if self.client_map[id]["recv_task"]:
        #             # TODO: these should go in disable logic
        #             self.client_map[id]["recv_task"].cancel()
        #             self.client_map[id]["recv_task"] = None
        #             self.client_map[id]["recv_handler"] = None
            # if client:
            #     client = dict()

        super().disable()

    def get_make(self):
        return self.config.make

    def get_model(self):
        return self.config.model

    def get_serial_number(self):
        return self.config.serial_number

    def get_host(self):
        return self.config.host

    def build_app_uid(self):
        parts = [self.get_make(), self.get_model(), self.get_serial_number()]
        return (self.ID_DELIM).join(parts)

    # def build_app_uid(self):
    #         parts = [self.config.type, self.config.name, self.config.uid]
    #         return (Controller.ID_DELIM).join(parts)

    def set_routes(self, enable: bool = True):
        print(f"set_core_routes:1")
        super(Controller, self).set_routes()
        print(f"set_core_routes:2")

        topic_base = self.get_id_as_topic()
        self.logger.debug("set_core_routes:controller", extra={"topic_base": topic_base})

        # self.set_route(
        #     subscription=f"{topic_base}/+/status/request",
        #     route_key=det.controller_status_request(),
        #     route=self.handle_status,
        #     enable=enable,
        # )

        # self.logger.debug(
        #     "set_config_request", extra={"sub": f"/{topic_base}/+/config/request"}
        # )
        # self.set_route(
        #     subscription=f"{topic_base}/+/config/request",
        #     route_key=det.controller_config_request(),
        #     route=self.handle_config,
        #     enable=enable,
        # )

        # self.set_route(
        #     subscription=f"{topic_base}/+/keepalive/request",
        #     route_key=det.controller_keepalive_request(),
        #     route=self.handle_keepalive,
        #     enable=enable,
        # )

        # self.set_route(
        #     subscription=f"{topic_base}/+/data/send",
        #     route_key=det.controller_data_send(),
        #     route=self.handle_data,
        #     enable=enable,
        # )

        self.set_route(
            subscription=f"{topic_base}/settings/request",
            route_key=det.sensor_settings_request(),
            route=self.handle_settings,
            enable=enable,
        )

        self.set_route(
            subscription=f"envds/controller/settings/request",
            route_key=det.sensor_settings_request(),
            route=self.handle_settings,
            enable=enable,
        )

        topic = f"envds/{self.core_settings.namespace_prefix}/controller/registry/ack"
        self.set_route(
            # subscription=f"{topic_base}/registry/ack",
            subscription = topic,
            route_key=det.device_definition_registry_ack(),
            route=self.handle_registry,
            enable=enable
        )

        self.set_route(
            # subscription=f"{topic_base}/+/control/request",
            subscription="webinterface/control/request",
            route_key=det.controller_control_request(),
            route=self.handle_controls,
            enable=enable,
        )

    async def settings_monitor(self):

        send_settings = True
        while True:
            try:
                await self.settings_check()
            except Exception as e:
                self.logger.error("settings_monitor", extra={"error": e})

            if self.enabled() and send_settings:
                # send settings every other second
                event = DAQEvent.create_sensor_settings_update(
                    # source="device.mockco-mock1-1234", data=record
                    source=self.get_id_as_source(),
                    data={"settings": self.settings.get_settings()},
                )
                destpath = f"{self.get_id_as_topic()}/settings/update"
                self.logger.debug(
                    "settings_monitor", extra={"data": event, "destpath": destpath}
                )
                event["destpath"] = destpath
                # message = Message(data=event, destpath=destpath)
                message = event
                # self.logger.debug("default_data_loop", extra={"m": message})
                await self.send_message(message)

            send_settings = not send_settings
            await asyncio.sleep(1)

    # each device should handle this as required
    async def settings_check(self):
        pass



    # def update_client_registry(
    #     self,
    #     client_id: str,
    #     source: str,
    #     keepalive: bool = False,
    #     deregister: bool = False,
    # ):
    #     self.logger.debug(
    #         "update_client_registry",
    #         extra={"client_id": client_id, "source": source, "keepalive": keepalive},
    #     )
    #     try:
    #         if deregister:
    #             del self.client_registry[client_id][source]
    #         elif keepalive:
    #             self.client_registry[client_id][source]["last_update"] = get_datetime()
    #         else:
    #             if client_id not in self.client_registry:
    #                 self.client_registry[client_id] = dict()
    #             if source not in self.client_registry[client_id]:
    #                 self.client_registry[client_id][source] = dict()
    #             self.client_registry[client_id][source]["last_update"] = get_datetime()
    #             # self.logger.debug(
    #             #     "client_registry", extra={"reg": self.client_registry}
    #             # )
    #     except KeyError:
    #         pass

    # async def client_registry_monitor(self):

    #     registry_expiration = 60  # if no activity in 5 minutes, expire the connection
    #     while True:
    #         try:
    #             for id, client in self.client_registry.items():
    #                 # self.logger.debug(
    #                 #     "registry_monitor", extra={"client_id": id, "client": client}
    #                 # )
    #                 for key in list(client.keys()):
    #                     # if time_expired, del client[key]
    #                     # self.logger.debug("reg monitor", extra={"key": key})
    #                     if (
    #                         seconds_elapsed(client[key]["last_update"])
    #                         > registry_expiration
    #                     ):
    #                         del client[key]
    #                 self.logger.debug(
    #                     "client_registry_monitor",
    #                     extra={"id": id, "connections": len(client)},
    #                 )
    #                 if (
    #                     len(client) == 0
    #                 ):  # and self.client_map[client_id].client.connected():
    #                     self.logger.debug("registry_monitor:2")
    #                     self.client_map[id]["client"].disable()
    #                     # if self.client_map[id]["recv_task"]:
    #                     #     # TODO: these should go in disable logic
    #                     #     self.client_map[id]["recv_task"].cancel()
    #                     #     self.client_map[id]["recv_task"] = None
    #                 else:
    #                     self.logger.debug("registry_monitor:3", extra={"client_map": self.client_map})
    #                     # enable client if needed
    #                     if not self.client_map[id]["client"].enabled():
    #                         self.client_map[id]["client"].enable()

    #                     self.logger.debug("registry_monitor:4")
    #                     # if self.client_map[id]["recv_task"] is None:
    #                     #     self.client_map[id]["recv_task"] = asyncio.create_task(
    #                     #         self.client_map[id]["recv_handler"]
    #                     #     )
    #                     #     self.logger.debug(
    #                     #         "create recv_task",
    #                     #         extra={"handler": self.client_map[id]["recv_handler"]},
    #                     #     )
    #                     self.logger.debug("registry_monitor:5")

    #                     # send client status update
    #                     destpath = f"{self.get_id_as_topic()}/{id}/status/update"
    #                     extra_header = {"path_id": id}
    #                     event = DAQEvent.create_status_update(
    #                         # source="envds.core", data={"test": "one", "test2": 2}
    #                         source=self.get_id_as_source(),
    #                         data=self.status.get_status(),
    #                         extra_header=extra_header,
    #                     )
    #                     event["destpath"] = destpath
    #                     self.logger.debug("status update", extra={"event": event})
    #                     # message = Message(data=event, destpath=destpath)
    #                     message = event
    #                     await self.send_message(message)
    #         except Exception as e:
    #             self.logger.error("client_registry_monitor", extra={"reg_error": e})
    #         await asyncio.sleep(2)

    async def handle_registry(self, message: CloudEvent):

        self.logger.debug("handle_registry", extra={"ce-type": message["type"]})
        # if message.data["type"] == det.sensor_registry_update():
        if message["type"] == det.controller_definition_registry_request():
            controller_id = message.data.get("controller-definition", None)
            if controller_id:
                if controller_id["make"] == self.config.make and controller_id["model"] == self.config.model:
                    self.device_definition_registered = False

        elif message["type"] == det.controller_definition_registry_ack():
            controller_id = message.data.get("controller-definition", None)
            if controller_id:
                self.logger.debug("handle_registry", extra={"make": self.config.make, "model": self.config.model})
                if controller_id["make"] == self.config.make and controller_id["model"] == self.config.model:
                    # self.device_definition_registered = True
                    self.device_definition_send_time = 60 # increase time between sends but don't actually stop
            self.logger.debug(
                "handle_registry",
                extra={
                    "controller_id": controller_id,
                    "registered": self.device_definition_registered,
                },
            )

    async def handle_settings(self, message: CloudEvent):
        # if message.data["type"] == det.sensor_settings_request():
        if message["type"] == det.controller_settings_request():
            try:
                # src = message.data["source"]
                # setting = message.data.data.get("settings", None)
                # requested = message.data.data.get("requested", None)
                src = message["source"]
                setting = message.data.get("settings", None)
                requested = message.data.get("requested", None)
                self.logger.debug(
                    "handle_settings", extra={"source": src, "setting": setting}
                )
                if setting and requested:
                    # name = setting["settings"]
                    # current = self.settings.get_setting(setting)
                    # self.settings.set_setting(
                    #     name=setting, requested=requested, actual=current["actual"]
                    # )
                    self.settings.set_requested(
                        name=setting, requested=requested
                    )

            except (KeyError, Exception) as e:
                self.logger.error("databuffer save error", extra={"error": e})

    # async def handle_config(self, message: Message):
    async def handle_config(self, message: CloudEvent):
        # self.logger.debug("interface.handle_config", extra={"config": message.data})

        # if message.data["type"] == det.interface_config_request():
        #     try:
        #         client_id = message.data["path_id"]
        #         # source = message.data["source"]
        #         sensor_interface_properties = message.data.data["config"][
        #             "device-interface-properties"
        #         ]

        #         self.client_map[client_id]["client"].set_sensor_interface_properties(
        #             iface_props=sensor_interface_properties
        #         )
        #         self.logger.debug("handle_config", extra={"client_map": self.client_map})
        #     except KeyError:
        #         self.logger.error("handle_config error", extra={"data": message.data})

        # # self.logger.debug("handle_status:1", extra={"data": message.data})
        # # await super(Interface, self).handle_status(message)
        pass

    # def enable(self):
    #     super().enable()
    #     self.status.set_requested(envdsStatus.ENABLED, envdsStatus.TRUE)
    #     # self.status.set_state_param(requested=envdsStatus.TRUE,
    #     #             actual=envdsStatus.TRUE)



    # async def handle_status(self, message: Message):
    async def handle_status(self, message: CloudEvent):
        # pass

        # self.logger.debug("handle_status:1", extra={"data": message.data})
        await super(Controller, self).handle_status(message)

        # if message.data["type"] == det.interface_status_request():
        #     self.logger.debug("interface connection keepalive", extra={"source": message.data["source"]})
        #     # update connection registry
        # self.logger.debug("interface handle_status", extra={"data": message.data})
        
        # if message.data["type"] == det.controller_status_request():
        if message["type"] == det.controller_status_request():
            try:
                # client_id = message.data["path_id"]
                # source = message.data["source"]
                # # sourcepath = message["sourcepath"]
                # state = message.data.data["state"]
                # requested = message.data.data["requested"]
                client_id = message["path_id"]
                source = message["source"]
                # sourcepath = message["sourcepath"]
                state = message.data["state"]
                requested = message.data["requested"]


                # if state := message.data["state"] == envdsStatus.ENABLED:
                if state == envdsStatus.ENABLED:
                    self.logger.debug(
                        # "interface status request", extra={"data": message.data}
                        "interface status request", extra={"data": message}
                    )
                    # self.update_client_registry(Message)
                    deregister = False
                    if requested != envdsStatus.TRUE:
                        deregister = True
                    self.update_client_registry(
                        client_id=client_id, source=source, deregister=deregister
                    )
                
                    #    self.register_client(data=message.data, sourcepath=message["sourcepath"])
                if requested == envdsStatus.TRUE:
                    print(f"id_as_topic: {self.get_id_as_topic()}")
                    self.enable()
                elif requested == envdsStatus.FALSE:
                    self.disable()

            except KeyError:
                self.logger.error(
                    # "unknown interface status request", extra={"data": message.data}
                    "unknown interface status request", extra={"data": message}
                )

    # async def handle_keepalive(self, message: Message):
    async def handle_keepalive(self, message: CloudEvent):
        pass

        # if message.data["type"] == det.interface_keepalive_request():

        #     try:
        #         client_id = message.data["path_id"]
        #         source = message.data["source"]
        #         # sourcepath = message["sourcepath"]
        #         # state = message.data["state"]
        #         # requested = message.data["requested"]
        #         self.update_client_registry(
        #             client_id=client_id, source=source, keepalive=True
        #         )
        #     except KeyError:
        #         self.logger.error(
        #             "unknown keepalive request", extra={"data": message.data}
        #         )

        #     self.logger.debug(
        #         "interface keepalive request", extra={"source": message.data["source"]}
        #     )
        #     # self.update_client_registry(Message)
        #     # update connection registry

        # # elif message.data["type"] == det.interface_connect_request():
        # #     self.logger.debug("interface connection request", extra={"data": message.data})
        # #     self.update_client_registry(Message)
        # #         #    self.register_client(data=message.data, sourcepath=message["sourcepath"])


    async def update_recv_data(self, client_id: str, data: dict):
        # self.logger.debug("update_recv_data", extra={"client_id": client_id, "data": data})
        destpath = f"{self.get_id_as_topic()}/{client_id}/data/update"
        # extra_header = {"sourcepath": id}
        # extra_header = {"path_id": client_id}
        extra_header = {"path_id": client_id, "destpath": destpath}
        # event = DAQEvent.create_data_update(
        event = DAQEvent.create_controller_data_update(
            # source="envds.core", data={"test": "one", "test2": 2}
            source=self.get_id_as_source(),
            data=data,
            extra_header=extra_header,
        )
        self.logger.debug("data update", extra={"event": event})
        # message = Message(data=event, destpath=destpath)
        message = event
        await self.send_message(message)
    
    async def send_data(self, event: DAQEvent):
        pass

    # async def handle_data(self, message: Message):
    async def handle_data(self, message: CloudEvent):

        # await super(Interface, self).handle_data(message)

        # TODO: handle send data
        # print(f"handle_data: {message.data}")
        print(f"handle_data: {message}")
        # if message.data["type"] == det.controller_data_send():
        #     self.logger.debug(
        #         "controller_data_send",
        #         extra={"data": message.data.data},
        #     )
        #     self.logger.debug("handle_data", extra={"md": message.data})
        #     await self.send_data(message.data)
        #     self.logger.debug("handle_data sent", extra={"md": message.data})
        if message["type"] == det.controller_data_send():
            self.logger.debug(
                "controller_data_send",
                extra={"data": message.data},
            )
            self.logger.debug("handle_data", extra={"md": message})
            await self.send_data(message)
            self.logger.debug("handle_data sent", extra={"md": message})



    # async def handle_controls(self, message: Message):
    async def handle_controls(self, message: CloudEvent):
        # if message.data["type"] == det.controller_control_request():
        #     self.logger.debug(
        #         "controller_webinterface_command",
        #         extra={"data": message.data.data},
        #     )
        #     self.logger.debug("webinterface_command", extra={"md": message.data})
        #     await self.send_data(message.data)
        #     self.logger.debug("webinterface_command sent", extra={"md": message.data})
        if message["type"] == det.controller_control_request():
            self.logger.debug(
                "controller_webinterface_command",
                extra={"data": message.data},
            )
            self.logger.debug("webinterface_command", extra={"md": message})
            await self.send_data(message)
            self.logger.debug("webinterface_command sent", extra={"md": message})


    async def client_monitor(self):

        while True:
            try:
                self.logger.debug("client_monitor:out", extra={"client": self.client})
                if self.client is None:
                    try:
                        self.logger.debug("client_monitor: 1")
                        client_module = self.client_config["client_module"]
                        self.logger.debug("client_monitor: 2")
                        client_class = self.client_config["client_class"]
                        self.logger.debug("client_monitor: 3")

                        client_id = f'{self.client_config["properties"]["host"]}::{self.get_id_string()}'
                        self.logger.debug("client_monitor: 4")
                        client_config = DAQClientConfig(
                            uid=client_id,
                            properties=self.client_config["properties"].copy(),
                        )
                        self.logger.debug("client_monitor: 5")
                        mod_ = importlib.import_module(client_module)
                        # print(f"here:5 {client_module}, {client_class}, {mod_}")
                        # path["client"] = getattr(mod_, client_class)(config=client_config)
                        self.logger.debug("client_monitor: 6")
                        cls_ = getattr(mod_, client_class)
                        # print(f"here:5.5 {cls_}")
                        self.logger.debug("client_monitor: 7")
                        self.client = cls_(config=client_config)
                        self.logger.debug("client_monitor: 8")

                        # TODO: where to start "run"?
                        await asyncio.sleep(1)
                        self.client.run()
                        self.logger.debug("client_monitor:in", extra={"client": self.client})

                        self.logger.debug("client_monitor: 9")
                    except (KeyError, ModuleNotFoundError, AttributeError) as e:
                        self.logger.error(
                            "client_monitor: could not create client",
                            extra={"error": e},
                        )
                        self.client = None
                    #     self.logger.debug(
                    #         "client_monitor", extra={"client_map": self.client_map}
                    #     )
                    # self.logger.debug("client monitor", extra={"id": id, "path": path})

            except Exception as e:
                self.logger.error("client_monitor", extra={"error": e})
            await asyncio.sleep(5)

    def build_data_record(
        self, meta: bool = False, variable_types: list[str] = ["main"]
    ) -> dict:
        # TODO: change data_format -> format_version
        # TODO: create record for any number of variable_types
        record = {
            # "time": get_datetime_string(),
            "timestamp": get_datetime_string(),
            # "instance": {
            #     "serial_number": self.config.serial_number,
            #     "sampling_mode": mode,
            # }
        }
        # print(record)
        if meta:
            record["attributes"] = self.config.metadata.dict()["attributes"]
            # print(record)
            record["attributes"]["serial_number"] = {
                "type": "char",
                "data": self.config.serial_number,
            }
            record["attributes"]["mode"] = {"type": "char", "data": "default"}
            record["attributes"]["variable_types"] = {
                "type": "string",
                "data": ",".join(variable_types),
            }
        else:
            record["attributes"] = {
                "make": {"data": self.config.make},
                "model": {"data": self.config.model},
                "serial_number": {"data": self.config.serial_number},
                "mode": {"data": "default"},
                "format_version": {"data": self.config.format_version},
                "variable_types": {"data": ",".join(variable_types)},
            }
        # record["attributes"]["serial_number"] = {"data": self.config.serial_number}
        # record["attributes"]["mode"] = {"data": "default"}
        # record["attributes"]["variable_types"] = {"data": ",".join(variable_type)}

        # print(record)

        #     "variables": {},
        # }

        record["dimensions"] = {"time": 1}
        record["variables"] = dict()

        # record["variables"] = dict()
        if meta:
            for name, variable in self.config.metadata.dict()["variables"].items():
                variable_type = variable["attributes"].get(
                    "variable_type", {"type": "string", "data": "main"}
                )
                if variable_type["data"] in variable_types:
                    record["variables"][name] = self.config.metadata.dict()[
                        "variables"
                    ][name]

            # record["variables"] = self.config.metadata.dict()["variables"]

            # print(1, record)
            for name, _ in record["variables"].items():
                record["variables"][name]["data"] = None
            # print(2, record)
        else:
            for name, variable in self.config.metadata.dict()["variables"].items():
                variable_type = variable["attributes"].get(
                    "variable_type", {"type": "string", "data": "main"}
                )
                print(f"variable_type: {variable_type}, {variable_types}")
                if variable_type["data"] in variable_types:
                    print(f"name: {name}")
                    record["variables"][name] = {
                        "attributes": {"variable_type": variable_type},
                        "data": None,
                    }
            # print(3, record)

            # record["variables"] = dict()
            # for name,_ in self.config.metadata.variables.items():
            #     record["variables"][name] = {"data": None}

        return record

    def get_definition_by_variable_type(
        self, device_def: dict, variable_type: str = "main"
    ) -> dict:

        result = dict()
        if device_def:
            result["attributes"] = device_def["attributes"]
            result["attributes"]["variable_types"] = {
                "type": "string",
                "data": variable_type,
            }
            result["dimensions"] = device_def["dimensions"]
            result["variables"] = dict()
            for name, variable in device_def["variables"].items():
                var_type = variable["attributes"].get(
                    "variable_type", {"type": "string", "data": "main"}
                )
                if var_type["data"] == variable_type:
                    result["variables"][name] = variable
        return result

    def build_settings_record(self, meta: bool = False, mode: str = "default") -> dict:
        # TODO: change data_format -> format_version

        record = {
            # "time": get_datetime_string(),
            "timestamp": get_datetime_string(),
            # "instance": {
            #     "serial_number": self.config.serial_number,
            #     "sampling_mode": mode,
            # }
        }
        # print(record)
        settings_meta = self.get_definition_by_variable_type(device_def=self.get_metadata(), variable_type="setting")

        if settings_meta:
        # if meta:
            record["attributes"] = settings_meta["attributes"]
            # print(record)
            record["attributes"]["serial_number"] = {
                "type": "char",
                "data": self.config.serial_number,
            }
            # record["attributes"]["mode"] = {
            #     "type": "char",
            # }
        else:
            record["attributes"] = {
                "make": {"data": self.config.make},
                "model": {"data": self.config.model},
                "serial_number": {"data": self.config.serial_number},
                # "serial_number": self.config.serial_number,
                # "sampling_mode": mode,
                "format_version": {"data": self.config.format_version},
            }
        # record["attributes"]["serial_number"] = {"data": self.config.serial_number}
        # record["attributes"]["mode"] = {"data": mode}

        # print(record)

        #     "variables": {},
        # }

        # record["variables"] = dict()
        if meta:
            record["settings"] = settings_meta["variables"]
            # print(record)
            for name, _ in record["settings"].items():
                # record["settings"][name]["data"] = None
                record["settings"][name]["data"] = {"requested": None, "actual": None}
            # print(record)
        else:
            record["settings"] = dict()
            for name, _ in settings_meta["variables"].items():
                # record["settings"][name] = {"data": None}
                record["settings"][name] = {"data": {"requested": None, "actual": None}}
        return record

    async def settings_monitor(self):

        send_settings = True
        while True:
            try:
                await self.settings_check()
            except Exception as e:
                self.logger.error("settings_monitor", extra={"error": e})

            self.logger.debug("settings_monitor", extra={"enabled": self.enabled(), "send_settings": send_settings})
            if self.enabled() and send_settings:
                
                record = self.build_settings_record()
                current_settings = self.settings.get_settings()

                for name, setting in record["settings"].items()
                    if name in current_settings:
                        setting[name] = current_settings[name]
                self.logger.debug("settings_monitor", extra={"settings_record": record})

                # send settings every other second
                event = DAQEvent.create_controller_settings_update(
                    # source="device.mockco-mock1-1234", data=record
                    source=self.get_id_as_source(),
                    # data={"settings": self.settings.get_settings()},
                    data=record
                )
                # destpath = f"{self.get_id_as_topic()}/settings/update"
                destpath = f"envds/{self.core_settings.namespace_prefix}/controller/settings/update"
                self.logger.debug(
                    "settings_monitor", extra={"data": event, "destpath": destpath}
                )
                event["destpath"] = destpath
                # message = Message(data=event, destpath=destpath)
                message = event
                # self.logger.debug("default_data_loop", extra={"m": message})
                await self.send_message(message)

            send_settings = not send_settings
            await asyncio.sleep(1)

    # each device should handle this as required
    async def settings_check(self):
        pass

