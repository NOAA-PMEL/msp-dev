import abc
import importlib

# import sys
# import uuid
from ulid import ULID
import asyncio

# import logging
from logfmter import Logfmter

# from typing import Union
from pydantic import BaseModel

from envds.core import envdsBase, envdsStatus
from envds.message.message import Message
from envds.daq.types import DAQEventType as det
from envds.daq.event import DAQEvent
from envds.daq.client import DAQClientConfig

class ControllerClientConfig(BaseModel):
    # controller: dict
    interface: dict


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

    # used by interface to send data to physical sensor
    async def send(self, data: str):
        await self.send_buffer.put(data)

    # processes buffered send data
    async def send_loop(self):

        while True:
            data = await self.send_buffer.get()
            await self.send_data(data)
            await asyncio.sleep(0.1)

    # can be overloaded by InterfacePath to handle client specific data
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


class ControllerConfig(BaseModel):
    """docstring for SensorConfig."""

    type: str
    name: str
    uid: str
    paths: dict | None = {}
    # properties: dict | None = {}
    # # variables: list | None = []
    # variables: dict | None = {}
    # interfaces: dict | None = {}
    # daq: str | None = "default"


class Controller(envdsBase):
    """docstring for Controller."""
    CONNECTED = "connected"    

    def __init__(self, config=None, **kwargs):
        super(Controller, self).__init__(config=config, **kwargs)

        self.default_client_module = "unknown"
        self.default_client_class = "unknown"

        self.min_recv_delay = 0.1

        self.update_id("app_group", "interface")
        self.update_id("app_ns", "envds")
        self.update_id("app_uid", f"interface-id-{ULID()}")
        self.logger.debug("interface id", extra={"self.id": self.id})

        self.status.set_id_AppID(self.id)

        self.client_map = {}
        self.multistep_data = []

    def configure(self):
        super(Controller, self).configure()
        self.logger.debug("configure()")

    def run_setup(self):
        super().run_setup()

        for name, path in self.config.paths.items():
            if name not in self.client_map:
                self.client_map[name] = {
                    "client_id": name,
                    "client": None,
                    "recv_handler": self.config.paths[name]["recv_handler"],
                    "recv_task": None,
                }

        self.logger.debug("run_setup", extra={"client_map": self.client_map})
        self.update_id("app_uid", self.build_app_uid())

    def disable(self):

        # remove all subscribers to each client to force disable
        for id, client in self.client_registry.items():
            if self.client_map[id]["client"]:
                self.client_map[id]["client"].disable()
                if self.client_map[id]["recv_task"]:
                    # TODO: these should go in disable logic
                    self.client_map[id]["recv_task"].cancel()
                    self.client_map[id]["recv_task"] = None
                    self.client_map[id]["recv_handler"] = None
            # if client:
            #     client = dict()

        super().disable()

    def set_core_routes(self, enable: bool = True):
        print(f"set_core_routes:1")
        super(Controller, self).set_core_routes()
        print(f"set_core_routes:2")

        topic_base = self.get_id_as_topic()
        self.logger.debug("set_core_routes:controller", extra={"topic_base": topic_base})

        self.set_route(
            subscription=f"/{topic_base}/+/status/request",
            route_key=det.interface_status_request(),
            route=self.handle_status,
            enable=enable,
        )

        self.logger.debug(
            "set_config_request", extra={"sub": f"/{topic_base}/+/config/request"}
        )
        self.set_route(
            subscription=f"/{topic_base}/+/config/request",
            route_key=det.interface_config_request(),
            route=self.handle_config,
            enable=enable,
        )

        self.set_route(
            subscription=f"/{topic_base}/+/keepalive/request",
            route_key=det.interface_keepalive_request(),
            route=self.handle_keepalive,
            enable=enable,
        )

        self.set_route(
            subscription=f"/{topic_base}/+/data/send",
            route_key=det.interface_data_send(),
            route=self.handle_data,
            enable=enable,
        )

    def update_client_registry(
        self,
        client_id: str,
        source: str,
        keepalive: bool = False,
        deregister: bool = False,
    ):
        pass

    async def client_registry_monitor(self):
        pass

    async def handle_config(self, message: Message):
        pass

    async def handle_status(self, message: Message):
        pass

    async def handle_keepalive(self, message: Message):
        pass

    async def update_recv_data(self, client_id: str, data: dict):
        # self.logger.debug("update_recv_data", extra={"client_id": client_id, "data": data})
        dest_path = f"/{self.get_id_as_topic()}/{client_id}/data/update"
        # extra_header = {"source_path": id}
        extra_header = {"path_id": client_id}
        # event = DAQEvent.create_data_update(
        event = DAQEvent.create_interface_data_recv(
            # source="envds.core", data={"test": "one", "test2": 2}
            source=self.get_id_as_source(),
            data=data,
            extra_header=extra_header,
        )
        self.logger.debug("data update", extra={"event": event})
        message = Message(data=event, dest_path=dest_path)
        await self.send_message(message)

    async def send_data(self, event: DAQEvent):
        pass

    async def handle_data(self, message: Message):

        # await super(Interface, self).handle_data(message)

        # TODO: handle send data
        print(f"handle_data: {message.data}")
        if message.data["type"] == det.interface_data_send():
            self.logger.debug(
                "controller_data_send",
                extra={"data": message.data.data},
            )
            self.logger.debug("handle_data", extra={"md": message.data})
            await self.send_data(message.data)
            self.logger.debug("handle_data sent", extra={"md": message.data})

    async def client_monitor(self):
        pass





