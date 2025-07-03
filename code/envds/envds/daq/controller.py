import abc
import importlib
from ulid import ULID
import asyncio

from logfmter import Logfmter

from pydantic import BaseModel

from envds.core import envdsBase, envdsStatus
from envds.message.message import Message
from envds.daq.types import DAQEventType as det
from envds.daq.event import DAQEvent
from envds.daq.client import DAQClientConfig

from envds.util.util import get_datetime, seconds_elapsed
from cloudevents.http import CloudEvent


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


class ControllerConfig(BaseModel):
    """docstring for SensorConfig."""

    type: str
    name: str
    uid: str
    paths: dict | None = {}


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
        self.update_id("app_uid", f"controller-id-{ULID()}")
        self.logger.debug("controller id", extra={"self.id": self.id})

        self.status.set_id_AppID(self.id)

        self.client_registry = {}
        self.client_map = {}
        self.multistep_data = []

        self.run_task_list.append(self.client_monitor())
        self.run_task_list.append(self.client_registry_monitor())

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
        # self.update_id("app_uid", self.build_app_uid())

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

    def build_app_uid(self):
            parts = [self.config.type, self.config.name, self.config.uid]
            return (Controller.ID_DELIM).join(parts)

    def set_core_routes(self, enable: bool = True):
        print(f"set_core_routes:1")
        super(Controller, self).set_core_routes()
        print(f"set_core_routes:2")

        topic_base = self.get_id_as_topic()
        self.logger.debug("set_core_routes:controller", extra={"topic_base": topic_base})

        self.set_route(
            subscription=f"{topic_base}/+/status/request",
            route_key=det.controller_status_request(),
            route=self.handle_status,
            enable=enable,
        )

        self.logger.debug(
            "set_config_request", extra={"sub": f"/{topic_base}/+/config/request"}
        )
        self.set_route(
            subscription=f"{topic_base}/+/config/request",
            route_key=det.controller_config_request(),
            route=self.handle_config,
            enable=enable,
        )

        self.set_route(
            subscription=f"{topic_base}/+/keepalive/request",
            route_key=det.controller_keepalive_request(),
            route=self.handle_keepalive,
            enable=enable,
        )

        self.set_route(
            subscription=f"{topic_base}/+/data/send",
            route_key=det.controller_data_send(),
            route=self.handle_data,
            enable=enable,
        )

        self.set_route(
            # subscription=f"{topic_base}/+/control/request",
            subscription="webinterface/control/request",
            route_key=det.controller_control_request(),
            route=self.handle_controls,
            enable=enable,
        )


    def update_client_registry(
        self,
        client_id: str,
        source: str,
        keepalive: bool = False,
        deregister: bool = False,
    ):
        self.logger.debug(
            "update_client_registry",
            extra={"client_id": client_id, "source": source, "keepalive": keepalive},
        )
        try:
            if deregister:
                del self.client_registry[client_id][source]
            elif keepalive:
                self.client_registry[client_id][source]["last_update"] = get_datetime()
            else:
                if client_id not in self.client_registry:
                    self.client_registry[client_id] = dict()
                if source not in self.client_registry[client_id]:
                    self.client_registry[client_id][source] = dict()
                self.client_registry[client_id][source]["last_update"] = get_datetime()
                # self.logger.debug(
                #     "client_registry", extra={"reg": self.client_registry}
                # )
        except KeyError:
            pass

    async def client_registry_monitor(self):

        registry_expiration = 60  # if no activity in 5 minutes, expire the connection
        while True:
            try:
                for id, client in self.client_registry.items():
                    # self.logger.debug(
                    #     "registry_monitor", extra={"client_id": id, "client": client}
                    # )
                    for key in list(client.keys()):
                        # if time_expired, del client[key]
                        # self.logger.debug("reg monitor", extra={"key": key})
                        if (
                            seconds_elapsed(client[key]["last_update"])
                            > registry_expiration
                        ):
                            del client[key]
                    self.logger.debug(
                        "client_registry_monitor",
                        extra={"id": id, "connections": len(client)},
                    )
                    if (
                        len(client) == 0
                    ):  # and self.client_map[client_id].client.connected():
                        self.logger.debug("registry_monitor:2")
                        self.client_map[id]["client"].disable()
                        # if self.client_map[id]["recv_task"]:
                        #     # TODO: these should go in disable logic
                        #     self.client_map[id]["recv_task"].cancel()
                        #     self.client_map[id]["recv_task"] = None
                    else:
                        self.logger.debug("registry_monitor:3", extra={"client_map": self.client_map})
                        # enable client if needed
                        if not self.client_map[id]["client"].enabled():
                            self.client_map[id]["client"].enable()

                        self.logger.debug("registry_monitor:4")
                        # if self.client_map[id]["recv_task"] is None:
                        #     self.client_map[id]["recv_task"] = asyncio.create_task(
                        #         self.client_map[id]["recv_handler"]
                        #     )
                        #     self.logger.debug(
                        #         "create recv_task",
                        #         extra={"handler": self.client_map[id]["recv_handler"]},
                        #     )
                        self.logger.debug("registry_monitor:5")

                        # send client status update
                        destpath = f"{self.get_id_as_topic()}/{id}/status/update"
                        extra_header = {"path_id": id}
                        event = DAQEvent.create_status_update(
                            # source="envds.core", data={"test": "one", "test2": 2}
                            source=self.get_id_as_source(),
                            data=self.status.get_status(),
                            extra_header=extra_header,
                        )
                        event["destpath"] = destpath
                        self.logger.debug("status update", extra={"event": event})
                        # message = Message(data=event, destpath=destpath)
                        message = event
                        await self.send_message(message)
            except Exception as e:
                self.logger.error("client_registry_monitor", extra={"reg_error": e})
            await asyncio.sleep(2)

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
        event = DAQEvent.create_controller_data_recv(
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
                for id, path in self.config.paths.items():
                    # if path["client"] is None:
                    # if id not in self.client_map:
                    #     self.client_map = {
                    #         self
                    #     }
                    # self.logger.debug(
                    #     "client_monitor",
                    #     extra={
                    #         "client_id": id,
                    #         "path": path,
                    #         "client_map": self.client_map,
                    #     },
                    # )
                    if self.client_map[id]["client"] is None:

                        self.logger.debug(
                            "client_monitor",
                            extra={
                                "client_id": id,
                                "path": path,
                                "client_map": self.client_map,
                            },
                        )

                        try:
                            client_module = path["client_config"]["attributes"][
                                "client_module"
                            ]["data"]
                            client_class = path["client_config"]["attributes"][
                                "client_class"
                            ]["data"]
                            client_config = DAQClientConfig(
                                uid=id,
                                properties=path["client_config"]["attributes"].copy(),
                            )
                            mod_ = importlib.import_module(client_module)
                            # print(f"here:5 {client_module}, {client_class}, {mod_}")
                            # path["client"] = getattr(mod_, client_class)(config=client_config)
                            cls_ = getattr(mod_, client_class)
                            # print(f"here:5.5 {cls_}")
                            self.client_map[id]["client"] = cls_(config=client_config)

                            # TODO: where to start "run"?
                            await asyncio.sleep(1)
                            self.client_map[id]["client"].run()
                            # self.client_map[id]["client"] = getattr(mod_, client_class)(
                            #     config=client_config
                            # )
                            # print(f"here:6 {self.client_map[id]['client']}")

                            if self.client_map[id]["recv_task"] is not None:
                                self.client_map[id]["recv_task"].cancel()

                            self.client_map[id]["recv_task"] = asyncio.create_task(
                                self.client_map[id]["recv_handler"]
                            )
                            self.logger.debug(
                                "create recv_task",
                                extra={"handler": self.client_map[id]["recv_handler"]},
                            )

                        except (KeyError, ModuleNotFoundError, AttributeError) as e:
                            self.logger.error(
                                "client_monitor: could not create client",
                                extra={"error": e},
                            )
                            self.client_map[id]["client"] = None
                    #     self.logger.debug(
                    #         "client_monitor", extra={"client_map": self.client_map}
                    #     )
                    # self.logger.debug("client monitor", extra={"id": id, "path": path})

                    # update status
                    if (client := self.client_map[id]["client"]):

                        topic_base = self.get_id_as_topic()
                        destpath = f"{topic_base}/{id}/status/update"
                        extra_header = {"path_id": id, "destpath": destpath}
                        event = DAQEvent.create_controller_status_update(
                            # source="envds.core", data={"test": "one", "test2": 2}
                            source=self.get_id_as_source(),
                            data=self.status.get_status(),
                            extra_header=extra_header
                        )
                        self.logger.debug("send_controller_status_update", extra={"event": event})
                        # message = Message(data=event, destpath="/envds/status/update")
                        # message = Message(data=event, destpath=destpath)
                        message = event
                        await self.send_message(message)
                        # self.logger.debug("heartbeat", extra={"msg": message})

            except Exception as e:
                self.logger.error("client_monitor", extra={"error": e})
            await asyncio.sleep(5)





