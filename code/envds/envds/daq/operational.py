from envds.daq.device import Device
from envds.core import envdsStatus
import asyncio
from envds.exceptions import envdsRunTransitionException, envdsRunWaitException, envdsRunErrorException
# from envds.message.message import Message
from envds.daq.types import DAQEventType as det
from envds.daq.event import DAQEvent
from cloudevents.http import CloudEvent



class Operational(Device):

    SAMPLING = "sampling"
    DEVICE_TYPE = "operational"

    def __init__(self, config=None, **kwargs):
        super(Operational, self).__init__(config=config, **kwargs)

        self.sampling_task_list = []
        self.sampling_tasks = []
        self.status.set_requested(Operational.SAMPLING, envdsStatus.FALSE)
        self.run_state = "STOPPED"
        self.status.set_state_param(Operational.SAMPLING, requested=envdsStatus.FALSE, actual=envdsStatus.FALSE)
        self.sampling_interval = 1 # default collection interval in seconds

    def configure(self):
        super(Operational, self).configure()
        pass

    # async def handle_status(self, message: Message):
    async def handle_status(self, message: CloudEvent):
        await super().handle_status(message)
        # if message.data["type"] == det.status_request():
        if message["type"] == det.status_request():
            try:
                # self.logger.debug("handle_status", extra={"data.data": message.data.data})
                # state = message.data.data.get("state", None)
                state = message.data.get("state", None)
                # self.logger.debug("handle_status", extra={"type": det.status_request(), "state": state})
                if state and state == self.SAMPLING:
                    # requested = message.data.data.get("requested", None)
                    requested = message.data.get("requested", None)
                    # self.logger.debug("handle_status", extra={"type": det.status_request(), "state": state, "requested": requested})
                    if requested:
                        # self.logger.debug("handle_status", extra={"type": det.status_request(), "state": state, "requested": requested})
                        if requested == envdsStatus.TRUE:
                            self.start()
                        elif requested == envdsStatus.FALSE:
                            self.stop()
                    await self.send_status_update()
            except Exception as e:
                self.logger.error("handle_status", extra={"error": e})
        pass

    def set_routes(self, enable: bool=True):
        super(Operational, self).set_routes()

        topic_base = self.get_id_as_topic()
        self.set_route(
            subscription=f"{topic_base}/settings/request",
            route_key=det.sensor_settings_request(),
            route=self.handle_settings,
            enable=enable
        )

        # self.set_route(
        #     topic = f"envds/{self.core_settings.namespace_prefix}/device/registry/ack"
        #     # subscription=f"{topic_base}/registry/ack",
        #     subscription = topic
        #     route_key=det.device_definition_registry_ack(),
        #     route=self.handle_registry,
        #     enable=enable
        # )

    async def status_check(self):

        # while True:

            # try:
        await super(Device,self).status_check()
        # pass

        if not self.status.get_health(): # something has changed
            if not self.status.get_health_state(Operational.SAMPLING):
                if self.status.get_requested(Operational.SAMPLING) == envdsStatus.TRUE:
                    try:
                        await self.do_start()
                    except (envdsRunTransitionException, envdsRunErrorException, envdsRunWaitException):
                        pass
                    # self.status.set_actual(Device.SAMPLING, envdsStatus.TRUE)
                else:
                    # self.disable()
                    # self.status.set_actual(Device.SAMPLING, envdsStatus.TRANSITION)
                    try:
                        await self.do_stop()
                    except envdsRunTransitionException:
                        pass

    # async def register_device(self):
        
    #     while True:
        
    #         # if self.enabled and not self.device_registered:
    #         if not self.device_registered:
                
    #             event = DAQEvent.create_device_registry_update(
    #                 # source="device.mockco-mock1-1234", data=record
    #                 source=self.get_id_as_source(),
    #                 data={"sensor-instance": self.metadata["attributes"]},
    #             )
    #             destpath = f"{self.get_id_as_topic()}/registry/update"
    #             self.logger.debug(
    #                 "register_device_definition", extra={"data": event, "destpath": destpath}
    #             )
    #             event["destpath"] = destpath
    #             # message = Message(data=event, destpath=destpath)
    #             message = event
    #             # self.logger.debug("default_data_loop", extra={"m": message})
    #             await self.send_message(message)
        
    #         await asyncio.sleep(5)

    # async def register_device_definition(self):
        
    #     while True:
        
    #         if not self.device_definition_registered:
    #             try:
    #                 event = DAQEvent.create_device_definition_registry_update(
    #                     # source="device.mockco-mock1-1234", data=record
    #                     source=self.get_id_as_source(),
    #                     data={"sensor-definition": self.metadata},
    #                 )
    #                 destpath = f"{self.get_id_as_topic()}/registry/update"
    #                 self.logger.debug(
    #                     "register_device_definition", extra={"data": event, "destpath": destpath}
    #                 )
    #                 event["destpath"] = destpath
    #                 # message = Message(data=event, destpath=destpath)
    #                 message = event
    #                 # self.logger.debug("default_data_loop", extra={"m": message})
    #                 await self.send_message(message)
    #             except Exception as e:
    #                 self.logger.error("register_device_definition", extra={"reason": e})
    #         await asyncio.sleep(5)


    def sampling(self) -> bool:
            # self.logger.debug("sensor.sampling")
            if self.status.get_requested(Operational.SAMPLING) == envdsStatus.TRUE:
                # self.logger.debug("sampling", extra={"health": self.status.get_health_state(Sensor.SAMPLING)})
                return self.status.get_health_state(Operational.SAMPLING)

    def start(self):

        if not self.enabled():
            self.enable()

        self.status.set_requested(Operational.SAMPLING, envdsStatus.TRUE)

    async def do_start(self):
    
        try:
            # print("do_start:1")
            # self.enable()
            # print("do_start:2")
            # print("do_start:1")
            requested = self.status.get_requested(Operational.SAMPLING)
            actual = self.status.get_actual(Operational.SAMPLING)

            if requested != envdsStatus.TRUE:
                raise envdsRunTransitionException(Operational.SAMPLING)

            if actual != envdsStatus.FALSE:
                raise envdsRunTransitionException(Operational.SAMPLING)
            print("do_start:2")

            # self.enable()
            # print("do_start:3")

            # if not (
            #     self.status.get_requested(envdsStatus.ENABLED) == envdsStatus.TRUE
            #     and self.status.get_health_state(envdsStatus.ENABLED)
            # ):
            #     return
            # while not self.status.get_health_state(envdsStatus.ENABLED):
            #     self.logger.debug("waiting for enable state to start sensor")
            #     await asyncio.sleep(1)

            if not self.enabled():
                raise envdsRunWaitException(Operational.SAMPLING)
                # return

            # while not self.enabled():
            #     self.logger.info("waiting for sensor to become enabled")
            #     await asyncio.sleep(1)
            # print("do_start:4")

            self.status.set_actual(Operational.SAMPLING, envdsStatus.TRANSITION)
            # print("do_start:5")

            for task in self.sampling_task_list:
                # print("do_start:6")
                self.sampling_tasks.append(asyncio.create_task(task))
                # print("do_start:7")

            # # TODO: enable all interfaces
            # for name, iface in self.iface_map.items():
            #     iface["status"].set_requested(envdsStatus.ENABLED, envdsStatus.TRUE)

            # may need to require sensors to set this but would rather not
            self.status.set_actual(Operational.SAMPLING, envdsStatus.TRUE)
            # print("do_start:8")
            self.logger.debug("do_start complete", extra={"status": self.status.get_status()})

        except (envdsRunWaitException, TypeError) as e:
            self.logger.warning("do_start", extra={"error": e})
            # self.status.set_actual(envdsStatus.ENABLED, envdsStatus.FALSE)
            # for task in self.enable_task_list:
            #     if task:
            #         task.cancel()
            raise envdsRunWaitException(Operational.SAMPLING)

        except envdsRunTransitionException as e:
            self.logger.warning("do_start", extra={"error": e})
            # self.status.set_actual(envdsStatus.ENABLED, envdsStatus.FALSE)
            # for task in self.enable_task_list:
            #     if task:
            #         task.cancel()
            raise envdsRunTransitionException(Operational.SAMPLING)

        # except (envdsRunWaitException, envdsRunTransitionException) as e:
        #     self.logger.warn("do_enable", extra={"error": e})
        #     # self.status.set_actual(envdsStatus.ENABLED, envdsStatus.FALSE)
        #     # for task in self.enable_task_list:
        #     #     if task:
        #     #         task.cancel()
        #     raise e(Sensor.SAMPLING)

        except (envdsRunErrorException, Exception) as e:
            self.logger.error("do_start", extra={"error": e})
            self.status.set_actual(Operational.SAMPLING, envdsStatus.FALSE)
            for task in self.sampling_task_list:
                if task:
                    task.cancel()
            raise envdsRunErrorException(Operational.SAMPLING)

        # self.run_state = "STARTING"
        # self.logger.debug("start", extra={"run_state": self.run_state})

    def stop(self):
        self.status.set_requested(Operational.SAMPLING, envdsStatus.FALSE)

    async def do_stop(self):
        self.logger.debug("do_stop")
        requested = self.status.get_requested(Operational.SAMPLING)
        actual = self.status.get_actual(Operational.SAMPLING)

        if requested != envdsStatus.FALSE:
            raise envdsRunTransitionException(Operational.SAMPLING)

        if actual != envdsStatus.TRUE:
            raise envdsRunTransitionException(Operational.SAMPLING)

        self.status.set_actual(Operational.SAMPLING, envdsStatus.TRANSITION)

        for task in self.sampling_tasks:
            task.cancel()

        self.status.set_actual(Operational.SAMPLING, envdsStatus.FALSE)