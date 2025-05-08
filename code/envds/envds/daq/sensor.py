from envds.daq.device import Device
from envds.core import envdsStatus
import asyncio
from envds.exceptions import envdsRunTransitionException, envdsRunWaitException, envdsRunErrorException
from envds.message.message import Message
from envds.daq.types import DAQEventType as det



class Sensor(Device):

    SAMPLING = "sampling"

    def __init__(self, config=None, **kwargs):
        super(Sensor, self).__init__(config=config, **kwargs)

        self.sampling_task_list = []
        self.sampling_tasks = []
        self.status.set_requested(Sensor.SAMPLING, envdsStatus.FALSE)
        self.run_state = "STOPPED"
        self.status.set_state_param(Sensor.SAMPLING, requested=envdsStatus.FALSE, actual=envdsStatus.FALSE)
        self.sampling_interval = 1 # default collection interval in seconds

    def configure(self):
        super(Sensor, self).configure()
        pass

    async def handle_status(self, message: Message):
        await super().handle_status(message)
        if message.data["type"] == det.status_request():
            try:
                # self.logger.debug("handle_status", extra={"data.data": message.data.data})
                state = message.data.data.get("state", None)
                # self.logger.debug("handle_status", extra={"type": det.status_request(), "state": state})
                if state and state == self.SAMPLING:
                    requested = message.data.data.get("requested", None)
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

    def sampling(self) -> bool:
            # self.logger.debug("sensor.sampling")
            if self.status.get_requested(Sensor.SAMPLING) == envdsStatus.TRUE:
                # self.logger.debug("sampling", extra={"health": self.status.get_health_state(Sensor.SAMPLING)})
                return self.status.get_health_state(Sensor.SAMPLING)

    def start(self):

        if not self.enabled():
            self.enable()

        self.status.set_requested(Sensor.SAMPLING, envdsStatus.TRUE)

    async def do_start(self):
    
        try:
            # print("do_start:1")
            # self.enable()
            # print("do_start:2")
            # print("do_start:1")
            requested = self.status.get_requested(Sensor.SAMPLING)
            actual = self.status.get_actual(Sensor.SAMPLING)

            if requested != envdsStatus.TRUE:
                raise envdsRunTransitionException(Sensor.SAMPLING)

            if actual != envdsStatus.FALSE:
                raise envdsRunTransitionException(Sensor.SAMPLING)
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
                raise envdsRunWaitException(Sensor.SAMPLING)
                # return

            # while not self.enabled():
            #     self.logger.info("waiting for sensor to become enabled")
            #     await asyncio.sleep(1)
            # print("do_start:4")

            self.status.set_actual(Sensor.SAMPLING, envdsStatus.TRANSITION)
            # print("do_start:5")

            for task in self.sampling_task_list:
                # print("do_start:6")
                self.sampling_tasks.append(asyncio.create_task(task))
                # print("do_start:7")

            # # TODO: enable all interfaces
            # for name, iface in self.iface_map.items():
            #     iface["status"].set_requested(envdsStatus.ENABLED, envdsStatus.TRUE)

            # may need to require sensors to set this but would rather not
            self.status.set_actual(Sensor.SAMPLING, envdsStatus.TRUE)
            # print("do_start:8")
            self.logger.debug("do_start complete", extra={"status": self.status.get_status()})

        except (envdsRunWaitException, TypeError) as e:
            self.logger.warn("do_start", extra={"error": e})
            # self.status.set_actual(envdsStatus.ENABLED, envdsStatus.FALSE)
            # for task in self.enable_task_list:
            #     if task:
            #         task.cancel()
            raise envdsRunWaitException(Sensor.SAMPLING)

        except envdsRunTransitionException as e:
            self.logger.warn("do_start", extra={"error": e})
            # self.status.set_actual(envdsStatus.ENABLED, envdsStatus.FALSE)
            # for task in self.enable_task_list:
            #     if task:
            #         task.cancel()
            raise envdsRunTransitionException(Sensor.SAMPLING)

        # except (envdsRunWaitException, envdsRunTransitionException) as e:
        #     self.logger.warn("do_enable", extra={"error": e})
        #     # self.status.set_actual(envdsStatus.ENABLED, envdsStatus.FALSE)
        #     # for task in self.enable_task_list:
        #     #     if task:
        #     #         task.cancel()
        #     raise e(Sensor.SAMPLING)

        except (envdsRunErrorException, Exception) as e:
            self.logger.error("do_start", extra={"error": e})
            self.status.set_actual(Sensor.SAMPLING, envdsStatus.FALSE)
            for task in self.sampling_task_list:
                if task:
                    task.cancel()
            raise envdsRunErrorException(Sensor.SAMPLING)

        # self.run_state = "STARTING"
        # self.logger.debug("start", extra={"run_state": self.run_state})

    def stop(self):
        self.status.set_requested(Sensor.SAMPLING, envdsStatus.FALSE)

    async def do_stop(self):
        self.logger.debug("do_stop")
        requested = self.status.get_requested(Sensor.SAMPLING)
        actual = self.status.get_actual(Sensor.SAMPLING)

        if requested != envdsStatus.FALSE:
            raise envdsRunTransitionException(Sensor.SAMPLING)

        if actual != envdsStatus.TRUE:
            raise envdsRunTransitionException(Sensor.SAMPLING)

        self.status.set_actual(Sensor.SAMPLING, envdsStatus.TRANSITION)

        for task in self.sampling_tasks:
            task.cancel()

        self.status.set_actual(Sensor.SAMPLING, envdsStatus.FALSE)