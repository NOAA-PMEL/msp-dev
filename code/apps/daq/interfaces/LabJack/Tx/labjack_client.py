import asyncio
import binascii
from envds.daq.client import DAQClient, DAQClientConfig, _StreamClient
from envds.core import envdsStatus
from envds.exceptions import (
    envdsRunTransitionException,
    envdsRunWaitException,
    envdsRunErrorException,
)
import random

from envds.util.util import time_to_next

from labjack import ljm


class LabJackClient(DAQClient):
    """docstring for self.LabJackClient."""

    def __init__(self, config: DAQClientConfig = None, **kwargs):
        # print("mock_client: 1")
        super(LabJackClient, self).__init__(config=config)
        # print("mock_client: 2")
        self.logger.debug("self.LabJackClient.init")
        # self.client_class = "_self.LabJackClient"
        self.logger.debug("self.LabJackClient.init", extra={"config": config})

        # TODO: set uid here? or let caller do it?
        self.config = config
        # print("mock_client: 3")

        self.labjack = None  # labjack handle

        # # self.mock_type = "1D"  # 2D, i2c
        # self.read_method = "readline"
        # self.send_method = "ascii"
        # self.decode_errors = "strict"
        # self.read_terminator = "\r"
        # # print("mock_client: 4")
        self.logger.debug("self.LabJackClient.init", extra={"config": self.config})
        self.data_buffer = asyncio.Queue()
        # self.enable_task_list.append(self.recv_loop())

        # self.logger.debug("init", extra={})
        # try:
        #     self.client = _MockClient()
        # except Exception as e:
        #     self.logger.error("init client error", extra={"error": e})
        #     self.client = None
        # self.logger.debug("init", extra={"client": self.client})

        # self.run_task_list.append(self.connection_monitor())
        try:
            if (
                "sample_mode" in self.config.properties
                and "unpolled_sample_frequency_sec"
                in self.config.properties
            ):
                if self.config.properties["sample_mode"]["data"]:
                    asyncio.create_task(
                        self.upolled_sample_loop(
                            self.config.properties[
                                "unpolled_sample_frequency_sec"
                            ]["data"]
                        )
                    )
        except:
            print("error: unpolled sampling not started")
            pass

    # async def connection_monitor(self):
    #     while True:
    #         try:
    #             host = self.config.properties["host"]["data"]
    #             self.logger.debug("connection_monitor", extra={"host": host, "self.labjack": self.labjack})
    #             if not self.labjack:
    #                 self.labjack = ljm.openS("ANY", "ANY", host)
    #                 info = ljm.getHandleInfo(self.labjack)
    #                 self.logger.info(
    #                     "connection_monitor: labjack info",
    #                     extra={
    #                         "device_type": info[0],
    #                         "connection_type": info[1],
    #                         "serial_number": info[2],
    #                         "ip_address": ljm.numberToIP(info[3]),
    #                         "port": info[4],
    #                         "max_bytes_per_mb": info[5],
    #                     },
    #                 )

    #                 # deviceType = info[0]

    #         except Exception as e:
    #             self.logger.error("connection_monitor", extra={"reason": e})
    #             self.labjack = None
    #         await asyncio.sleep(5)

    def set_labjack_handle(self, handle):
        self.labjack = handle
        info = ljm.getHandleInfo(self.labjack)
        self.logger.info(
            "set_labjack_info: labjack info",
            extra={
                "device_type": info[0],
                "connection_type": info[1],
                "serial_number": info[2],
                "ip_address": ljm.numberToIP(info[3]),
                "port": info[4],
                "max_bytes_per_mb": info[5],
            },
        )


    def hex_to_int(self, hex_val):
        if "Ox" not in hex_val:
            hex_val = f"0x{hex_val}"
        return int(hex_val, 16)

    async def recv_from_client(self):
        if True:
            try:
                data = await self.data_buffer.get()
                return data
            except Exception as e:
                self.logger.error("recv_from_client", extra={"e": e})
        # print("recv_from_client:5")
        return None

    async def send_to_client(self, data):
        pass

    async def upolled_sample_loop(self, sample_freq):
        handle = self.config.properties["unpolled_interval_handle"][
            "data"
        ]
        while self.config.properties["sample_mode"]["data"] == "unpolled":
            try:
                # freq = int(
                #     self.config.properties[
                #         "unpolled_sample_frequency_sec"
                #     ]["data"]
                #     * 1000000
                # )
                # ljm.startInterval(handle, freq)

                # skipped_intervals = ljm.waitForNextInterval(handle)
                # if skipped_intervals > 0:
                #     self.logger.debug(
                #         "upolled_sample_loop",
                #         extra={"skipped intervals": skipped_intervals},
                #     )

                data = {}
                if "unpolled_data" in self.config.properties:
                    data = self.config.properties["unpolled_data"]["data"]
                await self.send_to_client(data)
                await asyncio.sleep(time_to_next(self.config.properties["unpolled_sample_frequency_sec"]["data"]))
            except Exception as e:
                self.logger.error("unpolled_sample_loop", extra={"reason": e})

        # ljm.cleanInterval(handle)


class ADCClient(LabJackClient):
    """docstring for self.LabJackClient."""

    def __init__(self, config: DAQClientConfig = None, **kwargs):
        # print("mock_client: 1")
        super(ADCClient, self).__init__(config=config)
        # self.adc_started = False
        # asyncio.create_task(self.run_adc())

    # async def run_counter(self):
    #     while True:
    #         try:
    #             clock_channel = self.config.properties["channel"]["data"]
    #             if not self.counter_started:
    #                 ljm.eWriteName(self.labjack, f"DIO{clock_channel}_EF_INDEX", 7)            # Set DIO#_EF_INDEX to 7 - High-Speed Counter.
    #                 ljm.eWriteName(self.labjack, f"DIO{clock_channel}_EF_ENABLE", 1)
    #                 self.counter_started = True             # Enable the High-Speed Counter.
    #         except Exception as e:
    #             self.logger.error("start_counter", extra={"reason": e})
    #             self.counter_started = False

    async def send_to_client(self, data):
        try:

            channel = self.config.properties["channel"]["data"]
            dataRead = ljm.eReadName(self.labjack, f"AIN{channel}")
            output = {"data": dataRead}
            await self.data_buffer.put(output)

        except Exception as e:
            self.logger.error("send_to_client")


class DACClient(LabJackClient):
    """docstring for self.LabJackClient."""

    def __init__(self, config: DAQClientConfig = None, **kwargs):
        # print("mock_client: 1")
        super(DACClient, self).__init__(config=config)
        # self.adc_started = False
        # asyncio.create_task(self.run_adc())

    async def send_to_client(self, data):
        try:

            channel = self.config.properties["channel"]["data"]
            volts = data["ouput_volts"]
            ljm.WriteName(self.labjack, f"DAC{channel}", volts)

        except Exception as e:
            self.logger.error("send_to_client")

        # try:

        #     # send_method = data.get("send-method", self.send_method)

        #     print(f"send_to_client:1 props {self.config.properties}")
        #     props = self.config.properties["device-interface-properties"]["read-properties"]

        #     print(f"send_to_client:1.1 - sip {self.config.properties['device-interface-properties']}")
        #     print(f"send_to_client:1.2 - rp {self.config.properties['device-interface-properties']['read-properties']}")
        #     print(f"send_to_client:1.3 - sm {self.config.properties['device-interface-properties']['read-properties']['send-method']}")
        #     # read_method = props.get("read-method", self.read_method)
        #     # decode_errors = props.get("decode-errors", self.decode_errors)

        #     send_method = self.send_method
        #     if "send-method" in self.config.properties['device-interface-properties']['read-properties']:
        #         send_method = self.config.properties['device-interface-properties']['read-properties']['send-method']

        #     self.logger.debug("send_to_client", extra={"send_method": send_method, "data": data})
        #     if send_method == "binary":
        #         # if num of expected bytes not supplied, fail
        #         try:
        #             read_num_bytes = data["read-num-bytes"]
        #             self.client.return_packet_bytes.append(
        #                 data["read-num-bytes"]
        #             )

        #             # convert back to bytes
        #             await self.client.writebinary(data["data"])
        #             # await self.client.writebinary(binary_data)
        #         except KeyError:
        #             self.logger.error("binary write failed - read-num-bytes not specified")
        #             return
        #     else:
        #         try:
        #             await self.client.write(data["data"])
        #         except KeyError:
        #             self.logger.error("write failed - data not specified")
        #             return

        #     self.logger.debug(
        #         "send_to_client",
        #         extra={
        #             "send_method": send_method,
        #             "data": data["data"]
        #         },
        #     )
        # except Exception as e:
        #     self.logger.error("send_to_client error", extra={"error": e})


class DIOClient(LabJackClient):
    """docstring for self.LabJackClient."""

    def __init__(self, config: DAQClientConfig = None, **kwargs):
        # print("mock_client: 1")
        super(DIOClient, self).__init__(config=config)
        # self.adc_started = False
        # asyncio.create_task(self.run_adc())

    async def send_to_client(self, data):
        try:

            channel = self.config.properties["channel"]["data"]

            mode = data["dio_mode"]
            if mode.lower() == "output":  # write to DIO pin
                state = data["do_state"]
                ljm.eWriteName(self.labview, f"FIO{channel}", state)
            elif mode.lower == "input":
                dataRead = ljm.eReadName(self.labjack, f"FIO{channel}")
                output = {"data": dataRead}
                await self.data_buffer.put(output)

        except Exception as e:
            self.logger.error("send_to_client")


class PWMClient(LabJackClient):
    """docstring for self.LabJackClient."""

    def __init__(self, config: DAQClientConfig = None, **kwargs):
        # print("mock_client: 1")
        super(PWMClient, self).__init__(config=config)

    async def send_to_client(self, data):
        try:
            max_attempts = 30
            attempt = 0
            while self.labjack is None and attempt < max_attempts:
                await asyncio.sleep(1)
                attempt += 1
            # client_config = self.client_map[client_id]["client_config"]
            # data_buffer = self.client_map[client_id]["data_buffer"]
            # get i2c commands
            self.logger.debug("send_to_client", extra={"pwm-data": data, "config": self.config})
            clock_divisor = self.config.properties["clock_divisor"][
                "data"
            ]
            core_frequency = self.config.properties[
                "clock_core_frequency"
            ]["data"]
            desired_frequency = self.config.properties[
                "clock_desired_frequency"
            ]["data"]

            clockTickRate = core_frequency / clock_divisor
            clockRollValue = clockTickRate / desired_frequency

            pwm_data = data["data"]["pwm-data"]
            # duty_cycle = pwm_data["duty_cycle_percent"]
            duty_cycle = pwm_data
            pwmConfigA = int(clockRollValue * (duty_cycle / 100.0))

            clock_channel = self.config.properties["clock_channel"][
                "data"
            ]
            pwm_channel = self.config.properties["pwm_channel"]["data"]

            self.logger.debug("send_to_client:clock disable", extra={"clock_channel": clock_channel})
            ljm.eWriteName(
                self.labjack, f"DIO_EF_CLOCK{clock_channel}_ENABLE", 0
            )  # Enable Clock#, this will start the PWM signal.

            # configure clock
            self.logger.debug("send_to_client:div", extra={"clock_divisor": clock_divisor})
            ljm.eWriteName(
                self.labjack, f"DIO_EF_CLOCK{clock_channel}_DIVISOR", int(clock_divisor)
            )  # Set Clock Divisor.
            self.logger.debug("send_to_client:roll", extra={"roll_value": clockRollValue})
            ljm.eWriteName(
                self.labjack, f"DIO_EF_CLOCK{clock_channel}_ROLL_VALUE", int(clockRollValue)
            )  # Set calculated Clock Roll Value.

            # Configure PWM Registers
            self.logger.debug("send_to_client:pwm disable", extra={"pwm_channel": pwm_channel})
            ljm.eWriteName(
                self.labjack, f"DIO{pwm_channel}_EF_ENABLE", 0
            )  
            self.logger.debug("send_to_client:index", extra={"ef_index": 0})
            ljm.eWriteName(
                self.labjack, f"DIO{pwm_channel}_EF_INDEX", 0
            )  # Set DIO#_EF_INDEX to 0 - PWM Out.
            # ljm.eWriteName(
            #     self.labjack, f"DIO{pwm_channel}_EF_CLOCK_SOURCE", clock_channel
            # )  # Set DIO#_EF to use clock 0. Formerly DIO#_EF_OPTIONS, you may need to switch to this name on older LJM versions.
            self.logger.debug("send_to_client: ef_config", extra={"ef_config": pwmConfigA})
            ljm.eWriteName(
                self.labjack, f"DIO{pwm_channel}_EF_CONFIG_A", pwmConfigA
            )  # Set DIO#_EF_CONFIG_A to the calculated value.
            self.logger.debug("send_to_client: pwm enable", extra={"pwm_channel": pwm_channel})
            ljm.eWriteName(
                self.labjack, f"DIO{pwm_channel}_EF_ENABLE", 1
            )  # Enable the DIO#_EF Mode, PWM signal will not start until DIO_EF and CLOCK are enabled.

            self.logger.debug("send_to_client: clock enable", extra={"clock_channel": clock_channel})
            ljm.eWriteName(
                self.labjack, f"DIO_EF_CLOCK{clock_channel}_ENABLE", 1
            )  # Enable Clock#, this will start the PWM signal.
            self.logger.debug("send_to_client:done")

        # TODO turn off pwm on stop/disable

            await asyncio.sleep(1)
            dataRead = ljm.eReadName(
                self.labjack, f"DIO{pwm_channel}_EF_CONFIG_A"
            )  
            duty_cycle = (pwmConfigA/clockRollValue)*100.0
            output = {"input-value": pwmConfigA, "data": {"raw": dataRead, "duty_cycle": duty_cycle }}
            self.logger.debug("send_to_client: read", extra={"pwm-output": output})
            await self.data_buffer.put(output)

        except Exception as e:
            self.logger.error("send_to_client", extra={"reason": e})


class CounterClient(LabJackClient):
    """docstring for self.LabJackClient."""

    def __init__(self, config: DAQClientConfig = None, **kwargs):
        # print("mock_client: 1")
        super(CounterClient, self).__init__(config=config)
        self.counter_started = False
        asyncio.create_task(self.run_counter())

    async def run_counter(self):
        while True:
            try:
                clock_channel = self.config.properties["channel"]["data"]

                # TODO check for clock mode: "interrupt" (8) vs "high-speed" (7)
                counter_mode = 8
                if self.config.properties["counter_mode"]["data"].lower() == "high-speed":
                    counter_mode = 7
                if not self.counter_started:
                    ljm.eWriteName(self.labjack, f"DIO{clock_channel}_EF_ENABLE", 0)
                    self.counter_started = True  # Enable the High-Speed Counter.
                    ljm.eWriteName(
                        self.labjack, f"DIO{clock_channel}_EF_INDEX", counter_mode
                    )  # Set DIO#_EF_INDEX to 7 - High-Speed Counter.
                    ljm.eWriteName(self.labjack, f"DIO{clock_channel}_EF_ENABLE", 1)
                    self.counter_started = True  # Enable the High-Speed Counter.
            except Exception as e:
                self.logger.error("start_counter", extra={"handle": self.labjack, "reason": e})
                self.counter_started = False
            await asyncio.sleep(1)

    async def send_to_client(self, data):
        try:
            # client_config = self.client_map[client_id]["client_config"]
            # data_buffer = self.client_map[client_id]["data_buffer"]
            # get i2c commands
            self.logger.debug("send_to_client", extra={"send-data": data})
            self.logger.debug("send_to_client", extra={"handle": self.labjack, "config": self.config})
            clock_channel = self.config.properties["channel"]["data"]
            dataRead = ljm.eReadName(self.labjack, f"DIO{clock_channel}_EF_READ_A")
            output = {"data": dataRead}
            await self.data_buffer.put(output)

        except Exception as e:
            self.logger.error("send_to_client", extra={"reason": e})


class I2CClient(LabJackClient):
    """docstring for self.LabJackClient."""

    def __init__(self, config: DAQClientConfig = None, **kwargs):
        # print("mock_client: 1")
        super(I2CClient, self).__init__(config=config)

    async def send_to_client(self, data):
        try:
            self.logger.debug("send_to_client", extra={"i2c-data": data})
            # client_config = self.client_map[client_id]["client_config"]
            # data_buffer = self.client_map[client_id]["data_buffer"]
            # get i2c commands

            i2c_write = data["data"]["i2c-write"]
            # i2c_read = data["i2c-read"]

            address = self.hex_to_int(i2c_write["address"])
            write_data = [self.hex_to_int(hex_val) for hex_val in i2c_write["data"]]

            self.logger.debug("send_to_client", extra={"i2c-data": data, "config": self.config})

            ljm.eWriteName(
                self.labjack,
                "I2C_SDA_DIONUM",
                self.config.properties["sda_channel"]["data"],
            )  # CS is FIO2
            ljm.eWriteName(
                self.labjack,
                "I2C_SCL_DIONUM",
                self.config.properties["scl_channel"]["data"],
            )  # CLK is FIO3
            ljm.eWriteName(
                self.labjack,
                "I2C_SPEED_THROTTLE",
                self.config.properties["speed_throttle"]["data"],
            )  # CLK frequency approx 100 kHz
            ljm.eWriteName(self.labjack, "I2C_OPTIONS", 0)
            ljm.eWriteName(
                self.labjack, "I2C_SLAVE_ADDRESS", address
            )  # default address is 0x28 (40 decimal)

            ljm.eWriteName(self.labjack, "I2C_NUM_BYTES_TX", len(write_data))
            ljm.eWriteName(self.labjack, "I2C_NUM_BYTES_RX", 0)

            ljm.eWriteNameByteArray(
                self.labjack, "I2C_DATA_TX", len(write_data), write_data
            )
            # ljm.eWriteNameArray(self.labjack, "I2C_DATA_TX", 1, [0x00])
            # ljm.eWriteName(self.labjack, "I2C_DATA_TX", 0x00)
            ljm.eWriteName(self.labjack, "I2C_GO", 1)

            i2c_read = data["data"]["i2c-read"]
            # await asyncio.sleep(i2c_read.get("delay-ms",500)/1000.) # does this have to be 0.5?
            await asyncio.sleep(0.5)
            address = self.hex_to_int(i2c_read["address"])
            read_bytes = i2c_read["read-length"]

            # dataRead = ljm.eReadNameByteArray(self.labjack, "I2C_DATA_RX", 4)
            ljm.eWriteName(self.labjack, "I2C_NUM_BYTES_TX", 0)
            ljm.eWriteName(self.labjack, "I2C_NUM_BYTES_RX", read_bytes)
            ljm.eWriteName(self.labjack, "I2C_GO", 1)

            dataRead = ljm.eReadNameByteArray(self.labjack, "I2C_DATA_RX", read_bytes)
            self.logger.debug("send_to_client", extra={"dataread": dataRead})
            if dataRead:
                output = {"address": i2c_read["address"], "input-data": data, "data": dataRead}
                self.logger.debug("send_to_client", extra={"output": output})
                await self.data_buffer.put(output)

        except Exception as e:
            self.logger.error("send_i2c", extra={"reason": e})


class SPIClient(LabJackClient):
    """docstring for self.LabJackClient."""

    def __init__(self, config: DAQClientConfig = None, **kwargs):
        # print("mock_client: 1")
        super(SPIClient, self).__init__(config=config)

    async def send_to_client(self, data):
        try:
            # client_config = self.client_map[client_id]["client_config"]
            # data_buffer = self.client_map[client_id]["data_buffer"]
            # get i2c commands

            spi_write = data["spi-write"]
            # spi_read = data["spi-read"]

            address = self.hex_to_int(spi_write["address"])
            write_data = [self.hex_to_int(hex_val) for hex_val in spi_write["data"]]

            ljm.eWriteName(
                self.labjack,
                "I2C_SDA_DIONUM",
                self.config.properties["sda_channel"]["data"],
            )  # CS is FIO2
            ljm.eWriteName(
                self.labjack,
                "I2C_SCL_DIONUM",
                self.config.properties["scl_channel"]["data"],
            )  # CLK is FIO3
            ljm.eWriteName(
                self.labjack,
                "I2C_SPEED_THROTTLE",
                self.config.properties["speed_throttle"]["data"],
            )  # CLK frequency approx 100 kHz
            ljm.eWriteName(self.labjack, "I2C_OPTIONS", 0)
            ljm.eWriteName(
                self.labjack, "I2C_SLAVE_ADDRESS", address
            )  # default address is 0x28 (40 decimal)

            ljm.eWriteName(self.labjack, "I2C_NUM_BYTES_TX", len(write_data))
            ljm.eWriteName(self.labjack, "I2C_NUM_BYTES_RX", 0)

            ljm.eWriteNameByteArray(
                self.labjack, "I2C_DATA_TX", len(write_data), write_data
            )
            # ljm.eWriteNameArray(self.labjack, "I2C_DATA_TX", 1, [0x00])
            # ljm.eWriteName(self.labjack, "I2C_DATA_TX", 0x00)
            ljm.eWriteName(self.labjack, "I2C_GO", 1)

            spi_read = data["spi-read"]
            await asyncio.sleep(
                spi_read.get("delay-ms", 500) / 1000.0
            )  # does this have to be 0.5?

            address = self.hex_to_int(spi_read["address"])
            read_bytes = spi_read["read-length"]

            # dataRead = ljm.eReadNameByteArray(self.labjack, "I2C_DATA_RX", 4)
            ljm.eWriteName(self.labjack, "I2C_NUM_BYTES_TX", 0)
            ljm.eWriteName(self.labjack, "I2C_NUM_BYTES_RX", read_bytes)
            ljm.eWriteName(self.labjack, "I2C_GO", 1)

            dataRead = ljm.eReadNameByteArray(self.labjack, "I2C_DATA_RX", read_bytes)
            if dataRead:
                output = {"input-data": data, "data": dataRead}
                await self.data_buffer.put(output)

        except Exception as e:
            self.logger.error("send_spi")

        # try:

        #     # send_method = data.get("send-method", self.send_method)

        #     print(f"send_to_client:1 props {self.config.properties}")
        #     props = self.config.properties["device-interface-properties"]["read-properties"]

        #     print(f"send_to_client:1.1 - sip {self.config.properties['device-interface-properties']}")
        #     print(f"send_to_client:1.2 - rp {self.config.properties['device-interface-properties']['read-properties']}")
        #     print(f"send_to_client:1.3 - sm {self.config.properties['device-interface-properties']['read-properties']['send-method']}")
        #     # read_method = props.get("read-method", self.read_method)
        #     # decode_errors = props.get("decode-errors", self.decode_errors)

        #     send_method = self.send_method
        #     if "send-method" in self.config.properties['device-interface-properties']['read-properties']:
        #         send_method = self.config.properties['device-interface-properties']['read-properties']['send-method']

        #     self.logger.debug("send_to_client", extra={"send_method": send_method, "data": data})
        #     if send_method == "binary":
        #         # if num of expected bytes not supplied, fail
        #         try:
        #             read_num_bytes = data["read-num-bytes"]
        #             self.client.return_packet_bytes.append(
        #                 data["read-num-bytes"]
        #             )

        #             # convert back to bytes
        #             await self.client.writebinary(data["data"])
        #             # await self.client.writebinary(binary_data)
        #         except KeyError:
        #             self.logger.error("binary write failed - read-num-bytes not specified")
        #             return
        #     else:
        #         try:
        #             await self.client.write(data["data"])
        #         except KeyError:
        #             self.logger.error("write failed - data not specified")
        #             return

        #     self.logger.debug(
        #         "send_to_client",
        #         extra={
        #             "send_method": send_method,
        #             "data": data["data"]
        #         },
        #     )
        # except Exception as e:
        #     self.logger.error("send_to_client error", extra={"error": e})
