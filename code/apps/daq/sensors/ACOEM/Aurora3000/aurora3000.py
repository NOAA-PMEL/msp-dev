import asyncio
import signal
import sys
import os
import logging
import json
import yaml

from envds.core import envdsLogger
from envds.daq.sensor import Sensor
from envds.daq.device import DeviceConfig, DeviceMetadata
from envds.daq.types import DAQEventType as det
from envds.daq.event import DAQEvent
from cloudevents.http import CloudEvent
from pydantic import BaseModel

class Aurora3000(Sensor):
    def __init__(self, config=None, **kwargs):
        super(Aurora3000, self).__init__(config=config, **kwargs)
        self.data_rate = 1
        self.default_data_buffer = asyncio.Queue()
        self.sensor_definition_file = "ACOEM_Aurora3000_sensor_definition.json"
        
        # State persistence location
        self.state_file = "/app/data/aurora3000_state.json"
        
        # State Tracking
        self.cal_active = False
        self.module_address = "0"
        self.polling_task = None
        self.collecting = False
        self.current_valve_state = None

        try:            
            with open(self.sensor_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            sys.exit(1)

        self.enable_task_list.append(self.default_data_loop())
        self.enable_task_list.append(self.sampling_monitor())

    def load_local_state(self):
        """Loads persistent valve state and other settings from disk."""
        try:
            with open(self.state_file, "r") as f:
                state = json.load(f)
                for key, value in state.items():
                    if value is not None:
                        self.settings.set_setting(key, value)
            self.logger.info(f"Loaded local state from {self.state_file}")
        except FileNotFoundError:
            self.logger.info("No local state file found. Using JSON schema defaults.")
        except Exception as e:
            self.logger.error(f"Error loading local state: {e}")

    def save_local_state(self):
        """Saves current state values to disk."""
        state = {
            "ext_valve_state": self.settings.get_setting("ext_valve_state")
        }
        try:
            os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
            with open(self.state_file, "w") as f:
                json.dump(state, f, indent=4)
        except Exception as e:
            self.logger.error(f"Failed to save local state: {e}")

    def configure(self):
        super(Aurora3000, self).configure()
        try:
            with open("/app/config/sensor.conf", "r") as f:
                conf = yaml.safe_load(f)
        except FileNotFoundError:
            conf = {"serial_number": "UNKNOWN", "interfaces": {}}

        sensor_iface_properties = {
            "default": {
                "device-interface-properties": {
                    "connection-properties": {
                        "baudrate": 38400, "bytesize": 8, "parity": "N", "stopbit": 1,
                    },
                    "read-properties": {
                        "read-method": "readline", "decode-errors": "strict", "send-method": "ascii",
                    },
                }
            }
        }

        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                if name in sensor_iface_properties:
                    iface.update(sensor_iface_properties[name])

        settings_def = self.get_definition_by_variable_type(self.metadata, variable_type="setting")
        for name, setting in settings_def.get("variables", {}).items():
            requested = setting["attributes"].get("default_value", {}).get("data")
            if "settings" in conf and name in conf["settings"]:
                requested = conf["settings"][name]
            self.settings.set_setting(name, requested=requested)

        # Overwrite defaults with any previously saved local state
        self.load_local_state()

        meta = DeviceMetadata(
            attributes=self.metadata["attributes"],
            dimensions=self.metadata["dimensions"],
            variables=self.metadata["variables"],
            settings=settings_def.get("variables", {})
        )

        self.config = DeviceConfig(
            make=self.metadata["attributes"]["make"]["data"],
            model=self.metadata["attributes"]["model"]["data"],
            serial_number=conf.get("serial_number", "UNKNOWN"),
            metadata=meta,
            interfaces=conf.get("interfaces", {}),
            daq_id=conf.get("daq_id", "default"),
        )

        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                self.add_interface(name, iface)

    async def handle_interface_data(self, message: CloudEvent):
        await super(Aurora3000, self).handle_interface_data(message)
        if message["type"] == det.interface_data_recv():
            try:
                path_id = message["path_id"]
                if path_id == self.config.interfaces["default"]["path"]:
                    await self.default_data_buffer.put(message)
            except KeyError: pass

    async def polling_loop(self):
        """Continuously requests the 14-parameter string via the VI099 command."""
        while True:
            try:
                poll_cmd = f"VI0{self.module_address}99\r"
                await self.interface_send_data(data={"data": poll_cmd})
                await asyncio.sleep(self.data_rate)
            except Exception as e:
                self.logger.error(f"polling_loop error: {e}")
                await asyncio.sleep(self.data_rate)

    async def set_ext_valve(self, valve_state):
        """Commands the Digital Out Aux via DO command 04 to toggle the external 3-way valve."""
        state_val = "1" if valve_state == "position_1" else "0" 
        cmd = f"DO0{self.module_address}04{state_val}\r"
        
        await self.interface_send_data(data={"data": cmd})
        self.logger.info(f"External 3-way valve commanded to: {valve_state} ({cmd.strip()})")
        self.current_valve_state = valve_state

    async def settings_check(self):
        """Intercepts MQTT settings requests and acknowledges them."""
        await super().settings_check()
        
        if not self.settings.get_health():
            state_changed = False
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):
                    target_val = self.settings.get_setting(name)
                    
                    if name == "ext_valve_state":
                        await self.set_ext_valve(target_val)
                        self.settings.set_setting(name, target_val) # Acknowledge health
                        state_changed = True
                        
                    elif name == "sampling_state":
                        self.settings.set_setting(name, target_val)
                        
                    elif name == "calibration_routine":
                        self.settings.set_setting(name, target_val)

            # Persist to disk if the valve state was explicitly changed
            if state_changed:
                self.save_local_state()

    async def sampling_monitor(self):
        """State machine mapping envds settings to Aurora hardware routines."""
        self.polling_task = asyncio.create_task(self.polling_loop())
        
        # Ensure the valve aligns with persistent state on boot
        startup_valve = self.settings.get_setting("ext_valve_state")
        if startup_valve:
            await self.set_ext_valve(startup_valve)
            
        while True:
            try:
                state = self.settings.get_setting("sampling_state") or "idle"
                routine = self.settings.get_setting("calibration_routine") or "none"

                if self.sampling() and state.lower() == "sampling":
                    if not self.collecting:
                        await self.interface_send_data(data={"data": f"DO0{self.module_address}051\r"})
                        await self.interface_send_data(data={"data": f"**{self.module_address}J0\r"})
                        self.collecting = True
                
                elif state.lower() in ["idle", "maintenance"]:
                    if self.collecting:
                        await self.interface_send_data(data={"data": f"DO0{self.module_address}050\r"})
                        self.collecting = False
                
                elif state.lower() == "calibration":
                    if routine == "zero_cal" and not self.cal_active:
                        await self.interface_send_data(data={"data": f"**{self.module_address}J2\r"})
                        self.cal_active = True
                    elif routine == "span_cal_co2" and not self.cal_active:
                        await self.interface_send_data(data={"data": f"**{self.module_address}J1\r"})
                        self.cal_active = True
                    elif routine == "zero_check" and not self.cal_active:
                        await self.interface_send_data(data={"data": f"**{self.module_address}J4\r"})
                        self.cal_active = True
                    elif routine == "span_check" and not self.cal_active:
                        await self.interface_send_data(data={"data": f"**{self.module_address}J3\r"})
                        self.cal_active = True
                
                await asyncio.sleep(1)
            except Exception as e:
                self.logger.error(f"sampling_monitor error: {e}")
                await asyncio.sleep(1)

    def default_parse(self, data):
        """Parses the 14-field comma-delimited VI099 string."""
        if not data: return None
        
        record = self.build_data_record(meta=self.include_metadata)
        self.include_metadata = False
        record["timestamp"] = data.data["timestamp"]
        record["variables"]["time"]["data"] = data.data["timestamp"]
        
        parts = [p.strip() for p in data.data["data"].split(",")]
        
        if len(parts) >= 14:
            try:
                record["variables"]["aurora_date"]["data"] = parts[0]
                record["variables"]["aurora_time"]["data"] = parts[1]
                record["variables"]["scat_coef_ch1_red"]["data"] = float(parts[2])
                record["variables"]["scat_coef_ch2_green"]["data"] = float(parts[3])
                record["variables"]["scat_coef_ch3_blue"]["data"] = float(parts[4])
                record["variables"]["backscatter_ch1_red"]["data"] = float(parts[5])
                record["variables"]["backscatter_ch2_green"]["data"] = float(parts[6])
                record["variables"]["backscatter_ch3_blue"]["data"] = float(parts[7])
                record["variables"]["sample_T"]["data"] = float(parts[8])
                record["variables"]["enclosure_T"]["data"] = float(parts[9])
                record["variables"]["rh"]["data"] = float(parts[10])
                record["variables"]["pressure"]["data"] = float(parts[11])
                
                major_state = int(parts[12])
                record["variables"]["major_state"]["data"] = major_state
                record["variables"]["DIO_state"]["data"] = parts[13]

                if self.current_valve_state is not None:
                    record["variables"]["ext_valve_state"]["data"] = self.current_valve_state

                # Hardware tells us when a calibration finishes (state returns to 0)
                if self.cal_active and major_state == 0:
                    self.logger.info("Calibration sequence completed by Aurora firmware.")
                    self.settings.set_setting("calibration_routine", "none")
                    self.settings.set_setting("sampling_state", "sampling" if self.sampling() else "idle")
                    self.cal_active = False

                return record
            except (ValueError, TypeError) as e:
                self.logger.warning(f"Error casting data payload: {e}")
                return None
        return None

    async def default_data_loop(self):
        while True:
            try:
                data = await self.default_data_buffer.get()
                record = self.default_parse(data)
                
                if record and self.sampling():
                    event = DAQEvent.create_data_update(
                        source=self.get_id_as_source(),
                        data=record,
                    )
                    event["destpath"] = f"{self.get_id_as_topic()}/data/update"
                    await self.send_message(event)
            except Exception as e:
                self.logger.error(f"default_data_loop error: {e}")
            await asyncio.sleep(0.1)

async def shutdown(sensor):
    if sensor:
        await sensor.shutdown()

async def main(server_config: ServerConfig = None):
    if server_config is None: server_config = ServerConfig()
    envdsLogger(level=logging.DEBUG).init_logger()
    logger = logging.getLogger("ACOEM::Aurora3000")
    inst = Aurora3000()
    inst.run()
    await asyncio.sleep(2)
    inst.start()

    event_loop = asyncio.get_event_loop()
    global do_run
    do_run = True

    def shutdown_handler(*args):
        global do_run
        do_run = False

    event_loop.add_signal_handler(signal.SIGINT, shutdown_handler)
    event_loop.add_signal_handler(signal.SIGTERM, shutdown_handler)

    while do_run:
        await asyncio.sleep(1)

    await shutdown(inst)

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, BASE_DIR)
    config = ServerConfig()
    asyncio.run(main(config))