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
            self.logger.error("sensor_definition not found. Exiting")            
            sys.exit(1)

        self.enable_task_list.append(self.default_data_loop())
        self.enable_task_list.append(self.sampling_monitor())

    def configure(self):
        """Restores explicit version tracking from JSON metadata."""
        super(Aurora3000, self).configure()
        try:
            with open("/app/config/sensor.conf", "r") as f:
                conf = yaml.safe_load(f)
        except FileNotFoundError:
            conf = {"serial_number": "UNKNOWN", "interfaces": {}}

        sensor_iface_properties = {
            "default": {
                "device-interface-properties": {
                    "connection-properties": {"baudrate": 38400, "bytesize": 8, "parity": "N", "stopbit": 1},
                    "read-properties": {"read-method": "readline", "decode-errors": "strict", "send-method": "ascii"},
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

        # Restore format_version override
        try:
            self.device_format_version = self.config.metadata.attributes["format_version"].data
        except (KeyError, AttributeError):
            pass

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
        """Fix: Uses set_actual to prevent infinite recursion loop."""
        await super().settings_check()
        
        if not self.settings.get_health():
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):
                    
                    setting_obj = self.settings.get_setting(name)
                    # Safely extract requested value from framework dictionary
                    target_val = setting_obj.get("requested") if isinstance(setting_obj, dict) else setting_obj
                    
                    self.logger.info(f"MQTT Settings Request: changing {name} to {target_val}")
                    
                    if name == "ext_valve_state":
                        await self.set_ext_valve(target_val)
                        self.settings.set_actual(name, target_val) # Acknowledge health
                        
                    elif name in ["sampling_state", "calibration_routine"]:
                        self.settings.set_actual(name, target_val)

    async def sampling_monitor(self):
        """State machine mapping envds settings to Aurora hardware routines."""
        self.polling_task = asyncio.create_task(self.polling_loop())
        
        # Ensure the valve aligns with default schema state on boot
        startup_valve = self.settings.get_setting("ext_valve_state")
        if isinstance(startup_valve, dict):
            startup_valve = startup_valve.get("requested")
            
        if startup_valve:
            await self.set_ext_valve(startup_valve)
            
        while True:
            try:
                # Fix: Safely handle dictionary payloads from MQTT
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                state_str = str(state).lower()

                routine_obj = self.settings.get_setting("calibration_routine")
                routine = routine_obj.get("requested", "none") if isinstance(routine_obj, dict) else "none"

                if self.sampling() and state_str == "sampling":
                    if not self.collecting:
                        await self.interface_send_data(data={"data": f"DO0{self.module_address}051\r"})
                        await self.interface_send_data(data={"data": f"**{self.module_address}J0\r"})
                        self.collecting = True
                
                elif state_str in ["idle", "maintenance"]:
                    if self.collecting:
                        await self.interface_send_data(data={"data": f"DO0{self.module_address}050\r"})
                        self.collecting = False
                
                elif state_str == "calibration":
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
        """Fix: Uses variable_types to optimize payload size."""
        if not data: return None
        
        try:
            # Metadata filtering logic to reduce MQTT record size
            v_types = ["main", "setting", "calibration"] if self.include_metadata else ["main"]
            record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
            self.include_metadata = False

            raw_payload = data.data if isinstance(data.data, dict) else {}
            record["timestamp"] = raw_payload.get("timestamp")
            record["variables"]["time"]["data"] = raw_payload.get("timestamp")
            
            parts = [p.strip() for p in raw_payload.get("data", "").split(",")]
            
            if len(parts) >= 14:
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

                # Check if instrument has organically finished its routine
                if self.cal_active and major_state == 0:
                    self.logger.info("Calibration sequence completed by Aurora firmware.")
                    self.settings.set_setting("calibration_routine", "none")
                    self.settings.set_setting("sampling_state", "sampling" if self.sampling() else "idle")
                    self.cal_active = False

                return record
            return None
        except (ValueError, TypeError, Exception) as e:
            self.logger.warning(f"Error parsing data payload: {e}")
            return None

    async def default_data_loop(self):
        while True:
            try:
                data = await self.default_data_buffer.get()
                record = self.default_parse(data)
                
                if record and self.sampling():
                    event = DAQEvent.create_data_update(source=self.get_id_as_source(), data=record)
                    event["destpath"] = f"{self.get_id_as_topic()}/data/update"
                    await self.send_message(event)
            except Exception as e:
                self.logger.error(f"default_data_loop error: {e}")
            await asyncio.sleep(0.1)

class ServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 9080
    log_level: str = "info"

async def shutdown(sensor):
    if sensor: await sensor.shutdown()

async def main(server_config: ServerConfig = None):
    """Matches MAGIC 250 main entry logic."""
    if server_config is None: server_config = ServerConfig()
    envdsLogger(level=logging.DEBUG).init_logger()
    
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
    asyncio.run(main(ServerConfig()))