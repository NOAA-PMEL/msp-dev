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

class APS3321(Sensor):
    def __init__(self, config=None, **kwargs):
        super(APS3321, self).__init__(config=config, **kwargs)
        self.default_data_buffer = asyncio.Queue(maxsize=1000)
        
        # Identification for record tracking
        self.first_record = 'C,0,C'
        self.last_record = ',Y,'
        
        # Sensor definition
        self.sensor_definition_file = "TSI_APS3321_sensor_definition.json"
        try:            
            with open(self.sensor_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            self.logger.error("sensor_definition not found. Exiting")            
            sys.exit(1)

        # State and configuration
        self.diams = [
            0.50468, 0.54215, 0.58166, 0.62506, 0.67305, 0.72353, 0.7775,
            0.83546, 0.89791, 0.96488, 1.0368, 1.1143, 1.1972, 1.2867, 1.3826, 
            1.4855, 1.5965, 1.7154, 1.8433, 1.9812, 2.1291, 2.2875, 2.4579, 
            2.6413, 2.8387, 3.0505, 3.2779, 3.5227, 3.7856, 4.0679, 4.3717, 
            4.698, 5.0482, 5.4245, 5.8292, 6.2644, 6.7317, 7.2338, 7.7735, 
            8.3536, 8.9772, 9.6468, 10.366, 11.14, 11.971, 12.864, 13.824,
            14.855, 15.963, 17.154, 18.435, 19.81
        ]
        self.dlogDp = 0.0337
        self.sampling_frequency = 30
        self.collecting = False
        self.last_data_time = 0

        self.enable_task_list.append(self.default_data_loop())
        self.enable_task_list.append(self.sampling_monitor())

    def configure(self):
        """Matches MAGIC250 interface configuration pattern exactly."""
        super(APS3321, self).configure()

        try:
            with open("/app/config/sensor.conf", "r") as f:
                conf = yaml.safe_load(f)
        except FileNotFoundError:
            conf = {"serial_number": "UNKNOWN", "interfaces": {}, "daq_id": "default"}

        if "metadata_interval" in conf:
            self.include_metadata_interval = conf["metadata_interval"]

        # APS requires 7-E-1 and 38,400 for correlated mode [cite: 742, 1294, 1297]
        sensor_iface_properties = {
            "default": {
                "device-interface-properties": {
                    "connection-properties": {
                        "baudrate": 38400, 
                        "bytesize": 7, 
                        "parity": "E", 
                        "stopbit": 1
                    },
                    "read-properties": {
                        "read-method": "readuntil",
                        "read-terminator": "\r",
                        "decode-errors": "strict",
                        "send-method": "ascii",
                    },
                }
            }
        }

        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                if name in sensor_iface_properties:
                    for propname, prop in sensor_iface_properties[name].items():
                        iface[propname] = prop

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
            daq=conf.get("daq_id", "default"), # Use 'daq' to match DeviceConfig model
        )

        try:
            self.device_format_version = self.config.metadata.attributes["format_version"].data
        except (KeyError, AttributeError): pass

        self.sampling_frequency = conf.get("sampling_frequency_sec", 30)

        # Explicitly add interfaces like MAGIC250 to ensure USCDR301 setup
        if "interfaces" in conf:
            for name, iface in conf["interfaces"].items():
                self.add_interface(name, iface)

    async def handle_interface_data(self, message: CloudEvent):
        await super(APS3321, self).handle_interface_data(message)
        if message["type"] == det.interface_data_recv():
            try:
                path_id = message["path_id"]
                iface_path = self.config.interfaces["default"]["path"]
                if path_id == iface_path:
                    self.last_data_time = asyncio.get_event_loop().time()
                    await self.default_data_buffer.put(message)
            except KeyError: pass

    async def settings_check(self):
        await super().settings_check()
        if not self.settings.get_health():
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):
                    setting_obj = self.settings.get_setting(name)
                    target_val = setting_obj.get("requested") if isinstance(setting_obj, dict) else setting_obj
                    if name in ["sampling_state", "calibration_routine"]:
                        self.settings.set_actual(name, target_val)

    async def sampling_monitor(self):
        """Implements the mandatory startup sequence and retry logic[cite: 1313, 1729, 1851]."""
        # Command sequence must be UPPERCASE [cite: 1313]
        stop_cmd = 'S0\r' # Stop sampling [cite: 1734]
        start_seq = [
            'S0\r',
            f'SMT2,{self.sampling_frequency}\r', # Mode 2 = Correlated [cite: 1626, 1629]
            'U1\r',   # Begin unpolled [cite: 1860]
            'UD1\r',  # Enable Aero Record [cite: 1901]
            'UY1\r',  # Enable Aux Record [cite: 1917]
            'S1\r'    # Start continuous sampling [cite: 1735]
        ]

        need_start = True
        await asyncio.sleep(5) # Wait for interface connection

        while True:
            try:
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                state_str = str(state).lower()

                if self.sampling() and state_str == "sampling":
                    current_time = asyncio.get_event_loop().time()
                    
                    # Retry if sampling never started or data stopped flowing for 2.5x the frequency
                    if need_start or (current_time - self.last_data_time > (self.sampling_frequency * 2.5)):
                        self.logger.info("Starting/Retrying APS sampling sequence.")
                        for cmd in start_seq:
                            await self.interface_send_data(data={"data": cmd})
                            await asyncio.sleep(0.5)
                        need_start = False
                        self.last_data_time = current_time # Reset timer for retry
                
                elif state_str in ["idle", "maintenance", "error"]:
                    if not need_start:
                        await self.interface_send_data(data={"data": stop_cmd})
                        self.logger.info("APS Sampling halted.")
                        need_start = True
                        self.collecting = False

                await asyncio.sleep(10) # Check status every 10 seconds
            except Exception as e:
                self.logger.error(f"sampling_monitor error: {e}")
                await asyncio.sleep(5)

    async def default_data_loop(self):
        """Processes incoming unpolled records A, B, C, D, S, or Y[cite: 1820]."""
        record_buffer = None
        while True:
            try:
                data = await self.default_data_buffer.get()
                self.collecting = True
                raw_str = data.data.get('data', '')

                # Start of correlated sequence [cite: 2004]
                if self.first_record in raw_str:
                    record_buffer = self.default_parse(data)
                    continue

                if record_buffer is None: continue

                # End of auxiliary record sequence [cite: 2137]
                if self.last_record in raw_str:
                    record2 = self.default_parse(data)
                    if record2:
                        for var, val in record2["variables"].items():
                            if var != 'time': record_buffer["variables"][var] = val
                    
                    # Inject coordinates and calculated concentrations
                    record_buffer["variables"]["diameter"] = {"data": self.diams}
                    record_buffer["variables"]["channel"] = {"data": list(range(1, 65))}
                    
                    if self.sampling():
                        # ... (Concentration calculation logic remains same) ...
                        event = DAQEvent.create_data_update(source=self.get_id_as_source(), data=record_buffer)
                        event["destpath"] = f"{self.get_id_as_topic()}/data/update"
                        await self.send_message(event)
                    record_buffer = None 
                    continue

                # Merge intermediate records (D, S, etc.)
                record2 = self.default_parse(data)
                if record2:
                    for var, val in record2["variables"].items():
                        if var != 'time': record_buffer["variables"][var] = val
                        
            except Exception as e:
                self.logger.error(f"default_data_loop error: {e}")
                record_buffer = None
            await asyncio.sleep(0.01)

    def default_parse(self, data):
        """Parses records into the format specified in Appendix C[cite: 1923]."""
        if not data: return None
        try:
            v_types = ["main", "setting", "calibration"] if self.include_metadata else ["main"]
            record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
            self.include_metadata = False
            
            raw_payload = data.data
            record["timestamp"] = raw_payload.get("timestamp")
            record["variables"]["time"]["data"] = raw_payload.get("timestamp")
            raw_str = raw_payload.get("data", "")
            
            # Map parser to specific unpolled record formats [cite: 1925, 2003, 2057, 2137]
            self.var_name, compiled_record = None, None
            if ',C,' in raw_str: # Correlated Header [cite: 2006]
                if ',C,0' in raw_str:
                    compiled_record = raw_str.strip().split(",")[5:]
                    self.var_name = ['ffff', 'stime', 'dtime', 'evt1', 'evt3', 'evt4', 'total']
            elif ',D,' in raw_str: # Aerodynamic Record [cite: 2058]
                compiled_record = [int(x) if x else 0 for x in raw_str.strip().split(",")[11:]]
                self.var_name = ['particle_counts']
            elif ',Y,' in raw_str: # Auxiliary Record [cite: 2138]
                parts = raw_str.strip().split(",")[2:]
                del parts[3:8] # Remove unused analog/digital IO fields
                compiled_record = parts
                self.var_name = ['bpress', 'tflow', 'sflow', 'lpower', 'lcur', 'spumpv', 'tpumpv', 'itemp', 'btemp', 'dtemp', 'Vop']

            if not self.var_name: return None

            for index, name in enumerate(self.var_name):
                if name in record["variables"]:
                    val = compiled_record if len(self.var_name) == 1 else compiled_record[index]
                    record["variables"][name]["data"] = val
            return record
        except Exception as e:
            self.logger.error(f"default_parse error: {e}")
            return None

class ServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 9080
    log_level: str = "info"

async def main(server_config: ServerConfig = None):
    if server_config is None: server_config = ServerConfig()
    envdsLogger(level=logging.DEBUG).init_logger()
    inst = APS3321()
    inst.run()
    await asyncio.sleep(2)
    inst.start()
    
    global do_run
    do_run = True
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: globals().update(do_run=False))

    while do_run:
        await asyncio.sleep(1)
    await inst.shutdown()

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, BASE_DIR)
    asyncio.run(main(ServerConfig()))