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
        self.record_counter = 0
        self.var_name = None
        
        # Original Record Tracking
        self.first_record = 'C,0,C'
        self.last_record = ',Y,'
        self.array_buffer = []
        self.C_counter = 0
        self.S_counter = 0
        self.dlogDp = 0.0337
        self.diams = [
            0.50468, 0.54215, 0.58166, 0.62506, 0.67305, 0.72353, 0.7775,
            0.83546, 0.89791, 0.96488, 1.0368, 1.1143, 1.1972, 1.2867, 1.3826, 
            1.4855, 1.5965, 1.7154, 1.8433, 1.9812, 2.1291, 2.2875, 2.4579, 
            2.6413, 2.8387, 3.0505, 3.2779, 3.5227, 3.7856, 4.0679, 4.3717, 
            4.698, 5.0482, 5.4245, 5.8292, 6.2644, 6.7317, 7.2338, 7.7735, 
            8.3536, 8.9772, 9.6468, 10.366, 11.14, 11.971, 12.864, 13.824,
            14.855, 15.963, 17.154, 18.435, 19.81
        ]

        self.sensor_definition_file = "TSI_APS3321_sensor_definition.json"

        try:            
            with open(self.sensor_definition_file, "r") as f:
                self.metadata = json.load(f)
        except FileNotFoundError:
            self.logger.error("sensor_definition not found. Exiting")            
            sys.exit(1)

        self.enable_task_list.append(self.default_data_loop())
        self.enable_task_list.append(self.sampling_monitor())
        self.collecting = False
        self.sampling_frequency = 30 

    def configure(self):
        super(APS3321, self).configure()
        try:
            with open("/app/config/sensor.conf", "r") as f:
                conf = yaml.safe_load(f)
        except FileNotFoundError:
            conf = {"serial_number": "UNKNOWN", "interfaces": {}}

        if "metadata_interval" in conf:
            self.include_metadata_interval = conf["metadata_interval"]

        sensor_iface_properties = {
            "default": {
                "device-interface-properties": {
                    "connection-properties": {"baudrate": 38400, "bytesize": 7, "parity": "E", "stopbit": 1},
                    "read-properties": {"read-method": "readuntil", "read-terminator": "\r", "decode-errors": "strict", "send-method": "ascii"},
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
            daq_id=conf.get("daq_id", "default"),
        )

        try:
            self.device_format_version = self.metadata["attributes"]["format_version"]["data"]
        except (KeyError, AttributeError):
            pass

        # Pull sampling frequency properly to pass to SMT command
        self.sampling_frequency = conf.get("sampling_frequency_sec", 30)

        try:
            if "interfaces" in conf:
                for name, iface in conf["interfaces"].items():
                    self.add_interface(name, iface)
        except Exception as e:
            self.logger.error(f"Interface add error: {e}")

    async def handle_interface_data(self, message: CloudEvent):
        await super(APS3321, self).handle_interface_data(message)
        if message["type"] == det.interface_data_recv():
            try:
                path_id = message["path_id"]
                iface_path = self.config.interfaces["default"]["path"]
                if path_id == iface_path:
                    await self.default_data_buffer.put(message)
            except KeyError: pass

    async def settings_check(self):
        """FIX 1: Uses set_actual to prevent recursion depth error"""
        await super().settings_check()
        if not self.settings.get_health():
            for name in self.settings.get_settings().keys():
                if not self.settings.get_health_setting(name):
                    setting_obj = self.settings.get_setting(name)
                    target_val = setting_obj.get("requested") if isinstance(setting_obj, dict) else setting_obj
                    if name in ["sampling_state", "calibration_routine"]:
                        self.settings.set_actual(name, target_val)

    async def sampling_monitor(self):
        """FIX 2: Safely extract state dict, restores original start_cmds."""
        stop_command = 'S0\r'
        need_start, start_requested = True, False
        await asyncio.sleep(2)

        while True:
            try:
                state_obj = self.settings.get_setting("sampling_state")
                state = state_obj.get("requested", "idle") if isinstance(state_obj, dict) else "idle"
                state_str = str(state).lower()

                if self.sampling() and state_str == "sampling":
                    if need_start:
                        if self.collecting:
                            await self.interface_send_data(data={"data": stop_command})
                            await asyncio.sleep(2)
                            self.collecting = False
                            continue
                        else:
                            # ORIGINAL startup sequence including SMT and UC/US
                            start_cmds = ['S0\r', f'SMT2,{self.sampling_frequency}\r', 'UC1\r', 'UD1\r', 'US1\r', 'UY1\r', 'U1\r', 'S1\r']
                            for cmd in start_cmds:
                                await self.interface_send_data(data={"data": cmd})
                                await asyncio.sleep(.5)
                            need_start, start_requested = False, True
                            await asyncio.sleep(2)
                            continue
                    elif start_requested:
                        if self.collecting:
                            start_requested = False
                        else:
                            start_cmds = ['S0\r', f'SMT2,{self.sampling_frequency}\r', 'UC1\r', 'UD1\r', 'US1\r', 'UY1\r', 'U1\r', 'S1\r']
                            for cmd in start_cmds:
                                await self.interface_send_data(data={"data": cmd})
                                await asyncio.sleep(.5)
                            await asyncio.sleep(2)
                            continue
                            
                elif state_str in ["idle", "maintenance", "error", "calibration"]:
                    if self.collecting or not need_start:
                        await self.interface_send_data(data={"data": stop_command})
                        await asyncio.sleep(2)
                        self.collecting, need_start, start_requested = False, True, False

                await asyncio.sleep(1)
            except Exception as e:
                self.logger.error(f"sampling monitor error: {e}")
                await asyncio.sleep(1)

    async def default_data_loop(self):
        record_buffer = None  
        while True:
            try:
                data = await self.default_data_buffer.get()
                self.collecting = True
                
                # Safely extract raw data string
                raw_data = data.data if isinstance(data.data, dict) else {}
                raw_str = raw_data.get('data', '')

                # 1. Start of a new Correlated Sequence
                if self.first_record in raw_str:
                    record_buffer = self.default_parse(data)
                    self.record_counter += 1
                    continue

                # Ignore fragments if we haven't seen a header yet
                if record_buffer is None: 
                    continue

                # 2. End of the Sequence (Auxiliary Record)
                if self.last_record in raw_str:
                    record2 = self.default_parse(data)
                    if record2:
                        for var in record2["variables"]:
                            if var != 'time' and record2["variables"][var]["data"]:
                                record_buffer["variables"][var]["data"] = record2["variables"][var]["data"]
                    
                    # Safely assigning to ["data"] because variables are now in the JSON schema
                    # if "diameter" in record_buffer["variables"]:
                    #     record_buffer["variables"]["diameter"]["data"] = self.diams
                    # if "channel" in record_buffer["variables"]:
                    #     record_buffer["variables"]["channel"]["data"] = list(range(1, 65))
                    if "dN" in record_buffer["variables"]:
                        record_buffer["variables"]["dN"]["data"] = [None]*52
                    if "dlogDp" in record_buffer["variables"]:
                        record_buffer["variables"]["dlogDp"]["data"] = [self.dlogDp]*52
                    if "dNdlogDp" in record_buffer["variables"]:
                        record_buffer["variables"]["dNdlogDp"]["data"] = [None]*52
                    if "intN" in record_buffer["variables"]:
                        record_buffer["variables"]["intN"]["data"] = None

                    if self.sampling():
                        dN, dNdlogDp, intN = [], [], 0
                        flow_lpm = record_buffer["variables"].get("sflow", {}).get("data") or 1.0
                        flow_ccs = flow_lpm * (1000./60.)
                        counts = record_buffer["variables"].get("particle_counts", {}).get("data")
                        
                        if counts:
                            for cnt in counts:
                                conc = cnt / (flow_ccs * self.sampling_frequency)
                                intN += conc
                                dN.append(round(conc, 3))
                                dNdlogDp.append(round(conc / self.dlogDp, 3))
                        
                        if "intN" in record_buffer["variables"]:
                            record_buffer["variables"]["intN"]["data"] = intN
                        if "dN" in record_buffer["variables"]:
                            record_buffer["variables"]["dN"]["data"] = dN
                        if "dNdlogDp" in record_buffer["variables"]:
                            record_buffer["variables"]["dNdlogDp"]["data"] = dNdlogDp

                        event = DAQEvent.create_data_update(source=self.get_id_as_source(), data=record_buffer)
                        event["destpath"] = f"{self.get_id_as_topic()}/data/update"
                        
                        self.logger.debug("Publishing DAQ data event", extra={"destpath": str(event["destpath"])})
                        await self.send_message(event)
                        
                    record_buffer = None 
                    continue

                # 3. Intermediate Records (Aerodynamic or Side-Scatter)
                record2 = self.default_parse(data)
                if record2:
                    for var in record2["variables"]:
                        if var != 'time' and record2["variables"][var]["data"]:
                            record_buffer["variables"][var]["data"] = record2["variables"][var]["data"]
            
            except Exception as e:
                self.logger.error("default_data_loop error", extra={"error": str(e)})
                record_buffer = None 
            
            await asyncio.sleep(0.01)

    def check_array_buffer(self, data, array_cond = False):
        self.array_buffer.append(data)
        return self.array_buffer if array_cond else None

    def default_parse(self, data):
        """FIX 3: variable_types filtering and restores original complex parse loop."""
        if not data: return None
        try:
            v_types = ["main", "setting", "calibration"] if self.include_metadata else ["main"]
            record = self.build_data_record(meta=self.include_metadata, variable_types=v_types)
            self.include_metadata = False
            
            raw_payload = data.data if isinstance(data.data, dict) else {}
            record["timestamp"] = raw_payload.get("timestamp")
            record["variables"]["time"]["data"] = raw_payload.get("timestamp")
            raw_str = raw_payload.get("data", "")
            
            self.var_name, compiled_record = None, None

            if ',C,' in raw_str:
                if ',C,0' in raw_str:
                    compiled_record = raw_str.strip().split(",")[5:]
                    self.var_name, self.C_counter, self.array_buffer = ['ffff', 'stime', 'dtime', 'evt1', 'evt3', 'evt4', 'total'], 0, []
                else:
                    parts = [int(x) if x else 0 for x in raw_str.strip().split(",")[3:]]
                    parts.extend([0] * (64 - len(parts)))
                    self.var_name = ['particle_counts_accum']
                    if self.C_counter < 51:
                        self.check_array_buffer(parts, False)
                        self.C_counter += 1
                        return None 
                    compiled_record, self.array_buffer, self.C_counter = self.check_array_buffer(parts, True), [], 0

            elif ',D,' in raw_str:
                parts = [int(x) if x else 0 for x in raw_str.strip().split(",")[11:]]
                parts.extend([0] * (52 - len(parts)))
                self.var_name, compiled_record = ['particle_counts'], parts

            elif ',S,' in raw_str:
                if ',S,C' in raw_str:
                    self.S_counter, self.array_buffer = 0, []
                    return None
                parts = [int(x) if x else 0 for x in raw_str.strip().split(",")[3:]]
                parts.extend([0] * (52 - len(parts)))
                self.var_name = ['particle_counts_ss']
                if self.S_counter < 63:
                    self.check_array_buffer(parts, False)
                    self.S_counter += 1
                    return None
                compiled_record, self.array_buffer, self.S_counter = self.check_array_buffer(parts, True), [], 0
            
            elif ',Y,' in raw_str:
                parts = raw_str.strip().split(",")[2:]
                del parts[3:8]
                self.var_name, compiled_record = ['bpress', 'tflow', 'sflow', 'lpower', 'lcur', 'spumpv', 'tpumpv', 'itemp', 'btemp', 'dtemp', 'Vop'], parts

            if not self.var_name or compiled_record is None: return None

            for index, name in enumerate(self.var_name):
                if name in record["variables"]:
                    instvar = self.config.metadata.variables[name]
                    try:
                        val = compiled_record if len(self.var_name) == 1 else compiled_record[index]
                        # record["variables"][name]["data"] = int(val) if instvar.type == "int" else (float(val) if instvar.type == "float" else val)

                        # FIX: If the value is an array, it is already cast properly by our comprehensions.
                        # Only apply int() or float() casting if it's a scalar string.
                        if isinstance(val, list):
                            record["variables"][name]["data"] = val
                        else:
                            record["variables"][name]["data"] = int(val) if instvar.type == "int" else (float(val) if instvar.type == "float" else val)

                    except ValueError:
                        record["variables"][name]["data"] = "" if instvar.type in ("str", "char") else None
            return record
        except Exception as e:
            self.logger.error(f"default_parse error: {e}")
            return None

class ServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 9080
    log_level: str = "info"

async def shutdown(sensor):
    if sensor:
        await sensor.shutdown()

async def main(server_config: ServerConfig = None):
    if server_config is None: server_config = ServerConfig()
    
    sn = "9999"
    try:
        with open("/app/config/sensor.conf", "r") as f:
            conf = yaml.safe_load(f)
            sn = conf.get("serial_number", "9999")
    except FileNotFoundError: pass

    envdsLogger(level=logging.DEBUG).init_logger()
    logger = logging.getLogger(f"TSI::APS3321::{sn}")

    inst = APS3321()
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