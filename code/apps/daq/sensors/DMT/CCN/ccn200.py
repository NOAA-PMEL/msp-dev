import asyncio
import signal
import sys
import os
import logging
import yaml
from pydantic import BaseModel
from envds.core import envdsLogger
from ccn_base import CCNBase

class CCN200(CCNBase):
    def __init__(self, config=None, **kwargs):
        # Define strict CCN-200 boundaries
        self.sensor_definition_file = "DMT_CCN-200_sensor_definition.json"
        
        self.expected_h_len = 33
        self.expected_c_len = 46
        
        self.h_vars_A = ['Current_SS_A', 'Temp_Stabilized_A', 'Delta_T_A', 'T1_Read_A', 'T2_Read_A', 'T3_Read_A', 
                         'T_Nafion_A', 'T_Inlet_A', 'T_OPC_A', 'T_Sample_A', 'Sample_Flow_A', 'Sheath_Flow_A', 
                         'Sample_Pressure_A', 'Laser_Current_A', '1st_Stage_Mon_A', 'Valve_Set_A']
                         
        self.h_vars_B = ['Current_SS_B', 'Temp_Stabilized_B', 'Delta_T_B', 'T1_Read_B', 'T2_Read_B', 'T3_Read_B', 
                         'T_Nafion_B', 'T_Inlet_B', 'T_OPC_B', 'T_Sample_B', 'Sample_Flow_B', 'Sheath_Flow_B', 
                         'Sample_Pressure_B', 'Laser_Current_B', '1st_Stage_Mon_B', 'Valve_Set_B']
                         
        self.c_vars_A = ['Bin_Setting_A', 'Number_Conc_A', 'Overflow_A']
        self.c_vars_B = ['Bin_Setting_B', 'Number_Conc_B', 'Overflow_B']

        # Call base class initialization AFTER definitions are set
        super(CCN200, self).__init__(config=config, **kwargs)


class ServerConfig(BaseModel):
    host: str = "localhost"
    port: int = 9080
    log_level: str = "info"

async def shutdown(sensor):
    if sensor:
        await sensor.shutdown()

async def main(server_config: ServerConfig = None):
    if server_config is None:
        server_config = ServerConfig()

    sn = "9999"
    try:
        with open("/app/config/sensor.conf", "r") as f:
            conf = yaml.safe_load(f)
            sn = conf.get("serial_number", "9999")
    except FileNotFoundError: pass

    envdsLogger(level=logging.DEBUG).init_logger()
    logger = logging.getLogger(f"DMT::CCN-200::{sn}")
    
    inst = CCN200()
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