import logging

L = logging.getLogger("payload01_actions")

# --- STARTUP ACTIONS ---
async def turn_on_opc(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_on_opc"})
    return {"power_opc": 1}

async def turn_on_smps(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_on_smps"})
    return {"power_smps": 1}

async def turn_on_cpc(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_on_cpc"})
    return {"power_cpc": 1}

async def turn_on_aps(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_on_aps"})
    return {"power_aps": 1}

# --- SHUTDOWN ACTIONS ---
async def turn_off_aps(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_off_aps"})
    return {"power_aps": 0}

async def turn_off_cpc(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_off_cpc"})
    return {"power_cpc": 0}

async def turn_off_smps(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_off_smps"})
    return {"power_smps": 0}

async def turn_off_opc(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_off_opc"})
    return {"power_opc": 0}

# --- STATE ACTIONS ---
async def set_cpc_idle(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_cpc_idle"})
    return {"cpc_sampling_state": "idle"}

async def set_cpc_sampling(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_cpc_sampling"})
    return {"cpc_sampling_state": "sampling"}

async def set_smps_idle(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_smps_idle"})
    return {"smps_sampling_state": "idle"}

async def set_smps_sampling(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_smps_sampling"})
    return {"smps_sampling_state": "sampling"}

async def set_opc_idle(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_opc_idle"})
    return {"opc_sampling_state": "idle"}

async def set_opc_sampling(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_opc_sampling"})
    return {"opc_sampling_state": "sampling"}

async def set_aps_idle(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_aps_idle"})
    return {"aps_sampling_state": "idle"}

async def set_aps_sampling(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_aps_sampling"})
    return {"aps_sampling_state": "sampling"}