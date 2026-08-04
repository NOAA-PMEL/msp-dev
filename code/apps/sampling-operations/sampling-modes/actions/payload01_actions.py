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