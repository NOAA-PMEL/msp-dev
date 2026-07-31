import logging

L = logging.getLogger("payload02_actions")

# --- STARTUP ACTIONS ---
async def turn_on_24v_bus(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_on_24v_bus"})
    return {"power_24v_bus": 1}

async def turn_on_tec(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_on_tec"})
    return {"power_tec": 1}

async def turn_on_neph(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_on_neph"})
    return {"power_neph": 1}

async def turn_on_absorb(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_on_absorb"})
    return {"power_absorb": 1}

# --- SHUTDOWN ACTIONS ---
async def turn_off_absorb(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_off_absorb"})
    return {"power_absorb": 0}

async def turn_off_neph(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_off_neph"})
    return {"power_neph": 0}

async def turn_off_tec(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_off_tec"})
    return {"power_tec": 0}

async def turn_off_24v_bus(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_off_24v_bus"})
    return {"power_24v_bus": 0}

# --- VALVE ACTIONS ---
async def set_valve_total_aerosol(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_valve_total_aerosol"})
    return {"size_cut_valve_state_sp": 0}

async def set_valve_cyclone(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_valve_cyclone"})
    return {"size_cut_valve_state_sp": 1}