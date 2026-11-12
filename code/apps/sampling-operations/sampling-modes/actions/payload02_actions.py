import logging
from envds.daq.event import DAQEvent

L = logging.getLogger("payload02_actions")

async def _broadcast_setting_request(sampling_system, variable_name, value):
    """
    Looks up the physical hardware routing ID for a variable via the local 
    VariableMap cache, builds a strict CloudEvent, and broadcasts it to the edge.
    """
    try:
        target_device = "unknown"
        target_type = "sensor"
        dest_path = "envds/sensor/settings/request"
        
        for platform_name, vm_dict in sampling_system.variablemaps.get("platform", {}).items():
            for vm_name, time_dict in vm_dict.items():
                for valid_time, vm_obj in time_dict.items():
                    vm_data = vm_obj["variablemap"].get("data", {})
                    for vs_name, vs_def in vm_data.get("variablesets", {}).items():
                        for v_name, v_def in vm_data.get("variables", {}).items():
                            if v_name == variable_name:
                                active_var = vm_obj["variablesets"].get(vs_name, {}).get("variables", {}).get(v_name, {})
                                attrs = active_var.get("attributes", v_def.get("attributes", {}))
                                
                                target_device = attrs.get("source_id", {}).get("data", "unknown")
                                target_type = attrs.get("source_type", {}).get("data", "sensor").lower()
                                src_var = attrs.get("source_variable", {}).get("data", variable_name)
                                break
        
        if target_device == "unknown":
            L.error(f"Cannot broadcast setting for {variable_name}: hardware source_id not found in local variablemaps.")
            return

        if target_type == "controller":
            dest_path = "envds/controller/settings/request"
            ce_obj = DAQEvent.create_controller_settings_request(
                source=f"envds.{sampling_system.config.daq_id}.sampling-operations",
                data={"settings": {src_var: {"requested": value}}},
                extra_header={"controllerid": target_device, "destpath": dest_path}
            )
        else:
            dest_path = "envds/sensor/settings/request"
            ce_obj = DAQEvent.create_sensor_settings_request(
                source=f"envds.{sampling_system.config.daq_id}.sampling-operations",
                data={"settings": {src_var: {"requested": value}}},
                extra_header={"deviceid": target_device, "destpath": dest_path}
            )

        await sampling_system.send_to_mqtt(dest_path, ce_obj)
        L.info(f"Broadcasted network setting request: {src_var}={value} -> {target_device}")

    except Exception as e:
        L.error(f"Failed to broadcast network setting request for {variable_name}", extra={"reason": str(e)})

# --- STARTUP ACTIONS ---
async def turn_on_24v_bus(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_on_24v_bus", "target_val": 1})
    await _broadcast_setting_request(self, "power_24v_bus", 1)
    return {"power_24v_bus": 1}

async def turn_on_tec(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_on_tec", "target_val": 1})
    await _broadcast_setting_request(self, "power_tec", 1)
    return {"power_tec": 1}

async def turn_on_neph(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_on_neph", "target_val": 1})
    await _broadcast_setting_request(self, "power_neph", 1)
    return {"power_neph": 1}

async def turn_on_absorb(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_on_absorb", "target_val": 1})
    await _broadcast_setting_request(self, "power_absorb", 1)
    return {"power_absorb": 1}

# --- SHUTDOWN ACTIONS ---
async def turn_off_absorb(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_off_absorb", "target_val": 0})
    await _broadcast_setting_request(self, "power_absorb", 0)
    return {"power_absorb": 0}

async def turn_off_neph(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_off_neph", "target_val": 0})
    await _broadcast_setting_request(self, "power_neph", 0)
    return {"power_neph": 0}

async def turn_off_tec(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_off_tec", "target_val": 0})
    await _broadcast_setting_request(self, "power_tec", 0)
    return {"power_tec": 0}

async def turn_off_24v_bus(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_off_24v_bus", "target_val": 0})
    await _broadcast_setting_request(self, "power_24v_bus", 0)
    return {"power_24v_bus": 0}

# --- VALVE ACTIONS ---
async def set_valve_total_aerosol(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_valve_total_aerosol", "target_val": 0})
    await _broadcast_setting_request(self, "size_cut_valve_state_sp", 0)
    return {"size_cut_valve_state_sp": 0}

async def set_valve_cyclone(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_valve_cyclone", "target_val": 1})
    await _broadcast_setting_request(self, "size_cut_valve_state_sp", 1)
    return {"size_cut_valve_state_sp": 1}

# --- STATE ACTIONS ---
async def set_neph_idle(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_neph_idle"})
    await _broadcast_setting_request(self, "neph_sampling_state", "idle")
    return {"neph_sampling_state": "idle"}

async def set_neph_sampling(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_neph_sampling"})
    await _broadcast_setting_request(self, "neph_sampling_state", "sampling")
    return {"neph_sampling_state": "sampling"}

async def set_absorb_idle(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_absorb_idle"})
    await _broadcast_setting_request(self, "absorb_sampling_state", "idle")
    return {"absorb_sampling_state": "idle"}

async def set_absorb_sampling(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_absorb_sampling"})
    await _broadcast_setting_request(self, "absorb_sampling_state", "sampling")
    return {"absorb_sampling_state": "sampling"}