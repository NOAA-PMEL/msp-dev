import logging
from envds.daq.event import DAQEvent

L = logging.getLogger("payload01_actions")

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
async def turn_on_opc(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_on_opc", "target_val": 1})
    await _broadcast_setting_request(self, "power_opc", 1)
    return {"power_opc": 1}

async def turn_on_smps(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_on_smps", "target_val": 1})
    await _broadcast_setting_request(self, "power_smps", 1)
    return {"power_smps": 1}

async def turn_on_cpc(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_on_cpc", "target_val": 1})
    await _broadcast_setting_request(self, "power_cpc", 1)
    return {"power_cpc": 1}

async def turn_on_aps(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_on_aps", "target_val": 1})
    await _broadcast_setting_request(self, "power_aps", 1)
    return {"power_aps": 1}

# --- SHUTDOWN ACTIONS ---
async def turn_off_aps(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_off_aps", "target_val": 0})
    await _broadcast_setting_request(self, "power_aps", 0)
    return {"power_aps": 0}

async def turn_off_cpc(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_off_cpc", "target_val": 0})
    await _broadcast_setting_request(self, "power_cpc", 0)
    return {"power_cpc": 0}

async def turn_off_smps(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_off_smps", "target_val": 0})
    await _broadcast_setting_request(self, "power_smps", 0)
    return {"power_smps": 0}

async def turn_off_opc(self, **kwargs):
    L.info("action_triggered", extra={"action": "turn_off_opc", "target_val": 0})
    await _broadcast_setting_request(self, "power_opc", 0)
    return {"power_opc": 0}

# --- STATE ACTIONS ---
async def set_cpc_idle(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_cpc_idle"})
    await _broadcast_setting_request(self, "cpc_sampling_state", "idle")
    return {"cpc_sampling_state": "idle"}

async def set_cpc_sampling(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_cpc_sampling"})
    await _broadcast_setting_request(self, "cpc_sampling_state", "sampling")
    return {"cpc_sampling_state": "sampling"}

async def set_smps_idle(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_smps_idle"})
    await _broadcast_setting_request(self, "smps_sampling_state", "idle")
    return {"smps_sampling_state": "idle"}

async def set_smps_sampling(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_smps_sampling"})
    await _broadcast_setting_request(self, "smps_sampling_state", "sampling")
    return {"smps_sampling_state": "sampling"}

async def set_opc_idle(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_opc_idle"})
    await _broadcast_setting_request(self, "opc_sampling_state", "idle")
    return {"opc_sampling_state": "idle"}

async def set_opc_sampling(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_opc_sampling"})
    await _broadcast_setting_request(self, "opc_sampling_state", "sampling")
    return {"opc_sampling_state": "sampling"}

async def set_aps_idle(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_aps_idle"})
    await _broadcast_setting_request(self, "aps_sampling_state", "idle")
    return {"aps_sampling_state": "idle"}

async def set_aps_sampling(self, **kwargs):
    L.info("action_triggered", extra={"action": "set_aps_sampling"})
    await _broadcast_setting_request(self, "aps_sampling_state", "sampling")
    return {"aps_sampling_state": "sampling"}