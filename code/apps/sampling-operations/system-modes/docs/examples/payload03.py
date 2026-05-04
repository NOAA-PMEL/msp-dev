platform_payload03 = {
    "version": "v1",
    "kind": "Platform",
    "metadata": {
        "name": "MSPPayload03",
        "platform_type": "MSPPayload",
        "owner": "UW/CICOES",
        "description": "",
        "contacts": [""],
    },
    "interface": {
        "power-input": [
            "input-24v-ups",
            "input-24v",
            "input-110v"
        ],
        "network": [
            "internal",
            "external"
        ],
        "sample-input": [
            "sample-line-01"
        ],
    },

},

platform_variable_map_payload03 = {
    "version": "v1", 
    "kind": "PlatformVariableMap",
    "metadata": {
        "name": "MSPPayload03", # variable map name
        "platform": "MSPPayload03" # platform name
    },
    "valid-config-time": "2025-01-01T00:00:00Z",
    "variable_groups": {
        "main": {
            "timebase": 1
        },
        "sizing": {
            "timebase": 30
        },
    },
    "variables": {
        "cn": {  
            "type": "float",
            "shape": ["time"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "concentration": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "AerosolDynamics::MAGIC250::154",
                    "source_variable": "concentration",
                }
            },
            "direct-value": { # if source-variable name is same as variable name, don't need this
                "source-variable": "concentration"
            },
            "variable_group": "main",
            # "timebase": 1, # timebase for mapped variable - will map to this even timebase
            "index_method": ["round", "average"], # method used to map to time base in decreasing order
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                # "long_name": {
                #     "type": "string",
                #     "data": "Particle Number Concentration"
                # },
                # "standard_name": {
                #     "type": "string",
                #     "data": "number_concentration_of_aerosol_particles_at_stp_in_air"
                # },
                "units": {
                    "type": "string",
                    "data": "cm-3"
                },
                
            },

        },
        "smps_diameter": {     
            "type": "float",
            "shape": ["time", "diameter"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "smps_diameter": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "AerosolDynamics::SpiderMAGIC::002",
                    "source_variable": "diameter",
                }
            },
            "variable_group": "sizing",
            # "timebase": 30, # timebase for mapped variable - will map to this even timebase
            "index_method": ["round", "average"], # method used to map to time base in decreasing order
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                # "long_name": {
                #     "type": "string",
                #     "data": "SMPS Diameter"
                # },
                # "standard_name": {
                #     "type": "string",
                #     "data": "number_concentration_of_aerosol_particles_at_stp_in_air"
                # },
                "units": {
                    "type": "string",
                    "data": "um" # this will require conversion
                },
                
            },

        },
        "smps_dNdlogDp": {     
            "type": "float",
            "shape": ["time", "diameter"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "smps_dNdlogDp": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "AerosolDynamics::SpiderMAGIC::002",
                    "source_variable": "dNdlogDp",
                }
            },
            "variable_group": "sizing",
            # "timebase": 30, # timebase for mapped variable - will map to this even timebase
            "index_method": ["round", "average"], # method used to map to time base in decreasing order
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                "long_name": {
                    "type": "string",
                    "data": "SMPS Particle Number Size Distribution"
                },
                # "standard_name": {
                #     "type": "string",
                #     "data": "number_concentration_of_aerosol_particles_at_stp_in_air"
                # },
                "units": {
                    "type": "string",
                    "data": "cm-3"
                },
                
            },

        },
        "smps_intN": {     
            "type": "float",
            "shape": ["time"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "smps_intN": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "AerosolDynamics::SpiderMAGIC::002",
                    "source_variable": "intN",
                }
            },
            "variable_group": "sizing",
            # "timebase": 30, # timebase for mapped variable - will map to this even timebase
            "index_method": ["round", "average"], # method used to map to time base in decreasing order
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                "long_name": {
                    "type": "string",
                    "data": "SMPS Integral Number Concentration"
                },
                # "standard_name": {
                #     "type": "string",
                #     "data": "number_concentration_of_aerosol_particles_at_stp_in_air"
                # },
                "units": {
                    "type": "string",
                    "data": "cm-3"
                },
                
            },

        },
        "opc_diameter": {     
            "type": "float",
            "shape": ["time", "diameter"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "opc_diameter": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "Handix::POPS::??",
                    "source_variable": "diameter",
                }
            },
            "variable_group": "sizing",
            # "timebase": 30, # timebase for mapped variable - will map to this even timebase
            "index_method": ["round", "average"], # method used to map to time base in decreasing order
            "attributes": {
                # "variable_type": {
                #     "type": "string",
                #     "data": "main"
                # },
                # "long_name": {
                #     "type": "string",
                #     "data": "SMPS Diameter"
                # },
                # "standard_name": {
                #     "type": "string",
                #     "data": "number_concentration_of_aerosol_particles_at_stp_in_air"
                # },
                "units": {
                    "type": "string",
                    "data": "um" # this may require conversion
                },
                
            },

        },
        "opc_dNdlogDp": {     
            "type": "float",
            "shape": ["time", "diameter"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "opc_dNdlogDp": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "Handix::POPS::??",
                    "source_variable": "dNdlogDp",
                }
            },
            "variable_group": "sizing",
            # "timebase": 30, # timebase for mapped variable - will map to this even timebase
            "index_method": ["round", "average"], # method used to map to time base in decreasing order
            "attributes": {
                # "variable_type": {
                #     "type": "string",
                #     "data": "main"
                # },
                # "long_name": {
                #     "type": "string",
                #     "data": "SMPS Particle Number Size Distribution"
                # },
                # "standard_name": {
                #     "type": "string",
                #     "data": "number_concentration_of_aerosol_particles_at_stp_in_air"
                # },
                # "units": {
                #     "type": "string",
                #     "data": "cm-3"
                # },
                
            },

        },
        "opc_intN": {     
            "type": "float",
            "shape": ["time"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "opc_intN": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "Handix::POPS::??",
                    "source_variable": "intN",
                }
            },
            "variable_group": "sizing",
            # "timebase": 30, # timebase for mapped variable - will map to this even timebase
            "index_method": ["round", "average"], # method used to map to time base in decreasing order
            "attributes": {
                # "variable_type": {
                #     "type": "string",
                #     "data": "main"
                # },
                # "long_name": {
                #     "type": "string",
                #     "data": "SMPS Integral Number Concentration"
                # },
                # "standard_name": {
                #     "type": "string",
                #     "data": "number_concentration_of_aerosol_particles_at_stp_in_air"
                # },
                # "units": {
                #     "type": "string",
                #     "data": "cm-3"
                # },
                
            },

        },
        "aps_diameter": {     
            "type": "float",
            "shape": ["time", "diameter"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "aps_diameter": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "TSI::APS::??",
                    "source_variable": "diameter",
                }
            },
            "variable_group": "sizing",
            # "timebase": 30, # timebase for mapped variable - will map to this even timebase
            "index_method": ["round", "average"], # method used to map to time base in decreasing order
            "attributes": {
                # "variable_type": {
                #     "type": "string",
                #     "data": "main"
                # },
                # "long_name": {
                #     "type": "string",
                #     "data": "SMPS Diameter"
                # },
                # "standard_name": {
                #     "type": "string",
                #     "data": "number_concentration_of_aerosol_particles_at_stp_in_air"
                # },
                "units": {
                    "type": "string",
                    "data": "um" # this may require conversion
                },
                
            },

        },
        "aps_dNdlogDp": {     
            "type": "float",
            "shape": ["time", "diameter"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "aps_dNdlogDp": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "TSI::APS::??",
                    "source_variable": "dNdlogDp",
                }
            },
            "variable_group": "sizing",
            # "timebase": 30, # timebase for mapped variable - will map to this even timebase
            "index_method": ["round", "average"], # method used to map to time base in decreasing order
            "attributes": {
                # "variable_type": {
                #     "type": "string",
                #     "data": "main"
                # },
                # "long_name": {
                #     "type": "string",
                #     "data": "SMPS Particle Number Size Distribution"
                # },
                # "standard_name": {
                #     "type": "string",
                #     "data": "number_concentration_of_aerosol_particles_at_stp_in_air"
                # },
                # "units": {
                #     "type": "string",
                #     "data": "cm-3"
                # },
                
            },

        },
        "aps_intN": {     
            "type": "float",
            "shape": ["time"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "aps_intN": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "TSI::APS::??",
                    "source_variable": "intN",
                }
            },
            "variable_group": "sizing",
            # "timebase": 30, # timebase for mapped variable - will map to this even timebase
            "index_method": ["round", "average"], # method used to map to time base in decreasing order
            "attributes": {
                # "variable_type": {
                #     "type": "string",
                #     "data": "main"
                # },
                # "long_name": {
                #     "type": "string",
                #     "data": "SMPS Integral Number Concentration"
                # },
                # "standard_name": {
                #     "type": "string",
                #     "data": "number_concentration_of_aerosol_particles_at_stp_in_air"
                # },
                # "units": {
                #     "type": "string",
                #     "data": "cm-3"
                # },
                
            },

        },
        "sample_temparature": {     
            "type": "float",
            "shape": ["time"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "temperature": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "IST::HYT2xx::xxx",
                    "source_variable": "temperature",
                }
            },
            "direct-value": { # if source-variable name is same as variable name, don't need this
                "source-variable": "temperature"
            },
            "variable_group": "main",
            # "timebase": 1, # timebase for mapped variable - will map to this even timebase
            "index_method": ["round", "average"], # method used to map to time base in decreasing order
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                # "long_name": {
                #     "type": "string",
                #     "data": "Particle Number Concentration"
                # },
                # "standard_name": {
                #     "type": "string",
                #     "data": "number_concentration_of_aerosol_particles_at_stp_in_air"
                # },
                "units": {
                    "type": "string",
                    "data": "degrees_c"
                },
                
            },

        },
        "sample_rh": {     
            "type": "float",
            "shape": ["time"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "sample_rh": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "IST::HYT2xx::xxx",
                    "source_variable": "rh",
                }
            },
            "variable_group": "main",
            # "timebase": 1, # timebase for mapped variable - will map to this even timebase
            "index_method": ["round", "average"], # method used to map to time base in decreasing order
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                # "long_name": {
                #     "type": "string",
                #     "data": "Particle Number Concentration"
                # },
                # "standard_name": {
                #     "type": "string",
                #     "data": "number_concentration_of_aerosol_particles_at_stp_in_air"
                # },
                "units": {
                    "type": "string",
                    "data": "%"
                },
                
            },

        },
        "total_flow": {     
            "type": "float",
            "shape": ["time"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "flow_rate": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "PUMP::ID",
                    "source_variable": "flow_rate",
                }
            },
            "direct-value": { # if source-variable name is same as variable name, don't need this
                "source-variable": "flow_rate"
            },
            "variable_group": "main",
            # "timebase": 30, # timebase for mapped variable - will map to this even timebase
            "index_method": ["round", "average"], # method used to map to time base in decreasing order
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                # "long_name": {
                #     "type": "string",
                #     "data": "Particle Number Concentration"
                # },
                # "standard_name": {
                #     "type": "string",
                #     "data": "number_concentration_of_aerosol_particles_at_stp_in_air"
                # },
                "units": {
                    "type": "string",
                    "data": "l min-1"
                },
                
            },

        },
        "total_flow_sp": {     
            "type": "float",
            "shape": ["time"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "flow_rate_sp": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "PUMP::ID",
                    "source_variable": "flow_rate_sp",
                }
            },
            "direct-value": { # if source-variable name is same as variable name, don't need this
                "source-variable": "flow_rate_sp"
            },
            "variable_group": "main",
            # "timebase": 30, # timebase for mapped variable - will map to this even timebase
            "index_method": ["round", "average"], # method used to map to time base in decreasing order
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "setting"
                },
                # "long_name": {
                #     "type": "string",
                #     "data": "Particle Number Concentration"
                # },
                # "standard_name": {
                #     "type": "string",
                #     "data": "number_concentration_of_aerosol_particles_at_stp_in_air"
                # },
                "units": {
                    "type": "string",
                    "data": "l min-1"
                },
                
            },

        },
        "case_temparature_1": {     
            "type": "float",
            "shape": ["time"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "temperature": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "IST::HYT2xx::xxx",
                    "source_variable": "temperature",
                }
            },
            # if source-variable name is same as variable name, don't need this. Will override name if present
            "direct-value": { 
                "source-variable": "temperature"
            },
            "variable_group": "main",
            # "timebase": 1, # timebase for mapped variable - will map to this even timebase
            "index_method": ["round", "average"], # method used to map to time base in decreasing order
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                # "long_name": {
                #     "type": "string",
                #     "data": "Particle Number Concentration"
                # },
                # "standard_name": {
                #     "type": "string",
                #     "data": "number_concentration_of_aerosol_particles_at_stp_in_air"
                # },
                "units": {
                    "type": "string",
                    "data": "degrees_c"
                },
                
            },

        },
        "case_temparature_2": {     
            "type": "float",
            "shape": ["time"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "temperature": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "IST::HYT2xx::xxx",
                    "source_variable": "temperature",
                }
            },
            # if source-variable name is same as variable name, don't need this. Will override name if present
            "direct-value": { 
                "source-variable": "temperature"
            },
            "variable_group": "main",
            # "timebase": 1, # timebase for mapped variable - will map to this even timebase
            "index_method": ["round", "average"], # method used to map to time base in decreasing order
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                # "long_name": {
                #     "type": "string",
                #     "data": "Particle Number Concentration"
                # },
                # "standard_name": {
                #     "type": "string",
                #     "data": "number_concentration_of_aerosol_particles_at_stp_in_air"
                # },
                "units": {
                    "type": "string",
                    "data": "degrees_c"
                },
                
            },

        },
        "tec_sp": {     
            "type": "float",
            "shape": ["time"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "temperature_sp": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "TEC::?::?",
                    "source_variable": "temperature_sp",
                }
            },
            # if source-variable name is same as variable name, don't need this. Will override name if present
            "direct-value": { 
                "source-variable": "temperature_sp"
            },
            "variable_group": "main",
            # "timebase": 1, # timebase for mapped variable - will map to this even timebase
            "index_method": ["round", "average"], # method used to map to time base in decreasing order
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "setting"
                },
                # "long_name": {
                #     "type": "string",
                #     "data": "Particle Number Concentration"
                # },
                # "standard_name": {
                #     "type": "string",
                #     "data": "number_concentration_of_aerosol_particles_at_stp_in_air"
                # },
                "units": {
                    "type": "string",
                    "data": "degrees_c"
                },
                
            },

        },

    }
}


msppayload03_sampling_layout = {
    "version": "v1",
    "kind": "SamplingLayout",
    "metadata": {
        "name": "MSPPaylaod03",
        "platform": "MSPPayload03"
    },
    "valid-config-time": "2025-08-24T00:00:00Z",
    "interfaces": {
        "cpc-serial": {
            "kind": "Interface",
            "interface-id": "Lantronix::??::",
            "port": "10001"
        },
        "aps-serial": {
            "kind": "Interface",
            "interface-id": "USConverter::??::",
            "port": "4001"
        },
        "opc-serial": {
            "kind": "Interface",
            "interface-id": "USConverter::??::",
            "port": "4001"
        },
        "smps-serial": {
            "interface-id": "Lantronix::??::",
            "port": "10001"
        },
        "i2c-bus": {
            "kind": "Interface",
            "interface-id": "LabJack::Tx::",
            "path": "i2c"
        },
        "total-flow-adc": {
            "kind": "Interface",
            "interface-id": "LabJack::Tx::",
            "path": "adc-01"
        },
        "total-flow-pwm": {
            "kind": "Interface",
            "interface-id": "LabJack::Tx::",
            "path": "pwm-01"
        },
        "total-flow-rpm": {
            "kind": "Interface",
            "interface-id": "LabJack::Tx::",
            "path": "counter-01"
        },
    },
    "controllers": {
        "sensor-pdu": {
            "kind": "Controller",
            "controller-id": "Synaccess::NP05B::001",
            "variable-map": {
                "outlet-1": "outlet_1_power",
                "outlet-2": "outlet_2_power",
                "outlet-3": "outlet_3_power",
                "outlet-4": "outlet_4_power",
                "outlet-5": "outlet_5_power",
            },
            # relays?

        }
    },
    "devices": {
        "cpc": {
            "kind": "Sensor",
            "device-id": "AerosolDynamics::MAGIC250::154",
            "interface": {
                "default": "cpc-serial"
            },
            "power": {
                "controller": "sensor-pdu",
                "controller-variable": "outlet-1"
            }
        },
        "aps": {
            "kind": "Sensor",
            "device-id": "TSI::APS::??",
            "interface": {
                "default": "aps-serial"
            },
            "power": {
                "controller": "sensor-pdu",
                "controller-variable": "outlet-2"
            }
        },
        "opc": {
            "kind": "Sensor",
            "device-id": "Handix::POPS::???",
            "interface": {
                "default": "opc-serial"
            },
            "power": {
                "controller": "sensor-pdu",
                "controller-variable": "outlet-3"
            }
        },
        "smps": {
            "kind": "Sensor",
            "device-id": "AerosolDynamics::SpiderMagic::002",
            "interface": {
                "default": "smps-serial"
            },
            "power": {
                "controller": "sensor-pdu",
                "controller-variable": "outlet-4"
            }
        },
        "sample_t_rh": {
            "kind": "Operational",
            "device-id": "IST::HYT271::???",
            "interface": {
                "default": "i2c-bus"
            },
            "power": {
                "controller": "i2c-relay",
                "controller-variable": "channel-1"
            }
        },
        "total_flow": {
            "kind": "Operational",
            "device-id": "IST::Flow::???",
            "interface": {
                "default": "total-flow-adc",
            },
            "power": {
                "controller": "sensor-pdu",
                "controller-variable": "outlet-1"
            }
        },
        "total_flow_pump": {
            "kind": "Operational",
            "device-id": "mfg::pump::???",
            "interface": {
                "pwm": "total-flow-pwm",
                "rpm": "total-flow-rpm"
            },
            "power": {
                "controller": "sensor-pdu",
                "controller-variable": "outlet-1"
            }
        },
        "case_trh_1": {
            "kind": "Operational",
            "device-id": "IST::HYT271::???",
            "interface": {
                "default": "i2c-bus"
            },
            "power": {
                "controller": "i2c-relay",
                "controller-variable": "channel-1"
            }
        },
        "case_trh_2": {
            "kind": "Operational",
            "device-id": "IST::HYT271::???",
            "interface": {
                "default": "i2c-bus"
            },
            "power": {
                "controller": "i2c-relay",
                "controller-variable": "channel-1"
            }
        },
    },
    "sampling": {
        "internal": {
            "sample-line-01": {
                "kind": "SampleLine",
                "devices": [
                    {"device": "cpc", "sample-order-index": 1},
                    {"device": "aps", "sample-order-index": 2},
                    {"device": "opc", "sample-order-index": 3},
                    {"device": "smps", "sample-order-index": 4},
                    {"device": "sample_t_rh", "sample-order-index": 5},
                    {"device": "total_flow", "sample-order-index": 6},
                    {"device": "total_flow_pump", "sample-order-index": 7},
                ]
            }
        }
    },
    "environmental": {
        "thermal-controls": {
            "devices": [
                {"device": "tec"}
            ]
            # "tec": {
            #     "kind": "Operational",
            #     "device-id": "tec::mod::??",
            #     "interface-id": "USConverter::"
            # }
        },
        "thermal-devices": {
            "case-trh-1": {
                "kind": "Device",
                "device-id": "IST::HYT???::",
                "interface-id": "Labjack::"
            },
            "case-trh-2": {
                "kind": "Device",
                "device-id": "IST::HYT???::",
                "interface-id": "Labjack::"
            },
        }
    }
}

# PlatformDataset
msppayload03_physics_main = {
    "attributes": {
        "platform": {
            "type": "string",
            "data": "MSPPayload03"
        },
        "dataset": {
            "type": "string",
            "data": "AerosolPhysics"
        },
        "dataset_type": {
            "type": "string",
            "data": "main"
        },
        "description": {
            "type": "string",
            "data": "Main one second data from Aerosol Physics payload during BEACONS"
        },
        "tags": {
            "type": "string",
            "data": "aerosol, cpc, particles, concentration, sensor, physics"
        },
        "format_version": {
            "type": "string",
            "data": "1.0.0"
        },
        "variable_types": {
            "type": "string",
            "data": "main, operational, calibration"
        },
        "valid_time": {
            "type": "string",
            "data": "2025-06-26T19:00:00Z"
        },
        "timebase": {
            "type": "int",
            "data": 1
        },
       
        # "serial_number": {
        #     "type": "string",
        #     "data": ""
        # },
        # "device_type": {
        #     "type": "string",
        #     "data": "sensor"
        # },
        "change_log": {
            "type": "string",
            "data": ""
        } 
    },
    "dimensions": {
        "time": 0
    },
    "variables": {
        "time": {
            "type": "str",
            "shape": [
                "time"
            ],
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                "long_name": {
                    "type": "string",
                    "data": "Time"
                }
            }
        },
        "cn": {
            "type": "float",
            "shape": [
                "time"
            ],
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                "long_name": {
                    "type": "char",
                    "data": "Concentration"
                },
                "units": {
                    "type": "char",
                    "data": "cm-3"
                },
                "platform_varaiable_map": {
                    "type": "string",
                    "data": "MSPPayload03"
                },
                "platform_variable": {
                    "type": "string",
                    "data": "cn"
                },
            }
        },
    }
}

msppayload03_physics_sizing = {
    "attributes": {
        "platform": {
            "type": "string",
            "data": "MSPPayload03"
        },
        "dataset": {
            "type": "string",
            "data": "sizing"
        },
        "dataset_type": {
            "type": "string",
            "data": "sizing"
        },
        "description": {
            "type": "string",
            "data": "Main one second data from Aerosol Physics payload during BEACONS"
        },
        "tags": {
            "type": "string",
            "data": "aerosol, cpc, particles, concentration, sensor, physics"
        },
        "format_version": {
            "type": "string",
            "data": "1.0.0"
        },
        "variable_types": {
            "type": "string",
            "data": "main, operational, calibration"
        },
        "valid_time": {
            "type": "string",
            "data": "2025-06-26T19:00:00Z"
        },
        "timebase": {
            "type": "int",
            "data": 1
        },
        # "serial_number": {
        #     "type": "string",
        #     "data": ""
        # },
        # "device_type": {
        #     "type": "string",
        #     "data": "sensor"
        # },
        "change_log": {
            "type": "string",
            "data": ""
        } 
    },
    "dimensions": {
        "time": 0,
        "smps_diameter": 0,
        "opc_diameter": 0,
        "aps_diameter": 0
    },
    "variables": {
        "time": {
            "type": "str",
            "shape": [
                "time"
            ],
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                "long_name": {
                    "type": "string",
                    "data": "Time"
                }
            }
        },
        "smps_diameter": {
            "type": "float",
            "shape": [
                "time", "smps_diameter"
            ],
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                # "long_name": {
                #     "type": "char",
                #     "data": "SMPS Diameter"
                # },
                # "units": {
                #     "type": "char",
                #     "data": "cm-3"
                # },
                "platform_varaiable_map": {
                    "type": "string",
                    "data": "MSPPayload03"
                },
                "platform_variable": {
                    "type": "string",
                    "data": "smps_diameter"
                },
            }
        },
        "smps_dNdlogDp": {
            "type": "float",
            "shape": [
                "time", "smps_diameter"
            ],
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                # "long_name": {
                #     "type": "char",
                #     "data": "SMPS Diameter"
                # },
                # "units": {
                #     "type": "char",
                #     "data": "cm-3"
                # },
                "platform_varaiable_map": {
                    "type": "string",
                    "data": "MSPPayload03"
                },
                "platform_variable": {
                    "type": "string",
                    "data": "smps_dNdlogDp"
                },
            }
        },
        "smps_intN": {
            "type": "float",
            "shape": [
                "time"
            ],
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                # "long_name": {
                #     "type": "char",
                #     "data": "SMPS Diameter"
                # },
                # "units": {
                #     "type": "char",
                #     "data": "cm-3"
                # },
                "platform_varaiable_map": {
                    "type": "string",
                    "data": "MSPPayload03"
                },
                "platform_variable": {
                    "type": "string",
                    "data": "smps_intN"
                },
            }
        },
        "opc_diameter": {
            "type": "float",
            "shape": [
                "time", "opc_diameter"
            ],
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                # "long_name": {
                #     "type": "char",
                #     "data": "SMPS Diameter"
                # },
                # "units": {
                #     "type": "char",
                #     "data": "cm-3"
                # },
                "platform_varaiable_map": {
                    "type": "string",
                    "data": "MSPPayload03"
                },
                "platform_variable": {
                    "type": "string",
                    "data": "opc_diameter"
                },
            }
        },
        "opc_dNdlogDp": {
            "type": "float",
            "shape": [
                "time", "opc_diameter"
            ],
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                # "long_name": {
                #     "type": "char",
                #     "data": "SMPS Diameter"
                # },
                # "units": {
                #     "type": "char",
                #     "data": "cm-3"
                # },
                "platform_varaiable_map": {
                    "type": "string",
                    "data": "MSPPayload03"
                },
                "platform_variable": {
                    "type": "string",
                    "data": "opc_dNdlogDp"
                },
            }
        },
        "opc_intN": {
            "type": "float",
            "shape": [
                "time"
            ],
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                # "long_name": {
                #     "type": "char",
                #     "data": "SMPS Diameter"
                # },
                # "units": {
                #     "type": "char",
                #     "data": "cm-3"
                # },
                "platform_varaiable_map": {
                    "type": "string",
                    "data": "MSPPayload03"
                },
                "platform_variable": {
                    "type": "string",
                    "data": "opc_intN"
                },
            }
        },
        "aps_diameter": {
            "type": "float",
            "shape": [
                "time", "aps_diameter"
            ],
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                # "long_name": {
                #     "type": "char",
                #     "data": "SMPS Diameter"
                # },
                # "units": {
                #     "type": "char",
                #     "data": "cm-3"
                # },
                "platform_varaiable_map": {
                    "type": "string",
                    "data": "MSPPayload03"
                },
                "platform_variable": {
                    "type": "string",
                    "data": "aps_diameter"
                },
            }
        },
        "aps_dNdlogDp": {
            "type": "float",
            "shape": [
                "time", "aps_diameter"
            ],
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                # "long_name": {
                #     "type": "char",
                #     "data": "SMPS Diameter"
                # },
                # "units": {
                #     "type": "char",
                #     "data": "cm-3"
                # },
                "platform_varaiable_map": {
                    "type": "string",
                    "data": "MSPPayload03"
                },
                "platform_variable": {
                    "type": "string",
                    "data": "aps_dNdlogDp"
                },
            }
        },
        "aps_intN": {
            "type": "float",
            "shape": [
                "time"
            ],
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                # "long_name": {
                #     "type": "char",
                #     "data": "SMPS Diameter"
                # },
                # "units": {
                #     "type": "char",
                #     "data": "cm-3"
                # },
                "platform_varaiable_map": {
                    "type": "string",
                    "data": "MSPPayload03"
                },
                "platform_variable": {
                    "type": "string",
                    "data": "aps_intN"
                },
            }
        },
    }
}
