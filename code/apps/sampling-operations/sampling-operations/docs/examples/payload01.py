platform_payload01 = {
    "version": "v1",
    "kind": "Platform",
    "metadata": {
        "name": "MSPPayload01",
        "platform_type": "MSPMainPayload", # special payload to control enclosure
        "owner": "UW/CICOES",
        "description": "",
        "contacts": [""],
    },
    "interface": {
        "power-input": [
            # "input-24v-ups",
            # "input-24v",
            "input-110v-1", # from main Shelly relay
            "input-110v-2",
            "input-110v-3"
        ],
        "network": [
            "internal",
            "external"
        ],
        "sample-input": [
            "external", # ambient sensors
            "network" # UDP, etc from network
        ],
    },

},

platform_variable_map_payload01 = {
    "version": "v1", 
    "kind": "PlatformVariableMap",
    "metadata": {
        "name": "MSPPayload01", # variable map name
        "platform": "MSPPayload01" # platform name
    },
    "valid-config-time": "2025-01-01T00:00:00Z",
    "variables": {
        "relative_wind_speed": {  # platform variable that represents real-world parameter   
            "type": "float",
            "shape": ["time"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "rel_ws": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "Vaisala::WX??",
                    "source_variable": "Ws",
                }
            },
            "timebase": 1, # timebase for mapped variable - will map to this even timebase
            "timebase_method": ["round", "average"], # method used to map to time base in decreasing order
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
                    "data": "m/s"
                },
                
            },
        },
        "heading": {  # platform variable that represents real-world parameter   
            "type": "float",
            "shape": ["time"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "heading": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "Furuno::??:??",
                    "source_variable": "heading",
                }
            },
            "timebase": 1, # timebase for mapped variable - will map to this even timebase
            "timebase_method": ["round", "average"], # method used to map to time base in decreasing order
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
                    "data": "degrees"
                },
                
            },
        },
        "speed": {  # platform variable that represents real-world parameter   
            "type": "float",
            "shape": ["time"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "heading": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "Furuno::??:??",
                    "source_variable": "speed",
                }
            },
            "timebase": 1, # timebase for mapped variable - will map to this even timebase
            "timebase_method": ["round", "average"], # method used to map to time base in decreasing order
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
                    "data": "m/s"
                },
                
            },
        },
        "true_wind_speed": { # example - would be in payload01
            "type": "float",
            "shape": ["time"],
            "map_type": "calculate",
            "source": {
                "relative_ws": {
                    "source_type": "PlatformVariableMap",
                    "source": "MSPPayload01",
                    "source_variable": "relative_wind_speed"
                    
                },                
                "heading": {
                    "source_type": "PlatformVariableMap",
                    "source": "MSPPayload01",
                    "source_variable": "heading"
                },                
                "sog": {
                    "source_type": "PlatformVariableMap",
                    "source": "MSPPayload01",
                    "source_variable": "sog"
                },                
            },
            "timebase": 1,
            "timebase_method": ["round", "average"],
            "calculate_method": {
                "service": "service-name",
                "path": "/get_true_wind_speed/",
                # "parameters": ["relative_wind_speed", "heading", "speed"],
                "parameters": {
                    "relative_wind_speed": {
                        "source-variable": "relative_ws"
                    },
                    "heading": {
                        "source-variable": "heading"
                    },
                    "sog": {
                        "source-variable": "sog"
                    },
                }
            },
            "attributes": {
                "variable_type": {
                    "type": "string",
                    "data": "main"
                },
                "units": {
                    "type": "string",
                    "data": "m/s"
                },
                
            }
        },




        "smps_diameter": {  # platform variable that represents real-world parameter   
            "type": "float",
            "shape": ["time", "diameter"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "dp": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "AerosolDynamics::SpiderMAGIC::002",
                    "source_variable": "diameter",
                }
            },
            "timebase": 30, # timebase for mapped variable - will map to this even timebase
            "timebase_method": ["round", "average"], # method used to map to time base in decreasing order
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
        "smps_dNdlogDp": {  # platform variable that represents real-world parameter   
            "type": "float",
            "shape": ["time", "diameter"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "dNdlogDp": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "AerosolDynamics::SpiderMAGIC::002",
                    "source_variable": "dNdlogDp",
                }
            },
            "timebase": 30, # timebase for mapped variable - will map to this even timebase
            "timebase_method": ["round", "average"], # method used to map to time base in decreasing order
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
        "smps_intN": {  # platform variable that represents real-world parameter   
            "type": "float",
            "shape": ["time"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "intN": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "AerosolDynamics::SpiderMAGIC::002",
                    "source_variable": "intN",
                }
            },
            "timebase": 30, # timebase for mapped variable - will map to this even timebase
            "timebase_method": ["round", "average"], # method used to map to time base in decreasing order
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
        "opc_diameter": {  # platform variable that represents real-world parameter   
            "type": "float",
            "shape": ["time", "diameter"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "dp": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "Handix::POPS::??",
                    "source_variable": "diameter",
                }
            },
            "timebase": 30, # timebase for mapped variable - will map to this even timebase
            "timebase_method": ["round", "average"], # method used to map to time base in decreasing order
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
        "opc_dNdlogDp": {  # platform variable that represents real-world parameter   
            "type": "float",
            "shape": ["time", "diameter"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "dNdlogDp": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "Handix::POPS::??",
                    "source_variable": "dNdlogDp",
                }
            },
            "timebase": 30, # timebase for mapped variable - will map to this even timebase
            "timebase_method": ["round", "average"], # method used to map to time base in decreasing order
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
        "opc_intN": {  # platform variable that represents real-world parameter   
            "type": "float",
            "shape": ["time"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "intN": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "Handix::POPS::??",
                    "source_variable": "intN",
                }
            },
            "timebase": 30, # timebase for mapped variable - will map to this even timebase
            "timebase_method": ["round", "average"], # method used to map to time base in decreasing order
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
        "aps_diameter": {  # platform variable that represents real-world parameter   
            "type": "float",
            "shape": ["time", "diameter"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "dp": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "TSI::APS::??",
                    "source_variable": "diameter",
                }
            },
            "timebase": 30, # timebase for mapped variable - will map to this even timebase
            "timebase_method": ["round", "average"], # method used to map to time base in decreasing order
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
        "aps_dNdlogDp": {  # platform variable that represents real-world parameter   
            "type": "float",
            "shape": ["time", "diameter"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "dNdlogDp": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "TSI::APS::??",
                    "source_variable": "dNdlogDp",
                }
            },
            "timebase": 30, # timebase for mapped variable - will map to this even timebase
            "timebase_method": ["round", "average"], # method used to map to time base in decreasing order
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
        "aps_intN": {  # platform variable that represents real-world parameter   
            "type": "float",
            "shape": ["time"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "intN": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "TSI::APS::??",
                    "source_variable": "intN",
                }
            },
            "timebase": 30, # timebase for mapped variable - will map to this even timebase
            "timebase_method": ["round", "average"], # method used to map to time base in decreasing order
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
    }
}

# Platform Dataset
# msppayload01_physics_main = {
#     "attributes": {
#         "platform": {
#             "type": "string",
#             "data": "MSPPayload03"
#         },
#         "dataset": {
#             "type": "string",
#             "data": "AerosolPhysics"
#         },
#         "dataset_type": {
#             "type": "string",
#             "data": "main"
#         },
#         "description": {
#             "type": "string",
#             "data": "Main one second data from Aerosol Physics payload during BEACONS"
#         },
#         "tags": {
#             "type": "string",
#             "data": "aerosol, cpc, particles, concentration, sensor, physics"
#         },
#         "format_version": {
#             "type": "string",
#             "data": "1.0.0"
#         },
#         "variable_types": {
#             "type": "string",
#             "data": "main, operational, calibration"
#         },
#         "valid_time": {
#             "type": "string",
#             "data": "2025-06-26T19:00:00Z"
#         },
#         # "serial_number": {
#         #     "type": "string",
#         #     "data": ""
#         # },
#         # "device_type": {
#         #     "type": "string",
#         #     "data": "sensor"
#         # },
#         "change_log": {
#             "type": "string",
#             "data": ""
#         } 
#     },
#     "dimensions": {
#         "time": 0
#     },
#     "variables": {
#         "time": {
#             "type": "str",
#             "shape": [
#                 "time"
#             ],
#             "attributes": {
#                 "variable_type": {
#                     "type": "string",
#                     "data": "main"
#                 },
#                 "long_name": {
#                     "type": "string",
#                     "data": "Time"
#                 }
#             }
#         },
#         "cn": {
#             "type": "float",
#             "shape": [
#                 "time"
#             ],
#             "attributes": {
#                 "variable_type": {
#                     "type": "string",
#                     "data": "main"
#                 },
#                 "long_name": {
#                     "type": "char",
#                     "data": "Concentration"
#                 },
#                 "units": {
#                     "type": "char",
#                     "data": "cm-3"
#                 },
#                 "platform_varaiable_map": {
#                     "type": "string",
#                     "data": "MSPPayload03"
#                 },
#                 "platform_variable": {
#                     "type": "string",
#                     "data": "cn"
#                 },
#             }
#         },
#     }
# }

# msppayload01_physics_sizing = {
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

msppayload01_sampling_layout = {
    "version": "v1",
    "kind": "SamplingLayout",
    "metadata": {
        "name": "MSPPaylaod03",
        "platform": "MSPPayload03"
    },
    "valid-config-time": "2025-08-24T00:00:00Z",
    "interfaces": {},
    "controllers": {},
    "devices": {
        "cpc": {
            "kind": "Sensor",
            "device-id": "AerosolDynamics::MAGIC250::154",
            "interface-id": "Lantronix::??::",
            "sampling-position": 1
        },
        "aps": {
            "kind": "Sensor",
            "device-id": "TSI::APS::??",
            "sampling-position": 2
        },
        "opc": {
            "kind": "Sensor",
            "device-id": "Handix::POPS::???",
            "sampling-position": 3
        },
        "smps": {
            "kind": "Sensor",
            "device-id": "AerosolDynamics::SpiderMagic::002",
            "sampling-position": 4
        },
        "sample_t_rh": {
            "kind": "Operational",
            "device-id": "IST::HYT271::???",
            "sampling-position": 5
        },
        "total_flow": {
            "kind": "Operational",
            "device-id": "IST::Flow::???",
            "sampling-position": 6
        },
        "total_flow_pump": {
            "kind": "Operational",
            "device-id": "mfg::pump::???",
            "sampling-position": 7
        }
    },
    "sampling": {
        "sampl"
    },
    "environmental": {
        "thermal-controls": {
            "tec": {
                "kind": "Controller",
                "controller-id": "tec::mod::??",
                "interface-id": "USConverter::"
            }
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
