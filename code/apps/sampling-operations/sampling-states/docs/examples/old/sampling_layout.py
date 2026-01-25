layouts = [
    {
        "version": "v1",
        "kind": "SamplingLayout",
        "metadata": {
            "name": "AerosolPhysics-Beacons",
            "label": "AerosolPhysics",
            "platform": "Payload03"
        },
        "valid-config-time": "2025-08-24T00:00:00Z",
        "interface": {
            "power": [
                "input-24v-ups",
                "input-24v",
                "input-110v"
            ],
            "network": [
                "??"
            ],
            "data": [],
            "sample-source": [
                "sample-line-01"
            ],
        },
        "sampling": {
            "external-device": [],
            "internal-device": [
                {
                    "kind": "Device",
                    "metadata": {
                        "name": "SMPS",
                        "sample-source": "sample-line-01",
                        "sampling-source-position": "1" # order of sampling
                    },
                    "spec": {
                        "device-id": "AerosolDynamics::SpiderMagic::002"
                    },
                    "variable-map": {
                        "time": {
                            "device-variable": "time",
                        },
                        "diameter": {
                            "device-variable": "diameter",
                        },
                        "dndlogdp": {
                            "device-variable": "dnglogdp",
                        },
                        "intN": {
                            "device-variable": "intN",
                        },
                    }
                }, 
                "opc",
                "aps",
                "cpc"
            ],
            "flows": [
                "total-makeup"
            ],
        },
        "environmental": {
            "thermal-control": [
                "tec"
            ],
            "thermal-device": [
                "case-trh-1",
                "case-trh-2"
            ]

        }
    }
]

devices = [
    {
        "kind": "Device",
        "metadata": {
            "name": "SMPS",
            "device-type": "sensor",
        },
        # could look like or leave it to dataset, should be units, etc at leaset: 
        # "data": {
            # "atttibutes" : {},
            # "dimensions": {},
            # "variables"
        #}
        "variables": {
            "time": {}, # like def file
            "diameter": {},
            "dndlogdp": {},
            "intN": {},
        }
    }
]

platform_variable_maps = [
    platform03 = {
        "version": "v1",
        "kind": "PlatformVariableMap",
        "metadata": {
            "name": "MSPPayload03",
            "platform": "MSPPayload03"
        },
        "valid-config-time": "2025-01-01T00:00:00Z",
        "variables": {
            "cpc_concentration": {                
                "map_type": "direct",
                "source": {
                    "concentration": {
                        "source_type": "device",
                        "source_id": "AerosolDynamics::MAGIC250::154",
                        "source_variable": "concentration",
                    }
                },
                "timebase": 1,
                "timebase_method": ["round", "average"],

                "attributes": {
                    "variable_type": {
                        "type": "string",
                        "data": "main"
                    },
                    "long_name": {
                        "type": "string",
                        "data": "CPC Concentration"
                    },
                    "units": {
                        "type": "string",
                        "data": "cm-3"
                    },
                    
                }
            },
            "true_wind_speed": { # example - would be in payload01
                "map_type": "calculate",
                "source": {
                    "rel_wind_speed": {
                        "source_type": "platform_variable_map",
                        "source": "source::dataset-id",
                        "source_variable": "rel_wind_speed"
                        
                    },                
                    "ship_heading": {
                        "source_type": "platform_variable_map",
                        "source": "source::dataset-id",
                    },                
                    "ship_speed": {
                        "source_type": "platform_variable_map",
                        "source": "source::dataset-id",
                    },                
                },
                "timebase": 1,
                "timebase_method": ["round", "average"],
                "calculate_method": {
                    "service": "service-name",
                    "path": "/get_true_wind_speed/",
                    "parameters": ["rel_wind_speed", "ship_heading", "ship_speed"]
                },
                "attributes": {
                    "variable_type": {
                        "type": "string",
                        "data": "main"
                    },
                    
                }
            },
            "temperature": {
                "map_type": "priority",
                "priority_list": ["t1", "t2"],
                "source": {
                    "t1": {
                        "source_type": "device",
                        "source_id": "Viasala::WX::154",
                        "source_variable": "Ta",
                    },
                    "t2": {
                        "source_type": "device",
                        "source_id": "Viasala::WX::155",
                        "source_variable": "Ta",
                    }
                },
                "timebase": 1,
                "timebase_method": ["round", "average"],
            },
            "rh": {
                "map_type": "aggregate",
                "aggregate_method": ["average"],
                "source": {
                    "rh1": {
                        "source_type": "device",
                        "source_id": "Viasala::WX::154",
                        "source_variable": "Rh",
                    },
                    "rh2": {
                        "source_type": "device",
                        "source_id": "Viasala::WX::155",
                        "source_variable": "Rh",
                    }
                },
                "timebase": 1,
                "timebase_method": ["round", "average"],
            },
        },
        "data": {
            "attributes": {
                "name": {
                    "type": "string",
                    "data": "data-beacons-phase1"
                },
                "platform": {
                    "type": "string",
                    "data": "MSP-Beacons"
                },
                "description": {
                    "type": "string",
                    "data": "Water based Condensation Particle Counter (CPC) manufactured by Aerosol Dyanamics and distributed by Aerosol Devices/Handix"
                },
                "tags": {
                    "type": "char",
                    "data": "aerosol, cpc, particles, concentration, sensor, physics"
                },
                "format_version": {
                    "type": "char",
                    "data": "1.0.0"
                },
                "variable_types": {
                    "type": "string",
                    "data": "main, setting, calibration"
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
                    "data": "2.0.0: added device_type, change_log to attributes, removed settings and added variable_type to attributes of each variable"
                } 
            },
            "dimensions": {
                "time": 0,
                "diameter": 0
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
                        },
                        "source": {"??"} # How to specify source? Is NCO the right format? Plus, this should set to even tb
                    }
                },
                "latitude": {
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
                            "data": "Latitude"
                        }
                    }
                },
                "longitude": {
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
                            "data": "Longitude"
                        }
                    }
                },
                "concentration": {
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
                        }
                    }
                },
            }
        }
    }
]

# should there be groupings here to allow for different time base:?
#   1s, 30s, etc
dataset = {
    "version": "v1",
    "kind": "Dataset",
    "metadata": {
        "name": "dataset-beacons-phase1"
    },
    "data": {
        "attributes": {
            "name": {
                "type": "string",
                "data": "data-beacons-phase1"
            },
            "platform": {
                "type": "string",
                "data": "MSP-Beacons"
            },
            "description": {
                "type": "string",
                "data": "Water based Condensation Particle Counter (CPC) manufactured by Aerosol Dyanamics and distributed by Aerosol Devices/Handix"
            },
            "tags": {
                "type": "char",
                "data": "aerosol, cpc, particles, concentration, sensor, physics"
            },
            "format_version": {
                "type": "char",
                "data": "1.0.0"
            },
            "variable_types": {
                "type": "string",
                "data": "main, setting, calibration"
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
                "data": "2.0.0: added device_type, change_log to attributes, removed settings and added variable_type to attributes of each variable"
            } 
        },
        "dimensions": {
            "time": 0,
            "diameter": 0
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
                    },
                    "source": {"??"} # How to specify source? Is NCO the right format? Plus, this should set to even tb
                }
            },
            "latitude": {
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
                        "data": "Latitude"
                    }
                }
            },
            "longitude": {
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
                        "data": "Longitude"
                    }
                }
            },
            "concentration": {
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
                    }
                }
            },
        }
    }
}

variable_map = {

}