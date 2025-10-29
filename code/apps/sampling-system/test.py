sampling_system = {
    [
        {
            "kind": "Payload",
            "attributes": {
                "name": "payload03",
                "alias": "AerosolPhysics",
                "valid_date": "2025-08-03T13:00:00",
            },
            "layout": {
                [
                    {
                        "kind": "SampleSource",
                        "attributes": {
                            "name": "sample_line_1",
                        },
                        "samplers": 
                        [
                            {
                                "kind": "Sensor",
                                "attributes": {
                                    "name": "cpc",
                                    "long_name": "CPC",
                                    "description": "",
                                },
                                "dimensions": {"time": 0},
                                "variables": {
                                    "time": {
                                        "type": "str",
                                        "shape": ["time"],
                                        "attributes": {
                                            "variable_type": {"type": "string", "data": "main"},
                                            "long_name": {"type": "string", "data": "Time"},
                                            "source": {
                                                "device_id": "AerosolDynamics::MAGIC250::154",
                                                "variable": "time"
                                            }
                                        },
                                    },
                                    "concentration": {
                                        "type": "float",
                                        "shape": ["time"],
                                        "attributes": {
                                            "variable_type": {"type": "string", "data": "main"},
                                            "long_name": {"type": "char", "data": "Concentration"},
                                            "units": {"type": "char", "data": "cm-3"},
                                            "source": {
                                                "device_id": "AerosolDynamics::MAGIC250::154",
                                                "variable": "concentration"
                                            }
                                        },                                        
                                    }
                                }
                            },
                            {
                                "kind": "Sensor",
                                "attributes": {
                                    "name": "smps",
                                    "long_name": "SMPS",
                                    "description": "Particle Size Distribution (mobility)",
                                },
                                "dimensions": {"time": 0, "diameter": 53},
                                "variables": {
                                    "time": {
                                        "type": "str",
                                        "shape": ["time"],
                                        "attributes": {
                                            "variable_type": {"type": "string", "data": "main"},
                                            "long_name": {"type": "string", "data": "Time"},
                                            "source": {
                                                "device_id": "AerosolDynamics::Spider-MAGIC810::001",
                                                "variable": "time"
                                            }
                                        },
                                    },
                                    "diameter": {
                                        "type": "float",
                                        "shape": ["time", "diameter"],
                                        "attributes": {
                                            "variable_type": {"type": "string", "data": "main"},
                                            "long_name": {"type": "char", "data": "Diameter"},
                                            "units": {"type": "char", "data": "um"},
                                            "source": {
                                                "device_id": "AerosolDynamics::Spider-MAGIC810::001",
                                                "variable": "diameter",
                                                "units": "nm"
                                            }
                                        },
                                    },
                                    "dNdlogDp": {
                                        "type": "float",
                                        "shape": ["time", "diameter"],
                                        "attributes": {
                                            "variable_type": {"type": "string", "data": "main"},
                                            "long_name": {"type": "char", "data": "dN / dlogDp"},
                                            "units": {"type": "char", "data": ""},
                                            "source": {
                                                "device_id": "AerosolDynamics::Spider-MAGIC810::001",
                                                "variable": "dNdlogDp"
                                            }
                                        },                                        
                                    },
                                    "dN": {
                                        "type": "float",
                                        "shape": ["time", "diameter"],
                                        "attributes": {
                                            "variable_type": {"type": "string", "data": "main"},
                                            "long_name": {"type": "char", "data": "dN"},
                                            "units": {"type": "char", "data": "cm-3"},
                                            "source": {
                                                "device_id": "AerosolDynamics::Spider-MAGIC810::001",
                                                "variable": "dN"
                                            }
                                        },                                        
                                    },
                                    "intN": {
                                        "type": "float",
                                        "shape": ["time"],
                                        "attributes": {
                                            "variable_type": {"type": "string", "data": "main"},
                                            "long_name": {"type": "char", "data": "Integral N"},
                                            "units": {"type": "char", "data": "cm-3"},
                                            "source": {
                                                "device_id": "AerosolDynamics::Spider-MAGIC810::001",
                                                "variable": "intN"
                                            }
                                        },                                        
                                    },
                                }
                            },

                        ] 

                    }
                ]
            }

        }
    ]
}