beacons_project_dataset = { # these fields will be added to all the platform datasets before being sent off
    "attributes": {
        "project": {
            "type": "string",
            "data": "BEACONS"
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
    "variables": {
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
                },
                "standard_name": {
                    "type": "string",
                    "data": "latitude"
                },
                "units": {
                    "type": "string",
                    "data": "degrees_north"
                },
                "valid_min": {
                    "type": "float",
                    "data": -90.0
                },
                "valid_max": {
                    "type": "float",
                    "data": 90.0
                },
                "platform_varaiable_map": { # this will use dataset timebase to calculate proper data
                    "type": "string",
                    "data": "MSPPayload01"
                },
                "platform_variable": {
                    "type": "string",
                    "data": "latitude"
                },
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
                },
                "standard_name": {
                    "type": "string",
                    "data": "longitude"
                },
                "units": {
                    "type": "string",
                    "data": "degrees_east"
                },
                "valid_min": {
                    "type": "float",
                    "data": -180.0
                },
                "valid_max": {
                    "type": "float",
                    "data": 180.0
                },
                "platform_varaiable_map": {
                    "type": "string",
                    "data": "MSPPayload01"
                },
                "platform_variable": {
                    "type": "string",
                    "data": "longitude"
                },
            }
        },
        "altitude": {
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
                    "data": "Altitude"
                },
                "standard_name": {
                    "type": "string",
                    "data": "height above mean sea level"
                },
                "units": {
                    "type": "string",
                    "data": "m"
                },
                "valid_min": {
                    "type": "float",
                    "data": 0.0
                },
                "platform_varaiable_map": {
                    "type": "string",
                    "data": "MSPPayload01"
                },
                "platform_variable": {
                    "type": "string",
                    "data": "altitude"
                },
            }
        },
    }
}
