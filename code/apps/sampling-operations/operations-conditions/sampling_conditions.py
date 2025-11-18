sampling_conditions = [
    {
        "version": "envds.sampling.operations/v1",
        "kind": "SamplingCondition",
        "metadata": {
            "name": "cn_limit",
            "sampling_namespace": "pmel.noaa.gov",
            "valid_config_time": "2025-10-01T00:00:00Z",
            "revision": 1,
        },
        "sources": {
            "cn": {
                "variablemap_id": "abc",
                "variableset_id": "def",
                "variable": "cn"
            },
        },
        "criteria": {
            "all": [ # all, any, none
                {
                    "kind": "MinMaxLimit",
                    "source": "cn",
                    "max_val": 2000,
                    "min_val": 10,
                }
            ]
        }
            
    },
    {
        "version": "envds.sampling.operations/v1",
        "kind": "SamplingCondition",
        "metadata": {
            "name": "at_sea",
            "sampling_namespace": "pmel.noaa.gov",
            "valid_config_time": "2025-10-01T00:00:00Z",
            "revision": 1,
        },
        "sources": {
            "gps_latitude": {
                "variablemap_id": "abc",
                "variableset_id": "def",
                "variable": "latitude"
            },
            "gps_longitude": {
                "variablemap_id": "abc",
                "variableset_id": "def",
                "variable": "longitude"
            },
        },
        "criteria": {
            "none": [
                {
                    "kind": "LatLonRegionLocation",
                    "label": "San Diego, CA",
                    "coordinates": {"latitude": "gps_latitude", "longitude": "gps_longitude"},
                    "region": [
                        {"latitude": 32.943680, "longitude": -117.264183},
                        {"latitude": 32.726873, "longitude": -117.563043},
                        {"latitude": 32.314643, "longitude": -117.016473},
                    ]
                },
                {
                    "kind": "LatLonRegionLocation",
                    "label": "Hawaii",
                    "coordinates": {"latitude": "gps_latitude", "longitude": "gps_longitude"},
                    "region": [
                        {"latitude": 32.943680, "longitude": -117.264183},
                        {"latitude": 32.726873, "longitude": -117.563043},
                        {"latitude": 32.314643, "longitude": -117.016473},
                    ]
                },

            ]
        }
            
    },
    {
        "version": "envds.sampling.operations/v1",
        "kind": "SamplingCondition",
        "metadata": {
            "name": "isokinetic_inlet",
            "sampling_namespace": "pmel.noaa.gov",
            "valid_config_time": "2025-10-01T00:00:00Z",
            "revision": 1,
        },
        "sources": {
            "relative_wind_velocity": {
                "variablemap_id": "abc",
                "variableset_id": "def",
                "variable": "relative_wind_velocity"
            },
            "inlet_nozzle_velocity": {
                "variablemap_id": "abc",
                "variableset_id": "def",
                "variable": "inlet_nozzle_velocity"
            }
        },
        "criteria": {
            "all": [
                {
                    "kind": "CompareMean",
                    "sources": ["relative_wind_velocity","inlet_nozzle_velocity"],
                    "tolerance_pct": 5,
                    # "tolerance_abs": 1,
                }
            ]
        }
            
    },
    
]
