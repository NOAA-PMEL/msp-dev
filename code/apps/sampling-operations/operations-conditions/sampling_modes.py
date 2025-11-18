sampling_modes = [
    {
        "version": "envds.sampling.operations/v1",
        "kind": "PlatformSamplingMode",
        "metadata": {
            "name": "normal",
            "sampling_namespace": "pmel.noaa.gov",
            "valid_config_time": "2025-10-01T00:00:00Z",
            "revision": 1,
        },
        "requirements": [
            {
                "kind": "PlatformSamplingState",
                "name": "in_sector"
            },
        ]
    },
    {
        "version": "envds.sampling.operations/v1",
        "kind": "PlatformSamplingMode",
        "metadata": {
            "name": "powerup",
            "sampling_namespace": "pmel.noaa.gov",
            "valid_config_time": "2025-10-01T00:00:00Z",
            "revision": 1,
        }
    },
    {
        "version": "envds.sampling.operations/v1",
        "kind": "PlatformSamplingMode",
        "metadata": {
            "name": "startup",
            "sampling_namespace": "pmel.noaa.gov",
            "valid_config_time": "2025-10-01T00:00:00Z",
            "revision": 1,
        }
    },
    {
        "version": "envds.sampling.operations/v1",
        "kind": "PlatformSamplingMode",
        "metadata": {
            "name": "shutdown_inport",
            "sampling_namespace": "pmel.noaa.gov",
            "valid_config_time": "2025-10-01T00:00:00Z",
            "revision": 1,
        }
    },

]
