sampling_states = [
    {
        "version": "envds.sampling.operations/v1",
        "kind": "SamplingState",
        "metadata": {
            "name": "in_sector",
            "sampling_namespace": "pmel.noaa.gov",
            "valid_config_time": "2025-10-01T00:00:00Z",
            "revision": 1,
        },
        "requirements": [
            {
                "kind": "SamplingCondition",
                "name": "cn_limit",
                "required_time_to_transition": {
                    "to_become_false": 5,
                    "to_become_true": 10,
                },
            }
        ],
    },
    {
        "version": "envds.sampling.operations/v1",
        "kind": "PlatformSamplingState",
        "metadata": {
            "name": "at_sea",
            "sampling_namespace": "pmel.noaa.gov",
            "valid_config_time": "2025-10-01T00:00:00Z",
            "revision": 1,
        },
        "requirements": [
            {
                "kind": "SamplingCondition",
                "name": "at_sea",
                "required_time_to_transition": 1800,
            }
        ],
    },
    {
        "version": "envds.sampling.operations/v1",
        "kind": "PlatformSamplingState",
        "metadata": {
            "name": "isokinetic_inlet",
            "sampling_namespace": "pmel.noaa.gov",
            "valid_config_time": "2025-10-01T00:00:00Z",
            "revision": 1,
        },
        "requirements": [
            {
                "kind": "SamplingCondition",
                "name": "isokinetic_inlet",
                "required_time_to_transition": 10,
            }
        ],
    },
]
