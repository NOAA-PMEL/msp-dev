sampling_actions = [
    {
        "version": "envds.sampling.operations/v1",
        "kind": "SamplingAction",
        "metadata": {
            "name": "set_nominal_inlet_flow",
            "action_module": "beacons_msp",
            "action_def": "set_nominal_inlet_flow",
            "sampling_namespace": "pmel.noaa.gov",
            "valid_config_time": "2025-10-01T00:00:00Z",
            "revision": 1,
        },
        "source_max_age": 60, #seconds
        "sources": {
            "nominal_flow_rate": {  # constant flow rate
                "variablemap_name": "MSPPayload01",
                "variableset_name": "main",
                "variable": "nominal_flow_rate",
            },
        },
        "targets": {
            "inlet_flow_sp": {
                "variablemap_name": "MSPPayload01",
                "variableset_name": "main",
                "variable": "inlet_flow_sp",
            }
        },
    },
    {
        "version": "envds.sampling.operations/v1",
        "kind": "SystemSamplingAction",
        "metadata": {
            "name": "set_isokintic_inlet_flow",
            "sampling_namespace": "pmel.noaa.gov",
            "valid_config_time": "2025-10-01T00:00:00Z",
            "revision": 1,
        },
        "source_max_age": 10, #seconds
        "sources": {
            "relative_wind_speed": {
                "variablemap_name": "MSPPayload01",
                "variableset_name": "main",
                "variable": "relative_wind_speed",
            },
        },
        "targets": {
            "inlet_flow_sp": {
                "variablemap_name": "MSPPayload01",
                "variableset_name": "main",
                "variable": "inlet_flow_sp",
            }
        },
    },
]


sampling_mode_groups = [
    {
        "version": "envds.sampling.operations/v1",
        "kind": "ModeGroup",
        "metadata": {
            "name": "system-operations",
            "sampling_namespace": "pmel.noaa.gov",
            "valid_config_time": "2025-10-01T00:00:00Z",
            "revision": 1,
        },
        "rule": "single",  # only one mode can be set a at time
    },
    {
        "version": "envds.sampling.operations/v1",
        "kind": "ModeGroup",
        "metadata": {
            "name": "payload-operations",
            "sampling_namespace": "pmel.noaa.gov",
            "valid_config_time": "2025-10-01T00:00:00Z",
            "revision": 1,
        },
        "rule": "single",  # only one mode can be set a at time
    },
]

sampling_modes = [
    {
        "version": "envds.sampling.operations/v1",
        "kind": "SystemMode",
        "metadata": {
            "name": "normal",
            "mode_group": "system-operations",
            "sampling_namespace": "pmel.noaa.gov",
            "valid_config_time": "2025-10-01T00:00:00Z",
            "revision": 1,
        },
        "requirements": [
            {"kind": "SamplingMode", "name": "nominal_conditions"},
            # {"kind": "SamplingMode", "name": "nominal_location"},
        ],
    },
    {
        "version": "envds.sampling.operations/v1",
        "kind": "SystemMode",
        "metadata": {
            "name": "startup",
            "mode_group": "system-operations",
            "sampling_namespace": "pmel.noaa.gov",
            "valid_config_time": "2025-10-01T00:00:00Z",
            "revision": 1,
        },
        "requirements": [
            {"kind": "SamplingMode", "name": "system_startup"},
            # {"kind": "SamplingMode", "name": "payload_startup"},
        ],
        "transitions": {"true": [{"kind": "SystemMode", "name": "normal"}]},
    },
    {
        "version": "envds.sampling.operations/v1",
        "kind": "SamplingMode",
        "metadata": {
            "name": "nominal_conditions",
            "sampling_namespace": "pmel.noaa.gov",
            "valid_config_time": "2025-10-01T00:00:00Z",
            "revision": 1,
        },
        "requirements": [
            {
                "kind": "SamplingState",
                "name": "in_sector",
                # "actions": {
                #     "true": [
                #         {"kind": "SamplingAction", "name": "set_isokinetic_inlet_flow"},
                #     ],
                #     "false": [
                #         {"kind": "SamplingAction", "name": "set_reverse_inlet_flow"}
                #     ],
                # },
            },
            {
                "kind": "SamplingState",
                "name": "sampling_limits",
                # "actions": {
                #     "true": {
                #         "kind": "SamplingAction",
                #         "name": "set_isokinetic_inlet_flow",
                #     },
                #     "false": {
                #         "kind": "SamplingAction",
                #         "name": "set_reverse_inlet_flow",
                #     },
                # },
            },
        ],
        "actions": {
            "true": [{"kind": "SamplingAction", "name": "set_isokinetic_inlet_flow"}],
            "false": [{"kind": "SamplingAction", "name": "set_reverse_inlet_flow"}],
        },
    },
    {
        "version": "envds.sampling.operations/v1",
        "kind": "SamplingMode",
        "metadata": {
            "name": "nominal_location",
            "sampling_namespace": "pmel.noaa.gov",
            "valid_config_time": "2025-10-01T00:00:00Z",
            "revision": 1,
        },
        "requirements": [
            {
                "kind": "SamplingState",
                "name": "beacons_at_sea",
                "actions": {
                    "true": [
                        {"kind": "SamplingAction", "name": "set_isokinetic_inlet_flow"},
                        {"kind": "SamplingAction", "name": "physics_payload_normal"},
                        {"kind": "SamplingAction", "name": "optics_payload_normal"},
                    ],
                    "false": [
                        {"kind": "SamplingAction", "name": "set_reverse_inlet_flow"},
                        {"kind": "SamplingAction", "name": "physics_payload_shutdown"},
                        {"kind": "SamplingAction", "name": "optics_payload_shutdown"},
                    ],
                },
                "set-modes": {
                    "true": [
                        {"kind": "SamplingMode", "name": "physics_payload_normal"},
                        {"kind": "SamplingMode", "name": "optics_payload_normal"},
                    ],
                    "true": [
                        {"kind": "SamplingMode", "name": "physics_payload_shutdown"},
                        {"kind": "SamplingMode", "name": "optics_payload_shutdown"},
                    ],
                },
            },
            {
                "kind": "SamplingState",
                "name": "sampling_limits",
                "actions": {
                    "true": {
                        "kind": "SamplingAction",
                        "name": "set_isokinetic_inlet_flow",
                    },
                    "false": {
                        "kind": "SamplingAction",
                        "name": "set_reverse_inlet_flow",
                    },
                },
            },
        ],
    },
    {
        "version": "envds.sampling.operations/v1",
        "kind": "PlatformSamplingMode",
        "metadata": {
            "name": "powerup",
            "sampling_namespace": "pmel.noaa.gov",
            "valid_config_time": "2025-10-01T00:00:00Z",
            "revision": 1,
        },
    },
    {
        "version": "envds.sampling.operations/v1",
        "kind": "PlatformSamplingMode",
        "metadata": {
            "name": "startup",
            "sampling_namespace": "pmel.noaa.gov",
            "valid_config_time": "2025-10-01T00:00:00Z",
            "revision": 1,
        },
    },
    {
        "version": "envds.sampling.operations/v1",
        "kind": "PlatformSamplingMode",
        "metadata": {
            "name": "shutdown_inport",
            "sampling_namespace": "pmel.noaa.gov",
            "valid_config_time": "2025-10-01T00:00:00Z",
            "revision": 1,
        },
    },
]
