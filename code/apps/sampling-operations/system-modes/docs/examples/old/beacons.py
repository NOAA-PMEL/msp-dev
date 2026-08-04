platforms = [
    {
        "version": "v1",
        "kind": "Platform",
        "metadata": {
            "name": "MarjorieC",
            "platform_type": "ship",
            "owner": "Pasha",
            "description": "",
            "contacts": [""],
        },
    },
    {
        "version": "v1",
        "kind": "Platform",
        "metadata": {
            "name": "MSP01",
            "platform_type": "MSP",
            "owner": "UW/CICOES",
            "description": "",
            "contacts": [""],
        },
    },
    {
        "version": "v1",
        "kind": "Platform",
        "metadata": {
            "name": "Payload03",
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
            "network": {
                "host": "10.55.179.???"
            },
            "sample-input": [
                "sample-line-01"
            ],
        },

    },
]

projects = [
    {
        "version": "v1",
        "kind": "Project",
        "metadata": {
            "name": "BEACONS",
            "long_name": "",
            "description": "",
            "contacts": [""],
            "start_time": "2026-05-01T00:00:00Z",
            "end_time": "",
        },
    },
]

project_platform_layouts = [
    {
        "version": "v1",
        "kind": "ProjectPlatformLayout",
        "metadata": {
            "name": "Beacons-Aerosols",
            "project": "Beacons",
            "platform": "MarjorieC",
            "platform_layout": "Beacons-Aerosols",
        },
        "valid-config-time": "2025-01-01T00:00:00Z"
    }
]

sample_sources = [
    {
        "version": "v1",
        "kind": "SampleSource",
        "metadata": {
            "name": "Inlet01",
            "nozzle-diameter": "7/8"
        },
        "sample-lines": [
            "port01",
            "port02",
            "port03",
            "port04",
            "port05",
            "port06",
            "port07",
            "port08"
        ],
        "devices": [
            "inlet_t_rh",
            "inlet_flow",
            "inlet_flow_sp"
        ]
        "valid-config-time": "2025-01-01T00:00:00Z",
     }
]

platform_layouts = [
    {
        "version": "v1",
        "kind": "PlatformLayout",
        "metadata": {
            "name": "BEACONS-Aerosols",
        },
        "valid-config-time": "2025-01-01T00:00:00Z",
        "sample-sources": [
            {
                "version": "v1",
                "kind": "SampleSource",
                "metadata": {
                    "name": "Inlet01"
                },
                "devices": {
                    "inlet_t_rh": {
                        "kind": "Sensor",
                        "device-id": "IST::??:??" 
                    },
                    "inlet_flow": {
                        "kind": "Sensor",
                        "device-id": "IST::??:??" 
                    },
                    "inlet_flow_sp": {
                        "kind": "Operational",
                        "device-id": "IST::??:??" 
                    },
                }
            }
        ]
        "platforms": [
            {
                "name": "MSP01",
                "platforms": [
                    {
                        "name": "Payload01",
                    },
                
                    {
                        "name": "Payload02"
                    },
                    {
                        "metadata": {
                            "name": "Payload03",
                            "sampling-layout": "AerosolPhysics-Beacons"
                        },
                        "interface": {
                            "power-input": {
                                "input-24v-ups": {
                                    "kind": "Controller",
                                    "controller-id": "Shelly::??:??"
                                },
                                "input-24v": {
                                    "kind": "Controller",
                                    "controller-id": "Shelly::??:??"
                                },
                                "input-110v": {
                                    "kind": "Controller",
                                    "controller-id": "Shelly::??:??"
                                },
                            },
                            "sample-input": [ 
                                "sample-line-01": {
                                    "kind": "SampleSource",
                                    "name": "Inlet01",
                                    "sample-line": "port02"
                                }
                            ],

                        }
                    },
                ]
            }
        ]
    }
]

events = [
    {
        "version": "v1",
        "kind": "ProjectPlatformEvent",
        "metadata": {
            "name": "BEACONS",
            "project": "BEACONS",
            "platform": "MarjorieC",
            "event_type": "Deployment",
            "long_name": "",
            "description": "",
            "contacts": [""],
            "start_time": "2026-05-01T00:00:00Z",
            "end_time": "",
        },
    },
    {
        "version": "v1",
        "kind": "ProjectPlatformEvent",
        "metadata": {
            "name": "Phase1",
            "event_type": "Deployment",
            "long_name": "",
            "description": "",
            "contacts": [""],
            "project": "BEACONS",
            "platform": "MarjorieC",
            "start_time": "2026-05-01T00:00:00Z",
            "end_time": "2026-11-01T00:00:00Z",
            "creation_time": "",
            "modified_time": "",
        },
    },
    {
        "version": "v1",
        "kind": "ProjectPlatformEvent",
        "metadata": {
            "name": "Phase2",
            # "project": "BEACONS",
            # "platform": "MarjorieC",
            # "event_type": "Deployment",
            "long_name": "",
            "description": "",
            "contacts": [""],
            # "start_time": "2026-11-01T00:00:00Z",
            # "end_time": ""
        },
        "spec": {  # events don't generally have a spec
            "project": "BEACONS",
            "platform": "MarjorieC",
            "event_type": "deployment",
            "start_time": "2026-11-01T00:00:00Z",
            "end_time": "",
        },
    },
]

"""
When using a Kubernetes-style YAML structure for other applications, the metadata and spec fields serve distinct but complementary purposes, mirroring their roles in Kubernetes:
### metadata:
This section is used for information about the application or resource, rather than its operational configuration. It provides identifying and organizational details. Common sub-fields within metadata include:
name: A unique identifier for the resource.
labels: Key-value pairs for categorization and selection, similar to tags, allowing for grouping and filtering of resources.
annotations: Key-value pairs for non-identifying metadata, such as descriptions, build information, or tooling-specific configuration that should not be used for selection.
### spec:
This section defines the desired state or configuration of the application or resource. It contains the operational parameters that dictate how the application should run or how the resource should behave. The content of spec is highly dependent on the specific application or resource being defined. For example, in a custom application, spec might include:
image: The container image to use.
replicas: The number of instances to run.
ports: Network ports to expose.
environment: Environment variables for the application.
resources: Resource requests and limits (CPU, memory).
In essence, metadata describes what the object is and how it can be identified, while spec describes how the object should function or be configured. This separation promotes clarity, organization, and enables automated systems to understand and manage applications based on their desired state.
"""
