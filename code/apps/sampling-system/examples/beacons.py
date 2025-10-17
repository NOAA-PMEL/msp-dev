beacons_project = {
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
}

beacons_platform_layout = {
    "kind": "PlatformLayout",
    "name": "BEACONS",
    "platforms": {
        "ship": "MarjorieC",
        "Aerosols-1": "MSP01",
        "Aerosols-1-main": "MSPPayload01",
        "Aerosols-1-physics": "MSPPayload03",
        "Aerosols-1-optics": "MSPPayload02",
    },
    "layout": {
        "ship": {
            "children": {
                "Aerosols-1": {
                    "main-payload": "Aerosols-1-main",
                    "children": {
                        "Aerosols-1-phyics": {},
                        "Aerosols-1-optics": {},
                    }
                }
            }
            
        }
    }
}
