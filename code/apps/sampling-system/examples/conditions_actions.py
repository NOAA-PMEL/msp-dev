
sector_control_cn_limit = {
    "kind": "envCondition",
    "name": "cn_limit",
    "sources": {
        "cn": {
            "platform": "Aerosol-1-physics",
            "variable": "cn"
        }
    },
    "condition-type": "MinMax",
    "condition-criteria": {
        "any": [
            {
                "source": "cn",
                "limits": {
                    "min-val": 0,
                    "max-val": 2000
                },
                "actions": {
                    "true": {
                        "name": "set_sector_control",
                        # "name": "toggle_blower_flow",
                        "data": {"in-sector": "true"},
                        # "key": "sector-condition",
                        # "value": "in-sector"
                    },
                    "false": {
                        "name": "set_sector_control",
                        # "name": "toggle_blower_flow",
                        "data": {"in-sector": "false"},
                        # "value": "out-of-sector"
                    },
                }
            }
        ]
    }
}

sector_control_action = {
    "kind": "envAction",
    "name": "set_sector_control",
    "parameters": {
        "in-sector": {
            "required": True,
            "allowed-values": ["true", "false"]
        }
    },
    "action": {
        "apiVersion": "v1",
        "kind": "Service",
        "name": "sampling-operations",
        "uri": "/action/set_sector_control/"
    }




}