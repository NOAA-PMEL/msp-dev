# Operations and Sampling
This document is intended to describe the services designed to manage the operational and sampling needs of the data system. While the data acquistion and storage is designed to collect "raw" sensor data without regard to what it is actually being measured, the operations and sampling part of the system fills in the necessary metadata to specify what is being measured and how it is done. It can be broken down into multiple sections: system mapping and layout, event management and dataset generation. These are discussed in more detail below.

As suggested above, this section provides the "other half" of the data collection process: the metadata to describe what is actually being measured. This approach decouples the raw from the metadata to allow for a modular approach and allow for sensors/devices to be used in multiple deployments, calibrations, etc. without having to change the underying data source format. Instead, we use the metadata defined here to describe what the sensor data means based on configured parameters and event logs. 

## Nomenclature
Before describing the actual configuration and methods, it is useful to define some terms that are used to construct the resources defined below. 
 - Project - defines the overall effort designed to achieve the desired goal. An example of a project is BEACONS.
 - Platform - a location or entity that hosts sampling or sampling devices. Examples of platforms include a ship, MSP enclosure, or MSPPayload.
 - Deployment - defines when a Platform is put to use. Examples include the use of a ship during a project or installation and operation of an MSP enclosure during a leg of a cruise. You will notice below that Deployments are instatiated as Events that will be used to create datasets.
 - Event - This is a broad term as indicated above that defines "things that happen" at any time. There are any number of events that can occur. As just noted, a Deployment is a type of event. Another example would be a segment/leg of a cruise track (e.g., San Diego to Honlulu). 

## Configuration
 As is the case with much of the sampling system, configuration files are used to define all the metadata and operating parameters. The syntax is a mix of kubernetes style configs (yaml or json) and nco-json. These configurations are still in the development phase and there is some duplication of info but some of that will be explained in the implementation portion that follows. Each configuration type will show an example with comments or description after. Many files will have a valid-config-time type of field to allow for versioning and changes to the system.
### Sysetem Mapping and Layout
#### Project
 Define the project as described above
 ```python
     {
        "version": "v1", # version of this format/resource
        "kind": "Project", # type of config file
        "metadata": { 
            "name": "BEACONS", # name is used as id for all Projects
            "long_name": "",   # long_name is for human consumption
            "description": "", # tell about project
            "contacts": [""],  # list of contact
            "start_time": "2026-05-01T00:00:00Z", # start time for all things defined by project
            "end_time": "",                       # (optional) end time for all things defined by project
        },
    }
```
#### Platform
Define a platform as described above
```python
{
    "version": "v1",
    "kind": "Platform",
    "metadata": {
        "name": "MSPPayload03",
        "platform_type": "MSPPayload", # platform type: could be Ship, UAS, MSPEnclosure, MSPPayload, UASPayload
        "owner": "UW/CICOES",
        "description": "",
        "contacts": [""],
    },
    "interface": { # define connections/interface to platform - this is still experimental
        "power-input": [
            "input-24v-ups",
            "input-24v",
            "input-110v"
        ],
        "network": [
            "internal",
            "external"
        ],
        "sample-input": [
            "sample-line-01"
        ],
    },

}
```
#### PlatformVariableMap
I think these will be the workhorse of the system. These map device variables to a more generic, system usable name for use in multiple places. These don't necessarily define a "real-world" parameter rather they create a system variable that removes the need to know the device-id everywhere.
```python
{
    "version": "v1", 
    "kind": "PlatformVariableMap",
    "metadata": {
        "name": "MSPPayload03", # variable map name
        "platform": "MSPPayload03" # platform name
    },
    "valid-config-time": "2025-01-01T00:00:00Z",
    "variable_groups": { # allow for grouping of variables (timebese, etc) for reference purposes
        "main": {
            "timebase": 1 # values will be "reindexed" to this timebase
        },
        "sizing": {
            "timebase": 30
        },
    },
    "variables": { # list of platform variables to be used by other op/sampling resources
        "cn": {    
            "type": "float",
            "shape": ["time"],
            "map_type": "direct", # mapping type: direct, calculated, priority and aggregate
            "source": {
                "concentration": { # source variable name
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "AerosolDynamics::MAGIC250::154",
                    "source_variable": "concentration", # actual variable name in source
                }
            },
            "direct-value": { # if source-variable name is same as variable name, don't need this
                "source-variable": "concentration"
            },
            "variable_group": "main",
            # "timebase": 1, # timebase for mapped variable - will map to this even timebase
            "index_method": ["round", "average"], # method used to map to time base (or other index)
            "attributes": { # necessary attributes to be inherited by dataset
                "units": {
                    "type": "string",
                    "data": "cm-3"
                },
                
            },
        },
        "smps_dNdlogDp": {    
            "type": "float",
            "shape": ["time", "diameter"],
            "map_type": "direct", 
            "source": {
                "smps_dNdlogDp": { 
                    "source_type": "device", # source type: device (sensor, op), platform_variable_map, etc
                    "source_id": "AerosolDynamics::SpiderMAGIC::002",
                    "source_variable": "dNdlogDp",
                }
            },
            "variable_group": "sizing", # notice different variable_group
            "index_method": ["round", "average"], # method used to map to time base in decreasing order
            "attributes": {
                "units": {
                    "type": "string",
                    "data": "cm-3"
                },
                
            },

        },
    
    ...

    }
}
```
#### SamplingLayout
This configuration will likely be the most detailed and give a description of how everything fits together. It likely won't be used programatically until a "configurator" type interface is developed to help build other config files. This will also be available as an historical record. 
```python
{
    "version": "v1",
    "kind": "SamplingLayout",
    "metadata": {
        "name": "MSPPaylaod03",
        "platform": "MSPPayload03"
    },
    "valid-config-time": "2025-08-24T00:00:00Z",
    "interfaces": { # list of interfaces
        "cpc-serial": {
            "kind": "Interface",
            "interface-id": "Lantronix::??::",
            "port": "10001"
        },
    },
    "controllers": { # list of controllers
        "sensor-pdu": {
            "kind": "Controller",
            "controller-id": "Synaccess::NP05B::001",
            "variable-map": {
                "outlet-1": "outlet_1_power",
                "outlet-2": "outlet_2_power",
                "outlet-3": "outlet_3_power",
                "outlet-4": "outlet_4_power",
                "outlet-5": "outlet_5_power",
            },
            # relays?
        }
    },
    "devices": { # list of devices - notice the redundant info with variable-map
        "cpc": {
            "kind": "Sensor",
            "device-id": "AerosolDynamics::MAGIC250::154",
            "interface": {
                "default": "cpc-serial"
            },
            "power": {
                "controller": "sensor-pdu",
                "controller-variable": "outlet-1"
            }
        },
    },
    "sampling": { # define the sampling layout heirarchy
        "internal": {
            "sample-line-01": {
                "kind": "SampleLine", # specfies the sample line defined in Platform interface
                "devices": [ # not sure this how I want to do this yet
                    {"device": "cpc", "sample-order-index": 1},
                    {"device": "aps", "sample-order-index": 2},
                ]
            }
        }
    },
    "environmental": { # environmental components
        "thermal-controls": {
            "devices": [
                {"device": "tec"}
            ]
            # "tec": {
            #     "kind": "Operational",
            #     "device-id": "tec::mod::??",
            #     "interface-id": "USConverter::"
            # }
        },
        "thermal-devices": {
            "case-trh-1": {
                "kind": "Device",
                "device-id": "IST::HYT???::",
                "interface-id": "Labjack::"
            },
            "case-trh-2": {
                "kind": "Device",
                "device-id": "IST::HYT???::",
                "interface-id": "Labjack::"
            },
        }
    }
}
```

#### PlatformLayout
Add content
### Event Management
#### Events
Add content
##### Conditions and Actions
Add more content but the gist is to create Conditions that trigger Actions. For example, if relative_wind_direction goes out of sector, an Action would reverse flow in the inlet. 

### Dataset Generation

## Implementation 
As stated above, the variable maps will be used extensively to generate the values used downstream in the operational decision making process. Most other non-event files will be to provide building blocks to define references in the variable map. The services to implement this are described below

### System Mapping and Layout
#### Mapper service
This service will use variable maps to create another set of variables for use in the system. The service will do the following:
 - create an abstraction layer of system variables that don't require the use of specific device-id values to access data. This will allow changes to the system (e.g., swapping out a sensor) without needing to change any references in other config files. 
 - group the variables along common index values (more often than not, time) to allow easier comparison regardless of sampling frequency of each device in the acquisition process.

The service will listen for the configured device data and store locally. For each time period, the local data will be collated into the proper variable and sent off similar to a data update. This collation can take the following forms: 
 - direct - use device variable value as a 1:1 map to the mapped_variable
 - calculated - use one or more device variables to calculate the value of the mapped variable
 - priority - choose from a prioritized list of device variables to find the value of the mapped variable. If a higher priority value is missing, the next in the list is used
 - aggregate - create a single mapped value from a list of device variables. For example, generate a single ambient T value from a list of external T measurements by averaging them together.

Similarly, reindexing the data can be done in multiple ways:
 - round - round the index to the appropriate even interval. For example, if the index is time and the timebase=1, the variable would be slotted into the appropriate second using the fractional seconds. 
 - average - if there is more than one value in the collected data for a given period, average the values together to get the value for the given index period.

 As there is a need to wait for all the data to come in, there will be a slight delay in the mapped variable set. For example, using the timebase=1, the service will need to wait until until the 0.5 second mark is reached to assure that all data have been collected for collation.

Once collated, each group of variables will be sent out similar to a device/data/update to be consumed as needed downstream