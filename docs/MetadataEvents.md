# Metadata and Events Framework
In order to provide meaningful metadata to go with collected sensor data, a framework of concepts and terms must be established. Ideally, this would follow some sort of standard in the data collection world or, at least, within NOAA. At the very least, there would be a standard at PMEL to follow but there does not appear to be one. So, what follows is a proposal for how data collected from the Modular Sampling Platforms (MSP) using envDS (I think this name will change) will organize the metadata to provide information to process and analyze the sensor data and produce complete datasets.

## Background
The data collected by this system is done at the sensor level. This means that all data are collected and saved as output by the sensor. This means that, typically, derived data is not included in the original data file unless there is a strong reason to do so. Each data file (or record) is associated with the sensor's make, model and serial number without any information specific to the location and purpose of the measurements. For example, the sensor could be deployed in the field, in a lab or being calibrated but all the data will be available in the same format, regardless. This means that a comprehensive metadata record needs to accompany the data in order to complete the story. 

## Events
All data and metadata in the system are considered "events". Events are packaged with a "source" and "type" such that concerned services can handle whichever events are pertinent. Sensor data are identified by their sensor-id which has the form of make::model::serial_number and each record has a timestamp associated with it. Metadata will also have the "source" and "type" attributes but a more complex id/metadata-type nomenclature needs to be developed. What follows will be describe the metadata framework and events that will be used to transfer the information.

### Formats
Programmatically, all events are sent using the [CloudEvents](https://cloudevents.io/) specification and the payload carries the data or metadata. For data, an NCO-JSON format is used for each record. For metadata, each record will use a kubernetes manifest-like syntax. The data can be stored as yaml or json which are both easily converted to a python dictionary. The format/schema will look like:
```python
    {
        "kind": "<Kind of Metadata>",
        "metadata": {
            "name": "<name>",
            .
            .
            .
        },
        "data": {
            "<entry key>": "<entry value>",
            .
            .
            .
        }
    },
``` 

## Framework
Each section will present a metadata type and event types that are associated with, if possible, examples. As you will notice, some event types can span multiple metadata types but will only be introduced once.

### Project
A project can be considered the comprehensive container (or umbrella) for all events pertaining to the goals of said event. An example of a project could be [ATOMIC](https://psl.noaa.gov/atomic/) which had multiple ships, aircraft, stations and UxS that made up a large, international field campaign. 

#### EventType
Using the example of ATOMIC:
```python
    {
        "kind": "Project",
        "metadata": {
            "name": "ATOMIC",
            "long_name": "",
            "description": "",
            "contacts": [""],
            "url": "",
            "missions": [""],
            "platforms": ["RHBrown"],
        },
        "data": {
            "start_time": "2020-01-06T00:00:00Z",
            "end_time": "2020-02-14T00:00:00Z"
        }
    }
```

### Platform
A platform is a mobile or stationary location that can house sampling systems and sensors. Examples include: ships, UxS, moorings and stations. A platform can have a parent platform where an example would be a UAS flying on/off of a ship during a cruise.
#### EventType
Using a ship as an example:
```python
    {
        "kind": "Platform",
        "metadata": {
            "name": "RHBrown",
            "platform_type": "Ship",
            "long_name": "Ronald H Brown",
            "owner": "NOAA",
            "mfg": "Halter Marine",
            "class": "Thompson",
            "id": "R 104",
            "call_sign": "WTEC"
        }
    }
```

### SamplingSystem
This is a defined group of sensors/devices that map actual sensors to real observations. Such a mapping could include a temperature sensor (make::model::serial_number) to "ambient_temperature". Additionally, this mapping will group the sensors based on source regions. For example, a common inlet. This will allow the concept of upstream/downstream for the purposes of data processing when some event signals a change that could affect other sensors in the group.

#### EventType
An example from the UAS:
```python
    "ACG": {
        "kind": "sampling_system_owner",
        "CloudySky": { # CloudySky
            "kind": "sampling_system",
            "2023-07-01T00:00:00Z": { # effective date
                "kind": "sampling_system_valid_time",
                "sampling_map": { 
                    "kind": "sampling_system_map",
                    "inlet": {
                        "kind": "sample_source",
                        "line-01": {
                            "kind": "sample_source",
                            "source_parent": "inlet",
                            "dryer": {
                                "kind": "sample_conditioner",
                                "instance": "Permapure::MD-110-XXX::"
                            },
                            "sdist_mob": {
                                "kind": "sensor",
                                "attributes": {
                                    "long_name": "Mobility Size Distribution",
                                    "description": "",
                                },
                                "instance": "Brechtel::mSEMS9404::001",
                                "dimensions": {"time": None, "diameter": None},
                                "timebase": [30], #seconds
                                "variables": {
                                    "time": {
                                        "type": "str",
                                        "shape": ["time"],
                                        "attributes": {"long_name": {"type": "string", "data": "Time"}},
                                    },
```


### Site/Location
This is not fully thought out yet but moorings are located at WMO sites which have unique IDs and span a range of lat/lon. Stations are have a set position. 

#### Event Type
No example yet

### ProjectPlatform Event
This generic term covers multiple terms that are used more often: Deployment, Cruise, Leg, Flight, etc. The gist is that any platform that is used during a project can be described in a ProjectPlatformEvent. These events can be hierarchical with subevents defining their parent. 

#### EventType
This shows two events with one being a subevent:
```python
    {
        "kind": "ProjectPlatformEvent",
        "metadata": {
            "name": "Vandenberg2023", # required
            "project": "Vandenberg2023", # required
            "platform": "OWA-N17OWA", # required
            "sampling_systems": [ # required
                {
                    "owner": "ACG",
                    "name": "CloudySky"
                },
            ],
            "description": "Deployment to Vandenberg Space Force Base", # optional
            "long_name": "", # optional
        },
        "data": {
            "start_time": "2023-07-01T16:00:00Z", # required
            "end_time": "2023-08-05T12:00:00Z", # optional
            "notes": "" # optional
        }
    },
    {
        "kind": "ProjectPlatformEvent",
        "metadata": {
            "name": "Flight_01", # required
            "project": "Vandenberg2023", # required
            "platform": "OWA-N17OWA", # required
            "parent_event": "Vandenberg2023", # optional: parameters can be inherited from parent
            "sampling_systems": [ # required
                {
                    "owner": "ACG",
                    "name": "CloudySky"
                },
            ],
            "description": "", # optional
            "long_name": "", # optional
        },
        "data": {
            "start_time": "2023-07-20T20:00:00Z", # required
            "end_time": "2023-07-20T22:30:00Z", # optional
            "notes": "" # optional
        }
    }
```

### SamplingSystemEvent
