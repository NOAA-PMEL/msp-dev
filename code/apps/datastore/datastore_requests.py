from typing import List, Any, Dict
from pydantic import BaseModel


class DataStoreQuery(BaseModel):
    device_definition_id: str | None = None
    make: str | None = None
    model: str | None = None
    serial_number: str | None = None
    version: str | None = None
    device_type: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    last_n_seconds: int | None
    variable: List[str] | None = None


class DataUpdate(BaseModel):
    device_id: str | None = None
    make: str | None = None
    model: str | None = None
    serial_number: str | None = None
    version: str
    timestamp: float
    attributes: dict
    dimensions: dict
    variables: dict


class DataRequest(BaseModel):
    device_id: str | None = None
    make: str | None = None
    model: str | None = None
    serial_number: str | None = None
    version: str | None = None
    device_type: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    start_timestamp: float | None = None
    end_timestamp: float | None = None
    last_n_seconds: int | None
    variable: List[str] | None = None


class DeviceDefinitionUpdate(BaseModel):
    device_definition_id: str | None = None
    make: str | None = None
    model: str | None = None
    version: str
    device_type: str
    valid_time: str
    attributes: dict
    dimensions: dict
    variables: dict


class DeviceDefinitionRequest(BaseModel):
    device_definition_id: str | None = None
    make: str | None = None
    model: str | None = None
    version: str | None = None
    device_type: str | None = None
    valid_time: str | None = None


class DeviceInstanceUpdate(BaseModel):
    device_id: str | None = None
    make: str | None = None
    model: str | None = None
    serial_number: str | None = None
    version: str
    device_type: str
    attributes: dict


class DeviceInstanceRequest(BaseModel):
    device_id: str | None = None
    make: str | None = None
    model: str | None = None
    serial_number: str | None = None
    version: str | None = None
    device_type: str | None = None

class ControllerDataUpdate(BaseModel):
    controller_id: str | None = None
    make: str | None = None
    model: str | None = None
    serial_number: str | None = None
    version: str
    timestamp: float
    attributes: dict
    dimensions: dict
    variables: dict


class ControllerDataRequest(BaseModel):
    controller_id: str | None = None
    make: str | None = None
    model: str | None = None
    serial_number: str | None = None
    version: str | None = None
    # device_type: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    start_timestamp: float | None = None
    end_timestamp: float | None = None
    last_n_seconds: int | None
    variable: List[str] | None = None


class ControllerDefinitionUpdate(BaseModel):
    controller_definition_id: str | None = None
    make: str | None = None
    model: str | None = None
    version: str
    # device_type: str
    valid_time: str
    attributes: dict
    dimensions: dict
    variables: dict


class ControllerDefinitionRequest(BaseModel):
    controller_definition_id: str | None = None
    make: str | None = None
    model: str | None = None
    version: str | None = None
    # device_type: str | None = None
    valid_time: str | None = None


class ControllerInstanceUpdate(BaseModel):
    controller_id: str | None = None
    make: str | None = None
    model: str | None = None
    serial_number: str | None = None
    version: str
    # device_type: str
    attributes: dict


class ControllerInstanceRequest(BaseModel):
    controller_id: str | None = None
    make: str | None = None
    model: str | None = None
    serial_number: str | None = None
    version: str | None = None
    # device_type: str | None = None


class DatastoreRequest(BaseModel):
    database: str
    collection: str
    request: (
        DataUpdate
        | DataRequest
        | DeviceDefinitionUpdate
        | DeviceDefinitionRequest
        | DeviceInstanceUpdate
        | DeviceInstanceRequest
    )

class VariableSetDataUpdate(BaseModel):
    variableset_id: str | None = None
    variablemap_id: str | None = None
    variableset: str | None = None
    timestamp: float
    attributes: dict
    dimensions: dict
    variables: dict


class VariableSetDataRequest(BaseModel):
    variableset_id: str | None = None
    variablemap_id: str | None = None
    variableset: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    start_timestamp: float | None = None
    end_timestamp: float | None = None
    last_n_seconds: int | None
    variable: List[str] | None = None

class VariableSetDefinitionUpdate(BaseModel):
    variableset_definition_id: str | None = None
    variablemap_definition_id: str | None = None
    variableset: str | None = None
    index_type: str | None = None
    index_value: Any | None = None
    attributes: dict
    dimensions: dict
    variables: dict

class VariableSetDefinitionRequest(BaseModel):
    variableset_definition_id: str | None = None
    variablemap_definition_id: str | None = None
    variableset: str | None = None
    index_type: str | None = None
    index_value: Any | None = None

class VariableMapDefinitionUpdate(BaseModel):
    variablemap_definition_id: str | None = None
    variablemap_type: str | None = None
    variablemap_type_id: str | None = None
    variablemap: str | None = None
    valid_config_time: str | None = None
    revision: int | None = None
    attributes: dict
    variablesets: dict
    variables: dict


class VariableMapDefinitionRequest(BaseModel):
    variablemap_definition_id: str | None = None
    variablemap_type: str | None = None
    variablemap_type_id: str | None = None
    variablemap: str | None = None
    valid_config_time: str | None = None

