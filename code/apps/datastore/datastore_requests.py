from typing import List
from pydantic import BaseModel, Union


class DataStoreQuery(BaseModel):
    sensor_definition_id: str | None = None
    make: str | None = None
    model: str | None = None
    serial_number: str | None = None
    version: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    last_n_seconds: int | None
    variable: List[str] | None = None


class DataUpdate(BaseModel):
    sensor_definition_id: str | None = None
    make: str | None = None
    model: str | None = None
    serial_number: str | None = None
    version: str
    timestamp: str
    attributes: dict
    dimensions: dict
    variables: dict


class DataRequest(BaseModel):
    sensor_id: str | None = None
    make: str | None = None
    model: str | None = None
    serial_number: str | None = None
    version: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    last_n_seconds: int | None
    variable: List[str] | None = None


class DeviceDefinitionUpdate(BaseModel):
    device_id: str | None = None
    make: str | None = None
    model: str | None = None
    version: str
    valid_time: str
    attributes: dict
    dimensions: dict
    variables: dict


class DeviceDefinitionRequest(BaseModel):
    device_id: str | None = None
    make: str | None = None
    model: str | None = None
    version: str | None = None
    valid_time: str | None = None


class DeviceInstanceUpdate(BaseModel):
    device_id: str | None = None
    make: str | None = None
    model: str | None = None
    serial_number: str | None = None
    version: str
    attributes: dict


class DeviceInstanceRequest(BaseModel):
    device_id: str | None = None
    make: str | None = None
    model: str | None = None
    serial_number: str | None = None
    version: str | None = None


class DatastoreRequest(BaseModel):
    database: str
    collection: str
    request: Union[
        DataUpdate,
        DataRequest,
        DeviceDefinitionUpdate,
        DeviceDefinitionRequest,
        DeviceInstanceUpdate,
        DeviceInstanceRequest,
    ]
