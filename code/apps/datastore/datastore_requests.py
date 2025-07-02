from typing import List
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
    timestamp: str
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
    last_n_seconds: int | None
    variable: List[str] | None = None


class DeviceDefinitionUpdate(BaseModel):
    device_id: str | None = None
    make: str | None = None
    model: str | None = None
    version: str
    device_type: str
    valid_time: str
    attributes: dict
    dimensions: dict
    variables: dict


class DeviceDefinitionRequest(BaseModel):
    device_id: str | None = None
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
    request: (
        DataUpdate
        | DataRequest
        | DeviceDefinitionUpdate
        | DeviceDefinitionRequest
        | DeviceInstanceUpdate
        | DeviceInstanceRequest
    )
