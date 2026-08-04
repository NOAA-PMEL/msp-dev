from typing import Dict, List, Any, Optional
from pydantic import BaseModel, Field


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
    force_archive: bool = False


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
    start_time: str | None = None
    end_time: str | None = None
    start_timestamp: float | None = None
    end_timestamp: float | None = None
    last_n_seconds: int | None
    variable: List[str] | None = None
    force_archive: bool = False


class ControllerDefinitionUpdate(BaseModel):
    controller_definition_id: str | None = None
    make: str | None = None
    model: str | None = None
    version: str
    valid_time: str
    attributes: dict
    dimensions: dict
    variables: dict


class ControllerDefinitionRequest(BaseModel):
    controller_definition_id: str | None = None
    make: str | None = None
    model: str | None = None
    version: str | None = None
    valid_time: str | None = None


class ControllerInstanceUpdate(BaseModel):
    controller_id: str | None = None
    make: str | None = None
    model: str | None = None
    serial_number: str | None = None
    version: str
    attributes: dict


class ControllerInstanceRequest(BaseModel):
    controller_id: str | None = None
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
    deployment_ref: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    start_timestamp: float | None = None
    end_timestamp: float | None = None
    last_n_seconds: int | None
    variable: List[str] | None = None
    force_archive: bool = False


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


# ---------------------------------------------------------
# Common Metadata Model
# ---------------------------------------------------------
class DefinitionMetadata(BaseModel):
    name: str
    valid_config_time: Optional[str] = "2020-01-01T00:00:00Z"
    description: Optional[str] = ""
    tags: Optional[List[str]] = []


# ---------------------------------------------------------
# 1. Action Definition
# ---------------------------------------------------------
class ActionParameter(BaseModel):
    required: bool = False
    allowed_values: Optional[List[Any]] = Field(default_factory=list, alias="allowed-values")
    type: Optional[str] = "string"


class ActionTarget(BaseModel):
    apiVersion: Optional[str] = None
    kind: str  # e.g., "Service", "Topic", "Webhook"
    name: str
    uri: Optional[str] = None


class ActionDefinition(BaseModel):
    kind: str = "envAction"
    metadata: DefinitionMetadata
    parameters: Optional[Dict[str, ActionParameter]] = Field(default_factory=dict)
    action: ActionTarget

    class Config:
        allow_population_by_field_name = True


# ---------------------------------------------------------
# 2. Sampling Condition Definition
# ---------------------------------------------------------
class ConditionSource(BaseModel):
    platform: Optional[str] = None
    variablemap: Optional[str] = None
    variable: str


class ActionTrigger(BaseModel):
    name: str
    data: Optional[Dict[str, Any]] = Field(default_factory=dict)


class ConditionLimits(BaseModel):
    min_val: Optional[float] = Field(None, alias="min-val")
    max_val: Optional[float] = Field(None, alias="max-val")
    equals: Optional[Any] = None


class ConditionCriterion(BaseModel):
    source: str
    limits: Optional[ConditionLimits] = None
    actions: Dict[str, ActionTrigger]


class ConditionCriteria(BaseModel):
    any: Optional[List[ConditionCriterion]] = Field(default_factory=list)
    all: Optional[List[ConditionCriterion]] = Field(default_factory=list)


class SamplingConditionDefinition(BaseModel):
    kind: str = "envCondition"
    metadata: DefinitionMetadata
    sources: Dict[str, ConditionSource]
    condition_type: str = Field(..., alias="condition-type")
    condition_criteria: ConditionCriteria = Field(..., alias="condition-criteria")

    class Config:
        allow_population_by_field_name = True


# ---------------------------------------------------------
# 3. Sampling State Definition
# ---------------------------------------------------------
class SamplingStateDefinition(BaseModel):
    kind: str = "envState"
    metadata: DefinitionMetadata
    entry_actions: Optional[List[ActionTrigger]] = Field(default_factory=list, alias="entry-actions")
    exit_actions: Optional[List[ActionTrigger]] = Field(default_factory=list, alias="exit-actions")
    active_conditions: Optional[List[str]] = Field(default_factory=list, alias="active-conditions")

    class Config:
        allow_population_by_field_name = True


# ---------------------------------------------------------
# 4. Sampling Mode Definition
# ---------------------------------------------------------
class SamplingModeDefinition(BaseModel):
    kind: str = "envSamplingMode"
    metadata: DefinitionMetadata
    states: List[str]
    default_state: str = Field(..., alias="default-state")
    transitions: Optional[Dict[str, str]] = Field(default_factory=dict)

    class Config:
        allow_population_by_field_name = True


# ---------------------------------------------------------
# 5. System Mode Definition
# ---------------------------------------------------------
class SystemModeDefinition(BaseModel):
    kind: str = "envSystemMode"
    metadata: DefinitionMetadata
    default_sampling_mode: Optional[str] = Field(None, alias="default-sampling-mode")
    allowed_sampling_modes: Optional[List[str]] = Field(default_factory=list, alias="allowed-sampling-modes")
    system_actions: Optional[List[ActionTrigger]] = Field(default_factory=list, alias="system-actions")

    class Config:
        allow_population_by_field_name = True


class VariableSetInstanceUpdate(BaseModel):
    variableset_id: str | None = None
    variablemap_id: str | None = None
    variableset: str | None = None
    attributes: dict


class VariableSetInstanceRequest(BaseModel):
    variableset_id: str | None = None
    variablemap_id: str | None = None
    variableset: str | None = None