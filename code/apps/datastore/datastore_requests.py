# from typing import List, Any, Dict
# from pydantic import BaseModel

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
# Defines a specific executable action (e.g., triggering a relay)
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
# Evaluates sources (variables) against criteria to trigger actions
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
    actions: Dict[str, ActionTrigger]  # e.g., {"true": ActionTrigger(), "false": ActionTrigger()}

class ConditionCriteria(BaseModel):
    any: Optional[List[ConditionCriterion]] = Field(default_factory=list)
    all: Optional[List[ConditionCriterion]] = Field(default_factory=list)

class SamplingConditionDefinition(BaseModel):
    kind: str = "envCondition"
    metadata: DefinitionMetadata
    sources: Dict[str, ConditionSource]
    condition_type: str = Field(..., alias="condition-type")  # e.g., "MinMax", "Boolean"
    condition_criteria: ConditionCriteria = Field(..., alias="condition-criteria")

    class Config:
        allow_population_by_field_name = True


# ---------------------------------------------------------
# 3. Sampling State Definition
# Represents a specific state (e.g., "Purging", "Sampling") 
# and the conditions/actions tied to entering, holding, or exiting it.
# ---------------------------------------------------------
class SamplingStateDefinition(BaseModel):
    kind: str = "envState"
    metadata: DefinitionMetadata
    entry_actions: Optional[List[ActionTrigger]] = Field(default_factory=list, alias="entry-actions")
    exit_actions: Optional[List[ActionTrigger]] = Field(default_factory=list, alias="exit-actions")
    active_conditions: Optional[List[str]] = Field(default_factory=list, alias="active-conditions") # Condition names to monitor

    class Config:
        allow_population_by_field_name = True


# ---------------------------------------------------------
# 4. Sampling Mode Definition
# Orchestrates states and dictates how the system transitions between them.
# (e.g., "Vertical Profiling Mode" vs "Underway Transit Mode")
# ---------------------------------------------------------
class SamplingModeDefinition(BaseModel):
    kind: str = "envSamplingMode"
    metadata: DefinitionMetadata
    states: List[str]  # Allowed state names
    default_state: str = Field(..., alias="default-state")
    transitions: Optional[Dict[str, str]] = Field(default_factory=dict) # e.g., {"condition_cn_limit_true": "PurgingState"}

    class Config:
        allow_population_by_field_name = True


# ---------------------------------------------------------
# 5. System Mode Definition
# The top-level operational mode for the overarching system/payload.
# (e.g., "Startup", "Active", "SafeMode")
# ---------------------------------------------------------
class SystemModeDefinition(BaseModel):
    kind: str = "envSystemMode"
    metadata: DefinitionMetadata
    default_sampling_mode: Optional[str] = Field(None, alias="default-sampling-mode")
    allowed_sampling_modes: Optional[List[str]] = Field(default_factory=list, alias="allowed-sampling-modes")
    system_actions: Optional[List[ActionTrigger]] = Field(default_factory=list, alias="system-actions")

    class Config:
        allow_population_by_field_name = True