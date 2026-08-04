from envds.event.types import BaseEventType

# class BaseEventType(object):
class SamplingEventType(BaseEventType):
    """docstring for envdsBaseType."""
    TYPE_BASE = "envds"

    TYPE_PROJECT_DEFINITION = "project-definition"
    TYPE_PLATFORM_DEFINITION = "platform-definition"
    TYPE_DEPLOYMENT_DEFINITION = "deployment-definition"
    TYPE_CONTACT_DEFINITION = "contact-definition"
    TYPE_VARIABLEMAP_DEFINITION = "variablemap-definition"
    TYPE_VARIABLESET_DEFINITION = "variableset-definition"
    TYPE_SYSTEMMODE_DEFINITION = "systemmode-definition"
    TYPE_SAMPLINGMODE_DEFINITION = "samplingmode-definition"
    TYPE_SAMPLINGSTATE_DEFINITION = "samplingstate-definition"
    TYPE_SAMPLINGCONDITION_DEFINITION = "samplingcondition-definition"
    TYPE_ACTION_DEFINITION = "action-definition"
    TYPE_DATASET_DEFINITION = "dataset-definition"
    TYPE_VARIABLESET = "variableset"
    TYPE_DATASET = "dataset"
    TYPE_PROJECT = "project"
    TYPE_PLATFORM = "platform"
    TYPE_SAMPLING = "sampling"
    TYPE_SAMPLING_CONDITION = "samplingcondition"
    TYPE_SAMPLING_STATE = "samplingstate"
    TYPE_SAMPLING_MODE = "samplingmode"
    TYPE_SYSTEM_MODE = "systemmode"
    TYPE_SYSTEM = "system"
    TYPE_CONTROL = "control"

    # ACTION_RECV = "recv"
    # ACTION_SEND = "send"
    # ACTION_KEEPALIVE = "keepalive"

    def __init__(self):
        super(SamplingEventType, self).__init__()

    # helper functions
    @staticmethod
    def definition_registry_update(resource: str):
        """Dynamically build registry update types for various sampling resources
           e.g. returns 'envds.project-definition.registry.update'
        """
        # FIX: Force '-definition' suffix if it's not already there!
        res = resource if resource.endswith("-definition") else f"{resource}-definition"
        return ".".join([BaseEventType.get_type(res), BaseEventType.TYPE_REGISTRY, BaseEventType.ACTION_UPDATE])
    
    @staticmethod
    def definition_registry_request(resource: str):
        res = resource if resource.endswith("-definition") else f"{resource}-definition"
        return ".".join([BaseEventType.get_type(res), BaseEventType.TYPE_REGISTRY, BaseEventType.ACTION_REQUEST])
    
    @staticmethod
    def variablemap_definition_registry_request():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_VARIABLEMAP_DEFINITION), BaseEventType.TYPE_REGISTRY, BaseEventType.ACTION_REQUEST])

    @staticmethod
    def variablemap_definition_registry_update():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_VARIABLEMAP_DEFINITION), BaseEventType.TYPE_REGISTRY, BaseEventType.ACTION_UPDATE])

    @staticmethod
    def variableset_definition_registry_update():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_VARIABLESET_DEFINITION), BaseEventType.TYPE_REGISTRY, BaseEventType.ACTION_UPDATE])

    @staticmethod
    def variableset_definition_registry_request():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_VARIABLESET_DEFINITION), BaseEventType.TYPE_REGISTRY, BaseEventType.ACTION_REQUEST])

    @staticmethod
    def variableset_data_request():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_VARIABLESET), BaseEventType.TYPE_DATA, BaseEventType.ACTION_REQUEST])

    @staticmethod
    def variableset_data_update():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_VARIABLESET), BaseEventType.TYPE_DATA, BaseEventType.ACTION_UPDATE])

    @staticmethod
    def dataset_data_request():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_DATASET), BaseEventType.TYPE_DATA, BaseEventType.ACTION_REQUEST])

    @staticmethod
    def dataset_data_update():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_DATASET), BaseEventType.TYPE_DATA, BaseEventType.ACTION_UPDATE])

    @staticmethod
    def project_data_request():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_PROJECT), BaseEventType.TYPE_DATA, BaseEventType.ACTION_REQUEST])

    @staticmethod
    def project_data_update():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_PROJECT), BaseEventType.TYPE_DATA, BaseEventType.ACTION_UPDATE])

    @staticmethod
    def platform_data_request():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_PLATFORM), BaseEventType.TYPE_DATA, BaseEventType.ACTION_REQUEST])

    @staticmethod
    def platform_data_update():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_PLATFORM), BaseEventType.TYPE_DATA, BaseEventType.ACTION_UPDATE])

    @staticmethod
    def sampling_data_request():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_SAMPLING), BaseEventType.TYPE_DATA, BaseEventType.ACTION_REQUEST])

    @staticmethod
    def sampling_data_update():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_SAMPLING), BaseEventType.TYPE_DATA, BaseEventType.ACTION_UPDATE])

    @staticmethod
    def sampling_condition_status_request():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_SAMPLING_CONDITION), BaseEventType.TYPE_STATUS, BaseEventType.ACTION_REQUEST])

    @staticmethod
    def sampling_condition_status_update():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_SAMPLING_CONDITION), BaseEventType.TYPE_STATUS, BaseEventType.ACTION_UPDATE])

    @staticmethod
    def sampling_state_status_request():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_SAMPLING_STATE), BaseEventType.TYPE_STATUS, BaseEventType.ACTION_REQUEST])

    @staticmethod
    def sampling_state_status_update():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_SAMPLING_STATE), BaseEventType.TYPE_STATUS, BaseEventType.ACTION_UPDATE])

    @staticmethod
    def sampling_mode_status_request():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_SAMPLING_MODE), BaseEventType.TYPE_STATUS, BaseEventType.ACTION_REQUEST])

    @staticmethod
    def sampling_mode_status_update():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_SAMPLING_MODE), BaseEventType.TYPE_STATUS, BaseEventType.ACTION_UPDATE])

    # ---> ADD THESE TWO METHODS <---
    @staticmethod
    def system_mode_status_request():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_SYSTEM_MODE), BaseEventType.TYPE_STATUS, BaseEventType.ACTION_REQUEST])

    @staticmethod
    def system_mode_status_update():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_SYSTEM_MODE), BaseEventType.TYPE_STATUS, BaseEventType.ACTION_UPDATE])
    
    @staticmethod
    def system_control_request():
        # Yields: envds.system.control.request
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_SYSTEM), SamplingEventType.TYPE_CONTROL, BaseEventType.ACTION_REQUEST])

    @staticmethod
    def system_control_update():
        # Yields: envds.system.control.update
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_SYSTEM), SamplingEventType.TYPE_CONTROL, BaseEventType.ACTION_UPDATE])
    
    @staticmethod
    def dataset_definition_registry_request():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_DATASET_DEFINITION), BaseEventType.TYPE_REGISTRY, BaseEventType.ACTION_REQUEST])

    @staticmethod
    def dataset_definition_registry_update():
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_DATASET_DEFINITION), BaseEventType.TYPE_REGISTRY, BaseEventType.ACTION_UPDATE])

    @staticmethod
    def dataset_generate_request():
        # Yields: "envds.dataset.generate.request"
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_DATASET), "generate", BaseEventType.ACTION_REQUEST])