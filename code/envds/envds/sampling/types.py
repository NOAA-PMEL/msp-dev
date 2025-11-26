from envds.event.types import BaseEventType

# class BaseEventType(object):
class SamplingEventType(BaseEventType):
    """docstring for envdsBaseType."""
    TYPE_BASE = "envds"

    TYPE_VARIABLEMAP_DEFINITION = "variablemap-definition"
    TYPE_VARIABLESET_DEFINITION = "variableset-definition"
    TYPE_VARIABLESET = "variableset"
    TYPE_DATASET = "dataset"
    TYPE_PROJECT = "project"
    TYPE_PLATFORM = "platform"
    TYPE_SAMPLING = "sampling"
    
    # ACTION_RECV = "recv"
    # ACTION_SEND = "send"
    # ACTION_KEEPALIVE = "keepalive"

    def __init__(self):
        super(BaseEventType, self).__init__()

    # helper functions

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
        return ".".join([BaseEventType.get_type(SamplingEventType.TYPE_DATAESET), BaseEventType.TYPE_DATA, BaseEventType.ACTION_REQUEST])

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

