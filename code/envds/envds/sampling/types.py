from envds.event.types import BaseEventType

# class BaseEventType(object):
class SamplingEventType(BaseEventType):
    """docstring for envdsBaseType."""
    TYPE_BASE = "envds"

    TYPE_VARIABLESET = "variable-set"
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

