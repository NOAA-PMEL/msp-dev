import asyncio
import json
import logging 
from ulid import ULID

from cloudevents.http import CloudEvent, from_dict, from_json
from cloudevents.conversion import to_structured, to_json
from cloudevents.exceptions import InvalidStructuredJSON

# from envds.event.types import BaseEventType as et

from envds.event.event import envdsEvent
from envds.sampling.types import SamplingEventType as et


class SamplingEvent(envdsEvent):
    """docstring for DAQEvent."""
    def __init__(self,):
        super(SamplingEvent, self).__init__()

    @staticmethod
    def create_variablemap_definition_registry_request(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.variablemap_definition_registry_request(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_variablemap_definition_registry_update(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.variablemap_definition_registry_update(), source=source, data=data, extra_header=extra_header)
    
    @staticmethod
    def create_variableset_definition_registry_request(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.variableset_definition_registry_request(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_variableset_definition_registry_update(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.variableset_definition_registry_update(), source=source, data=data, extra_header=extra_header)
    
    @staticmethod
    def create_variableset_data_request(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.variableset_data_request(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_variableset_data_update(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.variableset_data_update(), source=source, data=data, extra_header=extra_header)
    
    @staticmethod
    def create_dataset_data_request(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.dataset_data_request(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_dataset_data_update(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.dataset_data_update(), source=source, data=data, extra_header=extra_header)
    
    @staticmethod
    def create_project_data_request(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.project_data_request(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_project_data_update(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.project_data_update(), source=source, data=data, extra_header=extra_header)
    
    @staticmethod
    def create_platform_data_request(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.platform_data_request(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_platform_data_update(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.platform_data_update(), source=source, data=data, extra_header=extra_header)
    
    @staticmethod
    def create_sampling_data_request(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.sampling_data_request(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_sampling_data_update(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.sampling_data_update(), source=source, data=data, extra_header=extra_header)
    




    # @staticmethod
    # def create_interface_connect_request(source: str, data: dict = {}, extra_header: dict = None):
    #     return DAQEvent.create(type=et.interface_connect_request(), source=source, data=data, extra_header=extra_header)

    # @staticmethod
    # def create_interface_connect_update(source: str, data: dict = {}, extra_header: dict = None):
    #     return DAQEvent.create(type=et.interface_connect_update(), source=source, data=data, extra_header=extra_header)

    # @staticmethod
    # def create_interface_connect_keepalive(source: str, data: dict = {}, extra_header: dict = None):
    #     return DAQEvent.create(type=et.interface_connect_keepalive(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_interface_keepalive_request(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.interface_keepalive_request(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_interface_config_request(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.interface_config_request(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_interface_status_request(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.interface_status_request(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_interface_status_update(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.interface_status_update(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_sensor_registry_update(source: str, data: dict = {}, extra_header: dict = None):
        return envdsEvent.create(
            type=et.sensor_registry_update(), source=source, data=data, extra_header=extra_header
        )

    @staticmethod
    def create_sensor_registry_request(source: str, data: dict = {}, extra_header: dict = None):
        return envdsEvent.create(
            type=et.sensor_registry_request(),
            source=source,
            data=data,
            extra_header=extra_header,
        )

    @staticmethod
    def create_device_definition_registry_update(source: str, data: dict = {}, extra_header: dict = None):
        return envdsEvent.create(
            type=et.device_definition_registry_update(), source=source, data=data, extra_header=extra_header
        )

    @staticmethod
    def create_device_definition_registry_request(source: str, data: dict = {}, extra_header: dict = None):
        return envdsEvent.create(
            type=et.device_definition_registry_request(),
            source=source,
            data=data,
            extra_header=extra_header,
        )

    @staticmethod
    def create_device_definition_registry_ack(source: str, data: dict = {}, extra_header: dict = None):
        return envdsEvent.create(
            type=et.device_definition_registry_ack(), source=source, data=data, extra_header=extra_header
        )

    def create_device_registry_update(source: str, data: dict = {}, extra_header: dict = None):
        return envdsEvent.create(
            type=et.device_registry_update(), source=source, data=data, extra_header=extra_header
        )

    @staticmethod
    def create_device_registry_request(source: str, data: dict = {}, extra_header: dict = None):
        return envdsEvent.create(
            type=et.device_registry_request(),
            source=source,
            data=data,
            extra_header=extra_header,
        )

    @staticmethod
    def create_interface_registry_update(source: str, data: dict = {}, extra_header: dict = None):
        return envdsEvent.create(
            type=et.interface_registry_update(), source=source, data=data, extra_header=extra_header
        )

    @staticmethod
    def create_interface_registry_request(source: str, data: dict = {}, extra_header: dict = None):
        return envdsEvent.create(
            type=et.interface_registry_request(),
            source=source,
            data=data,
            extra_header=extra_header,
        )

    @staticmethod
    def create_sensor_settings_request(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.sensor_settings_request(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_sensor_settings_update(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.sensor_settings_update(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_controller_settings_request(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.controller_settings_request(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_controller_settings_update(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.controller_settings_update(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_controller_data_recv(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.controller_data_recv(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_controller_data_send(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.controller_data_send(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_controller_keepalive_request(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.controller_keepalive_request(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_controller_config_request(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.controller_config_request(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_controller_status_request(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.controller_status_request(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_controller_status_update(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.controller_status_update(), source=source, data=data, extra_header=extra_header)

    @staticmethod
    def create_controller_control_request(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.controller_control_request(), source=source, data=data, extra_header=extra_header)
    
    @staticmethod
    def create_controller_control_update(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.controller_control_update(), source=source, data=data, extra_header=extra_header)
    
    @staticmethod
    def create_controller_registry_update(source: str, data: dict = {}, extra_header: dict = None):
        return envdsEvent.create(
            type=et.controller_registry_update(), source=source, data=data, extra_header=extra_header
        )

    @staticmethod
    def create_controller_registry_request(source: str, data: dict = {}, extra_header: dict = None):
        return envdsEvent.create(
            type=et.controller_registry_request(),
            source=source,
            data=data,
            extra_header=extra_header,
        )

    @staticmethod
    def create_controller_definition_registry_update(source: str, data: dict = {}, extra_header: dict = None):
        return envdsEvent.create(
            type=et.controller_definition_registry_update(), source=source, data=data, extra_header=extra_header
        )

    @staticmethod
    def create_controller_definition_registry_request(source: str, data: dict = {}, extra_header: dict = None):
        return envdsEvent.create(
            type=et.controller_definition_registry_request(),
            source=source,
            data=data,
            extra_header=extra_header,
        )

    @staticmethod
    def create_controller_definition_registry_ack(source: str, data: dict = {}, extra_header: dict = None):
        return envdsEvent.create(
            type=et.controller_definition_registry_ack(), source=source, data=data, extra_header=extra_header
        )

    @staticmethod
    def create_controller_definition_registry_update(source: str, data: dict = {}, extra_header: dict = None):
        return envdsEvent.create(
            type=et.controller_definition_registry_update(), source=source, data=data, extra_header=extra_header
        )

    @staticmethod
    def create_controller_definition_registry_request(source: str, data: dict = {}, extra_header: dict = None):
        return envdsEvent.create(
            type=et.controller_definition_registry_request(),
            source=source,
            data=data,
            extra_header=extra_header,
        )

    @staticmethod
    def create_controller_definition_registry_ack(source: str, data: dict = {}, extra_header: dict = None):
        return envdsEvent.create(
            type=et.controller_definition_registry_ack(), source=source, data=data, extra_header=extra_header
        )

    @staticmethod
    def create_controller_data_update(source: str, data: dict = {}, extra_header: dict = None):
        return SamplingEvent.create(type=et.controller_data_update(), source=source, data=data, extra_header=extra_header)
