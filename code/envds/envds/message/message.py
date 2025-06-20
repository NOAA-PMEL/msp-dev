#import asyncio
import logging

#from typing import Union
#from pydantic import BaseSettings, Field

# from asyncio_mqtt import Client, MqttError

from cloudevents.http import CloudEvent#, event, from_json, to_structured
#from cloudevents.exceptions import InvalidStructuredJSON, MissingRequiredFields


# class EncryptedMessageConfig(BaseSettings):
#     encrypted: Union[bool, None] = False


# TODO: this could be pydantic model
class Message(object):
    """docstring for Message."""

    def __init__(
        self,
        data: CloudEvent,
        destpath: str = None,
        sourcepath: str = None
        # data_type: str = "cloudevent",
        # encryption: EncryptedMessageConfig = None,
    ):
        super(Message, self).__init__()
        
        self.logger = logging.getLogger(__name__)

        self.data = data
        self.destpath = destpath
        self.sourcepath = sourcepath
        # self.data_type = data_type
        # self.encryption = encryption

    # def test(self):
    #     # print(self.logger)
    #     self.logger.info("Test Message", extra={"key1": "value1"})
