import asyncio
from datetime import datetime, timezone
import json
import logging

from cloudevents.http import CloudEvent, from_http, from_json
from cloudevents.conversion import to_structured # , from_http
from cloudevents.exceptions import InvalidStructuredJSON

# from cloudevents.conversion import from_http
# from cloudevents.conversion import to_structured  # , from_http

from cloudevents.pydantic import CloudEvent

# from typing import Union
import httpx
from logfmter import Logfmter
from pydantic import BaseModel, BaseSettings, Field

from ulid import ULID

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.INFO)

def StoreFactory(config):
    pass

class Datastore():
    def __init__(self, config):
        pass

    def configure(self):
        # create backend services for:
        #   - ephemeral: redis or mongodb
        #   - persistent: local or erddap