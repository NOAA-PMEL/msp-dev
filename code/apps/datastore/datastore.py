import asyncio
import json
import logging
import math
from time import sleep

# import numpy as np
from ulid import ULID
from pathlib import Path
import os

import httpx
from logfmter import Logfmter

# from registry import registry
# from flask import Flask, request
from pydantic import BaseSettings
from cloudevents.http import CloudEvent, from_http

# from cloudevents.http.conversion import from_http
from cloudevents.conversion import to_structured  # , from_http
from cloudevents.exceptions import InvalidStructuredJSON

from datetime import datetime, timezone
# import pymongo

import uvicorn

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.INFO)


# test

class Settings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = False
    # knative_broker: str = (
    #     "http://kafka-broker-ingress.knative-eventing.svc.cluster.local/default/default"
    # )
    # mongodb_user_name: str = ""
    # mongodb_user_password: str = ""
    # mongodb_connection: str = (
    #     "mongodb://uasdaq:password@uasdaq-mongodb-0.uasdaq-mongodb-svc.mongodb.svc.cluster.local:27017,uasdaq-mongodb-1.uasdaq-mongodb-svc.mongodb.svc.cluster.local:27017,uasdaq-mongodb-2.uasdaq-mongodb-svc.mongodb.svc.cluster.local:27017/data?replicaSet=uasdaq-mongodb&ssl=false"
    # )
    # erddap_http_connection: str = (
    #     "http://uasdaq.pmel.noaa.gov/uasdaq/dataserver/erddap"
    # )
    # erddap_https_connection: str = (
    #     "https://uasdaq.pmel.noaa.gov/uasdaq/dataserver/erddap"
    # )
    # # erddap_author: str = "fake_author"

    dry_run: bool = False

    class Config:
        env_prefix = "DATASTORE_"
        case_sensitive = False

class TestClass():
    """docstring for TestClass."""
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("TestClass instantiated")
    
async def shutdown():
    print("shutting down")
    # for task in task_list:
    #     print(f"cancel: {task}")
    #     task.cancel()

async def main(config):
    config = uvicorn.Config(
        "main:app",
        host=config.host,
        port=config.port,
        # log_level=server_config.log_level,
        root_path="/msp/datastore",
        # log_config=dict_config,
    )

    server = uvicorn.Server(config)
    # test = logging.getLogger()
    # test.info("test")
    L.info(f"server: {server}")
    await server.serve()

    print("starting shutdown...")
    await shutdown()
    print("done.")


if __name__ == "__main__":
    # app.run(debug=config.debug, host=config.host, port=config.port)
    # app.run()
    config = Settings()
    asyncio.run(main(config))