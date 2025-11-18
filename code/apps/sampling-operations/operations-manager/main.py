# import asyncio
from datetime import datetime, timezone
# import json
import logging

from fastapi import FastAPI, Request, Query, status  # , APIRouter
from fastapi.middleware.cors import CORSMiddleware

# from cloudevents.http import from_http
from cloudevents.http import CloudEvent, from_http, from_json
from cloudevents.conversion import to_structured, to_json  # , from_http
from cloudevents.exceptions import InvalidStructuredJSON
from cloudevents.pydantic import CloudEvent

# import httpx
from logfmter import Logfmter
from typing import Annotated, List
from pydantic import BaseModel, BaseSettings, Field
from ulid import ULID
# import traceback

from operations_manager import OperationsManager
# from datastore_requests import (
#     # DataStoreQuery,
#     DataRequest,
#     DeviceDefinitionRequest,
#     DeviceInstanceRequest,
#     # ControllerDataUpdate,
#     ControllerDataRequest,
#     ControllerDefinitionRequest,
#     # ControllerDefinitionUpdate,
#     ControllerInstanceRequest,
#     # ControllerInstanceUpdate,
# )

print("pre-logger: 1")
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.DEBUG)


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
        env_prefix = "SAMPLING_OPERATIONS_"
        case_sensitive = False


# from apis.router import api_router
L.debug("main: here:1")
app = FastAPI()
L.debug("main: here:2")

# origins = ["*"]  # dev
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=origins,
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# router = APIRouter()
# home_router = APIRouter()

# @home_router.get("/")
# async def home():
#     return {"message": "Hello World"}

# home_router.include_router(api_router)
# router.include_router(home_router)#, prefix="/envds/home")

# app.include_router(api_router)#, prefix="/envds/home")
# app.include_router(router)

# @app.on_event("startup")
# async def start_system():
#     print("starting system")


# @app.on_event("shutdown")
# async def start_system():
#     print("stopping system")

# class DeviceDataSearch(BaseModel):

#     start_time: str | None = None
#     end_time: str | None = None
#     custom: dict | None = None

# datastore = Datastore()
print("starting operations_manager")
operations_manager = OperationsManager()
print(f"operations_manager started: {operations_manager}")


@app.get("/")
async def root():
    return {"message": "Hello World from SamplingSystem"}


@app.post("/device/data/update/", status_code=status.HTTP_202_ACCEPTED)
async def device_data_update(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        L.debug(request.headers)
        L.debug("device_data_update", extra={"ce": ce, "destpath": ce["destpath"]})
        # await adapter.send_to_mqtt(ce)
        await operations_manager.device_data_update(ce)
    except Exception as e:
        L.error("device_data_update", extra={"reason": e})
        return "", 204


# @app.get("/device/data/get/")
# # async def device_data_get(query: Annotated[DataStoreQuery, Query()]):
# async def device_data_get(
#     device_id: str | None = None,
#     make: str | None = None,
#     model: str | None = None,
#     serial_number: str | None = None,
#     version: str | None = None,
#     device_type: str | None = None,
#     start_time: str | None = None,
#     end_time: str | None = None,
#     last_n_seconds: int | None = None,
#     variable: List[str] | None = None,
# ):
#     L.debug("main:device_data_get", extra={"device_id": device_id})
#     query = DataRequest(
#         device_id=device_id,
#         make=make,
#         model=model,
#         serial_number=serial_number,
#         version=version,
#         device_type=device_type,
#         start_time=start_time,
#         end_time=end_time,
#         last_n_seconds=last_n_seconds,
#         variable=variable,
#     )
#     L.debug("main:device_data_get", extra={"query": query})
#     return await datastore.device_data_get(query)


@app.post("/controller/data/update/", status_code=status.HTTP_202_ACCEPTED)
async def controller_data_update(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        L.debug(request.headers)
        L.debug("controller_data_update", extra={"ce": ce, "destpath": ce["destpath"]})
        # await adapter.send_to_mqtt(ce)
        await operations_manager.controller_data_update(ce)
    except Exception as e:
        L.error("controller_data_update", extra={"reason": e})
        return "", 204


# @app.get("/controller/data/get/")
# # async def controller_data_get(query: Annotated[DataStoreQuery, Query()]):
# async def controller_data_get(
#     controller_id: str | None = None,
#     make: str | None = None,
#     model: str | None = None,
#     serial_number: str | None = None,
#     version: str | None = None,
#     # device_type: str | None = None,
#     start_time: str | None = None,
#     end_time: str | None = None,
#     last_n_seconds: int | None = None,
#     variable: List[str] | None = None,
# ):
#     L.debug("main:controller_data_get", extra={"controller_id": controller_id})
#     query = ControllerDataRequest(
#         controller_id=controller_id,
#         make=make,
#         model=model,
#         serial_number=serial_number,
#         version=version,
#         # device_type=device_type,
#         start_time=start_time,
#         end_time=end_time,
#         last_n_seconds=last_n_seconds,
#         variable=variable,
#     )
#     L.debug("main:controller_data_get", extra={"query": query})
#     return await datastore.controller_data_get(query)

