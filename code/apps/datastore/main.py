import asyncio
from datetime import datetime, timezone
import json
import logging

from fastapi import FastAPI, Request, Query, status  # , APIRouter
from fastapi.middleware.cors import CORSMiddleware

# from cloudevents.http import from_http
from cloudevents.http import CloudEvent, from_http, from_json
from cloudevents.conversion import to_structured, to_json # , from_http
from cloudevents.exceptions import InvalidStructuredJSON
from cloudevents.pydantic import CloudEvent

import httpx
from logfmter import Logfmter
from typing import Annotated
from pydantic import BaseModel, BaseSettings, Field
from ulid import ULID

from datastore_requests import Datastore, DataStoreQuery, DataRequest, DeviceDefinitionRequest, DeviceInstanceRequest

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
        env_prefix = "DATASTORE_"
        case_sensitive = False


# from apis.router import api_router

app = FastAPI()

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

datastore = Datastore()

@app.get("/")
async def root():
    return {"message": "Hello World from Datastore"}


# @app.post("/ce")
# async def handle_ce(ce: CloudEvent):
#     # print(ce.data)
#     # print(from_http(ce))
#     # header, data = from_http(ce)
#     print(f"type: {ce['type']}, source: {ce['source']}, data: {ce.data}, id: {ce['id']}")
#     print(f"attributes: {ce}")
#     # event = from_http(ce.headers, ce.get_data)
#     # print(event)


# @app.get("/device/data/request/{device_id}")
# async def get_device_data(device_id: str, search_opts: DeviceDataSearch):

#     return None

# @app.get("/data/request")
# async def data_request()
#     pass

@app.post("/sensor/data/update", status_code=status.HTTP_202_ACCEPTED)
async def data_sensor_update(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        L.debug(request.headers)
        L.debug("sensor_data_update", extra={"ce": ce, "destpath": ce["destpath"]})
        # await adapter.send_to_mqtt(ce)
        await datastore.sensor_data_update(ce)
    except Exception as e:
        # L.error("send", extra={"reason": e})
        pass

    return "",204
    

@app.get("/sensor/data/get")
async def data_sensor_get(query: Annotated[DataStoreQuery, Query()]):
#     sensor_id: str | None = None,
#     make: str | None = None,
#     model: str | None = None,
#     serial_number: str | None = None,
#     version: str | None = None,
#     start_time: str | None = None,
#     end_time: str | None = None
# ):

    # query_params = request.query_params
    # query = DataStoreQuery(
    #     sensor_id=sensor_id,
    #     make=make,
    #     model=model,
    #     serial_number=serial_number,
    #     version=version,
    #     start_time=start_time,
    #     end_time=end_time
    # )
    result = await datastore.sensor_data_get(query)
    return {"result": result}
    
@app.post("/sensor/settings/update", status_code=status.HTTP_202_ACCEPTED)
async def sensor_settings_update(request: Request):

    # attributes = {
    #     # "type": "envds.controller.control.request",
    #     "type": "message.ack",
    #     "source": "datastore",
    #     "id": str(ULID()),
    #     "datacontenttype": "application/json; charset=utf-8",
    # }
    # response = {"message": "ok"}
    try:
        # ce = from_json(await request.json())
        # print(ce)
        # pass
        # print("sensor_settings_update")
        ce = from_http(request.headers, await request.body())
        # print(ce)
        # L.debug(request.headers,)
        L.debug("sensor_settings_update", extra={"ce": ce})#, "destpath": ce["destpath"]})
        # await adapter.send_to_mqtt(ce)
        # await datastore.data_sensor_update(ce)
    except Exception as e:
        # print(e)
        L.error("send", extra={"reason": e})
        pass
    # return "ok", 200
    # ce = CloudEvent(attributes=attributes, data=response)
    # return to_json(ce)
    return {"message": "OK"}

    return '', 204
    # return "",204
    

@app.post("/status/update", status_code=status.HTTP_202_ACCEPTED)
async def status_update(request: Request):

    # attributes = {
    #     # "type": "envds.controller.control.request",
    #     "type": "message.ack",
    #     "source": "datastore",
    #     "id": str(ULID()),
    #     "datacontenttype": "application/json; charset=utf-8",
    # }
    # response = {"message": "ok"}
    try:
        # ce = from_json(await request.json())
        # print(ce)
        L.debug(await request.body())
        # pass
        # print("sensor_settings_update")
        ce = from_http(request.headers, await request.body())
        # print(ce)
        # L.debug(request.headers,)
        L.debug("status_update", extra={"ce": ce})#, "destpath": ce["destpath"]})
        # await adapter.send_to_mqtt(ce)
        # await datastore.data_sensor_update(ce)
    except Exception as e:
        # print(e)
        L.error("status_update", extra={"reason": e})
        pass
    # return "ok", 200
    # ce = CloudEvent(attributes=attributes, data=response)
    # return to_json(ce)
    return {"message": "OK"}

# @app.post("/settings/update")
# async def settings_update(ce: CloudEvent):

#     # examine and route cloudevent to the proper handler
#     return 200

# @app.post("/status/update")
# async def status_update(ce: CloudEvent):

#     # examine and route cloudevent to the proper handler
#     return 200

# @app.post("/event/update")
# async def status_update(ce: CloudEvent):

#     # examine and route cloudevent to the proper handler
#     return 200

@app.post("/device/registry/update", status_code=status.HTTP_202_ACCEPTED)
async def device_registry_update(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        L.debug(request.headers)
        L.debug("sensor_data_update", extra={"ce": ce, "destpath": ce["destpath"]})
        # await adapter.send_to_mqtt(ce)
        await datastore.device_registry_update(ce)
    except Exception as e:
        # L.error("send", extra={"reason": e})
        pass

    return "",204
    
@app.get("/device/registry/get")
async def device_registry_get(query: Annotated[DeviceInstanceRequest, Query()], device_type: str="sensor", ):
#     sensor_id: str | None = None,
#     make: str | None = None,
#     model: str | None = None,
#     serial_number: str | None = None,
#     version: str | None = None,
#     start_time: str | None = None,
#     end_time: str | None = None
# ):

    # query_params = request.query_params
    # query = DataStoreQuery(
    #     sensor_id=sensor_id,
    #     make=make,
    #     model=model,
    #     serial_number=serial_number,
    #     version=version,
    #     start_time=start_time,
    #     end_time=end_time
    # )
    result = await datastore.registry_device_get(query, device_type=device_type)
    return {"result": result}

@app.post("/device-definition/registry/update", status_code=status.HTTP_202_ACCEPTED)
async def device_definition_registry_update(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        L.debug(request.headers)
        L.debug("sensor_data_update", extra={"ce": ce, "destpath": ce["destpath"]})
        # await adapter.send_to_mqtt(ce)
        await datastore.device_definition_registry_update(ce)
    except Exception as e:
        # L.error("send", extra={"reason": e})
        pass

    return "",204
    
@app.get("/device/registry/get")
async def device_definition_registry_get(query: Annotated[DeviceDefinitionRequest, Query()], device_type: str="sensor", ):
#     sensor_id: str | None = None,
#     make: str | None = None,
#     model: str | None = None,
#     serial_number: str | None = None,
#     version: str | None = None,
#     start_time: str | None = None,
#     end_time: str | None = None
# ):

    # query_params = request.query_params
    # query = DataStoreQuery(
    #     sensor_id=sensor_id,
    #     make=make,
    #     model=model,
    #     serial_number=serial_number,
    #     version=version,
    #     start_time=start_time,
    #     end_time=end_time
    # )
    result = await datastore.device_definition_registry_get(query, device_type=device_type)
    return {"result": result}
