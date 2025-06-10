import asyncio
from datetime import datetime, timezone
import json
import logging

from fastapi import FastAPI, Request  # , APIRouter
from fastapi.middleware.cors import CORSMiddleware

# from cloudevents.http import from_http
from cloudevents.http import CloudEvent, from_http, from_json
from cloudevents.conversion import to_structured # , from_http
from cloudevents.exceptions import InvalidStructuredJSON
from cloudevents.pydantic import CloudEvent

import httpx
from logfmter import Logfmter
from pydantic import BaseModel, BaseSettings, Field

from datastore import Datastore

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.INFO)

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

origins = ["*"]  # dev
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
    
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

@app.post("/data/update")
async def data_update(request: Request):

    # examine and route cloudevent to the proper handler
    return 200

@app.post("/settings/update")
async def settings_update(ce: CloudEvent):

    # examine and route cloudevent to the proper handler
    return 200

@app.post("/status/update")
async def status_update(ce: CloudEvent):

    # examine and route cloudevent to the proper handler
    return 200

@app.post("/event/update")
async def status_update(ce: CloudEvent):

    # examine and route cloudevent to the proper handler
    return 200
