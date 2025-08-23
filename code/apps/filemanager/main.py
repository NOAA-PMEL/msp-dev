# import asyncio
from datetime import datetime, timezone
# import json
import logging

from fastapi import FastAPI, Request, Query, status, Response  # , APIRouter
# from fastapi.middleware.cors import CORSMiddleware

# from cloudevents.http import from_http
from cloudevents.http import from_http
# from cloudevents.conversion import to_structured, to_json  # , from_http
# from cloudevents.exceptions import InvalidStructuredJSON
# from cloudevents.pydantic import CloudEvent

import httpx
from logfmter import Logfmter
from typing import Annotated, List
from pydantic import BaseSettings


from filemanager import Filemanager

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

    class Config:
        env_prefix = "FILEMANAGER_"
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
# def get_response_event(msg, status):
#     # response_data = {"processed_data": event.data}
#     try:
#         # Construct the response CloudEvent
#         response_event = CloudEvent({
#             "source": "envds.datastore",
#             "type": "envds.response.event",
#             "specversion": "1.0",
#             "datacontenttype": "application/json"
#         }, msg)

#         # Return the CloudEvent as a structured HTTP response
#         headers, body = to_structured(response_event)
#         # return jsonify(body), 200, headers
#         return body, status, headers # fastapi converts json
#     except Exception as e:
#         L.error("get_response_event", extra={"reason": e})
#         return {}, 500, ""


filemanager = Filemanager()

@app.get("/")
async def root():
    return {"message": "Hello World from Datastore"}

# @app.post("/device/data/save/", status_code=status.HTTP_202_ACCEPTED)
@app.post("/device/data/save/")
async def device_data_save(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        L.debug(
            "data_save", extra={"ce": ce}
        )  # , "destpath": ce["destpath"]})
        # await adapter.send_to_mqtt(ce)
        # await datastore.data_sensor_update(ce)
        await filemanager.data_save(ce, data_type="device")
        return Response(status_code=status.HTTP_204_NO_CONTENT)
        # msg = {"result": "OK"}
        # return get_response_event(msg, 202)

    except Exception as e:
        # print(e)
        L.error("send", extra={"reason": e})
        # return "", 204
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
        # msg = {"result": "NOTOK"}
        # return get_response_event(msg, 500)

# @app.post("/controller/data/save/", status_code=status.HTTP_202_ACCEPTED)
@app.post("/controller/data/save/")
async def controller_data_save(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        L.debug(
            "data_save", extra={"ce": ce}
        )  # , "destpath": ce["destpath"]})
        # await adapter.send_to_mqtt(ce)
        # await datastore.data_sensor_update(ce)
        await filemanager.data_save(ce, data_type="controller")
        return Response(status_code=status.HTTP_204_NO_CONTENT)
        # msg = {"result": "OK"}
        # return get_response_event(msg, 202)
    except Exception as e:
        # print(e)
        L.error("send", extra={"reason": e})
        # return "", 204
        # return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
        return Response(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
        # msg = {"result": "NOTOK"}
        # return get_response_event(msg, 500)
