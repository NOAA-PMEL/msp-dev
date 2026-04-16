# import asyncio
from datetime import datetime, timezone
# import json
import logging

from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.responses import StreamingResponse
from starlette.background import BackgroundTask
from fastapi.middleware.cors import CORSMiddleware

# from cloudevents.http import from_http
from cloudevents.http import CloudEvent, from_http, from_json
from cloudevents.conversion import to_structured, to_json  # , from_http
from cloudevents.exceptions import InvalidStructuredJSON
from cloudevents.pydantic import CloudEvent

import httpx
from logfmter import Logfmter
from typing import Annotated, List
from pydantic import BaseSettings
from ulid import ULID


from erddap_util import ERDDAPUtil

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
        env_prefix = "ERDDAP_"
        case_sensitive = False


# from apis.router import api_router

app = FastAPI()

ERDDAP_SERVICE_URL = "http://127.0.0.1:8080/erddap"
TRIGGER_FILE_PATH = "/shared-state/trigger_datasets"
client = httpx.AsyncClient()

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


util = ERDDAPUtil()

@app.on_event("shutdown")
async def shutdown_event():
    await client.aclose()

@app.get("/")
async def root():
    return {"message": "Hello World from ERDDAPUtil"}

# --- 1. The Trigger Endpoint ---
@app.post("/reload-datasets/")
async def reload_datasets():
    try:
        # # Create an empty file to signal the ERDDAP container
        # with open(TRIGGER_FILE_PATH, "w") as f:
        #     f.write("run")
        
        return {"status": "accepted", "message": "Signal sent to ERDDAP container to run datasets.d.sh"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- 2. ERDDAP Proxy (Standard) ---
@app.api_route("/erddap/{path_name:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def proxy_erddap(request: Request, path_name: str):
    
    L.debug("proxy_erddap", extra={"req": request, "pn": path_name})
    # 1. Build the target URL for the internal Tomcat server
    #    If path_name is empty (e.g. just /erddap/), handle gracefully
    target_path = path_name if path_name else "index.html"
    url = f"{ERDDAP_SERVICE_URL}/{target_path}"
    
    # 2. Prepare Headers
    #    We convert the immutable Headers object to a mutable dict
    req_headers = dict(request.headers)
    
    # --- CRITICAL SECTION: HEADER MANIPULATION ---
    
    # A. Remove 'host' header. 
    #    If we leave it, Tomcat sees the external hostname (e.g. domain.com)
    #    but might reject it if configured strictly. It's safer to let httpx 
    #    set the Host header to '127.0.0.1:8080' automatically.
    req_headers.pop("host", None)
    
    # B. Set X-Forwarded-Prefix
    #    This tells ERDDAP: "I am actually living at /msp/dataserver"
    req_headers["X-Forwarded-Prefix"] = "/msp/data"
    
    # C. (Optional) Ensure Proto/Port are correct if your Ingress doesn't set them
    # req_headers["X-Forwarded-Proto"] = "https" 
    
    # ---------------------------------------------

    # 3. Build the Proxy Request
    rp_req = client.build_request(
        request.method,
        url,
        headers=req_headers,
        content=await request.body(),
        params=request.query_params # Forward query parameters (e.g. ?searchFor=...)
    )
    L.debug("proxy_erddap", extra={"req": rp_req, "h": req_headers})
    # 4. Send Request to Tomcat
    try:
        rp_resp = await client.send(rp_req, stream=True)
    except httpx.ConnectError:
        return Response("ERDDAP container is not ready or reachable.", status_code=503)

    # 5. Stream the Response back to the client
    return StreamingResponse(
        rp_resp.aiter_raw(),
        status_code=rp_resp.status_code,
        headers=rp_resp.headers,
        background=BackgroundTask(rp_resp.aclose),
    )# # @app.post("/device/data/save/", status_code=status.HTTP_202_ACCEPTED)
    L.debug("proxy_erddap", extra={"resp": rp_resp})
# @app.post("/data/save/")
# async def data_save(request: Request):
#     try:
#         ce = from_http(request.headers, await request.body())
#         L.debug(
#             "data_save", extra={"ce": ce}
#         )  # , "destpath": ce["destpath"]})
#         # await adapter.send_to_mqtt(ce)
#         # await datastore.data_sensor_update(ce)
#         # await filemanager.data_save(ce, data_type="device")
#         await filemanager.save(ce)
#         return Response(status_code=status.HTTP_204_NO_CONTENT)
#         # msg = {"result": "OK"}
#         # return get_response_event(msg, 202)

#     except Exception as e:
#         # print(e)
#         L.error("send", extra={"reason": e})
#         # return "", 204
#         return Response(status_code=status.HTTP_204_NO_CONTENT)
#         # msg = {"result": "NOTOK"}
#         # return get_response_event(msg, 500)

# @app.post("/logs/save/")
# async def logs_save(request: Request):
#     try:
#         ce = from_http(request.headers, await request.body())
#         L.debug(
#             "logs_save", extra={"ce": ce}
#         )  # , "destpath": ce["destpath"]})
#         # await adapter.send_to_mqtt(ce)
#         # await datastore.data_sensor_update(ce)
#         # await filemanager.data_save(ce, data_type="device")
#         await filemanager.save(ce)
#         return Response(status_code=status.HTTP_204_NO_CONTENT)
#         # msg = {"result": "OK"}
#         # return get_response_event(msg, 202)

#     except Exception as e:
#         # print(e)
#         L.error("send", extra={"reason": e})
#         # return "", 204
#         return Response(status_code=status.HTTP_204_NO_CONTENT)
#         # msg = {"result": "NOTOK"}
#         # return get_response_event(msg, 500)
