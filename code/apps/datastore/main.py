import asyncio
from datetime import datetime, timezone
import json
import logging

from fastapi import FastAPI, Request, Query, status, Response
from fastapi.middleware.cors import CORSMiddleware

from cloudevents.http import CloudEvent, from_http, from_json
from cloudevents.conversion import to_structured, to_json
from cloudevents.exceptions import InvalidStructuredJSON

import httpx
from logfmter import Logfmter
from typing import Annotated, List, Any
from pydantic import BaseModel, BaseSettings, Field
from ulid import ULID
import traceback

from datastore import Datastore
from datastore_requests import (
    DataRequest,
    DeviceDefinitionRequest,
    DeviceInstanceRequest,
    ControllerDataRequest,
    ControllerDefinitionRequest,
    ControllerInstanceRequest,
    VariableSetDataRequest,
    VariableSetDefinitionRequest,
    VariableMapDefinitionRequest,
    VariableSetInstanceRequest,
)

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.DEBUG)


class Settings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8000
    debug: bool = False
    dry_run: bool = False

    class Config:
        env_prefix = "DATASTORE_"
        case_sensitive = False


app = FastAPI()

datastore = None

@app.on_event("startup")
async def start_system():
    global datastore
    datastore = Datastore()
    await datastore.setup()
    L.info("Datastore initialized and background tasks started.")

@app.on_event("shutdown")
async def shutdown_system():
    global datastore
    if datastore:
        await datastore.close_http_client()
        L.info("Datastore HTTP client closed safely.")
        
def get_response_event(msg, status):
    try:
        response_event = CloudEvent({
            "source": "envds.datastore",
            "type": "envds.response.event",
            "specversion": "1.0",
            "datacontenttype": "application/json"
        }, msg)

        headers, body = to_structured(response_event)
        return body, status, headers
    except Exception as e:
        L.error("get_response_event", extra={"reason": e})
        return {}, 500, ""


@app.get("/")
async def root():
    return {"message": "Hello World from Datastore"}


@app.post("/device/settings/update/")
async def device_settings_update(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        L.debug("device_settings_update", extra={"ce": ce})
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as e:
        L.error("send", extra={"reason": e})
        return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post("/status/update/")
async def status_update(request: Request):
    try:
        L.debug(await request.body())
        ce = from_http(request.headers, await request.body())
        L.debug("status_update", extra={"ce": ce}) 
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as e:
        L.error("status_update", extra={"reason": e})
        return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post("/device/data/update/")
async def device_data_update(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        L.debug(request.headers)
        L.debug("device_data_update", extra={"ce": ce, "destpath": ce["destpath"]})
        await datastore.device_data_update(ce)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as e:
        L.error("device_data_update", extra={"reason": e})
        return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/device/data/get/")
async def device_data_get(
    device_id: str | None = None,
    make: str | None = None,
    model: str | None = None,
    serial_number: str | None = None,
    version: str | None = None,
    device_type: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    last_n_seconds: int | None = None,
    variable: List[str] | None = None,
):
    L.debug("main:device_data_get", extra={"device_id": device_id})
    query = DataRequest(
        device_id=device_id,
        make=make,
        model=model,
        serial_number=serial_number,
        version=version,
        device_type=device_type,
        start_time=start_time,
        end_time=end_time,
        last_n_seconds=last_n_seconds,
        variable=variable,
    )
    L.debug("main:device_data_get", extra={"query": query})
    return await datastore.device_data_get(query)


@app.post("/device-instance/registry/update/")
async def device_instance_registry_update(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        L.debug(request.headers)
        L.debug("device_instance_registry_update", extra={"ce": ce, "destpath": ce["destpath"]})
        await datastore.device_instance_registry_update(ce)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as e:
        return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post("/device-definition/registry/update/")
async def device_definition_registry_update(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        L.debug(request.headers)
        L.debug(
            "device_definition_registry_update",
            extra={"ce": ce, "destpath": ce["destpath"]},
        )
        await datastore.device_definition_registry_update(ce)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as e:
        return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/device-definition/registry/ids/get/")
async def device_definition_get_ids():
    L.debug("device_definition_get_ids")
    result = await datastore.device_definition_registry_get_ids()
    L.debug("device_definition_get_ids", extra={"result": result})
    return result


@app.get("/device-definition/registry/get/")
async def device_definition_registry_get(
    device_definition_id: str | None = None,
    make: str | None = None,
    model: str | None = None,
    version: str | None = None,
    device_type: str | None = None,
    valid_time: str | None = None,
):
    query = DeviceDefinitionRequest(
        device_definition_id=device_definition_id,
        make=make,
        model=model,
        version=version,
        device_type=device_type,
        valid_time=valid_time,
    )
    return await datastore.device_definition_registry_get(query)


@app.get("/device-instance/registry/ids/get/")
async def device_instance_get_ids():
    L.debug("device_instance_get_ids")
    result = await datastore.device_instance_registry_get_ids()
    L.debug("device_instance_get_ids", extra={"result": result})
    return result


@app.get("/device-instance/registry/get/")
async def device_instance_registry_get(
    device_id: str | None = None,
    make: str | None = None,
    model: str | None = None,
    serial_number: str | None = None,
    version: str | None = None,
    device_type: str | None = None,
):
    query = DeviceInstanceRequest(
        device_id=device_id,
        make=make,
        model=model,
        serial_number=serial_number,
        version=version,
        device_type=device_type,
    )
    L.debug("device_instance_registry_get", extra={"query": query})
    results = await datastore.device_instance_registry_get(query)
    L.debug("device_instance_registry_get", extra={"results": results})
    return results


@app.post("/controller/data/update/")
async def controller_data_update(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        L.debug(request.headers)
        L.debug("controller_data_update", extra={"ce": ce, "destpath": ce["destpath"]})
        await datastore.controller_data_update(ce)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as e:
        L.error("controller_data_update", extra={"reason": e})
        return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/controller/data/get/")
async def controller_data_get(
    controller_id: str | None = None,
    make: str | None = None,
    model: str | None = None,
    serial_number: str | None = None,
    version: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    last_n_seconds: int | None = None,
    variable: List[str] | None = None,
):
    L.debug("main:controller_data_get", extra={"controller_id": controller_id})
    query = ControllerDataRequest(
        controller_id=controller_id,
        make=make,
        model=model,
        serial_number=serial_number,
        version=version,
        start_time=start_time,
        end_time=end_time,
        last_n_seconds=last_n_seconds,
        variable=variable,
    )
    L.debug("main:controller_data_get", extra={"query": query})
    return await datastore.controller_data_get(query)


@app.post("/controller-instance/registry/update/")
async def controller_instance_registry_update(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        L.debug(request.headers)
        L.debug(
            "controller_instance_registry_update", extra={"ce": ce, "destpath": ce["destpath"]}
        )
        await datastore.controller_instance_registry_update(ce)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as e:
        L.error("controller_instance_registry_update", extra={"reason": e})
        print(traceback.format_exc())
        return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post("/controller-definition/registry/update/")
async def controller_definition_registry_update(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        L.debug(request.headers)
        L.debug(
            "controller_definition_registry_update",
            extra={"ce": ce, "destpath": ce["destpath"]},
        )
        await datastore.controller_definition_registry_update(ce)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as e:
        L.error("datastore_register_controller_definition", extra={"reason": e})
        print(traceback.format_exc())
        return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/controller-definition/registry/ids/get/")
async def controller_definition_get_ids():
    L.debug("controller_definition_get_ids")
    result = await datastore.controller_definition_registry_get_ids()
    L.debug("controller_definition_get_ids", extra={"result": result})
    return result


@app.get("/controller-definition/registry/get/")
async def controller_definition_registry_get(
    controller_definition_id: str | None = None,
    make: str | None = None,
    model: str | None = None,
    version: str | None = None,
    valid_time: str | None = None,
):
    query = ControllerDefinitionRequest(
        controller_definition_id=controller_definition_id,
        make=make,
        model=model,
        version=version,
        valid_time=valid_time,
    )
    return await datastore.controller_definition_registry_get(query)
    

@app.get("/controller-instance/registry/ids/get/")
async def controller_instance_get_ids():
    L.debug("controller_instance_get_ids")
    result = await datastore.controller_instance_registry_get_ids()
    L.debug("controller_instance_get_ids", extra={"result": result})
    return result


@app.get("/controller-instance/registry/get/")
async def controller_instance_registry_get(
    controller_id: str | None = None,
    make: str | None = None,
    model: str | None = None,
    serial_number: str | None = None,
    version: str | None = None,
):
    query = ControllerInstanceRequest(
        controller_id=controller_id,
        make=make,
        model=model,
        serial_number=serial_number,
        version=version,
    )
    L.debug("controller_instance_registry_get", extra={"query": query})
    results = await datastore.controller_instance_registry_get(query)
    L.debug("controller_instance_registry_get", extra={"results": results})
    return results


@app.post("/variableset/data/update/")
async def variableset_data_update(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        L.debug(request.headers)
        L.debug("variableset_data_update", extra={"ce": ce, "destpath": ce["destpath"]})
        await datastore.variableset_data_update(ce)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as e:
        L.error("variableset_data_update", extra={"reason": e})
        return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/variableset/data/get/")
async def variableset_data_get(
    variableset_id: str | None = None,
    variablemap_definition_id: str | None = None,
    variableset: str | None = None,
    start_time: str | None = None,
    end_time: str | None = None,
    start_timestamp: float | None = None,
    end_timestamp: float | None = None,
    last_n_seconds: int | None = None,
    variable: List[str] | None = None
):
    L.debug("main:variableset_data_get", extra={"device_id": variableset_id})
    query = VariableSetDataRequest(
        variableset_id=variableset_id,
        variablemap_definition_id=variablemap_definition_id,
        variableset=variableset,
        start_time=start_time,
        end_time=end_time,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
        last_n_seconds=last_n_seconds,
        variable=variable,
    )
    L.debug("main:variableset_data_get", extra={"query": query})
    return await datastore.variableset_data_get(query)


@app.post("/variableset-definition/registry/update/")
async def variableset_definition_registry_update(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        L.debug(request.headers)
        L.debug(
            "variableset_definition_registry_update",
            extra={"ce": ce, "destpath": ce["destpath"]},
        )
        await datastore.variableset_definition_registry_update(ce)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as e:
        return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/variableset-definition/registry/get/")
async def variableset_definition_registry_get(
    variableset_definition_id: str | None = None,
    variablemap_definition_id: str | None = None,
    variableset: str | None = None,
    index_type: str | None = None,
    index_value: str | None = None
):
    query = VariableSetDefinitionRequest(
        variableset_definition_id=variableset_definition_id,
        variablemap_definition_id=variablemap_definition_id,
        variableset=variableset,
        index_type=index_type,
        index_value=index_value
    )
    return await datastore.variableset_definition_registry_get(query)


@app.post("/variablemap-definition/registry/update/")
async def variablemap_definition_registry_update(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        L.debug(request.headers)
        L.debug(
            "variablemap_definition_registry_update",
            extra={"ce": ce, "destpath": ce["destpath"]},
        )
        await datastore.variablemap_definition_registry_update(ce)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as e:
        return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/variablemap-definition/registry/get/")
async def variablemap_definition_registry_get(
    variablemap_definition_id: str | None = None,
    variablemap_type: str | None = None,
    variablemap_type_id: str | None = None,
    variablemap: str | None = None,
    valid_config_time: str | None = None,
):
    query = VariableMapDefinitionRequest(
        variablemap_definition_id=variablemap_definition_id,
        variablemap_type=variablemap_type,
        variablemap_type_id=variablemap_type_id,
        variablemap=variablemap,
        valid_config_time=valid_config_time,
    )
    return await datastore.variablemap_definition_registry_get(query)


@app.get("/variableset-definition/registry/ids/get/")
async def variableset_definition_get_ids():
    return await datastore.variableset_definition_registry_get_ids()


@app.get("/variablemap-definition/registry/ids/get/")
async def variablemap_definition_get_ids():
    return await datastore.variablemap_definition_registry_get_ids()

@app.get("/variableset-instance/registry/ids/get/")
async def variableset_instance_get_ids():
    L.debug("variableset_instance_get_ids")
    result = await datastore.variableset_instance_registry_get_ids()
    L.debug("variableset_instance_get_ids", extra={"result": result})
    return result


@app.get("/variableset-instance/registry/get/")
async def variableset_instance_registry_get(
    variableset_id: str | None = None,
    variablemap_id: str | None = None,
    variableset: str | None = None,
):
    query = VariableSetInstanceRequest(
        variableset_id=variableset_id,
        variablemap_id=variablemap_id,
        variableset=variableset
    )
    L.debug("variableset_instance_registry_get", extra={"query": query})
    results = await datastore.variableset_instance_registry_get(query)
    L.debug("variableset_instance_registry_get", extra={"results": results})
    return results

SAMPLING_RESOURCE_TYPES = [
    "platform", "project", "systemmode", "samplingmode", "samplingstate", "samplingcondition", "action"
]

def create_sampling_routes(resource: str):
    @app.get(f"/{resource}-definition/registry/ids/get/")
    async def get_ids():
        return await datastore.sampling_definition_registry_get_ids(resource)

    @app.get(f"/{resource}-definition/registry/get/")
    async def get_def(name: str | None = None):
        query = {"name": name}
        return await datastore.sampling_definition_registry_get(resource, query)

    @app.post(f"/{resource}-definition/registry/update/")
    async def update_def(request: Request):
        try:
            ce = from_http(request.headers, await request.body())
            await datastore.sampling_definition_registry_update(resource, ce)
            return Response(status_code=status.HTTP_204_NO_CONTENT)
        except Exception:
            return Response(status_code=status.HTTP_204_NO_CONTENT)

# Initialize routes for each resource type
for res in SAMPLING_RESOURCE_TYPES:
    create_sampling_routes(res)

