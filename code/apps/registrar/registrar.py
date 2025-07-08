import asyncio
import json
import logging
import math
import sys
from time import sleep

# import numpy as np
from ulid import ULID
from pathlib import Path
import os

import httpx
from logfmter import Logfmter

# from registry import registry
import uvicorn

# from flask import Flask, request
from pydantic import BaseSettings
from cloudevents.http import CloudEvent, from_http

# from cloudevents.http.conversion import from_http
from cloudevents.conversion import to_structured  # , from_http
from cloudevents.exceptions import InvalidStructuredJSON

from datetime import datetime, timezone

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.INFO)

# import pymongo
# from erddapy import ERDDAP

# class Settings(BaseSettings):
#     host: str = '0.0.0.0'
#     port: int = 8787
#     debug: bool = False
#     url: str = 'https://localhost:8444/erddap/tabledap'
#     author: str = 'super_secret_author'
#     dry_run: bool = False

#     class Config:
#         env_prefix = 'IOT_ERDDAP_INSERT_'
#         case_sensitive = False

# uasdaq_user_password = os.environ.get("REGISTRAR_MONGODB_USER_PW")
# L.info("pw", extra={"pw": uasdaq_user_password})


class Settings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8787
    debug: bool = False
    namespace_prefix: str | None = None
    knative_broker: str = (
        "http://kafka-broker-ingress.knative-eventing.svc.cluster.local/default/default"
    )

    class Config:
        env_prefix = "REGISTRAR_"
        case_sensitive = False


# app = Flask(__name__)
config = Settings()

class Registrar():
    """docstring for Registrar."""
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug("TestClass instantiated")
        self.logger.setLevel(logging.DEBUG)
        self.config = Settings()

        self.datastore_url = f"datastore.{self.config.namespace_prefix}-system"

        self.task_list = []
        self.task_list.append(self.get_device_definitions_loop())
        self.task_list.append(self.get_device_instances_loop())
        for task in self.task_list:
            asyncio.create_task(task)

    async def submit_request(self, database: str, collection: str, query: dict):
        results = httpx.get(f"http://{self.datastore_url}/{collection}/{database}/get/", parmams=query)
        return results

    async def get_device_definitions_loop(self):

        while True:
            query = {}
            results = await self.submit_request(database="registry", collection="device-definition", query=query)
            # results = httpx.get(f"http://{self.datastore_url}/device-definition/registry/get/", parmams=query)
            print(f"results: {results}")

            await asyncio.sleep(5)
        pass
    
    async def get_device_instance(self, devices: list[str]) -> list:
        pass        

    async def get_device_instances_loop(self):
        while True:

            await asyncio.sleep(5)

# def build_sensor_registry_document(sensor_def: dict):
#     L.debug("build_sensor_registry_document", extra={"sd": sensor_def})
#     try:
#         make = sensor_def["attributes"]["make"]["data"]
#         model = sensor_def["attributes"]["model"]["data"]
#         format_version = sensor_def["attributes"]["format_version"]["data"]
#         parts = format_version.split(".")
#         version = f"v{parts[0]}"

#     except KeyError as e:
#         L.error(
#             "register sensor: Invalid sensor_config", extra={"e": e, "metadata": sensor_def}
#         )
#         # TODO throw exception
#         return None
#         # return "invalid configuration data"

#     attributes = sensor_def.get("attributes", {})
#     dimensions = sensor_def.get("dimensions", {"time": None})
#     variables = sensor_def.get("variables", {})
#     id = f"{make}::{model}::{version}"

#     doc = {
#         "_id": id,
#         "make": make,
#         "model": model,
#         "version": version,
#         "attributes": sensor_def["attributes"],
#         "dimensions": dimensions,
#         "variables": variables,
#         # "last_update": datetime.now(tz=timezone.utc)
#     }

#     return doc


# def build_doc_by_path(doc: dict, path_list: list, data):
#     # subdoc = {}
#     if len(path_list) > 1:
#         if path_list[0] not in doc:
#             doc[path_list[0]] = dict()
#         build_doc_by_path(doc[path_list[0]], path_list[1:], data)
#     else:
#         try:
#             if isinstance(data, str) and "_uasdaq_registry_list___" in data:
#                 # d = json.loads(data.replace("_uasdaq_registry_list___", "[").replace("___","]").replace(";;", ","))
#                 d = json.loads(json.dumps(data)).split("___")[1].split(";;")
#             else:
#                 d = json.loads(json.dumps(data).replace(";;", ","))
#             if isinstance(d, float) and math.isnan(d):
#                 d = None
#             doc[path_list[0]] = d
#         except json.JSONDecodeError as e:
#             L.error(
#                 "build doc error",
#                 extra={"path": path_list[0], "d": data, "dt": type(data), "e": e},
#             )
#     # L.info("doc", extra={"doc": doc})
#     return doc


# def erddap_sensor_definition_to_document(df) -> dict:
#     if df.empty:
#         return None

#     doc = {}
#     df.sort_values(by=["make", "model", "version", "path_index"])
#     path = df["path"].to_list()
#     data = df["path_data"].to_list()

#     for p, d in zip(path, data):
#         parts = p.split("::")
#         doc = build_doc_by_path(doc, parts, d)

#     L.info("s2d", extra={"dimtype": type(doc["dimensions"]["time"]), "document": doc})
#     return build_sensor_registry_document(doc)


# def update_sensor_definition_db(sensor_definition_id: str) -> bool:

#     parts = sensor_definition_id.split("::")
#     if not parts or len(parts) != 3:
#         return False

#     query = {"_id": sensor_definition_id}
#     sensor = db_client.find_one("registry", "sensor_definition", query=query)
#     L.info("sensor: {sensor}")
#     if sensor:
#         return True

#     try:
#         dataset_id = "registry_sensor_definition"
#         constraints = {
#             "make=": parts[0],
#             "model=": parts[1],
#             "version=": parts[2],
#         }
#         reg = erddap_client.to_pandas(dataset_id=dataset_id, constraints=constraints)
#         L.info("reg", extra={"reg": reg})

#         if reg is not None and reg.size > 0:

#             try:
#                 doc = erddap_sensor_definition_to_document(reg)
#                 # L.info("erddap == sensor_def", extra={"equals": (sensor_def==doc)})
#                 if doc:

#                     filter = {
#                         "_id": doc["_id"]
#                         # "make": make,
#                         # "model": model,
#                         # "version": version,
#                         # "serial_number": serial_number,
#                         # # "timestamp": timestamp,
#                     }

#                     update = {"last_update": datetime.now(tz=timezone.utc)}

#                     result = db_client.update_one(
#                         database="registry",
#                         collection="sensor_definition",
#                         filter=filter,
#                         update=update,
#                         document=doc,
#                         upsert=True,
#                     )

#                     # result = db_client.insert_one("registry", "sensor_definition", doc)
#                     L.info("sensor_def reg insert", extra={"result": result})

#             except json.JSONDecodeError:
#                 L.error(
#                     "sensor definition decode error",
#                     extra={"definition": reg.definition[0]},
#                 )
#                 return False

#             return True

#         else:
#             return False

#     except httpx.HTTPError:
#         return False

# def update_sensor_definition_db_all():

#     dataset_id = "registry_sensor_definition"
#     variables = ["make", "model", "version"]
#     distinct = True
#     df = erddap_client.to_pandas(dataset_id=dataset_id, variables=variables, distinct=distinct)
#     makes = df.make.where(df.make != "make").dropna().to_list()
#     models = df.model.where(df.make != "make").dropna().to_list()
#     versions = df.version.where(df.make != "make").dropna().to_list()

#     if df is not None and df.size > 0:
#         for make, model, version in zip(makes, models, versions):
#             id = f"{make}::{model}::{version}"
#             L.info(f"update {id}")
#             update_sensor_definition_db(sensor_definition_id=id)


# # def sensor_definition_registered(make: str, model: str, version: str) -> bool:
# def sensor_definition_registered(sensor_def) -> bool:

#     # meta = ce.data["sensor-definition"]
#     try:
#         make = sensor_def["attributes"]["make"]["data"]
#         model = sensor_def["attributes"]["model"]["data"]
#         format_version = sensor_def["attributes"]["format_version"]["data"]
#         parts = format_version.split(".")
#         version = f"v{parts[0]}"

#     except KeyError:
#         L.error(
#             "register sensor: Invalid sensor_config", extra={"metadata": sensor_def}
#         )
#         # TODO throw exception
#         return False
#         # return "invalid configuration data"

#     # check mongodb
#     # db_client = clients.get_db(config.mongodb_connection)
#     # if not db:
#     #     # TODO throw exception
#     #     return False

#     # erddap_client = clients.get_erddap(config.erddap_connection)
#     # if not erddap:
#     #     return False

#     # db = db_client.registry
#     # sensor_defs = db.sensor_definition
#     query = {"make": make, "model": model, "version": version}
#     sensor = db_client.find_one("registry", "sensor_definition", query=query)
#     # sensor = sensor_defs.find_one({"make": make, "model": model, "version": version})

#     # if not in db, check erddap
#     if sensor:
#         # query = {"make": make, "model": model, "version": version},
#         # update =  {"last_update": datetime.now(tz=timezone.utc)}
#         # result = db_client.update_one("registry", "sensor_definition", sensor, update)
#         # # result = sensor_defs.update_one({"make": make, "model": model, "version": version}, {"$set": {"last_update": datetime.now(tz=timezone.utc)}}, upsert=False)
#         # L.info("sensor_def reg update", extra={"result": result})
#         return True
#     else:
#         sensor_definition_id = "::".join([make, model, version])
#         return update_sensor_definition_db(sensor_definition_id)
#         # try:
#         #     dataset_id = "registry_sensor_definition"
#         #     constraints = {
#         #         "make=": make,
#         #         "model=": model,
#         #         "version=": version,
#         #     }
#         #     reg = erddap_client.to_pandas(dataset_id=dataset_id, constraints=constraints)
#         #     L.info("reg", extra={"reg": reg})

#         #     # return True # testing

#         #     if reg is not None and reg.size > 0:

#         #         try:
#         #             # doc = build_sensor_registry_document(erddap_sensor_def)
#         #             # doc = build_sensor_registry_document(sensor_def)
#         #             doc = erddap_sensor_definition_to_document(reg)
#         #             # L.info("erddap == sensor_def", extra={"equals": (sensor_def==doc)})
#         #             if doc:
#         #                 result = db_client.insert_one("registry", "sensor_definition", doc)
#         #                 L.info("sensor_def reg insert", extra={"result": result})

#         #         except json.JSONDecodeError:
#         #             L.error("sensor definition decode error", extra={"definition": reg.definition[0]})
#         #             return False

#         #         return True

#         #     else:
#         #         return False

#         # except httpx.HTTPError:
#         #     return False


# def get_path_record(path_data: list, path: str, data):
#     try:
#         for n, v in data.items():
#             record = get_path_record(path_data, "::".join([path, n]), v)
#     except (AttributeError, TypeError) as e:
#         if isinstance(data, list):
#             data = f'_uasdaq_registry_list___{";;".join(data)}___'
#             # list_data = "_uasdaq_registry_list___"
#             # try:
#             #     list_data +=
#             # for val in data:

#             # data = f'_uasdaq_registry_list___time, diameter___'
#             # data = f'_uasdaq_registry_list___{json.dumps(data).replace("[", "").replace("]", "")}___'
#             L.info("registry_list", extra={"rl": data})
#         path_data.append({"path": path, "data": json.dumps(data).replace(",", ";;")})
#     return path_data


# def build_erddap_record(definition: dict) -> list:

#     # should be proper ncoJSON
#     if "attributes" not in definition or "variables" not in definition:
#         L.error("unknown format of definition", extra={"definition": definition})
#         return None

#     try:
#         make = definition["attributes"]["make"]["data"]
#         model = definition["attributes"]["model"]["data"]
#         format_version = definition["attributes"]["format_version"]["data"]
#         parts = format_version.split(".")
#         version = f"v{parts[0]}"

#     except KeyError:
#         L.error("Invalid definition", extra={"definition": definition})
#         # TODO throw exception
#         return None

#     isofmt = "%Y-%m-%dT%H:%M:%S.%fZ"
#     ts = datetime.utcnow().strftime(isofmt)
#     L.info("erddap time", extra={"erddap_time": ts})
#     record = []
#     index = 0

#     path_data = []
#     path_data = get_path_record(path_data, "attributes", definition["attributes"])

#     if "dimensions" not in definition:
#         definition["dimensions"] = {"time": None}
#     path_data = get_path_record(path_data, "dimensions", definition["dimensions"])

#     path_data = get_path_record(path_data, "variables", definition["variables"])
#     # L.info("path_data", extra={"path_data": path_data})
#     for d in path_data:
#         record.append(
#             {
#                 "make": make,
#                 "model": model,
#                 "version": version,
#                 "path_index": index,
#                 "path": d["path"],
#                 "path_data": d["data"],
#                 "time": ts,
#                 "author": config.erddap_author,
#             }
#         )
#         index += 1

#     return record


# def register_sensor_definition(sensor_def: dict):

#     try:
#         make = sensor_def["attributes"]["make"]["data"]
#         model = sensor_def["attributes"]["model"]["data"]
#         format_version = sensor_def["attributes"]["format_version"]["data"]
#         parts = format_version.split(".")
#         version = f"v{parts[0]}"

#     except KeyError:
#         L.error(
#             "register sensor: Invalid sensor_config", extra={"metadata": sensor_def}
#         )
#         # TODO throw exception
#         return "bad sensor defintion"

#     # save to erddap
#     dataset_id = "registry_sensor_definition"
#     # url = f"{config.erddap_connection}/registry_sensor_definition.insert"

#     record = build_erddap_record(sensor_def)
#     # return ("testing")
#     if record:
#         for params in record:
#             erddap_client.http_insert(dataset_id, params=params)
#             sleep(0.1)

#         doc = build_sensor_registry_document(sensor_def)
#         if doc:

#             filter = {"_id": doc["_id"]}

#             update = {"last_update": datetime.now(tz=timezone.utc)}

#             result = db_client.update_one(
#                 database="registry",
#                 collection="sensor_definition",
#                 filter=filter,
#                 update=update,
#                 document=doc,
#                 upsert=True,
#             )

#             # result = db_client.insert_one("registry", "sensor_definition", doc)
#             # result = sensor_defs.insert_one(doc)
#             L.info("sensor_def reg insert", extra={"result": result})

#         # except Exception as e:
#         #     L.error("sensor definition register error", extra={"error": e})
#         #     return str(e)
#         return "ok"
#     return "bad sensor_def"


# def register_sensor_instance(sensor_instance: dict):

#     try:
#         make = sensor_instance["attributes"]["make"]["data"]
#         model = sensor_instance["attributes"]["model"]["data"]
#         serial_number = sensor_instance["attributes"]["serial_number"]["data"]
#         format_version = sensor_instance["attributes"]["format_version"]["data"]
#         parts = format_version.split(".")
#         version = f"v{parts[0]}"
#         attributes = sensor_instance["attributes"]
#         # timestamp = sensor_instance["timestamp"]
#         id = "::".join([make, model, serial_number])

#     except KeyError:
#         L.error(
#             "register sensor-instance: Invalid sensor",
#             extra={"metadata": sensor_instance},
#         )
#         # TODO throw exception
#         return "bad sensor instance", 400

#     try:
#         doc = {
#             # "_id": id,
#             "make": make,
#             "model": model,
#             "serial_number": serial_number,
#             "version": version,
#             # "timestamp": timestamp,
#             "attributes": attributes,
#             # "dimensions": dimensions,
#             # "variables": variables,
#             # "last_update": datetime.now(tz=timezone.utc),
#         }

#         filter = {
#             "make": make,
#             "model": model,
#             "version": version,
#             "serial_number": serial_number,
#             # "timestamp": timestamp,
#         }

#         update = {"last_update": datetime.now(tz=timezone.utc)}

#         result = db_client.update_one(
#             database="registry",
#             collection="sensor",
#             filter=filter,
#             update=update,
#             document=doc,
#             upsert=True,
#         )
#         L.info(
#             "sensor-handler instance update result",
#             extra={"result": result, "sensor-instance": doc},
#         )

#         return "ok", 200

#         # # save to erddap
#         # dataset_id = "registry_sensor_definition"
#         # # url = f"{config.erddap_connection}/registry_sensor_definition.insert"

#         # record = build_erddap_record(sensor_instance)
#         # # return ("testing")
#         # if record:
#         #     for params in record:
#         #         erddap_client.http_insert(dataset_id, params=params)
#         #         sleep(.1)

#         #     doc = build_sensor_registry_document(sensor_instance)
#         #     if doc:
#         #         result = db_client.insert_one("registry", "sensor_definition", doc)
#         #         # result = sensor_defs.insert_one(doc)
#         #         L.info("sensor_def reg insert", extra={"result": result})

#         #     # except Exception as e:
#         #     #     L.error("sensor definition register error", extra={"error": e})
#         #     #     return str(e)
#         #     return "ok"
#     except Exception as e:
#         return f"bad sensor-instance: {e}", 400


# # @app.route("/register/sensor/update", methods=["POST"])
# # def register_sensor():
# #     #     L.info("verify message")
# #     #     L.info("verify request", extra={"verify-request": request.data})
# #     try:
# #         ce = from_http(headers=request.headers, data=request.get_data())
# #         # to support local testing...
# #         if isinstance(ce.data, str):
# #             ce.data = json.loads(ce.data)
# #     except InvalidStructuredJSON:
# #         L.error("not a valid cloudevent")
# #         return "not a valid cloudevent", 400

# #     parts = Path(ce["source"]).parts
# #     L.info(
# #         "register sensor",
# #         extra={"ce-source": ce["source"], "ce-type": ce["type"], "ce-data": ce.data},
# #     )

# #     if "sensor-definition" in ce.data:

# #         sensor_def = ce.data["sensor-definition"]
# #         try:
# #             make = sensor_def["attributes"]["make"]["data"]
# #             model = sensor_def["attributes"]["model"]["data"]
# #             format_version = sensor_def["attributes"]["format_version"]["data"]
# #             parts = format_version.split(".")
# #             major_version = f"v{parts[0]}"

# #         except KeyError:
# #             L.error(
# #                 "register sensor: Invalid sensor_config", extra={"metadata": sensor_def}
# #             )
# #             return "invalid configuration data"

# #         # if not sensor_definition_registered(make, model, major_version):
# #         if not sensor_definition_registered(sensor_def):
# #             register_sensor_definition(sensor_def)

# #     elif "sensor-instance" in ce.data:

# #         sensor_instance = ce.data["sensor-instance"]
# #         try:
# #             make = sensor_instance["attributes"]["make"]["data"]
# #             model = sensor_instance["attributes"]["model"]["data"]
# #             format_version = sensor_instance["attributes"]["format_version"]["data"]
# #             serial_number = sensor_instance["attributes"]["serial_number"]["data"]
# #             # parts = format_version.split(".")
# #             # major_version = f"v{parts[0]}"

# #         except KeyError:
# #             L.error(
# #                 "register sensor - instance: Invalid sensor_data",
# #                 extra={"metadata": sensor_instance},
# #             )
# #             return "invalid sensor data", 400

# #         # if not sensor_definition_registered(make, model, major_version):
# #         # if not sensor_definition_registered(sensor_def):
# #         return register_sensor_instance(sensor_instance)

# #     return {"result": "ok"}


# # @app.route("/register/init", methods=["POST"])
# # def register_init():
# #     L.info("register init")
# #     L.info("init request", extra={"init-request": request.data})
# #     try:
# #         ce = from_http(headers=request.headers, data=request.get_data())
# #         # to support local testing...
# #         if isinstance(ce.data, str):
# #             ce.data = json.loads(ce.data)
# #     except InvalidStructuredJSON:
# #         L.error("not a valid cloudevent")
# #         return "not a valid cloudevent", 400

# #     parts = Path(ce["source"]).parts
# #     L.info(
# #         "register init",
# #         extra={"ce-source": ce["source"], "ce-type": ce["type"], "ce-data": ce.data},
# #     )

# #     try:
# #         registries = ce.data["registries"]
# #     except KeyError:
# #         return {"find right way to return error"}

# #     client = pymongo.MongoClient(config.mongodb_connection)
# #     L.info("client", extra={"client": client})
# #     registry = client.registry
# #     L.info("db", extra={"db": registry})

# #     try:
# #         for register, cfg in ce.data["registries"].items():
# #             if cfg["operation"] == "create":
# #                 try:
# #                     collection = registry.create_collection(register, check_exists=True)
# #                     if cfg["ttl"]:
# #                         collection.create_index(
# #                             [("last_update", 1)], expireAfterSeconds=cfg["ttl"]
# #                         )
# #                 except pymongo.errors.CollectionInvalid:
# #                     continue
# #     except KeyError as e:
# #         L.error("db init error", extra={"db_error": e})
# #         pass

# #     # sensor_type_registry = registry.get_collection("sensor_type")
# #     # L.info("collection", extra={"collection": client} )
# #     # result = sensor_type_registry.insert_one(ce.data)
# #     # L.info("insert", extra={"result": client} )

# #     return "ok"


# # @app.route("/register/sensor/sync", methods=["POST"])
# # def register_sensor_sync():
# #     try:
# #         ce = from_http(headers=request.headers, data=request.get_data())
# #         # to support local testing...
# #         if isinstance(ce.data, str):
# #             ce.data = json.loads(ce.data)
# #     except InvalidStructuredJSON:
# #         L.error("not a valid cloudevent")
# #         return "not a valid cloudevent", 400

# #     parts = Path(ce["source"]).parts
# #     L.info(
# #         "register sensor",
# #         extra={"ce-source": ce["source"], "ce-type": ce["type"], "ce-data": ce.data},
# #     )

# #     if "sensor-definition-id-list" in ce.data:
# #         for id in ce.data["sensor-definition-id-list"]:
# #             update_sensor_definition_db(id)

# #     return "ok", 200

# # @app.route("/register/sensor/request", methods=["POST"])
# # def register_sensor_request():
# #     try:
# #         ce = from_http(headers=request.headers, data=request.get_data())
# #         # to support local testing...
# #         if isinstance(ce.data, str):
# #             ce.data = json.loads(ce.data)
# #     except InvalidStructuredJSON:
# #         L.error("not a valid cloudevent")
# #         return "not a valid cloudevent", 400

# #     parts = Path(ce["source"]).parts
# #     L.info(
# #         "register sensor update-all",
# #         extra={"ce-source": ce["source"], "ce-type": ce["type"], "ce-data": ce.data},
# #     )

# #     # {"register-request": "update-sensor-definition-all"}
# #     if "register-sensor-request" in ce.data:
# #         reg_request = ce.data["register-sensor-request"]
# #         if reg_request == "update-sensor-definition-all":
# #             update_sensor_definition_db_all()

# #     return "ok", 200


# # if __name__ == "__main__":
# #     app.run(debug=config.debug, host=config.host, port=config.port)
# #     # app.run()

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
    # asyncio.run(main(config))

    try:
        index = sys.argv.index("--host")
        host = sys.argv[index + 1]
        config.host = host
    except (ValueError, IndexError):
        pass

    try:
        index = sys.argv.index("--port")
        port = sys.argv[index + 1]
        config.port = int(port)
    except (ValueError, IndexError):
        pass

    try:
        index = sys.argv.index("--log_level")
        ll = sys.argv[index + 1]
        config.log_level = ll
    except (ValueError, IndexError):
        pass

    asyncio.run(main(config))

