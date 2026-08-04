import logging
from fastapi import FastAPI, Request, status, Response

from cloudevents.http import from_http
from logfmter import Logfmter
from pydantic import BaseSettings
from filemanager import Filemanager

handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.DEBUG)

app = FastAPI()

# Global filemanager instance
filemanager = None

@app.on_event("startup")
async def start_system():
    global filemanager
    filemanager = Filemanager()
    await filemanager.setup()
    L.info("Filemanager initialized and background tasks started.")

@app.on_event("shutdown")
async def shutdown_system():
    global filemanager
    if filemanager:
        for path, rf in filemanager.file_map.items():
            rf.close()
        L.info("Filemanager tasks shut down safely.")

@app.get("/")
async def root():
    return {"message": "Hello World from Filemanager"}

@app.post("/data/save/")
async def data_save(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        L.debug("data_save API push", extra={"ce_type": ce["type"]})
        
        await filemanager.save(ce)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as e:
        L.error("data_save API error", extra={"reason": str(e)})
        return Response(status_code=status.HTTP_204_NO_CONTENT)

@app.post("/logs/save/")
async def logs_save(request: Request):
    try:
        ce = from_http(request.headers, await request.body())
        L.debug("logs_save API push", extra={"ce_type": ce["type"]})
        
        await filemanager.save(ce)
        return Response(status_code=status.HTTP_204_NO_CONTENT)
    except Exception as e:
        L.error("logs_save API error", extra={"reason": str(e)})
        return Response(status_code=status.HTTP_204_NO_CONTENT)