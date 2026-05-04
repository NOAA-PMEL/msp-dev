import logging
from fastapi import FastAPI, Request, Query, status, Response
from cloudevents.http import from_http
from logfmter import Logfmter

from manager import SamplingModesManager

# Configure structured logging
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger("SamplingModesAPI")
L.setLevel(logging.DEBUG)

app = FastAPI(title="Sampling Modes Service")
manager = None

@app.on_event("startup")
async def start_system():
    global manager
    manager = SamplingModesManager()
    await manager.setup()
    L.info("SamplingModesManager initialized and background tasks started.")

@app.on_event("shutdown")
async def shutdown_system():
    global manager
    if manager and hasattr(manager, "http_client") and manager.http_client:
        await manager.http_client.aclose()
        L.info("SamplingModesManager HTTP client closed safely.")

@app.get("/")
async def root():
    return {"message": "Hello World from Sampling Modes Service"}

@app.post("/action/manual_request/")
async def manual_action_request(kind: str = Query(...), name: str = Query(...)):
    """
    Allows a dashboard or primary controller to manually trigger a SamplingAction.
    """
    if manager:
        # Pushes a manual override request into the manager's action buffer
        action = {
            "action": {"kind": kind, "name": name},
            "is_manual_override": True
        }
        await manager.actions_buffer.put(action)
        L.info(f"Manual action triggered: {kind} -> {name}")
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@app.post("/variableset/data/update/", status_code=status.HTTP_202_ACCEPTED)
async def variableset_data_update(request: Request):
    """
    Knative Eventing HTTP target for incoming sensor/variableset telemetry.
    Updates the data caches used by ActionEvaluators.
    """
    try:
        ce = from_http(request.headers, await request.body())
        L.debug("variableset_data_update received", extra={"source": ce.get("source"), "destpath": ce.get("destpath")})
        
        if manager:
            # Replicate the data ingestion logic from the MQTT listener for HTTP triggers
            src_id = ce.get("source", "").split(".")[-1]
            dt = ce.data.get("variables", {}).get("time", {}).get("data")
            
            for evaluator in manager.actions.values():
                for req_src in evaluator.config.get("sources", {}).values():
                    req_id = f"{req_src['variablemap_name']}::{req_src['variableset_name']}"
                    if req_id == src_id:
                        if req_id not in evaluator.sources["data"]: 
                            evaluator.sources["data"][req_id] = {}
                        for v_name, v_data in ce.data.get("variables", {}).items():
                            evaluator.sources["data"][req_id][v_name] = {"data": v_data.get("data"), "last_update": dt}
                            
    except Exception as e:
        L.error("variableset_data_update failed", extra={"reason": str(e)})
    
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@app.post("/requirement/status/update/", status_code=status.HTTP_202_ACCEPTED)
async def requirement_status_update(request: Request):
    """
    Knative Eventing HTTP target for incoming mode status updates (from SystemModes or conditions).
    Feeds into the SamplingMode requirement evaluation engine.
    """
    try:
        ce = from_http(request.headers, await request.body())
        L.debug("requirement_status_update received", extra={"source": ce.get("source"), "destpath": ce.get("destpath")})
        
        if manager:
            status_data = ce.data.get("status", {})
            for mode in manager.modes.values():
                await mode.update(status_data)
                
    except Exception as e:
        L.error("requirement_status_update failed", extra={"reason": str(e)})
        
    return Response(status_code=status.HTTP_204_NO_CONTENT)