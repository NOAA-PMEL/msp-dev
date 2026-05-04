import logging
from fastapi import FastAPI, Request, Query, status, Response
from cloudevents.http import from_http
from logfmter import Logfmter

from manager import SystemModesManager

# Configure structured logging
handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger("SystemModesAPI")
L.setLevel(logging.DEBUG)

app = FastAPI(title="System Modes Service")
manager = None

@app.on_event("startup")
async def start_system():
    global manager
    manager = SystemModesManager()
    await manager.setup()
    L.info("SystemModesManager initialized and background tasks started.")

@app.on_event("shutdown")
async def shutdown_system():
    global manager
    if manager and hasattr(manager, "http_client") and manager.http_client:
        await manager.http_client.aclose()
        L.info("SystemModesManager HTTP client closed safely.")

@app.get("/")
async def root():
    return {"message": "Hello World from System Modes Service"}

@app.post("/system/control/update/")
async def system_control_update(mode: str = Query(..., description="'auto' or 'manual'")):
    """
    Direct endpoint for a dashboard to toggle the system between 'auto' and 'manual' control.
    """
    if manager:
        manager.system_control = mode.lower()
        L.info(f"System control mode set to: {mode.lower()}")
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@app.post("/system/transition/remote_request/")
async def remote_transition_request(
    kind: str = Query("SystemMode", description="Type of mode (e.g., SystemMode)"), 
    name: str = Query(..., description="Target mode name (e.g., startup, normal)")
):
    """
    Direct endpoint for a primary node or dashboard to force a mode transition.
    Bypasses 'auto' restrictions.
    """
    if manager:
        transition_data = {
            "transition": {"kind": kind, "name": name},
            "is_remote_command": True
        }
        await manager.transitions_buffer.put(transition_data)
        L.info(f"Remote transition requested: {kind} -> {name}")
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@app.post("/system/transition/event/", status_code=status.HTTP_202_ACCEPTED)
async def transition_event_update(request: Request):
    """
    Knative Eventing HTTP target for incoming transition requests over the event bus.
    """
    try:
        ce = from_http(request.headers, await request.body())
        L.debug("transition_event_update received", extra={"source": ce.get("source")})
        
        if manager and ce.get("type") == "envds.system-modes.transition.request":
            await manager.transitions_buffer.put(ce.data)
            
    except Exception as e:
        L.error("transition_event_update failed", extra={"reason": str(e)})
        
    return Response(status_code=status.HTTP_204_NO_CONTENT)

@app.post("/requirement/status/update/", status_code=status.HTTP_202_ACCEPTED)
async def requirement_status_update(request: Request):
    """
    Knative Eventing HTTP target for incoming mode status updates.
    Feeds into the SystemMode requirement evaluation engine.
    """
    try:
        ce = from_http(request.headers, await request.body())
        L.debug("requirement_status_update received", extra={"source": ce.get("source")})
        
        if manager:
            # Replicate the status caching logic from the MQTT listener for HTTP triggers
            status_data = ce.data.get("status", {})
            kind = status_data.get("kind")
            name = status_data.get("name")
            state = status_data.get("status")
            
            if kind and name:
                if kind not in manager.requirement_status_map:
                    manager.requirement_status_map[kind] = {}
                manager.requirement_status_map[kind][name] = state
                
    except Exception as e:
        L.error("requirement_status_update failed", extra={"reason": str(e)})
        
    return Response(status_code=status.HTTP_204_NO_CONTENT)