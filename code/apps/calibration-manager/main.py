import logging
from fastapi import FastAPI, Request, Response
from cloudevents.http import from_http
from cloudevents.exceptions import MissingRequiredFields

from manager import CalibrationManager

# Set up basic logging for the entry point
L = logging.getLogger(__name__)

# Initialize FastAPI app and our Manager
app = FastAPI(title="Calibration Manager", description="ENVDS Service for managing device calibration coefficients.")
manager = CalibrationManager()

@app.on_event("startup")
async def startup_event():
    """Lifecycle hook to initialize the manager's async tasks and clients."""
    L.info("Starting up Calibration Manager...")
    await manager.setup()

@app.on_event("shutdown")
async def shutdown_event():
    """Lifecycle hook to gracefully close connections."""
    L.info("Shutting down Calibration Manager...")
    if getattr(manager, 'http_client', None):
        await manager.http_client.aclose()

@app.get("/health")
async def health_check():
    """Basic health probe endpoint for Kubernetes."""
    return {"status": "healthy", "service": "calibration-manager"}

@app.post("/calibration/registry/update/")
@app.post("/calibration/registry/update")
async def handle_calibration_update(request: Request):
    """
    Knative Eventing endpoint.
    Receives CloudEvents from the broker and routes them into the manager's event buffer.
    """
    try:
        body = await request.body()
        # Parse the HTTP request into a CloudEvent object
        event = from_http(request.headers, body)
        L.debug(f"Received knative event via HTTP: {event.get('type')}")
        
        # Route the parsed CloudEvent to the manager's unified processing buffer
        await manager.mqtt_buffer.put(event)
        
        return Response(status_code=202)
        
    except MissingRequiredFields:
        L.warning("Received invalid CloudEvent payload (Missing required fields).")
        return Response(status_code=400, content="Invalid CloudEvent")
    except Exception as e:
        L.error(f"Error processing CloudEvent HTTP Post: {e}")
        return Response(status_code=500, content=str(e))