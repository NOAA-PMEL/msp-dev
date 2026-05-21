import uvicorn
from fastapi import FastAPI
from fastapi.middleware.wsgi import WSGIMiddleware

# Import the Dash app instance from our renamed file
from envops_app import app as dash_app

app = FastAPI(title="EnvOps API")

# Mount the Dash app inside FastAPI
# Traefik strips the /msp/envops prefix, so FastAPI serves this at the root.
app.mount("/", WSGIMiddleware(dash_app.server))

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8080, reload=True)