import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
import httpx
import logging
from pydantic import BaseSettings

L = logging.getLogger(__name__)

dash.register_page(
    __name__,
    path="/assets",
    name="Asset Management",
    nav_bar=True,
    order=2
)

# --- DYNAMIC CONFIGURATION ---
class Settings(BaseSettings):
    daq_id: str = "default"
    class Config:
        env_prefix = "ENVOPS_"
        case_sensitive = False

config = Settings()
datastore_url = f"datastore.{config.daq_id}-system.svc.cluster.local"
# -----------------------------

def layout():
    return html.Div([
        dbc.Row([
            dbc.Col(html.H2("Hardware Asset Registry", className="text-primary"), width=8),
            dbc.Col(dbc.Button("Refresh Registry", id="asset-refresh-btn", color="secondary", className="float-end"), width=4)
        ], className="mb-4 mt-3"),

        dbc.Card([
            dbc.CardBody([
                dag.AgGrid(
                    id="asset-registry-grid",
                    rowData=[],
                    columnDefs=[
                        {"field": "device_id", "headerName": "Device ID", "flex": 2},
                        {"field": "make", "headerName": "Make", "filter": True},
                        {"field": "model", "headerName": "Model", "filter": True},
                        {"field": "serial_number", "headerName": "S/N"},
                        {"field": "action", "headerName": "Telemetry", "cellRenderer": "markdown"}
                    ],
                    dashGridOptions={"pagination": True, "paginationPageSize": 20},
                    style={"height": "600px"}
                )
            ])
        ], className="shadow-sm border-dark")
    ])

@callback(
    Output("asset-registry-grid", "rowData"),
    Input("asset-refresh-btn", "n_clicks"),
    prevent_initial_call=False
)
def fetch_asset_registry(n_clicks):
    """Fetches all active hardware devices from the registry."""
    try:
        # Using the dynamic datastore_url
        url = f"http://{datastore_url}/device-instance/registry/get/"
        response = httpx.get(url, params={"device_type": "sensor"}, timeout=10.0)
        
        if response.status_code == 200:
            results = response.json().get("results", [])
            row_data = []
            for doc in results:
                make = doc.get("make")
                model = doc.get("model")
                sn = doc.get("serial_number")
                device_id = f"{make}::{model}::{sn}"
                
                row_data.append({
                    "device_id": device_id,
                    "make": make,
                    "model": model,
                    "serial_number": sn,
                    # Routes to sensor.py utilizing the K8s strip-prefix path
                    "action": f"[View Raw Telemetry](/envds/envops/sensor/{device_id})" 
                })
            return row_data
    except Exception as e:
        L.error(f"Asset fetch failed: {e}")
    
    return []