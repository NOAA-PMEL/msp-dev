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
            dbc.Col(html.H2("Asset Registry", className="text-primary"), width=8),
            dbc.Col(dbc.Button("Refresh Registry", id="asset-refresh-btn", color="secondary", className="float-end"), width=4)
        ], className="mb-4 mt-3"),

        dbc.Tabs([
            # --- TAB 1: HARDWARE (Sensors, Operational, Controllers) ---
            dbc.Tab(
                dbc.Card([
                    dbc.CardBody([
                        dag.AgGrid(
                            id="hardware-registry-grid",
                            rowData=[],
                            columnDefs=[
                                {"field": "type", "headerName": "Type", "filter": True, "width": 120},
                                {"field": "make", "headerName": "Make", "filter": True, "width": 150},
                                {"field": "model", "headerName": "Model", "filter": True, "width": 150},
                                {"field": "serial_number", "headerName": "S/N", "width": 120},
                                {"field": "description", "headerName": "Description", "flex": 2},
                                {"field": "action", "headerName": "Action", "cellRenderer": "markdown", "width": 180}
                            ],
                            dashGridOptions={"pagination": True, "paginationPageSize": 20},
                            style={"height": "600px"}
                        )
                    ])
                ], className="shadow-sm border-dark border-top-0"),
                label="Hardware & Controllers",
                tab_id="tab-hardware"
            ),
            
            # --- TAB 2: PLATFORMS ---
            dbc.Tab(
                dbc.Card([
                    dbc.CardBody([
                        dag.AgGrid(
                            id="platform-registry-grid",
                            rowData=[],
                            columnDefs=[
                                {"field": "platform_id", "headerName": "Platform ID", "flex": 1, "filter": True},
                                {"field": "display_name", "headerName": "Display Name", "flex": 1, "filter": True},
                                {"field": "description", "headerName": "Description", "flex": 2}
                            ],
                            dashGridOptions={"pagination": True, "paginationPageSize": 20},
                            style={"height": "600px"}
                        )
                    ])
                ], className="shadow-sm border-dark border-top-0"),
                label="Platforms",
                tab_id="tab-platforms"
            )
        ], id="assets-tabs", active_tab="tab-hardware", className="mt-3")
    ])

@callback(
    Output("hardware-registry-grid", "rowData"),
    Output("platform-registry-grid", "rowData"),
    Input("asset-refresh-btn", "n_clicks"),
    prevent_initial_call=False
)
def fetch_all_registries(n_clicks):
    """Fetches devices, controllers, and platforms to populate the grids."""
    hardware_data = []
    platform_data = []
    
    timeout = httpx.Timeout(5.0)

    # ==========================================
    # 1. FETCH HARDWARE (Sensors & Operational)
    # ==========================================
    # We hit device-instance/registry/get/ for both sensor and operational types
    for dev_type in ["sensor", "operational"]:
        try:
            url = f"http://{datastore_url}/device-instance/registry/get/"
            response = httpx.get(url, params={"device_type": dev_type}, timeout=timeout)
            
            if response.status_code == 200:
                for doc in response.json().get("results", []):
                    make = doc.get("make", "unknown")
                    model = doc.get("model", "unknown")
                    sn = doc.get("serial_number", "")
                    device_id = f"{make}::{model}::{sn}"
                    
                    # Extract rich description from attributes
                    attrs = doc.get("attributes", {})
                    description = attrs.get("description", {}).get("data", "N/A")
                    
                    hardware_data.append({
                        "type": dev_type.capitalize(),
                        "make": make,
                        "model": model,
                        "serial_number": sn,
                        "description": description,
                        # Route sensors/operational to the settings & telemetry page
                        "action": f"[Telemetry & Settings]({dash.get_relative_path(f'/sensor/{device_id}')})" 
                    })
        except Exception as e:
            L.error(f"Asset fetch failed for {dev_type}: {e}")

    # ==========================================
    # 2. FETCH CONTROLLERS
    # ==========================================
    try:
        url = f"http://{datastore_url}/controller-instance/registry/get/"
        response = httpx.get(url, timeout=timeout)
        
        if response.status_code == 200:
            for doc in response.json().get("results", []):
                make = doc.get("make", "unknown")
                model = doc.get("model", "unknown")
                sn = doc.get("serial_number", "")
                device_id = f"{make}::{model}::{sn}"
                
                # Extract rich description from attributes
                attrs = doc.get("attributes", {})
                description = attrs.get("description", {}).get("data", "N/A")
                
                hardware_data.append({
                    "type": "Controller",
                    "make": make,
                    "model": model,
                    "serial_number": sn,
                    "description": description,
                    # Route controllers to a dedicated page for controls
                    "action": f"[Telemetry & Controls]({dash.get_relative_path(f'/controller/{device_id}')})" 
                })
    except Exception as e:
        L.error(f"Controller fetch failed: {e}")

    # ==========================================
    # 3. FETCH PLATFORMS
    # ==========================================
    try:
        id_url = f"http://{datastore_url}/platform-definition/registry/ids/get/"
        id_response = httpx.get(id_url, timeout=timeout)
        
        if id_response.status_code == 200:
            for p_id in id_response.json().get("results", []):
                if not p_id: continue
                
                def_url = f"http://{datastore_url}/platform-definition/registry/get/"
                def_response = httpx.get(def_url, params={"name": p_id}, timeout=timeout)
                
                if def_response.status_code == 200:
                    docs = def_response.json().get("results", [])
                    if docs:
                        doc = docs[0]
                        meta = doc.get("metadata", {})
                        data = doc.get("data", {})
                        
                        platform_data.append({
                            "platform_id": meta.get("name", p_id),
                            "display_name": data.get("display_name", "N/A"),
                            "description": data.get("description", "N/A")
                        })
    except Exception as e:
        L.error(f"Platform fetch failed: {e}")

    return hardware_data, platform_data