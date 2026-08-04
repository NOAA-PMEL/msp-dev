import dash
from dash import html, callback, dcc, Input, Output, State, no_update
import dash_bootstrap_components as dbc
#import dash_daq as daq
from dash_extensions import WebSocket
from ulid import ULID
import dash_daq as daq
from datetime import date
import numpy as np
import pandas as pd
import httpx
from pydantic import BaseSettings
import logging
from logfmter import Logfmter
import plotly.express as px
import json
import dash_ace
import traceback
import os


handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.DEBUG)


# dash.register_page(__name__, path='/')
dash.register_page(__name__, path='/variable_map', title="Variable Map", order=4)

CONTENT_STYLE = {
    "margin-left": "18rem",
    "margin-right": "2rem",
    "padding": "2rem 1rem",
}


class Settings(BaseSettings):
    # host: str = "0.0.0.0"
    # port: int = 8787
    # debug: bool = False
    daq_id: str = "default"

    external_hostname: str = "localhost"
    http_use_tls: bool = False
    http_port: int = 80
    https_port: int = 443
    ws_use_tls: bool = False
    ws_port: int = 80
    wss_port: int = 443
 
    knative_broker: str = (
        "http://kafka-broker-ingress.knative-eventing.svc.cluster.local/default/default"
    )
    dry_run: bool = False

    class Config:
        env_prefix = "DASHBOARD_"
        case_sensitive = False


config = Settings()

datastore_url = f"datastore.{config.daq_id}-system"
http_url_base = f"http://{config.external_hostname}:{config.http_port}"
if config.http_use_tls:
    http_url_base = f"https://{config.external_hostname}:{config.https_port}"
ws_url_base = f"ws://{config.external_hostname}:{config.ws_port}:"
if config.ws_use_tls:
    ws_url_base = f"wss://{config.external_hostname}:{config.wss_port}"

link_url_base = f"{http_url_base}/msp/dashboardtest"

ws_send_buffer = html.Div(id="variable-map-ws-send-instance-buffer", style={"display": "none"})


# Simulated initial data for testing
initial_variable_map = {
    "system_name": "Aerosol Monitor 01",
    "vessel_id": "IMO_9876543",
    "sampling_rate_hz": 10,
    "sensors": [
        {"id": "flow_in", "register": 4001},
        {"id": "pm25_optical", "register": 4002}
    ],
    "test_mode": True
}



def debug_local_files():
    try:
        # Get the current working directory
        cwd = os.getcwd()
        files = os.listdir(cwd)
        
        L.debug(f"DEBUG: Current Working Directory: {cwd}")
        L.debug(f"DEBUG: Files in repo: {files}")
    except Exception as e:
        L.error(f"DEBUG: Could not list files: {e}")

# Call this at the top level of your script or inside get_layout()
debug_local_files()




def get_variablemap_ids():
    """
    Fetches the list of all registered Variable Map IDs and logs them.
    """
    url = f"http://{datastore_url}/variablemap-definition/registry/ids/get/"
    L.debug(f"DEBUG: id url {url}")
    
    try:
        timeout = httpx.Timeout(5.0)
        response = httpx.get(url, timeout=timeout)
        response.raise_for_status()
        
        data = response.json()
        L.debug(f"DEBUG: id data {data}")
        
        # Extract the list from common wrapper keys
        ids = []
        if isinstance(data, dict):
            ids = data.get("results", data.get("ids", []))
        elif isinstance(data, list):
            ids = data

        # --- DEBUG STATEMENT ---
        L.debug(f"DEBUG: Found {len(ids)} available IDs: {ids}")
        # If your logger isn't visible, you can also use:
        # print(f"DEBUG IDs: {ids}") 
        
        return ids

    except Exception as e:
        L.error(f"DEBUG: Failed to retrieve IDs from {url}. Error: {e}")
        return []



def get_variablemap_def(variablemap_definition_id: str):
    
    url = f"http://{datastore_url}/variablemap-definition/registry/get/"
    L.debug(f"variablemap-definition-get: {url}")

    query = {"variablemap_definition_id": variablemap_definition_id}

    try:
        timeout = httpx.Timeout(10.0, read=10.0)
        response = httpx.get(url, params=query, timeout=timeout)

        results = response.json()
        L.debug(f"variablemap_results: {results}")

        if "results" in results and results["results"]:
            return results["results"][0]
        
    except Exception as e:
        L.error(f"variablemap_error: {e}")
        L.error(traceback.format_exc())


# def get_layout():
#     get_variablemap_def(variablemap_definition_id='raz1::main')
#     layout = html.Div([
        # html.H1('Variable Map'),
        # html.Div(id='status-message', style={'margin-bottom': '10px', 'font-weight': 'bold'}),
        # html.Div([
        #     # dcc.Textarea(
        #     #     id='variable-map-editor',
        #     #     value=json.dumps(initial_variable_map, indent=4),
        #     #     style={'width': '100%', 'height': 500, 'font-family': 'monospace'}
        #     # ),
        #     dash_ace.DashAceEditor(
        #         id='variable-map-editor',
        #         value=json.dumps(initial_variable_map, indent=4),
        #         mode='json',        # Enables JSON syntax highlighting
        #         theme='monokai',     # Dark theme (change to 'github' for light)
        #         tabSize=4,
        #         showGutter=True,     # This shows the line numbers
        #         showPrintMargin=False,
        #         style={'width': '100%', 'height': '600px'}
        #     ),
        #     html.Br(),
        #     html.Button('Save Variable Map', id='save-button', n_clicks=0),
        #     html.Hr()
        # ]),
        # WebSocket(
        #         id="ws-variable-map-instance",
        #         # url=f"{ws_url_base}/msp/dashboardtest/ws/controller/{controller_id}",
        #     ),
        #     ws_send_buffer
#     ])

#     return layout



def get_layout():
    # Trigger the retrieval and the debug print
    available_ids = get_variablemap_ids()
    
    # Optional: Log the first one specifically to see what we might load
    if available_ids:
        L.debug(f"DEBUG: Selecting '{available_ids[0]}' for initial load.")
        api_data = get_variablemap_def(variablemap_definition_id=available_ids[0])
    else:
        L.warning("DEBUG: No IDs found in registry, loading initial_variable_map fallback.")
        api_data = initial_variable_map

    layout = html.Div([
        html.H1('Variable Map'),
        html.Div(id='status-message', style={'margin-bottom': '10px', 'font-weight': 'bold'}),
        html.Div([
            # dcc.Textarea(
            #     id='variable-map-editor',
            #     value=json.dumps(initial_variable_map, indent=4),
            #     style={'width': '100%', 'height': 500, 'font-family': 'monospace'}
            # ),
            dash_ace.DashAceEditor(
                id='variable-map-editor',
                # value=json.dumps(initial_variable_map, indent=4),
                value = json.dumps(api_data, indent=4),
                mode='json',        # Enables JSON syntax highlighting
                theme='monokai',     # Dark theme (change to 'github' for light)
                tabSize=4,
                showGutter=True,     # This shows the line numbers
                showPrintMargin=False,
                style={'width': '100%', 'height': '600px'}
            ),
            html.Br(),
            html.Button('Save Variable Map', id='save-button', n_clicks=0),
            html.Hr()
        ]),
        WebSocket(
                id="ws-variable-map-instance",
                # url=f"{ws_url_base}/msp/dashboardtest/ws/controller/{controller_id}",
            ),
            ws_send_buffer
    ])
    return layout



# layout = get_layout()
layout = get_layout

@callback(
    [Output('variable-map-editor', 'value'),
     Output('status-message', 'children'),
     Output('status-message', 'style')],
    [Input('save-button', 'n_clicks')],
    [State('variable-map-editor', 'value')]
)
def manage_variable_map(n_clicks, current_text):
    if  n_clicks == 0:
        return no_update, "Ready to edit...", {'color': 'gray'}
    
    try:
        new_map = json.loads(current_text)
        # Check for mandatory keys to simulate a "Strict" config
        required_keys = ["system_name", "sensors"]
        missing = [k for k in required_keys if k not in new_map]
        
        if missing:
            return no_update, f"Validation Error: Missing keys {missing}", {'color': 'orange'}
        
        return (
            json.dumps(new_map, indent=4), 
            "Map saved and updated successfully!", 
            {'color': 'green'}
        )
    
    except json.JSONDecodeError as e:
        # If there's a syntax error, we DON'T update the textarea (so user doesn't lose work)
        # but we do show the error message.
        return no_update, f"JSON Error: {str(e)}", {'color': 'red'}
    


@callback(
    Output("ws-variable-map-instance", "send"),
    Input("variable-map-ws-send-instance-buffer", "children"),
)
def send_to_instance(value):
    print(f"sending: {value}")
    return value