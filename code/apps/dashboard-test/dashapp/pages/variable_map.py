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


handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.DEBUG)


# dash.register_page(__name__, path='/')
dash.register_page(__name__, path='/variable_map', title="Variable Map", order=3)

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


# def get_variablemap_def(device_id: str, device_type: str="sensor"):
def get_variablemap_def(variableset_id: str):
    
    # query = {"device_type": device_type, "device_id": device_id}
    # url = f"http://{datastore_url}/device/data/get/"
    # url = f"http://{datastore_url}/variableset-definition/registry/ids/get/"
    url = f"http://{datastore_url}/variableset/data/get/"
    # print(f"variablemap-definition-get: {url}, query: {query}")
    L.debug(f"variablemap-definition-get: {url}")

    try:
        timeout = httpx.Timeout(10.0, read=None)
        # response = httpx.get(url, params=query, timeout=timeout)
        response = httpx.get(url)
        results = response.json()
        L.debug(f"variablemap_results: {results}")
        if "results" in results and results["results"]:
            return results["results"]
    except Exception as e:
        # L.error("get_device_data", extra={"reason": e})
        # return []
        # L.warning(f"Connection failed to {device_id}. Returning mock data for testing.")
        # Return mock data for testing
        L.debug(f"variablemap_error: {e}")
        return [{"timestamp": "2024-01-01T00:00:00", "value": 1.23, "status": 0}]


# print("here:1")
def get_layout():
    get_variablemap_def(variableset_id='raz1::main')
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
                value=json.dumps(initial_variable_map, indent=4),
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

layout = get_layout()

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