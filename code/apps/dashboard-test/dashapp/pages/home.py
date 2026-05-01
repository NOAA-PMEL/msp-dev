import dash
from dash import html, callback, dcc, Input, Output, State, dash_table, Patch
from dash.exceptions import PreventUpdate
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
import random
import json
import traceback
import time


handler = logging.StreamHandler()
handler.setFormatter(Logfmter())
logging.basicConfig(handlers=[handler])
L = logging.getLogger(__name__)
L.setLevel(logging.DEBUG)


# dash.register_page(__name__, path='/')
dash.register_page(__name__, path='/home', order=0)

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
ws_url_base = f"ws://{config.external_hostname}:{config.ws_port}"
if config.ws_use_tls:
    ws_url_base = f"wss://{config.external_hostname}:{config.wss_port}"

link_url_base = f"{http_url_base}/msp/dashboardtest"

# websocket = WebSocket(
#     id="ws-sensor", url=f"ws://uasdaq.pmel.noaa.gov/uasdaq/dashboard/ws/sensor/main"
# )
ws_send_buffer = html.Div(id="ws-send-instance-buffer", style={"display": "none"})





# print("here:1")
def get_layout():
    layout = html.Div([
        html.H1('BEACONS MSP Deployment'),
        # html.Div([
        #     dcc.DatePickerRange(
        #         id='my-date-picker-range',
        #         min_date_allowed=date(1995, 8, 5),
        #         max_date_allowed=date(2017, 9, 19),
        #         initial_visible_month=date(2017, 8, 5),
        #         end_date=date(2017, 8, 25)
        #     ),
        #     html.Div(id='output-container-date-picker-range')
        # ]),
        # html.Div([
        #     daq.BooleanSwitch(
        #         id='boolean-switch-1',
        #         on=False
        #     )
        # ]),
        # html.Div([
        #     daq.NumericInput(
        #         id='numeric-input-1',
        #         value=0
        #     )
        # ]),
        dbc.Card([
            dbc.CardBody([
                html.Div(id="global-status-container")
            ])
        ], id="main-status-card", className="mb-4"),

        # 2. Expandable Detail Section
        html.Div(id="detail-container", className="mb-5"),
    
    dbc.Row([
        dbc.Col(width=12, children=[
            # Clickable Header that looks like a Card Header
            html.Div(
                dbc.Button(
                    "Vessel Trajectory (Click to Show/Hide)",
                    id="trajectory-toggle",
                    n_clicks=0,
                    color="light",
                    className="w-100 text-start shadow-sm",
                    style={"border": "1px solid #dfe2e5", "borderRadius": "5px"}
                ),
                className="mb-1"
            ),
            # The Collapsible area
            dbc.Collapse(
                dbc.Card([
                    dbc.CardBody([
                            dcc.Graph(id='trajectory', style={"height": "500px"}, figure={"data": [], "layout": {}})
                    ])
                ]),
                id="trajectory-collapse",
                is_open=True, # Set to False if you want it closed by default
            )
        ]),
    ], className="mb-4"),
    WebSocket(
        id="ws-variableset-instance",
        # url=f"{ws_url_base}/msp/dashboardtest/ws/variableset/{variableset_id}"
        url=f"{ws_url_base}/msp/dashboardtest/ws/variableset/raz1::main"
    ),
    dcc.Store(id='variableset-store', data=[]),
    dcc.Store(id='variableset-data-buffer', data={}),
    dcc.Interval(
        id='variableset-interval',
        interval=5*1000,
        n_intervals=0
    )
    ])

    return layout

layout = get_layout()

# @callback(
#     Output('ws', "send"),
#     Input("input", "value")
# )
# def send(value):
#     print(f"sending: {value}")
#     return value

# @callback(Output("message", "children"), Input("ws", "message"))
# def message(e):
#     if e:
#         return f"Response from websocket: {e['data']}"
#     else:
#         return "No response"
    

# @callback(
#     Output('ws_pb', "send"),
#     Input("power-button-1", "on")
# )
# def send(value):
#     print(f"sending: {value}")
#     return str(value)



# @callback(
#     Output('output-container-date-picker-range', 'children'),
#     Input('my-date-picker-range', 'start_date'),
#     Input('my-date-picker-range', 'end_date'))
# def update_output(start_date, end_date):
#     string_prefix = 'You have selected: '
#     if start_date is not None:
#         start_date_object = date.fromisoformat(start_date)
#         start_date_string = start_date_object.strftime('%B %d, %Y')
#         string_prefix = string_prefix + 'Start Date: ' + start_date_string + ' | '
#     if end_date is not None:
#         end_date_object = date.fromisoformat(end_date)
#         end_date_string = end_date_object.strftime('%B %d, %Y')
#         string_prefix = string_prefix + 'End Date: ' + end_date_string
#     if len(string_prefix) == len('You have selected: '):
#         return 'Select a date to see it displayed here'
#     else:
#         return string_prefix


@callback(
    Output("variableset-store", "data"),
    Input("variableset-interval", "n_intervals"),
    State("variableset-store", "data")
)
def update_variableset_list(count, current_sets):
    update = False
    new_data = []
    
    try:
        # url = f"http://{datastore_url}/variableset-definition/registry/get/"
        url = f"http://{datastore_url}/variableset-definition/registry/ids/get/"
        L.debug(f"variableset-definition-get: {url}")
        response = httpx.get(url)
        results = response.json()
        L.debug(f"variableset-results: {results}")
        if "results" in results and results["results"]:
            for id in results["results"]:
                if id is not None:
                    parts = id.split("::")
                    # sensor_def = {
                    #     "variab": id,
                    #     "make": parts[0],
                    #     "model": parts[1],
                    #     "version": parts[2],
                    # }
                    variableset = parts
                    L.debug(f"variableset-definition-get_1: {variableset}")
                    if variableset not in current_sets:
                        current_sets.append(variableset)
                        update = True
                    new_data.append(variableset)


        remove_data = []
        for index, data in enumerate(current_sets):
            if data not in new_data:
                update = True
                remove_data.insert(0, index)
        for index in remove_data:
            current_sets.pop(index)

        if update:
            return current_sets
        else:
            return dash.no_update

    except Exception as e:
        L.error(f"update_sensor_definitions error: {e}")
        L.error(traceback.format_exc())
        return dash.no_update
    


@callback(
        Output("variableset-data-buffer", "data"),
        Input("ws-variableset-instance", "message")
          )
def update_variableset_buffers(event):
    L.debug(f"update_variableset_buffers: {event}")
    if event is not None and "data" in event:
        event_data = json.loads(event["data"])
        L.debug(f"update_variableset_buffers: {event}")
        if "data-update" in event_data:
            try:
                # msg = json.loads(event["data-update"])
                # print(f"update_controller_data: {event_data}")
                if event_data["data-update"]:
                    return [event_data["data-update"]]
            except Exception as event:
                L.debug(f"data buffer update error: {event}")
            
    return [dash.no_update]



@callback(
    Output("trajectory", "figure"),
    Input("variableset-data-buffer", "data"),
    State("trajectory", "figure"),
    prevent_initial_call=False
)
def update_trajectory(vs_data, current_figure):
    # Directly get the fake data
    if not vs_data:
        return dash.no_update
    
    if vs_data:
        try:
            vs_data = vs_data[0].get("data-update", {})
            L.debug(f"variableset-data-trajectory{vs_data}")

            variables = vs_data.get('variables', {})
            
            if 'latitude' not in variables or 'longitude' not in variables:
                return dash.no_update
                
            lats = variables['latitude'].get('data')
            lons = variables['longitude'].get('data')
            
            flattened_data = [
                {
                    'latitude': lats,
                    'longitude': lons
                }
            ]

            L.debug(f"variableset-data-trajectory-flattened: {flattened_data}")

            # center_lat = lats.mean()
            # center_lon = lons.mean()
            
            # # Dynamic Zoom Logic
            # max_diff = max(np.abs(lats - center_lat).max(), np.abs(lons - center_lon).max(), 0.01)
            # zoom_level = 8 - np.log2(max_diff)

            # Create the Map
            fig = px.scatter_map(
                # vs_data, 
                flattened_data,
                lat='latitude', 
                lon='longitude', 
                # zoom=zoom_level, 
                # center={"lat": center_lat, "lon": center_lon},
                title="Current Vessel Trajectory"
            )

            # Maritime Styling
            fig.update_traces(marker={'size': 8, 'color': '#007bff'}) # Nautical Blue
            fig.update_layout(
                margin={"r":0,"t":30,"l":0,"b":0},
                mapbox_style="open-street-map" # Reliable, no-token-needed map style
            )
            
            return fig
        
        except Exception as e:
            L.error(f"variableset-data-trajectory-error: {e}")
            return dash.no_update

    else:
        raise PreventUpdate



@callback(
    Output("trajectory-collapse", "is_open"),
    [Input("trajectory-toggle", "n_clicks")],
    [State("trajectory-collapse", "is_open")],
    prevent_initial_call=True # Prevents it from firing before the first click
)
def toggle_trajectory_map(n_clicks, is_open):
    # This will print in your VS Code/Terminal window
    print(f"Button clicked! Total clicks: {n_clicks}. Current state: {is_open}")
    
    if n_clicks:
        return not is_open
    return is_open



def get_sensor_data():
    sensors = ['Flow Meter', 'Laser Diode', 'Vacuum Pump', 'Inlet Temp']
    processed = []
    
    for s in sensors:
        # Randomly decide if this sensor is Healthy, Warning, or Critical
        test_case = random.choices(['nominal', 'warning', 'critical', 'status_code_error'],
                                  weights=[0.50, 0.3, 0.1, 0.1])[0]
        # test_case = random.choice(['nominal', 'warning'])
        
        val = 0.0
        code = 0
        
        if test_case == 'nominal':
            val = 10.0 # Standard safe value
            code = 0
        elif test_case == 'warning':
            val = 16.5 # Trips the 'Yellow' threshold in your assign_sensor_status
            code = 0
        elif test_case == 'critical':
            val = 99.9 # Trips the 'Red' threshold
            code = 0
        elif test_case == 'status_code_error':
            val = 10.0
            code = random.choice([0x01, 0x0E, 0xFF]) # Random hex errors
            
        # Use your existing logic to get the color and message
        status, details = assign_sensor_status(s, val, code)
        
        processed.append({
            'Sensor': s,
            'Value': val,
            'Status': status,
            'Details': details,
            'Code': f"0x{code:02X}" if code > 0 else "-"
        })
        
    return pd.DataFrame(processed)


def assign_sensor_status(sensor_name, value, status_code=0):
    """
    Assigns status based on hex/int codes first, then numerical thresholds.
    Returns: (color, display_message)
    """
    # 1. Handle Status Codes (High Priority)
    # Convert to hex string for professional display (e.g., 0x05)
    hex_code = f"0x{int(status_code):02X}" 
    
    if status_code != 0:
        # You can define which codes are 'Yellow' vs 'Red' if you want,
        # but for now, we assume any non-zero code is an error.
        return 'Red', f"System Error: {hex_code}"

    # 2. Threshold Configuration (Fallback)
    # [Lower Critical, Lower Warning, Upper Warning, Upper Critical]
    thresholds = {
        'Flow Meter':  [5.0, 8.0, 15.0, 18.0],
        'Laser Diode': [5.0, 8.0, 15.0, 18.0],
        'Vacuum Pump': [5.0, 8.0, 15.0, 18.0],
    }

    if sensor_name not in thresholds:
        return 'Green', "Nominal"

    limits = thresholds[sensor_name]

    # 3. Numerical Logic
    if value < limits[0] or value > limits[3]:
        return 'Red', f"Value Alert: {value}"
    elif value < limits[1] or value > limits[2]:
        return 'Yellow', f"Value Warning: {value}"
    
    return 'Green', "Nominal"
    

def process_system_health(raw_data_list):
    """
    Takes a list of dicts: [{'sensor': 'Flow Meter', 'value': 12.5}, ...]
    Returns a list of dicts with Status and Details appended.
    """
    processed_data = []
    
    for entry in raw_data_list:
        name = entry['sensor']
        val = entry['value']
        
        status, details = assign_sensor_status(name, val, entry.get('status_code', 0))
        
        processed_data.append({
            'Sensor': name,
            'Value': val,
            'Status': status,
            'Details': details
        })
        
    return processed_data



@callback(
    [Output("global-status-container", "children"),
     Output("detail-container", "children"),
     Output("main-status-card", "color")],
    [Input("global-status-container", "id")] # Triggered on load
)
def update_dashboard(_):
    df = get_sensor_data()
    
    # Determine overall system state
    if 'Red' in df['Status'].values:
        system_health = 'Error'
        card_color = "danger"
        # Filter for only the problems to show at the top
        problems = df[df['Status'] != 'Green']
    elif 'Yellow' in df['Status'].values:
        system_health = 'Warning'
        card_color = "warning"
        problems = df[df['Status'] != 'Green']
    else:
        system_health = 'Healthy'
        card_color = "success"
        problems = pd.DataFrame()

    # Header Content
    header_content = html.H2(f"System Status: {system_health}", className="text-center text-white")

    # Detail Content: Only show table if not "Healthy"
    if system_health == 'Healthy':
        detail_content = dbc.Alert("All systems operating within normal parameters.", color="success")
    else:
        detail_content = html.Div([
            html.H4("Action Required: Maintenance Needed on Following Sensors"),
            dash_table.DataTable(
                data=problems.to_dict('records'),
                columns=[{"name": i, "id": i} for i in problems.columns],
                style_cell={'textAlign': 'left', 'padding': '10px'},
                style_header={'backgroundColor': '#f8f9fa', 'fontWeight': 'bold'},
                style_data_conditional=[
                    {
                        'if': {'filter_query': '{Status} eq "Red"'},
                        'backgroundColor': '#ffcccc', 'color': 'black', 'fontWeight': 'bold'
                    },
                    {
                        'if': {'filter_query': '{Status} eq "Yellow"'},
                        'backgroundColor': '#fff3cd', 'color': 'black'
                    }
                ]
            )
        ])

    return header_content, detail_content, card_color

