import dash
from dash import html, callback, dcc, Input, Output, dash_table
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
ws_url_base = f"ws://{config.external_hostname}:{config.ws_port}:"
if config.ws_use_tls:
    ws_url_base = f"wss://{config.external_hostname}:{config.wss_port}"

link_url_base = f"{http_url_base}/msp/dashboardtest"

# websocket = WebSocket(
#     id="ws-sensor", url=f"ws://uasdaq.pmel.noaa.gov/uasdaq/dashboard/ws/sensor/main"
# )
ws_send_buffer = html.Div(id="ws-send-instance-buffer", style={"display": "none"})



def get_device_data(device_id: str, device_type: str="sensor"):
    
    query = {"device_type": device_type, "device_id": device_id}
    url = f"http://{datastore_url}/device/data/get/"
    print(f"device-data-get: {url}, query: {query}")
    try:
        timeout = httpx.Timeout(10.0, read=None)
        response = httpx.get(url, params=query, timeout=timeout)
        results = response.json()
        # print(f"results: {results}")
        if "results" in results and results["results"]:
            return results["results"]
    except Exception as e:
        L.error("get_device_data", extra={"reason": e})
    return []



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
                        dcc.Loading(
                            dcc.Graph(id='trajectory', style={"height": "500px"})
                        )
                    ])
                ]),
                id="trajectory-collapse",
                is_open=True, # Set to False if you want it closed by default
            )
        ]),
    ], className="mb-4")
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

def get_fake_trajectory_data(num_points=100, start_lat=50.0, start_lon=-1.0):
    """
    Generates a DataFrame with a simulated ship trajectory.
    """
    lats = [start_lat]
    lons = [start_lon]
    
    # Simulate movement: small random changes + a slight northward/eastward drift
    for i in range(1, num_points):
        # Adding small increments (~0.01 degree is roughly 1km)
        new_lat = lats[-1] + np.random.uniform(-0.005, 0.015) 
        new_lon = lons[-1] + np.random.uniform(-0.005, 0.025)
        
        lats.append(new_lat)
        lons.append(new_lon)
    
    df = pd.DataFrame({
        'latitude': lats,
        'longitude': lons,
        'timestamp': pd.date_range(start='2024-01-01', periods=num_points, freq='1min')
    })
    
    return df


def get_dataset():
    """Simple wrapper to return fake data."""
    return get_fake_trajectory_data()


@callback(
    Output("trajectory", "figure"),
    Input("global-status-container", "id") # Triggers when the health status container loads
)
def plot_trajectory(_):
    # Directly get the fake data
    df = get_dataset()
    
    lats = df['latitude']
    lons = df['longitude']
    
    center_lat = lats.mean()
    center_lon = lons.mean()
    
    # Dynamic Zoom Logic
    max_diff = max(np.abs(lats - center_lat).max(), np.abs(lons - center_lon).max(), 0.01)
    zoom_level = 8 - np.log2(max_diff)

    # Create the Map
    fig = px.scatter_map(
        df, 
        lat='latitude', 
        lon='longitude', 
        zoom=zoom_level, 
        center={"lat": center_lat, "lon": center_lon},
        title="Current Vessel Trajectory"
    )

    # Maritime Styling
    fig.update_traces(marker={'size': 8, 'color': '#007bff'}) # Nautical Blue
    fig.update_layout(
        margin={"r":0,"t":30,"l":0,"b":0},
        mapbox_style="open-street-map" # Reliable, no-token-needed map style
    )
    
    return fig

# @callback(
#     Output("trajectory", "figure"),
#     # Input("dataset_options", "value"),
#     Input("global-status-container", "id"),
#     # prevent_initial_call=True
# )
# def plot_trajectory(sel_dataset):
#     df = get_dataset()
#     # if sel_dataset:
#     #     if sel_dataset:
#     #         # TESTING MODE: If you select a "Test" option, use the fake function
#     #         if sel_dataset == "TEST_MODE":
#     #             df = get_fake_trajectory_data(500)
#     #         else:
#     #             ds = get_dataset(sel_dataset)
#     #             if not ds:
#     #                 return go.Figure()
#                 # df = ds.to_dataframe()
#             # ds = get_dataset(sel_dataset)
            
#             # if ds:
#             #     df = ds.to_dataframe()
#     lats = df['latitude']
#     lons = df['longitude']
#     center_lat = np.mean(lats)
#     center_lon = np.mean(lons)
#     max_lat_diff = np.max(np.abs(lats - center_lat))
#     max_lon_diff = np.max(np.abs(lons - center_lon))
#     zoom_level = 8 - np.log2(max(max_lat_diff, max_lon_diff))
#     if len(lats) > 500000:
#         df_sub = df.iloc[::20, :]
#     else:
#         df_sub = df
#     # fig = px.scatter_map(df_sub, lat='latitude', lon='longitude', zoom=zoom_level, 
#     #                 center={"lat": df['latitude'].mean(), "lon": df['longitude'].mean()})
#     # input_trig = ctx.triggered_id
#     # if (input_trig != '1D_graph') and (input_trig != '2D_graph') :
#     #     fig = px.scatter_map(df_sub, lat='latitude', lon='longitude', zoom=zoom_level, 
#     #                 center={"lat": df['latitude'].mean(), "lon": df['longitude'].mean()})
#     #     return fig

#     # if tab_container == 'tab-1':               
#     #     fig = px.scatter_map(df_sub, lat='latitude', lon='longitude', zoom=zoom_level, 
#     #                 center={"lat": df['latitude'].mean(), "lon": df['longitude'].mean()}) 
#     #     try:
#     #         start_time = one_zoom_data['xaxis.range[0]']
#     #         end_time = one_zoom_data['xaxis.range[1]']

#     #         df_sel = df_sub.loc[start_time: end_time]

#     #         trace = go.Scattermap(
#     #             lat=df_sel['latitude'],
#     #             lon=df_sel['longitude'],
#     #             marker={'size': 7, 'color': 'red'})
#     #         fig.add_trace(trace)

#     #     except: 
#     #         pass

#     # if tab_container == 'tab-2':
#     #     fig = px.scatter_map(df_sub, lat='latitude', lon='longitude', zoom=zoom_level, 
#     #                 center={"lat": df['latitude'].mean(), "lon": df['longitude'].mean()})
#     #     try: 
#     #         start_time = two_zoom_data['xaxis.range[0]']
#     #         end_time = two_zoom_data['xaxis.range[1]']

#     #         df_sel = df_sub.loc[start_time: end_time]
#     #         trace = go.Scattermap(
#     #             lat=df_sel['latitude'],
#     #             lon=df_sel['longitude'],
#     #             marker={'size': 7, 'color': 'red'})
#     #         fig.add_trace(trace)
#     #         fig.update_traces()
#     #         # fig.update_traces()
#     #         # fig.update_layout()
#     #         # print('update fig')
#     #         # return fig
#     #     except:
#     #         pass
#     #fig.update_maps(zoom=zoom_level, center_lat=center_lat, center_lon=center_lon)
#     fig.update_traces()
#     fig.update_layout()
#     return fig
#     else:
#         #fig = go.Figure({'data': [], 'layout': {'autosize': True, 'xaxis': {'autorange': True}, 'yaxis': {'autorange': True}}})
#         df_empty = pd.DataFrame({'lat': [], 'lon': []})
#         fig = px.scatter_map(df_empty, lat='lat', lon='lon')
#         fig.update_layout({'autosize': True})
#         return fig
#         #return no_update


# Mock data - In a real app, this would come from your SQL or InfluxDB
# def get_sensor_data():
#     # In reality, this dict comes from your sensors/DB
#     raw_data = [
#         {'sensor': 'Flow Meter', 'value': 12.5, 'status_code': 0},
#         {'sensor': 'Laser Diode', 'value': 98.2, 'status_code': 0},
#         {'sensor': 'Vacuum Pump', 'value': 0.85, 'status_code': 14}, # 0x0E
#         {'sensor': 'Inlet Temp', 'value': 22.1, 'status_code': 0}
#     ]
    
#     processed = []
#     for entry in raw_data:
#         status, details = assign_sensor_status(entry['sensor'], entry['value'], entry['status_code'])
#         processed.append({
#             'Sensor': entry['sensor'],
#             'Value': entry['value'],
#             'Status': status,
#             'Details': details,
#             'Code': f"0x{entry['status_code']:02X}" if entry['status_code'] > 0 else "-"
#         })
#     return pd.DataFrame(processed)

def get_sensor_data():
    sensors = ['Flow Meter', 'Laser Diode', 'Vacuum Pump', 'Inlet Temp']
    processed = []
    
    for s in sensors:
        # Randomly decide if this sensor is Healthy, Warning, or Critical
        test_case = random.choice(['nominal', 'warning', 'critical', 'status_code_error'])
        
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
        'Laser Diode': [80.0, 85.0, 105.0, 110.0],
        'Vacuum Pump': [0.2, 0.4, 0.9, 1.2],
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
        system_health = 'Critical'
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
