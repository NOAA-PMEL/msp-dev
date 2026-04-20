import dash
from dash import html, callback, dcc, Input, Output, State
import dash_bootstrap_components as dbc
from dash_extensions import WebSocket
from ulid import ULID
import dash_daq as daq
# from aiomqtt import Client
import paho.mqtt.client as mqtt
import json
from pydantic import BaseSettings
import traceback
import httpx

# dash.register_page(__name__, path='/')
dash.register_page(__name__, path='/powercontrol', name='Power Control')

class Settings(BaseSettings):
    host: str = "0.0.0.0"
    port: int = 8787
    debug: bool = False
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

    class Config:
        env_prefix = "DASHBOARD_"
        case_sensitive = False

config = Settings()
print(f"config: {config}")



CONTENT_STYLE = {
    "margin-left": "18rem",
    "margin-right": "2rem",
    "padding": "2rem 1rem",
}



shelly_channels = dbc.Row([
                dbc.Col(html.Div([daq.PowerButton(id='shelly_power-button-0', label="Channel 1", color='#14c208', persistence=True)])),
                dbc.Col(html.Div([daq.PowerButton(id='shelly_power-button-1', label="Channel 2", color='#14c208', persistence=True)])),
                dbc.Col(html.Div([daq.PowerButton(id='shelly_power-button-2', label="Channel 3", color='#14c208', persistence=True)])),
                 ])

pdu_outlets = dbc.Row([
                dbc.Col(html.Div([daq.PowerButton(id='pdu_power-button-1', label="Outlet 1", color='#14c208', persistence=True)])),
                dbc.Col(html.Div([daq.PowerButton(id='pdu_power-button-2', label="Outlet 2", color='#14c208', persistence=True)])),
                dbc.Col(html.Div([daq.PowerButton(id='pdu_power-button-3', label="Outlet 3", color='#14c208', persistence=True)])),
                dbc.Col(html.Div([daq.PowerButton(id='pdu_power-button-4', label="Outlet 4", color='#14c208', persistence=True)])),
                dbc.Col(html.Div([daq.PowerButton(id='pdu_power-button-5', label="Outlet 5", color='#14c208', persistence=True)])),
                 ])


datastore_url = f"datastore.{config.daq_id}-system"
# link_url_base = f"http://{config.external_hostname}/msp/dashboardtest"

http_url_base = f"http://{config.external_hostname}:{config.http_port}"
if config.http_use_tls:
    http_url_base = f"https://{config.external_hostname}:{config.https_port}"
ws_url_base = f"ws://{config.external_hostname}:{config.ws_port}"
if config.ws_use_tls:
    ws_url_base = f"wss://{config.external_hostname}:{config.wss_port}"

link_url_base = f"{http_url_base}/msp/dashboardtest"


# print("here:1")
def get_layout():
    layout = html.Div([
        html.H1('Power Control'),
        html.Hr(),
        html.Div(id = 'controller-list'),
        html.Div([
            dbc.Card([
                html.H5("Main Payload", className="card-title"),
                dbc.Stack([
                    dbc.Row(dbc.Col(html.Div("Main Control Shelly"))),
                    shelly_channels
                ], gap=3)
            ]),
        ],
        className="p-4"
        ),
        html.Div([
            dbc.Card([
                html.H5("Physics Payload", className="card-title"),
                dbc.Stack([
                    dbc.Row(dbc.Col(html.Div("Physics Instruments PDU"))),
                    pdu_outlets
                ], gap=3)
            ]),
        ],
        className="p-4"
        ),
        html.Div([
            dbc.Card([
                html.H5("Optics Payload", className="card-title"),
                dbc.Stack([
                    # dbc.Row(dbc.Col(html.Div("Physics Instruments PDU"))),
                    pdu_outlets
                ], gap=3)
            ]),
        ], className="p-4"
        ),
        dcc.Interval(
                id="active-controllers-interval", interval=(1 * 10000), n_intervals=0
            ),
        dcc.Store(id='controller-list', data=[], storage_type='memory'),
        html.Div(id='controller-display'),
        # WebSocket(id="ws", url=f"ws://uasdaq.pmel.noaa.gov/uasdaq/dashboard/wwss/sensor/main")
        # WebSocket(id="ws", url=f"{config.ws_protocol}://{config.external_hostname}/msp/dashboardtest/ws/test/testhome"),
        WebSocket(id="ws", url=f"{ws_url_base}/msp/dashboardtest/ws/test/testhome"),
        # WebSocket(id="ws_pb", url=f"ws://mspbase01:8080/msp/dashboardtest/ws/test/pb")
        # url=f"ws://{config.ws_hostname}/msp/dashboardtest/ws/sensor-registry/main"
        # WebSocket(id="ws_pb", url=f"{config.ws_protocol}://{config.external_hostname}/msp/dashboardtest/ws/test/pb")
        WebSocket(id="ws_pb", url=f"{ws_url_base}/msp/dashboardtest/ws/test/pb"),
    # ], style=CONTENT_STYLE)
    ])
    # try:
    #     send("init request")
    # except Exception as e:
    #     print(e)

    return layout

layout = get_layout()

    

def send(id, value):
    print(f"sending: {value}")
    ser_dict = json.dumps({"id": str(id), "data": str(value)})
    return ser_dict


@callback(
    Output('ws_pb', "send"),
    Input("shelly_power-button-0", "on"),
    Input("shelly_power-button-1", "on"),
    Input("shelly_power-button-2", "on"),
    Input("pdu_power-button-1", "on"),
    Input("pdu_power-button-2", "on"),
    Input("pdu_power-button-3", "on"),
    Input("pdu_power-button-4", "on"),
    Input("pdu_power-button-5", "on")
)  
def send_pb_state(s_pb1, s_pb2, s_pb3, pdu_pb1, pdu_pb2, pdu_pb3, pdu_pb4, pdu_pb5):
    ctx = dash.callback_context
    id = ctx.triggered_id
    value = ctx.triggered[0]['value']
    # id = ctx.triggered[0]['prop_id'].split('.')[0]
    print(send(id, value))
    return send(id, value)

@callback(Output("power_button_message", "children"),
          Output("pdu_light-1", "color"),
          Output("pdu_light-2", "color"),
          Output("pdu_light-3", "color"),
          Output("pdu_light-4", "color"),
          Output("pdu_light-5", "color"),
          Input("ws_pb", "message"))
def message(i):
    ctx = dash.callback_context
    if i:
        print('returned i', i)
        state = i['data']
        if "True" in state:
            color = '#14c208'
        elif "False" in state:
            color = '#e60707'
        else:
            color = '#491a8b'
        return None, color
    else:
        color = '#491a8b'
        return None, color
        # return None, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update


# # startup code
# send("startup request")

@callback(
        Output("controller-display", "children"),
        Input("controller-list", "data")
)
def update_display(stored_data):
    if not stored_data:
            return dbc.Alert("No controllers found.", color="warning")
        
        # Extract the info you want to show from the dictionary
    return [
        html.Div([
            html.B(f"Controller: {c['controller_id']}"),
            html.P(f"Make: {c['make']} | Model: {c['model']}")
        ], style={'borderBottom': '1px solid #ccc', 'marginBottom': '10px'}) 
        for c in stored_data
    ]

@callback(
    Output("controller-list", "data"),
    Input("active-controllers-interval", "n_intervals"),
    State("controller-list", "data")
)
def retrieve_active_controllers(count, current_list):

    update = False
    new_data = []
    rel_path = dash.get_relative_path("/")
    print(f"*** rel_path: {rel_path}")

    try:
        query = {"device_type": "controller"}
        url = f"http://{datastore_url}/controller-instance/registry/get/"
        print(f"controller-instance-get: {url}")
        response = httpx.get(url, params=query)
        results = response.json()
        print(f"results: {results}")
        if "results" in results and results["results"]:
            for doc in results["results"]:
                make = doc["make"]
                model = doc["model"]
                serial_number = doc["serial_number"]
                controller_id = "::".join([make, model, serial_number])
                sampling_system_id = "unknown::unknown::unknown"

                controller = {
                    "controller_id": f"[{controller_id}]({link_url_base}/dash/controller/{controller_id})",
                    "make": make,
                    "model": model,
                    "serial_number": serial_number,
                    "sampling_system_id": f"[{sampling_system_id}]{link_url_base}/uasdaq/dashboard/dash/sampling-system/{sampling_system_id})",
                }
                if current_list is None:
                    current_list = []
                if controller not in current_list:
                    current_list.append(controller)
                    update = True
                new_data.append(controller)

        remove_data = []
        for index, data in enumerate(current_list):
            if data not in new_data:
                update = True
                remove_data.insert(0, index)
        for index in remove_data:
            current_list.pop(index)

        if update:
            return current_list
        else:
            return dash.no_update


    except Exception as e:
        print(f"update_active_controllers error: {e}")
        print(traceback.format_exc())
        return dash.no_update

