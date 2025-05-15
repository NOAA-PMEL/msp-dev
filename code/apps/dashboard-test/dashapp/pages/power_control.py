import dash
from dash import html, callback, dcc, Input, Output
import dash_bootstrap_components as dbc
from dash_extensions import WebSocket
from ulid import ULID
import dash_daq as daq
# from aiomqtt import Client
import paho.mqtt.client as mqtt
import json

# dash.register_page(__name__, path='/')
dash.register_page(__name__, path='/powercontrol', name='Power Control')


CONTENT_STYLE = {
    "margin-left": "18rem",
    "margin-right": "2rem",
    "padding": "2rem 1rem",
}


shelly_channels = dbc.Row([
                dbc.Col(html.Div([daq.PowerButton(id='power-button-1', label="Channel 1", color='#14c208', persistence=True)])),
                dbc.Col(html.Div([daq.PowerButton(id='power-button-2', label="Channel 2", color='#14c208', persistence=True)])),
                dbc.Col(html.Div([daq.PowerButton(id='power-button-3', label="Channel 3", color='#14c208', persistence=True)])),
                 ])




# print("here:1")
def get_layout():
    layout = html.Div([
        html.H1('This is our power control page'),
        html.Hr(),
        html.Div([
            dbc.Stack([
                dbc.Row(dbc.Col(html.Div("Main Control Shelly"))),
                shelly_channels
            ], gap=3)
        ]),
        # dbc.Card('This is our Home page content.', body=True),
        # html.Div('This is our Home page content.'),
        # dcc.Input(id="input", autoComplete="off", debounce=True),
        # html.Div(id="message"),
        # html.Div([
        #     daq.PowerButton(
        #         id='power-button-1',
        #         color='#14c208',
        #         persistence=True
        #     )
        # ]),
        html.Div(id="power_button_message"),
        html.Div([
            daq.Indicator(
                id='indicator-1',
                label='sensor indicator',
                # color='#14c208'
            )
        ]),
        # WebSocket(id="ws", url=f"ws://uasdaq.pmel.noaa.gov/uasdaq/dashboard/wwss/sensor/main")
        WebSocket(id="ws", url=f"ws://mspbase01:8080/msp/dashboardtest/ws/test/testhome"),
        WebSocket(id="ws_pb", url=f"ws://mspbase01:8080/msp/dashboardtest/ws/test/pb")
    # ], style=CONTENT_STYLE)
    ])
    # try:
    #     send("init request")
    # except Exception as e:
    #     print(e)

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
    

def send(id, value):
    print(f"sending: {value}")
    ser_dict = json.dumps({"id": str(id), "data": str(value)})
    return ser_dict


@callback(
    Output('ws_pb', "send"),
    Input("power-button-1", "on"),
    Input("power-button-2", "on"),
    Input("power-button-3", "on")
)  
def send_pb_state(pb1, pb2, pb3):
    ctx = dash.callback_context
    id = ctx.triggered_id
    value = ctx.triggered[0]['value']
    # id = ctx.triggered[0]['prop_id'].split('.')[0]
    print(send(id, value))
    return send(id, value)

@callback(Output("power_button_message", "children"),
          Output("indicator-1", "color"),
          Input("ws_pb", "message"))
def message(i):
    if i:
        print('returned i', i)
        state = i['data']
        if "True" in state:
            color = '#14c208'
        elif "False" in state:
            color = '#e60707'

        else:
            color = '#491a8b'
        # return f"Response from websocket: {i['data']}", color
        return None, color
    else:
        color = '#491a8b'
        # return "No response", color
        return None, color


# # startup code
# send("startup request")