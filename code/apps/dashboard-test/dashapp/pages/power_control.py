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

# pdu_outlets_lights = dbc.Row([
#                 dbc.Col(html.Div([daq.Indicator(id='pdu_light-1', label="Outlet 1", color='#14c208')])),
#                 dbc.Col(html.Div([daq.Indicator(id='pdu_light-2', label="Outlet 2", color='#14c208')])),
#                 dbc.Col(html.Div([daq.Indicator(id='pdu_light-3', label="Outlet 3", color='#14c208')])),
#                 dbc.Col(html.Div([daq.Indicator(id='pdu_light-4', label="Outlet 4", color='#14c208')])),
#                 dbc.Col(html.Div([daq.Indicator(id='pdu_light-5', label="Outlet 5", color='#14c208')])),
#                  ])

# pdu_outlets = dbc.Row([
#                 dbc.Col(html.Div([html.Button("Outlet 1", id='pdu_power-button-1', n_clicks=0)])),
#                 dbc.Col(html.Div([html.Button("Outlet 2", id='pdu_power-button-2', n_clicks=0)])),
#                 dbc.Col(html.Div([html.Button("Outlet 3", id='pdu_power-button-3', n_clicks=0)])),
#                 dbc.Col(html.Div([html.Button("Outlet 4", id='pdu_power-button-4', n_clicks=0)])),
#                 dbc.Col(html.Div([html.Button("Outlet 5", id='pdu_power-button-5', n_clicks=0)])),
#                  ])


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
        html.Div([
            dbc.Stack([
                dbc.Row(dbc.Col(html.Div("Physics Instruments PDU"))),
                pdu_outlets
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
        # html.Div(id="power_button_message"),
        # html.Div([
        #     dbc.Stack([
        #         pdu_outlets_lights
        #     ], gap=3)
        # ]),
        # WebSocket(id="ws", url=f"ws://uasdaq.pmel.noaa.gov/uasdaq/dashboard/wwss/sensor/main")
        WebSocket(id="ws", url=f"ws://mspbase02:8080/msp/dashboardtest/ws/test/testhome"),
        WebSocket(id="ws_pb", url=f"ws://mspbase02:8080/msp/dashboardtest/ws/test/pb")
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

# @callback(Output("power_button_message", "children"),
#           Output("pdu_light-1", "color"),
#           Output("pdu_light-2", "color"),
#           Output("pdu_light-3", "color"),
#           Output("pdu_light-4", "color"),
#           Output("pdu_light-5", "color"),
#           Input("ws_pb", "message"))
# def message(i):
#     ctx = dash.callback_context
#     if i:
#         print('returned i', i)
#         state = i['data']
#         if "True" in state:
#             color = '#14c208'
#         elif "False" in state:
#             color = '#e60707'
#         else:
#             color = '#491a8b'
#         return None, color
#         # if "pdu_power-button-1" == ctx.triggered_id:
#         #     return None, color, dash.no_update, dash.no_update, dash.no_update, dash.no_update
#         # if "pdu_power-button-2" == ctx.triggered_id:
#         #     return None, dash.no_update, color, dash.no_update, dash.no_update, dash.no_update
#         # if "pdu_power-button-3" == ctx.triggered_id:
#         #     return None, dash.no_update, dash.no_update, color, dash.no_update, dash.no_update
#         # if "pdu_power-button-4" == ctx.triggered_id:
#         #     return None, dash.no_update, dash.no_update, dash.no_update, color, dash.no_update
#         # if "pdu_power-button-5" == ctx.triggered_id:
#         #     return None, dash.no_update, dash.no_update, dash.no_update, dash.no_update, color
#     else:
#         color = '#491a8b'
#         return None, color
#         # return None, dash.no_update, dash.no_update, dash.no_update, dash.no_update, dash.no_update


# # startup code
# send("startup request")