import dash
from dash import html, callback, dcc, Input, Output
import dash_bootstrap_components as dbc
from dash_extensions import WebSocket
from ulid import ULID
import dash_daq as daq
from datetime import date

# dash.register_page(__name__, path='/')
dash.register_page(__name__, path='/home')

CONTENT_STYLE = {
    "margin-left": "18rem",
    "margin-right": "2rem",
    "padding": "2rem 1rem",
}

# print("here:1")
def get_layout():
    layout = html.Div([
        html.H1('This is our Home page'),
        # dbc.Card('This is our Home page content.', body=True),
        # html.Div('This is our Home page content.'),
        # dcc.Input(id="input", autoComplete="off", debounce=True),
        # html.Div(id="message"),
        # html.Div([
        #     daq.PowerButton(
        #         id='power-button-1',
        #         color='#14c208'
        #     )
        # ]),
        # html.Div(id="power_button_message"),
        # html.Div([
        #     daq.StopButton(
        #         id='stop-button-1'
        #     )
        # ]),
        html.Div([
            dcc.DatePickerRange(
                id='my-date-picker-range',
                min_date_allowed=date(1995, 8, 5),
                max_date_allowed=date(2017, 9, 19),
                initial_visible_month=date(2017, 8, 5),
                end_date=date(2017, 8, 25)
            ),
            html.Div(id='output-container-date-picker-range')
        ]),
        html.Div([
            daq.BooleanSwitch(
                id='boolean-switch-1',
                on=False
            )
        ]),
        html.Div([
            daq.NumericInput(
                id='numeric-input-1',
                value=0
            )
        ]),
        # html.Div([
        #     daq.Indicator(
        #         id='indicator-1',
        #         label='sensor name',
        #         # color='#14c208'
        #     )
        # ]),
        # WebSocket(id="ws", url=f"ws://uasdaq.pmel.noaa.gov/uasdaq/dashboard/wwss/sensor/main")
        # WebSocket(id="ws", url=f"ws://mspbase01:8080/msp/dashboardtest/ws/test/testhome"),
        # WebSocket(id="ws_pb", url=f"ws://mspbase01:8080/msp/dashboardtest/ws/test/pb")
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
    

# @callback(
#     Output('ws_pb', "send"),
#     Input("power-button-1", "on")
# )
# def send(value):
#     print(f"sending: {value}")
#     return str(value)

# @callback(Output("power_button_message", "children"),
#           Output("indicator-1", "color"),
#           Input("ws_pb", "message"))
# def message(i):
#     if i:
#         state = i['data']
#         if "True" in state:
#             color = '#14c208'
#         elif "False" in state:
#             color = '#e60707'
#         else:
#             color = '#491a8b'
#         return f"Response from websocket: {i['data']}", color
#     else:
#         color = '#491a8b'
#         return "No response", color

@callback(
    Output('output-container-date-picker-range', 'children'),
    Input('my-date-picker-range', 'start_date'),
    Input('my-date-picker-range', 'end_date'))
def update_output(start_date, end_date):
    string_prefix = 'You have selected: '
    if start_date is not None:
        start_date_object = date.fromisoformat(start_date)
        start_date_string = start_date_object.strftime('%B %d, %Y')
        string_prefix = string_prefix + 'Start Date: ' + start_date_string + ' | '
    if end_date is not None:
        end_date_object = date.fromisoformat(end_date)
        end_date_string = end_date_object.strftime('%B %d, %Y')
        string_prefix = string_prefix + 'End Date: ' + end_date_string
    if len(string_prefix) == len('You have selected: '):
        return 'Select a date to see it displayed here'
    else:
        return string_prefix


# # startup code
# send("startup request")