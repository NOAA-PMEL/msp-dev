import dash
from flask import Flask
from dash import Dash, html, dcc, Input, Output, State, dash_table
import dash_bootstrap_components as dbc
from dash_extensions import WebSocket
import os
import pandas as pd
from datetime import datetime
# from dash_extensions.enrich import html, dcc, Output, Input, DashProxy, State


server = Flask(__name__, instance_relative_config=False)
app = Dash(
    # app = DashProxy(
    __name__,
    server=server,
    use_pages=True,
    requests_pathname_prefix="/msp/dashboardtest/dash/",#sensor/",
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)

sidebar_header = dbc.Row(
    [
        dbc.Col(html.H4("MSP-DAQ", className="display-4")),
        dbc.Col(
            html.Button(
                # use the Bootstrap navbar-toggler classes to style the toggle
                html.Span(className="navbar-toggler-icon"),
                className="navbar-toggler",
                # the navbar-toggler classes don't set color, so we do it here
                style={
                    "color": "rgba(0,0,0,.5)",
                    "border-color": "rgba(0,0,0,.1)",
                },
                id="toggle",
            ),
            # the column containing the toggle will be only as wide as the
            # toggle, resulting in the toggle being right aligned
            width="auto",
            # vertically align the toggle in the center
            align="center",
        ),
    ]
)

sidebar = html.Div(
    [
        sidebar_header,
        # we wrap the horizontal rule and short blurb in a div that can be
        # hidden on a small screen
        # html.Div(
        #     [
        #         html.Hr(),
        #         html.P(
        #             "A responsive sidebar layout with collapsible navigation " "links.",
        #             className="lead",
        #         ),
        #     ],
        #     id="blurb",
        # ),
        # use the Collapse component to animate hiding / revealing links
        dbc.Collapse(
            dbc.Nav(
                # [
                #     dbc.NavLink("Home", href="/uasdaq/dashboard/sensor", active="exact"),
                #     dbc.NavLink("Page 1", href="/uasdaq/dashboard/sensor1", active="exact"),
                #     dbc.NavLink("Page 2", href="/uasdaq/dashboard/sensor2", active="exact"),
                # ],
                [
                    dbc.NavLink(
                        html.Div(page["title"], className="ms-2"),
                        href=f'/msp/dashboardtest/dash{page["path"]}',
                        # href=dash.get_relative_path(page["path"]),
                        active="exact",
                    )
                    for page in dash.page_registry.values()
                    if page.get("nav_bar", True)
                ],
                vertical=True,
                pills=True,
            ),
            id="collapse",
        ),
        html.Hr(),
        html.Div([
            html.P("Tools", className="small text-uppercase text-muted fw-bold"),
            dbc.Button(
                [html.Span("📜", className="me-2"), "System Logbook"],
                id="global-open-notes",
                color="warning", # Yellow distinguishes it from the blue pills
                className="w-100 shadow-sm fw-bold",
                style={"borderRadius": "8px"}
            ),
        ], style={"padding": "10px"}),
    ],
    id="sidebar"
)


# app.layout = get_layout()
app.layout = html.Div([
                dcc.Location(id="url"),
                sidebar,
                dash.page_container,
                dbc.Offcanvas([
                    html.H5("Notes"),
                    dbc.Textarea(id="global-note-input", placeholder="Enter notes...", style={'height': '150px'}),
                    dbc.Button("Post Note", id="global-save-note-btn", color="primary", className="w-100 mt-2 mb-4"),
                    
                    html.H6("Recent History:"),
                    dash_table.DataTable(
                        id="global-notes-table",
                        columns=[{"name": i, "id": i} for i in ["Timestamp", "Note"]],
                        style_cell={'textAlign': 'left', 'fontSize': '12px', 'whiteSpace': 'normal', 'height': 'auto'},
                        style_header={'backgroundColor': '#f8f9fa', 'fontWeight': 'bold'},
                        page_size=15,
                    )
                ], id="global-offcanvas", title="Shared System Notes", is_open=False, style={"width": "600px"}),
            ])



NOTES_FILE = "system_notes.csv"

def save_shared_note(user_text):
    """Appends a note to the shared CSV file."""
    new_note = pd.DataFrame([{
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Note": user_text
    }])
    header = not os.path.exists(NOTES_FILE)
    new_note.to_csv(NOTES_FILE, mode='a', index=False, header=header)

def load_shared_notes():
    """Reads the shared CSV file."""
    if os.path.exists(NOTES_FILE):
        df = pd.read_csv(NOTES_FILE)
        return df.sort_values(by="Timestamp", ascending=False)
    return pd.DataFrame(columns=["Timestamp", "Note"])


@app.callback(
    Output("sidebar", "className"),
    Input("toggle", "n_clicks"),
    State("sidebar", "className"),
)
def toggle_classname(n, classname):
    if n and classname == "":
        return "collapsed"
    return ""


@app.callback(
    Output("collapse", "is_open"),
    Input("toggle", "n_clicks"),
    State("collapse", "is_open"),
)
def toggle_collapse(n, is_open):
    if n:
        return not is_open
    return is_open

# @app.callback(
#     Output('chat-ws', "send"),
#     [Input("chat-input", "value")]
# )
# def send(value):
#     print(f"sending to chat: {value}")
#     return value

# @app.callback(Output("chat-output", "children"), [Input("chat-ws", "message")])
# def message(e):
#     if e:
#         print(f"e: {e}, data: {e['data']}")
#         return e['data']
#         # return f"Response from chat: {e['data']}"
#     else:
#         return "No response"

@app.callback(
    Output("global-offcanvas", "is_open"),
    Input("global-open-notes", "n_clicks"),
    State("global-offcanvas", "is_open"),
)
def toggle_global_notes(n, is_open):
    if n:
        return not is_open
    return is_open

@app.callback(
    [Output("global-notes-table", "data"),
     Output("global-note-input", "value")],
    [Input("global-save-note-btn", "n_clicks"),
     Input("global-open-notes", "n_clicks")], 
    [State("global-note-input", "value")],
    prevent_initial_call=True
)
def handle_global_notes(save_n, open_n, text):
    ctx = dash.callback_context
    trigger = ctx.triggered[0]['prop_id'].split('.')[0]

    if trigger == "global-save-note-btn" and text:
        save_shared_note(text)
        
    df = load_shared_notes()
    return df.to_dict('records'), ""

if __name__ == "__main__":
    server.run(debug=True)