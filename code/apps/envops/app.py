import os
import pandas as pd
from datetime import datetime
from flask import Flask
import dash
from dash import Dash, html, dcc, Input, Output, State, dash_table, no_update
import dash_bootstrap_components as dbc

# --- INITIALIZATION ---
server = Flask(__name__, instance_relative_config=False)
app = Dash(
    __name__,
    server=server,
    use_pages=True,
    routes_pathname_prefix="/",
    requests_pathname_prefix="/envds/envops/",
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
    suppress_callback_exceptions=True,  # ADDED: Prevents multi-page React crashes
)

# --- SIDEBAR COMPONENT ---
sidebar_header = dbc.Row([
    dbc.Col(html.H4("EnvOps", className="display-6 fw-bold text-primary")),
    dbc.Col(
        html.Button(
            html.Span(className="navbar-toggler-icon"),
            className="navbar-toggler",
            style={"color": "rgba(0,0,0,.5)", "borderColor": "rgba(0,0,0,.1)"},
            id="toggle",
        ),
        width="auto",
        align="center",
    ),
])

sidebar = html.Div(
    [
        sidebar_header,
        html.Hr(),
        dbc.Collapse(
            dbc.Nav(
                [
                    dbc.NavLink(
                        html.Div(page["name"], className="ms-2 fw-bold"),
                        href=page["relative_path"],
                        active="exact",
                    )
                    for page in dash.page_registry.values()
                    if page.get("nav_bar", True) # Exclude drill-down pages from main nav
                ],
                vertical=True,
                pills=True,
            ),
            id="collapse",
        ),
        html.Hr(),
        html.Div([
            html.P("Ops Tools", className="small text-uppercase text-muted fw-bold"),
            dbc.Button(
                "System Logbook",
                id="global-open-notes",
                color="warning",
                className="w-100 shadow-sm fw-bold",
                style={"borderRadius": "8px"}
            ),
        ], style={"padding": "10px"}),
    ],
    id="sidebar"
)

# --- GLOBAL OFFCANVAS (LOGBOOK) ---
NOTES_FILE = "envops_system_notes.csv"

def save_shared_note(operator, user_text):
    new_note = pd.DataFrame([{
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Operator": operator if operator else "Unknown",
        "Note": user_text
    }])
    header = not os.path.exists(NOTES_FILE)
    new_note.to_csv(NOTES_FILE, mode='a', index=False, header=header)

def load_shared_notes():
    if os.path.exists(NOTES_FILE):
        df = pd.read_csv(NOTES_FILE)
        if "Operator" not in df.columns:
            df["Operator"] = "-"
        return df.sort_values(by="Timestamp", ascending=False)
    return pd.DataFrame(columns=["Timestamp", "Operator", "Note"])

offcanvas_logbook = dbc.Offcanvas([
    html.H5("Operations Logbook"),
    html.P("Record global system notes.", className="text-muted small"),
    
    dbc.Label("Operator Name:"),
    dcc.Dropdown(
        id="global-operator-input",
        options=[
            {'label': 'Derek Coffman', 'value': 'Derek Coffman'},
            {'label': 'Hanna Best', 'value': 'Hanna Best'},
            {'label': 'Lucia Upchurch', 'value': 'Lucia Upchurch'},
            {'label': 'Guest', 'value': 'Guest'}
        ],
        placeholder="Select Operator...",
        className="mb-3",
        style={'color': '#333'} 
    ),
    
    dbc.Label("Note:"),
    dbc.Textarea(id="global-note-input", placeholder="Enter details here...", style={'height': '150px'}),
    dbc.Button("Post Note", id="global-save-note-btn", color="primary", className="w-100 mt-3 mb-4"),
    
    html.H6("Recent History:"),
    dash_table.DataTable(
        id="global-notes-table",
        columns=[{"name": i, "id": i} for i in ["Timestamp", "Operator", "Note"]],
        style_cell={'textAlign': 'left', 'fontSize': '12px', 'whiteSpace': 'normal', 'height': 'auto'},
        style_header={'backgroundColor': '#f8f9fa', 'fontWeight': 'bold'},
        style_data_conditional=[{
            'if': {'column_id': 'Operator'},
            'fontWeight': 'bold',
            'color': '#007bff'
        }],
        page_size=15,
    )
], id="global-offcanvas", title="Shared System Notes", is_open=False, style={"width": "600px"})

# --- APP LAYOUT ---
app.layout = html.Div([
    dcc.Location(id="url"),
    sidebar,
    
    # WRAP THE PAGE CONTAINER WITH THE CORRECT ID
    html.Div(
        dash.page_container, 
        id="page-content"
    ), 
    
    offcanvas_logbook
])
# --- CALLBACKS ---
@app.callback(
    Output("sidebar", "className"),
    Input("toggle", "n_clicks"),
    State("sidebar", "className"),
)
def toggle_classname(n, classname):
    if n and classname == "": return "collapsed"
    return ""

@app.callback(
    Output("collapse", "is_open"),
    Input("toggle", "n_clicks"),
    State("collapse", "is_open"),
)
def toggle_collapse(n, is_open):
    if n: return not is_open
    return is_open

@app.callback(
    Output("global-offcanvas", "is_open"),
    Input("global-open-notes", "n_clicks"),
    State("global-offcanvas", "is_open"),
)
def toggle_global_notes(n, is_open):
    if n: return not is_open
    return is_open

@app.callback(
    [Output("global-notes-table", "data"), Output("global-note-input", "value")],
    [Input("global-save-note-btn", "n_clicks"), Input("global-open-notes", "n_clicks")], 
    [State("global-operator-input", "value"), State("global-note-input", "value")],
    prevent_initial_call=True
)
def handle_global_notes(save_n, open_n, operator, text):
    ctx = dash.callback_context
    trigger = ctx.triggered[0]['prop_id'].split('.')[0]

    if trigger == "global-save-note-btn":
        if not operator: return no_update, no_update
        if text: save_shared_note(operator, text)
        
    df = load_shared_notes()
    return df.to_dict('records'), ""

if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8000)