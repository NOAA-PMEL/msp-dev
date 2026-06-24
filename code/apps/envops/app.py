import os
import json
import pandas as pd
from datetime import datetime
from flask import Flask
import dash
import httpx
import logging
from dash import Dash, html, dcc, Input, Output, State, dash_table, no_update, Patch
import dash_bootstrap_components as dbc
from pydantic import BaseSettings
from dash_extensions import WebSocket

L = logging.getLogger(__name__)

# --- CONFIG ---
class Settings(BaseSettings):
    daq_id: str = "default"
    external_hostname: str = "localhost"
    ws_port: int = 80
    class Config:
        env_prefix = "ENVOPS_"
        case_sensitive = False

config = Settings()
datastore_url = f"datastore.{config.daq_id}-system.svc.cluster.local"
ws_url_base = f"ws://{config.external_hostname}:{config.ws_port}"

# # --- HELPER: REST FETCH ---
# def fetch_registry_data(resource_type: str):
#     url = f"http://{datastore_url}/{resource_type}-definition/registry/ids/get/"
#     docs = []
#     try:
#         timeout = httpx.Timeout(5.0)
#         id_response = httpx.get(url, timeout=timeout)
#         if id_response.status_code == 200:
#             ids = id_response.json().get("results", [])
#             for doc_id in ids:
#                 if doc_id:
#                     doc_url = f"http://{datastore_url}/{resource_type}-definition/registry/get/"
#                     doc_response = httpx.get(doc_url, params={"name": doc_id}, timeout=timeout) 
#                     if doc_response.status_code == 200:
#                         doc_results = doc_response.json().get("results", [])
#                         if doc_results: docs.append(doc_results[0])
#     except Exception as e:
#         L.error(f"Sidebar fetch failed for {resource_type}: {e}")
#     return docs

# --- HELPER: REST FETCH ---
def fetch_registry_data(resource_type: str, query_params: dict = None):
    """Fetches registry documents dynamically without N+1 looping."""
    if query_params is None: 
        query_params = {}
        
    url = f"http://{datastore_url}/{resource_type}-definition/registry/get/"
    try:
        timeout = httpx.Timeout(5.0)
        # RediSearch will return all matching documents directly!
        response = httpx.get(url, params=query_params, timeout=timeout) 
        if response.status_code == 200:
            return response.json().get("results", [])
    except Exception as e:
        L.error(f"Registry fetch failed for {resource_type}: {e}")
    return []

# --- INITIALIZATION ---
server = Flask(__name__, instance_relative_config=False)
app = Dash(
    __name__,
    server=server,
    use_pages=True,
    routes_pathname_prefix="/",
    requests_pathname_prefix="/envds/envops/",
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
    suppress_callback_exceptions=True, 
)

# --- SIDEBAR COMPONENT ---
sidebar_header = dbc.Row([
    dbc.Col([
        html.H4([
            html.I(className="bi bi-radar me-2 text-primary"), 
            "EnvOps"
        ], className="fw-bold text-dark mb-0", style={"fontSize": "1.4rem", "letterSpacing": "0.5px"})
    ]),
    # CHAT TOGGLE BUTTON ADDED HERE
    dbc.Col(
        dbc.Button([html.I(className="bi bi-chat-dots-fill text-primary")], id="nav-chat-btn", color="light", size="sm", className="shadow-sm border rounded-circle"),
        width="auto", className="pe-1"
    ),
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
], className="mb-4 align-items-center border-bottom pb-3")

sidebar = html.Div(
    [
        sidebar_header,
        
        # 1. Global Navigation
        html.Div("Global Views", className="text-muted fw-bold text-uppercase mb-2 px-2", style={"fontSize": "0.65rem", "letterSpacing": "0.5px"}),
        dbc.Nav(
            [
                dbc.NavLink([html.I(className="bi bi-globe-americas me-2"), "Fleet Overview"], href=dash.get_relative_path("/"), active="exact", className="fw-bold mb-1 rounded text-dark"),
                dbc.NavLink([html.I(className="bi bi-server me-2"), "Asset Registry"], href=dash.get_relative_path("/assets"), active="exact", className="fw-bold mb-1 rounded text-dark"),
                dbc.NavLink([html.I(className="bi bi-journal-bookmark me-2"), "Documentation"], href=dash.get_relative_path("/docs"), active="exact", className="fw-bold mb-3 rounded text-dark"),
            ],
            vertical=True,
            pills=True,
        ),
        
        html.Hr(className="text-secondary opacity-25 my-3"),
        
        # 2. Dynamic Mission Hierarchy
        html.Div("Active Missions", className="text-muted fw-bold text-uppercase mb-2 px-2", style={"fontSize": "0.65rem", "letterSpacing": "0.5px"}),
        dcc.Loading(
            html.Div(id="sidebar-dynamic-missions", style={"maxHeight": "50vh", "overflowY": "auto"}, className="pe-1"),
            type="dot"
        ),
        
        html.Hr(className="text-secondary opacity-25 my-3"),
        
        # 3. Ops Tools
        html.Div([
            html.Div("Ops Tools", className="text-muted fw-bold text-uppercase mb-2", style={"fontSize": "0.65rem", "letterSpacing": "0.5px"}),
            dbc.Button(
                [html.I(className="bi bi-journal-text me-2"), "System Logbook"],
                id="global-open-notes",
                color="dark",
                outline=True,
                className="w-100 shadow-sm fw-bold text-start px-3",
                style={"borderRadius": "6px"}
            ),
        ], className="px-2"),
        
        dcc.Interval(id="sidebar-interval", interval=60000, n_intervals=0) 
    ],
    id="sidebar",
    className="bg-light shadow-sm border-end"
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
        if "Operator" not in df.columns: df["Operator"] = "-"
        return df.sort_values(by="Timestamp", ascending=False)
    return pd.DataFrame(columns=["Timestamp", "Operator", "Note"])

offcanvas_logbook = dbc.Offcanvas([
    html.Div([
        html.H5([html.I(className="bi bi-journal-bookmark-fill me-2 text-primary"), "Operations Logbook"], className="fw-bold mb-0"),
        html.P("Record and review global system events.", className="text-muted small mt-1 border-bottom pb-3"),
    ]),
    
    dbc.Label("Operator Name", className="fw-bold text-muted text-uppercase mt-2", style={"fontSize": "0.65rem", "letterSpacing": "0.5px"}),
    dcc.Dropdown(
        id="global-operator-input",
        options=[
            {'label': 'Derek Coffman', 'value': 'Derek Coffman'},
            {'label': 'Hanna Best', 'value': 'Hanna Best'},
            {'label': 'Lucia Upchurch', 'value': 'Lucia Upchurch'},
            {'label': 'Guest', 'value': 'Guest'}
        ],
        placeholder="Select Operator...",
        className="mb-3 shadow-sm"
    ),
    
    dbc.Label("Log Entry", className="fw-bold text-muted text-uppercase", style={"fontSize": "0.65rem", "letterSpacing": "0.5px"}),
    dbc.Textarea(id="global-note-input", placeholder="Enter operational details, hardware changes, or mission events...", style={'height': '120px'}, className="shadow-sm"),
    
    dbc.Button([html.I(className="bi bi-send me-2"), "Post to Logbook"], id="global-save-note-btn", color="primary", className="w-100 mt-3 mb-4 fw-bold shadow-sm"),
    
    html.Div("Recent History", className="fw-bold text-muted text-uppercase border-bottom pb-2 mb-3", style={"fontSize": "0.65rem", "letterSpacing": "0.5px"}),
    
    dash_table.DataTable(
        id="global-notes-table",
        columns=[{"name": i, "id": i} for i in ["Timestamp", "Operator", "Note"]],
        style_cell={'textAlign': 'left', 'fontSize': '12px', 'whiteSpace': 'normal', 'height': 'auto', 'fontFamily': 'sans-serif'},
        style_header={'backgroundColor': '#f8f9fa', 'fontWeight': 'bold', 'textTransform': 'uppercase', 'fontSize': '10px'},
        style_data_conditional=[{'if': {'column_id': 'Operator'}, 'fontWeight': 'bold', 'color': '#0d6efd'}],
        page_size=15,
        style_table={'overflowX': 'auto'}
    )
], id="global-offcanvas", title="", is_open=False, style={"width": "600px"}, className="border-start shadow")


# --- EPHEMERAL LIVE CHAT WIDGET ---
floating_chat_widget = html.Div([
    dbc.Card([
        dbc.CardHeader([
            html.I(className="bi bi-chat-dots-fill me-2"), 
            "Comms Channel",
            html.Button(html.I(className="bi bi-x-lg"), id="close-chat-btn", className="btn-close btn-close-white float-end", style={"fontSize": "0.6rem"})
        ], className="bg-primary text-white py-2 fw-bold small shadow-sm"),
        
        dbc.CardBody([
            dcc.Dropdown(
                id="chat-user-select",
                options=[
                    {'label': 'Derek Coffman', 'value': 'Derek'},
                    {'label': 'Hanna Best', 'value': 'Hanna'},
                    {'label': 'Lucia Upchurch', 'value': 'Lucia'},
                    {'label': 'Guest', 'value': 'Guest'}
                ],
                placeholder="Identify yourself...",
                className="mb-2 shadow-sm"  # <-- Removed size="sm" from here!
            ),
            
            # Chat history rendering box
            html.Div(
                id="chat-messages-container", 
                children=[], 
                style={"height": "220px", "overflowY": "auto", "fontSize": "0.85rem"}, 
                className="mb-2 p-2 bg-light border rounded shadow-inner"
            ),
            
            dbc.InputGroup([
                dbc.Input(id="chat-message-input", placeholder="Message fleet...", size="sm", className="border-end-0"),
                dbc.Button(html.I(className="bi bi-send"), id="chat-send-btn", color="primary", size="sm")
            ], className="shadow-sm")
        ], className="p-2")
    ], className="shadow-lg border-0 h-100")
], id="chat-floating-window", style={
    "position": "fixed", 
    "bottom": "20px", 
    "right": "20px", 
    "width": "320px", 
    "zIndex": 1050, 
    "display": "none",
    "borderRadius": "8px"
})


# --- APP LAYOUT ---
app.layout = html.Div([
    dcc.Location(id="url"),
    sidebar,
    html.Div(dash.page_container, id="page-content"), 
    offcanvas_logbook,
    floating_chat_widget,
    WebSocket(id="ws-chat-channel", url=f"{ws_url_base}/envds/envops/ws/chat")
])

# --- CALLBACKS ---

# 1. Toggle Chat Widget Visibility
@app.callback(
    Output("chat-floating-window", "style"),
    [Input("nav-chat-btn", "n_clicks"), Input("close-chat-btn", "n_clicks")],
    State("chat-floating-window", "style"),
    prevent_initial_call=True
)
def toggle_chat(open_clicks, close_clicks, style):
    ctx = dash.callback_context
    if not ctx.triggered: return dash.no_update
    trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]

    if trigger_id == "nav-chat-btn":
        style["display"] = "block"
    elif trigger_id == "close-chat-btn":
        style["display"] = "none"
    return style

# 2. Transmit Chat Message
@app.callback(
    Output("ws-chat-channel", "send"),
    Output("chat-message-input", "value"),
    Input("chat-send-btn", "n_clicks"),
    Input("chat-message-input", "n_submit"),
    State("chat-message-input", "value"),
    State("chat-user-select", "value"),
    prevent_initial_call=True
)
def send_chat(n_clicks, n_submit, message, user):
    if not message or not user:
        return dash.no_update, dash.no_update
        
    payload = json.dumps({
        "user": user, 
        "message": message, 
        "timestamp": datetime.now().strftime("%H:%M:%S")
    })
    
    # Return payload to send to WS, and clear the input box
    return payload, ""

# 3. Receive & Display Chat Message (Using Patch to prevent re-renders)
@app.callback(
    Output("chat-messages-container", "children"),
    Input("ws-chat-channel", "message"),
    prevent_initial_call=True
)
def receive_chat(msg):
    if not msg or "data" not in msg: return dash.no_update
        
    data = json.loads(msg["data"])
    
    # Build a clean message bubble
    new_msg = html.Div([
        html.Div([
            html.Span(data['user'], className="fw-bold text-primary", style={"fontSize": "0.75rem"}),
            html.Span(data['timestamp'], className="text-muted ms-2", style={"fontSize": "0.6rem"}),
        ]),
        html.Div(data['message'], className="bg-white border rounded p-1 shadow-sm mt-1 mb-2", style={"fontSize": "0.85rem", "display": "inline-block"})
    ], className="mb-1")
    
    # Append it directly to the DOM using Patch()
    patched_list = Patch()
    patched_list.append(new_msg)
    
    return patched_list


@app.callback(
    Output("sidebar-dynamic-missions", "children"),
    Input("sidebar-interval", "n_intervals")
)
def update_sidebar_missions(n):
    """Fetches deployments and builds a nested navigation accordion."""
    deployments = fetch_registry_data("deployment")
    projects = fetch_registry_data("project")
    
    if not deployments:
        return html.P("No active missions.", className="text-muted small px-2 fst-italic")
        
    platform_to_dep = {d.get("data", {}).get("platform_ref"): d for d in deployments}
    
    # Isolate only the Top-Level Host deployments (like enc01) for the sidebar
    host_deployments = []
    for dep in deployments:
        host_pref = dep.get("data", {}).get("host_platform_ref")
        if not host_pref or host_pref not in platform_to_dep:
            host_deployments.append(dep)
            
    # Group Hosts by Project
    hosts_by_project = {}
    for dep in host_deployments:
        proj_ref = dep.get("data", {}).get("project_ref", "Unknown Project")
        if proj_ref not in hosts_by_project:
            hosts_by_project[proj_ref] = []
        hosts_by_project[proj_ref].append(dep)
        
    proj_dict = {p.get("metadata", {}).get("name"): p.get("data", {}).get("display_name", p.get("metadata", {}).get("name")) for p in projects}
    
    accordion_items = []
    for proj_ref, hosts in hosts_by_project.items():
        proj_name = proj_dict.get(proj_ref, proj_ref)
        
        host_links = []
        for host in hosts:
            host_id = host.get("metadata", {}).get("name")
            host_display = host.get("data", {}).get("display_name", host_id)
            
            host_links.append(html.Div([
                html.Span([html.I(className="bi bi-hdd-network me-2 text-primary"), host_display], className="d-block fw-bold small text-dark mb-1 mt-3 px-2 text-uppercase", style={"letterSpacing": "0.5px"}),
                dbc.NavLink([html.I(className="bi bi-sliders me-2"), "Command & Control"], href=dash.get_relative_path(f"/deployment/{host_id}"), active="exact", className="small py-1 rounded text-muted fw-bold"),
                dbc.NavLink([html.I(className="bi bi-graph-up me-2"), "Telemetry Plots"], href=dash.get_relative_path(f"/variablesets/{host_id}"), active="exact", className="small py-1 rounded text-muted fw-bold border-bottom pb-2")
            ]))
            
        accordion_items.append(
            dbc.AccordionItem(
                dbc.Nav(host_links, vertical=True, pills=True), 
                title=html.Div([html.I(className="bi bi-folder2-open me-2"), proj_name], className="fw-bold"), 
                class_name="bg-transparent border-0 px-0"
            )
        )
        
    return dbc.Accordion(accordion_items, flush=True, start_collapsed=False)

@app.callback(Output("sidebar", "className"), Input("toggle", "n_clicks"), State("sidebar", "className"))
def toggle_classname(n, classname):
    base_classes = "bg-light shadow-sm border-end"
    if n and "collapsed" not in classname: 
        return f"{base_classes} collapsed"
    return base_classes

@app.callback(Output("collapse", "is_open"), Input("toggle", "n_clicks"), State("collapse", "is_open"))
def toggle_collapse(n, is_open):
    if n: return not is_open
    return is_open

@app.callback(Output("global-offcanvas", "is_open"), Input("global-open-notes", "n_clicks"), State("global-offcanvas", "is_open"))
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