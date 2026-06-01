import os
import pandas as pd
from datetime import datetime
from flask import Flask
import dash
import httpx
import logging
from dash import Dash, html, dcc, Input, Output, State, dash_table, no_update
import dash_bootstrap_components as dbc
from pydantic import BaseSettings

L = logging.getLogger(__name__)

# --- CONFIG ---
class Settings(BaseSettings):
    daq_id: str = "default"
    class Config:
        env_prefix = "ENVOPS_"
        case_sensitive = False

config = Settings()
datastore_url = f"datastore.{config.daq_id}-system.svc.cluster.local"

# --- HELPER: REST FETCH ---
def fetch_registry_data(resource_type: str):
    url = f"http://{datastore_url}/{resource_type}-definition/registry/ids/get/"
    docs = []
    try:
        timeout = httpx.Timeout(5.0)
        id_response = httpx.get(url, timeout=timeout)
        if id_response.status_code == 200:
            ids = id_response.json().get("results", [])
            for doc_id in ids:
                if doc_id:
                    doc_url = f"http://{datastore_url}/{resource_type}-definition/registry/get/"
                    doc_response = httpx.get(doc_url, params={"name": doc_id}, timeout=timeout) 
                    if doc_response.status_code == 200:
                        doc_results = doc_response.json().get("results", [])
                        if doc_results: docs.append(doc_results[0])
    except Exception as e:
        L.error(f"Sidebar fetch failed for {resource_type}: {e}")
    return docs

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
    dbc.Col(html.H4("EnvOps", className="display-6 fw-bold text-primary mb-0")),
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
], className="mb-4 align-items-center")

sidebar = html.Div(
    [
        sidebar_header,
        
        # 1. Global Navigation
        html.H6("Global Views", className="text-muted small text-uppercase fw-bold px-2 mb-2"),
        dbc.Nav(
            [
                dbc.NavLink("⎈ Fleet Overview", href=dash.get_relative_path("/"), active="exact", className="fw-bold mb-1 rounded"),
                dbc.NavLink("🗄 Asset Registry", href=dash.get_relative_path("/assets"), active="exact", className="fw-bold mb-3 rounded"),
            ],
            vertical=True,
            pills=True,
        ),
        
        html.Hr(className="text-secondary"),
        
        # 2. Dynamic Mission Hierarchy
        html.H6("Active Missions", className="text-muted small text-uppercase fw-bold px-2 mb-2"),
        dcc.Loading(
            html.Div(id="sidebar-dynamic-missions", style={"maxHeight": "50vh", "overflowY": "auto"}),
            type="dot"
        ),
        
        html.Hr(className="text-secondary"),
        
        # 3. Ops Tools
        html.Div([
            html.H6("Ops Tools", className="small text-uppercase text-muted fw-bold mb-2"),
            dbc.Button(
                "System Logbook",
                id="global-open-notes",
                color="warning",
                className="w-100 shadow-sm fw-bold text-dark",
                style={"borderRadius": "8px"}
            ),
        ]),
        
        # Refreshes the dynamic sidebar every 60 seconds
        dcc.Interval(id="sidebar-interval", interval=60000, n_intervals=0) 
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
        if "Operator" not in df.columns: df["Operator"] = "-"
        return df.sort_values(by="Timestamp", ascending=False)
    return pd.DataFrame(columns=["Timestamp", "Operator", "Note"])

offcanvas_logbook = dbc.Offcanvas([
    html.H5("Operations Logbook", className="fw-bold text-primary"),
    html.P("Record global system notes.", className="text-muted small"),
    
    dbc.Label("Operator Name:", className="fw-bold small"),
    dcc.Dropdown(
        id="global-operator-input",
        options=[
            {'label': 'Derek Coffman', 'value': 'Derek Coffman'},
            {'label': 'Hanna Best', 'value': 'Hanna Best'},
            {'label': 'Lucia Upchurch', 'value': 'Lucia Upchurch'},
            {'label': 'Guest', 'value': 'Guest'}
        ],
        placeholder="Select Operator...",
        className="mb-3"
    ),
    
    dbc.Label("Note:", className="fw-bold small"),
    dbc.Textarea(id="global-note-input", placeholder="Enter details here...", style={'height': '150px'}),
    dbc.Button("Post Note", id="global-save-note-btn", color="primary", className="w-100 mt-3 mb-4 fw-bold shadow-sm"),
    
    html.H6("Recent History:", className="fw-bold border-bottom pb-2"),
    dash_table.DataTable(
        id="global-notes-table",
        columns=[{"name": i, "id": i} for i in ["Timestamp", "Operator", "Note"]],
        style_cell={'textAlign': 'left', 'fontSize': '12px', 'whiteSpace': 'normal', 'height': 'auto'},
        style_header={'backgroundColor': '#f8f9fa', 'fontWeight': 'bold'},
        style_data_conditional=[{'if': {'column_id': 'Operator'}, 'fontWeight': 'bold', 'color': '#007bff'}],
        page_size=15,
    )
], id="global-offcanvas", title="Shared System Notes", is_open=False, style={"width": "600px"})

# --- APP LAYOUT ---
app.layout = html.Div([
    dcc.Location(id="url"),
    sidebar,
    html.Div(dash.page_container, id="page-content"), 
    offcanvas_logbook
])

# --- CALLBACKS ---

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
                html.Span(host_display, className="d-block fw-bold small text-dark mb-1 mt-2 px-2"),
                dbc.NavLink("🎛 Command & Control", href=dash.get_relative_path(f"/deployment/{host_id}"), active="exact", className="small py-1 rounded text-muted"),
                dbc.NavLink("📈 Telemetry Plots", href=dash.get_relative_path(f"/variablesets/{host_id}"), active="exact", className="small py-1 rounded text-muted border-bottom pb-2")
            ]))
            
        accordion_items.append(
            dbc.AccordionItem(
                dbc.Nav(host_links, vertical=True, pills=True), 
                title=f"🗂 {proj_name}", 
                class_name="bg-transparent border-0 px-0"
            )
        )
        
    return dbc.Accordion(accordion_items, flush=True, start_collapsed=False)

@app.callback(Output("sidebar", "className"), Input("toggle", "n_clicks"), State("sidebar", "className"))
def toggle_classname(n, classname):
    if n and classname == "": return "collapsed"
    return ""

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