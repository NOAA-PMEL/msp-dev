import dash
from dash import html, dcc, callback, Input, Output, State, ctx
import dash_bootstrap_components as dbc
import plotly.express as px
import plotly.graph_objects as go
import httpx
import logging
from pydantic import BaseSettings
import json

L = logging.getLogger(__name__)

dash.register_page(
    __name__,
    path="/",
    name="Fleet Overview",
    nav_bar=True
)

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

# --- HELPER: REST FETCH ---
def fetch_registry_data(resource_type: str):
    """Fetches GitOps definitions synchronously for initial layout rendering."""
    url = f"http://{datastore_url}/{resource_type}-definition/registry/ids/get/"
    docs = []
    try:
        timeout = httpx.Timeout(10.0)
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
        L.error(f"Failed to fetch {resource_type} definitions: {e}")
    return docs

# --- LAYOUT ---
def layout():
    return html.Div([
        dbc.Row([
            dbc.Col(html.H2("Fleet Operations", className="text-primary"), width=8),
            dbc.Col(
                dbc.Button("Refresh Fleet Data", id="home-refresh-btn", color="secondary", className="float-end"),
                width=4
            )
        ], className="mb-4 mt-3"),

        # Caching Stores
        dcc.Store(id="store-projects", data=[]),
        dcc.Store(id="store-deployments", data=[]),
        dcc.Store(id="store-platforms", data=[]),
        dcc.Store(id="live-fleet-locations", data={}), # Holds live lat/lon data
        
        # Background updater
        dcc.Interval(id="home-sync-interval", interval=5*60*1000, n_intervals=0),

        dbc.Row([
            # Left Column: Map
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader(html.H5("Active Fleet Map", className="mb-0")),
                    dbc.CardBody(dcc.Loading(dcc.Graph(id="fleet-map", style={"height": "600px"})))
                ], className="shadow-sm border-dark"),
                width=7
            ),
            
            # Right Column: Projects & Deployments Accordion
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader(html.H5("Active Projects", className="mb-0")),
                    dbc.CardBody(
                        dcc.Loading(html.Div(id="projects-accordion-container", style={"maxHeight": "600px", "overflowY": "auto"}))
                    )
                ], className="shadow-sm border-dark"),
                width=5
            )
        ])
    ])

# --- CALLBACKS ---
@callback(
    Output("store-projects", "data"),
    Output("store-deployments", "data"),
    Output("store-platforms", "data"),
    Input("home-sync-interval", "n_intervals"),
    Input("home-refresh-btn", "n_clicks"),
    prevent_initial_call=False
)
def sync_fleet_state(n_intervals, n_clicks):
    """Fetches GitOps definitions from Datastore."""
    projects = fetch_registry_data("project")
    deployments = fetch_registry_data("deployment")
    platforms = fetch_registry_data("platform")
    return projects, deployments, platforms

@callback(
    Output("projects-accordion-container", "children"),
    Output("fleet-map", "figure"),
    Input("store-projects", "data"),
    Input("store-deployments", "data"),
    Input("live-fleet-locations", "data"),
    prevent_initial_call=True
)
def render_fleet_ui(projects, deployments, live_locations):
    """Builds the UI based on definitions and handles the Planned vs Live map logic."""
    
    # 1. Base Map Figure
    fig = go.Figure(go.Scattermapbox(lat=[], lon=[], hoverinfo="text"))
    fig.update_layout(
        mapbox_style="carto-positron", 
        margin={"r":0,"t":0,"l":0,"b":0},
        mapbox=dict(center=dict(lat=39.8, lon=-98.5), zoom=3)
    )

    if not projects and not deployments:
        return html.P("No active projects found.", className="text-muted"), fig

    deps_by_project = {}
    planned_lats, planned_lons, planned_text = [], [], []
    live_lats, live_lons, live_text = [], [], []

    # 2. Process Deployments and Route to Traces
    for dep in deployments:
        dep_data = dep.get("data", {})
        proj_ref = dep_data.get("project_ref", "unknown")
        dep_name = dep.get("metadata", {}).get("name", "Unknown_Deployment")
        dep_display = dep_data.get("display_name", dep_name)
        
        if proj_ref not in deps_by_project:
            deps_by_project[proj_ref] = []
        deps_by_project[proj_ref].append(dep)

        # Check if we have live data for this deployment
        if live_locations and dep_name in live_locations:
            live_lats.append(live_locations[dep_name]["lat"])
            live_lons.append(live_locations[dep_name]["lon"])
            live_text.append(f"{dep_display}<br><b>(Live)</b>")
        else:
            # Fallback to planned bounding box center
            lat_min = dep_data.get("planned_geospatial_lat_min")
            lon_min = dep_data.get("planned_geospatial_lon_min")
            if lat_min is not None and lon_min is not None:
                planned_lats.append(lat_min)
                planned_lons.append(lon_min)
                planned_text.append(f"{dep_display}<br><i>(Estimated/Planned)</i>")

    # 3. Add Traces to Map
    if planned_lats:
        fig.add_trace(go.Scattermapbox(
            lat=planned_lats, lon=planned_lons, text=planned_text,
            mode='markers', marker=go.scattermapbox.Marker(size=12, color='gray', opacity=0.6),
            name="Planned Locations"
        ))
    
    if live_lats:
        fig.add_trace(go.Scattermapbox(
            lat=live_lats, lon=live_lons, text=live_text,
            mode='markers', marker=go.scattermapbox.Marker(size=14, color='red'),
            name="Live Operations"
        ))
        # Recenter map on live data if available
        fig.update_layout(mapbox=dict(center=dict(lat=sum(live_lats)/len(live_lats), lon=sum(live_lons)/len(live_lons)), zoom=4))

    # 4. Build Accordion
    accordion_items = []
    for proj in projects:
        proj_name = proj.get("metadata", {}).get("name", "Unknown")
        proj_display = proj.get("data", {}).get("display_name", proj_name)
        
        proj_deps = deps_by_project.get(proj_name, [])
        dep_list = []
        
        for dep in proj_deps:
            dep_name = dep.get("metadata", {}).get("name")
            dep_display = dep.get("data", {}).get("display_name", dep_name)
            
            btn = dbc.Button(
                "Command & Control ⭢", 
                href=f"/envds/envops/deployment/{dep_name}",
                color="info", size="sm", className="mt-2 w-100 fw-bold"
            )
            
            dep_card = dbc.Card(dbc.CardBody([
                html.H6(dep_display, className="card-title"),
                html.P(f"Platform: {dep.get('data', {}).get('platform_ref', 'N/A')}", className="card-text small mb-1"),
                btn
            ]), className="mb-3 border-secondary shadow-sm")
            dep_list.append(dep_card)

        if not dep_list:
            dep_list = [html.P("No active deployments in this project.", className="text-muted small")]

        accordion_items.append(dbc.AccordionItem(dep_list, title=f"📁 {proj_display}"))

    return dbc.Accordion(accordion_items, start_collapsed=False), fig