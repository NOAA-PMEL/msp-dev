import dash
import json
from dash import html, dcc, callback, Input, Output, State, ctx
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import httpx
import logging
from pydantic import BaseSettings
from dash_extensions import WebSocket

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
        dcc.Store(id="live-fleet-locations", data={}),
        dcc.Store(id="live-health-store", data={}), # Holds live status map
        
        # WebSockets & Timers
        dcc.Interval(id="home-sync-interval", interval=5*60*1000, n_intervals=0),
        WebSocket(id="ws-fleet-status", url=f"{ws_url_base}/envds/envops/ws/fleet/status"),
        WebSocket(id="ws-fleet-telemetry", url=f"{ws_url_base}/envds/envops/ws/fleet/telemetry"),

        dbc.Row([
            dbc.Col(
                dbc.Card([
                    dbc.CardHeader(html.H5("Active Fleet Map", className="mb-0")),
                    dbc.CardBody(dcc.Loading(dcc.Graph(id="fleet-map", style={"height": "600px"})))
                ], className="shadow-sm border-dark"),
                width=7
            ),
            
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
    projects = fetch_registry_data("project")
    deployments = fetch_registry_data("deployment")
    platforms = fetch_registry_data("platform")
    return projects, deployments, platforms

@callback(
    Output("live-health-store", "data"),
    Input("ws-fleet-status", "message"),
    State("live-health-store", "data"),
    prevent_initial_call=True
)
def update_live_health(message, current_health):
    """Listens to fleet-wide status events and updates the health dictionary."""
    if not message or "data" not in message:
        return dash.no_update
        
    try:
        status_data = json.loads(message["data"])
        app_uid = status_data.get("id", {}).get("app_uid", "")
        app_group = status_data.get("id", {}).get("app_group", "")
        state_dict = status_data.get("state", {})
        
        # Determine a basic health state (You can customize this logic based on conditions)
        health = "ok"
        status_text = "AUTO"
        
        if app_group == "systemmode" or "system_mode" in state_dict:
            actual = state_dict.get("system_mode", {}).get("actual", "unknown").lower()
            status_text = actual.upper()
            if actual == "manual":
                health = "warning"
            elif actual == "error" or actual == "degraded":
                health = "danger"

        # Update the store for this specific deployment/UID
        if app_uid:
            if current_health is None:
                current_health = {}
            current_health[app_uid] = {"health": health, "text": status_text}
            return current_health
            
    except Exception as e:
        L.error(f"Fleet Health Stream Error: {e}")
        
    return dash.no_update

@callback(
    Output("projects-accordion-container", "children"),
    Output("fleet-map", "figure"),
    Input("store-projects", "data"),
    Input("store-deployments", "data"),
    Input("live-fleet-locations", "data"),
    Input("live-health-store", "data"), # Re-renders UI when health changes
    prevent_initial_call=True
)
def render_fleet_ui(projects, deployments, live_locations, health_store):
    """Builds the nested Host/Sub UI and processes map logic."""
    if health_store is None: health_store = {}
    if live_locations is None: live_locations = {}
    
    fig = go.Figure(go.Scattermapbox(lat=[], lon=[], hoverinfo="text"))
    fig.update_layout(
        mapbox_style="carto-positron", margin={"r":0,"t":0,"l":0,"b":0},
        mapbox=dict(center=dict(lat=39.8, lon=-98.5), zoom=3)
    )

    if not projects and not deployments:
        return html.P("No active projects found.", className="text-muted"), fig

    # --- 1. Organize Deployments by Project and Host vs Sub ---
    # NOTE: Change 'parent_ref' to 'host_ref' if your GitOps uses a different key
    parent_ref_key = "parent_ref" 
    
    hosts_by_project = {} # { proj_name: { host_name: {"host": dep, "subs": []} } }
    planned_lats, planned_lons, planned_text = [], [], []
    live_lats, live_lons, live_text = [], [], []

    # First pass: map the hosts and their map coordinates
    for dep in deployments:
        dep_data = dep.get("data", {})
        proj_ref = dep_data.get("project_ref", "unknown")
        dep_name = dep.get("metadata", {}).get("name", "Unknown_Deployment")
        
        if proj_ref not in hosts_by_project:
            hosts_by_project[proj_ref] = {}
            
        if not dep_data.get(parent_ref_key):
            # It's a Host
            hosts_by_project[proj_ref][dep_name] = {"host": dep, "subs": []}
            
            h_display = dep_data.get('display_name', dep_name)
            
            # Map coordinates: Check if live location exists by deployment name OR platform ref
            platform_ref = dep_data.get('platform_ref', '')
            live_loc = live_locations.get(dep_name) or live_locations.get(platform_ref)
            
            if live_loc:
                live_lats.append(live_loc["lat"])
                live_lons.append(live_loc["lon"])
                live_text.append(f"{h_display}<br><b>(Live)</b>")
            else:
                # Fallback to planned location
                lat_min = dep_data.get("planned_geospatial_lat_min")
                lon_min = dep_data.get("planned_geospatial_lon_min")
                if lat_min is not None and lon_min is not None:
                    planned_lats.append(lat_min)
                    planned_lons.append(lon_min)
                    planned_text.append(f"{h_display}<br><i>(Estimated/Planned)</i>")

    # Second pass: map the subs to their hosts
    for dep in deployments:
        dep_data = dep.get("data", {})
        parent = dep_data.get(parent_ref_key)
        proj_ref = dep_data.get("project_ref", "unknown")
        if parent and proj_ref in hosts_by_project and parent in hosts_by_project[proj_ref]:
            hosts_by_project[proj_ref][parent]["subs"].append(dep)

    # Add Map Traces
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
            name="Live Locations"
        ))
        # Recenter map if we have live data
        fig.update_layout(mapbox=dict(center=dict(lat=sum(live_lats)/len(live_lats), lon=sum(live_lons)/len(live_lons)), zoom=4))

    # --- 2. Build the UI Hierarchy ---
    accordion_items = []
    
    # Helper for rendering health indicators
    def get_health_badge(uid, display_name):
        h_data = health_store.get(uid, {"health": "secondary", "text": "UNKNOWN"})
        color = "success" if h_data["health"] == "ok" else h_data["health"]
        return dbc.Badge(f"{display_name}: {h_data['text']}", color=color, className="me-2 mb-1")

    for proj in projects:
        proj_name = proj.get("metadata", {}).get("name", "Unknown")
        proj_display = proj.get("data", {}).get("display_name", proj_name)
        proj_hosts = hosts_by_project.get(proj_name, {})
        
        dep_list = []
        proj_health_status = "ok" # Default to OK

        for host_name, group in proj_hosts.items():
            host_data = group["host"]
            subs = group["subs"]
            
            h_display = host_data.get("data", {}).get("display_name", host_name)
            
            # Collect nested healths
            host_health_badge = get_health_badge(host_name, "Host")
            sub_badges = [get_health_badge(s.get("metadata", {}).get("name"), s.get("data", {}).get("display_name", "Sub")) for s in subs]
            
            # Roll up project health logic (If any host/sub is warning, project is warning)
            host_state = health_store.get(host_name, {}).get("health", "ok")
            if host_state == "danger": proj_health_status = "danger"
            elif host_state == "warning" and proj_health_status != "danger": proj_health_status = "warning"
            
            for s in subs:
                s_state = health_store.get(s.get("metadata", {}).get("name"), {}).get("health", "ok")
                if s_state == "danger": proj_health_status = "danger"
                elif s_state == "warning" and proj_health_status != "danger": proj_health_status = "warning"

            # Render Unified Card (Using get_relative_path!)
            btn = dbc.Button(
                "Command & Control ⭢", 
                href=dash.get_relative_path(f"/deployment/{host_name}"), 
                color="info", size="sm", className="mt-2 w-100 fw-bold"
            )
            
            dep_card = dbc.Card([
                dbc.CardHeader(html.H6(h_display, className="mb-0")),
                dbc.CardBody([
                    html.P(f"Platform: {host_data.get('data', {}).get('platform_ref', 'N/A')}", className="small mb-2 text-muted"),
                    html.Div([host_health_badge] + sub_badges, className="mb-3"), # Nested healths shown here
                    btn
                ])
            ], className="mb-3 border-secondary shadow-sm")
            dep_list.append(dep_card)

        if not dep_list:
            dep_list = [html.P("No active deployments in this project.", className="text-muted small")]

        # Render Project Accordion Title with Rolled-Up Health
        title_color = "text-success" if proj_health_status == "ok" else f"text-{proj_health_status}"
        title_icon = "🟢" if proj_health_status == "ok" else ("🟡" if proj_health_status == "warning" else "🔴")
        accordion_title = html.Span([f"📁 {proj_display} ", html.Span(title_icon, className=title_color)])

        accordion_items.append(dbc.AccordionItem(dep_list, title=accordion_title))

    return dbc.Accordion(accordion_items, start_collapsed=False), fig

@callback(
    Output("live-fleet-locations", "data"),
    Input("ws-fleet-telemetry", "message"),
    State("live-fleet-locations", "data"),
    prevent_initial_call=True
)
def update_live_locations(message, current_locations):
    """Listens to fleet-wide nav events and updates the map coordinates."""
    if not message or "data" not in message: 
        return dash.no_update
        
    try:
        payload = json.loads(message["data"])
        target_id = payload.get("target_id") # This could be the deployment or platform name
        data = payload.get("data", {})
        
        variables = data.get("variables", {})
        
        # Check for either 'latitude' or 'lat' depending on your config mapping
        lat = variables.get("latitude", {}).get("data") or variables.get("lat", {}).get("data")
        lon = variables.get("longitude", {}).get("data") or variables.get("lon", {}).get("data")
        
        if lat is not None and lon is not None and target_id:
            if current_locations is None: 
                current_locations = {}
            
            # Rounding to 5 decimal places (~1.1 meters) to avoid micro-jitter re-renders
            lat_val, lon_val = round(float(lat), 5), round(float(lon), 5)
            
            curr_lat = current_locations.get(target_id, {}).get("lat")
            curr_lon = current_locations.get(target_id, {}).get("lon")
            
            # Only update the store (which triggers a map redraw) if they actually moved
            if curr_lat != lat_val or curr_lon != lon_val:
                current_locations[target_id] = {"lat": lat_val, "lon": lon_val}
                return current_locations
                
    except Exception as e:
        L.error(f"Live Location Parse Error: {e}")
        
    return dash.no_update