import dash
import json
from dash import html, dcc, callback, Input, Output, State, ctx, Patch
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import httpx
import logging
from pydantic import BaseSettings
from dash_extensions import WebSocket
from datetime import datetime, timezone

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

# # --- HELPER: REST FETCH ---
# def fetch_registry_data(resource_type: str):
#     url = f"http://{datastore_url}/{resource_type}-definition/registry/ids/get/"
#     docs = []
#     try:
#         timeout = httpx.Timeout(10.0)
#         id_response = httpx.get(url, timeout=timeout)
#         if id_response.status_code == 200:
#             ids = id_response.json().get("results", [])
#             for doc_id in ids:
#                 if doc_id:
#                     doc_url = f"http://{datastore_url}/{resource_type}-definition/registry/get/"
#                     doc_response = httpx.get(doc_url, params={"name": doc_id}, timeout=timeout) 
#                     if doc_response.status_code == 200:
#                         doc_results = doc_response.json().get("results", [])
#                         if doc_results: 
#                             # Safe for overlapping names across namespaces!
#                             docs.extend(doc_results)
#     except Exception as e:
#         L.error(f"Failed to fetch {resource_type} definitions: {e}")
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

# --- LAYOUT ---
# --- LAYOUT ---
def layout():
    base_fig = go.Figure()
    # Trace 0: Planned Locations (Index 0 in Patch)
    base_fig.add_trace(go.Scattermapbox(lat=[], lon=[], text=[], mode='markers', marker=dict(size=10, color='gray', opacity=0.5), name="Planned Locations")) 
    # Trace 1: Live Locations (Index 1 in Patch)
    base_fig.add_trace(go.Scattermapbox(lat=[], lon=[], text=[], mode='markers', marker=dict(size=14, color='#0d6efd'), name="Live Locations")) 
    
    base_fig.update_layout(
        mapbox_style="carto-positron", 
        margin={"r":0,"t":0,"l":0,"b":0},
        mapbox=dict(center=dict(lat=20, lon=0), zoom=1.5), # Zoomed out for global view
        uirevision="constant-fleet-map",
        legend=dict(yanchor="top", y=0.99, xanchor="left", x=0.01, bgcolor="rgba(255,255,255,0.8)")
    )

    return html.Div([
        # --- HEADER ---
        dbc.Row([
            dbc.Col(html.H2([html.I(className="bi bi-globe-americas me-3 text-primary"), "Global Fleet Overview"], className="fw-bold mb-0 text-dark"), width=8, align="center"),
            dbc.Col(
                dbc.Button([html.I(className="bi bi-arrow-clockwise me-2"), "Refresh Registry"], id="home-refresh-btn", color="secondary", outline=True, className="float-end fw-bold shadow-sm"),
                width=4, align="center"
            )
        ], className="mb-4 mt-3 border-bottom pb-3"),

        # Caching Stores
        dcc.Store(id="store-projects", data=[]),
        dcc.Store(id="store-deployments", data=[]),
        dcc.Store(id="store-platforms", data=[]),
        dcc.Store(id="store-allocations", data=[]), # <-- NEW
        dcc.Store(id="live-fleet-locations", data={}),
        dcc.Store(id="live-health-store", data={}), 
        
        # WebSockets & Timers
        dcc.Interval(id="home-sync-interval", interval=5*60*1000, n_intervals=0),
        WebSocket(id="ws-fleet-status", url=f"{ws_url_base}/envds/envops/ws/fleet/status"),
        WebSocket(id="ws-fleet-telemetry", url=f"{ws_url_base}/envds/envops/ws/fleet/telemetry"),

        # --- HERO MAP (Full Width) ---
        dbc.Row([
            dbc.Col(
                dbc.Card([
                    # THE FIX: Removed dcc.Loading wrapper around the dcc.Graph
                    dbc.CardBody(dcc.Graph(id="fleet-map", figure=base_fig, style={"height": "450px"}), className="p-1")
                ], className="shadow-sm border-0 mb-4"),
                width=12
            )
        ]),
        
        # --- PROJECT GRID (Horizontal Tiling) ---
        html.H5([html.I(className="bi bi-diagram-3 me-2 text-primary"), "Active Projects & Deployments"], className="fw-bold text-dark mb-3"),
        
        # THE FIX: Removed dcc.Loading wrapper around the html.Div container
        html.Div(id="projects-grid-container")
    ])

# --- CALLBACKS ---

@callback(
    Output("store-projects", "data"),
    Output("store-deployments", "data"),
    Output("store-platforms", "data"),
    Output("store-allocations", "data"), # <-- NEW
    Input("home-sync-interval", "n_intervals"),
    Input("home-refresh-btn", "n_clicks"),
    prevent_initial_call=False
)
def sync_fleet_state(n_intervals, n_clicks):
    projects = fetch_registry_data("project")
    deployments = fetch_registry_data("deployment")
    platforms = fetch_registry_data("platform")
    allocations = fetch_registry_data("projectallocation") # <-- NEW
    return projects, deployments, platforms, allocations

@callback(
    Output("live-health-store", "data"),
    Input("ws-fleet-status", "message"),
    State("live-health-store", "data"),
    prevent_initial_call=True
)
def update_live_health(message, current_health):
    if not message or "data" not in message:
        return dash.no_update
        
    try:
        # status_data = json.loads(message["data"])
        # app_uid = status_data.get("id", {}).get("app_uid", "")
        # app_group = status_data.get("id", {}).get("app_group", "")
        # state_dict = status_data.get("state", {})
        
        # ---> THE FIX: Parse the full CloudEvent envelope <---
        ce = json.loads(message["data"])
        dep_ref = ce.get("deploymentref", "")
        if not dep_ref: return dash.no_update
        
        status_data = ce.get("data", {})
        app_uid = status_data.get("id", {}).get("app_uid", "")
        app_group = status_data.get("id", {}).get("app_group", "")
        state_dict = status_data.get("state", {})
        # -----------------------------------------------------

        health = "ok"
        status_text = "AUTO"
        
        if app_group == "system" or "system_active" in state_dict:
            # We want to read the name of the active mode (e.g. "normal" or "manual")
            actual = app_uid.lower() 
            status_text = actual.upper()
            
            if actual == "manual":
                health = "warning"
            elif actual in ["error", "degraded", "maintenance"]:
                health = "danger"

        # if app_uid:
        #     current_health = current_health or {}
        #     existing = current_health.get(app_uid, {})
            
        #     if existing.get("health") != health or existing.get("text") != status_text:
        #         new_health = current_health.copy()
        #         new_health[app_uid] = {"health": health, "text": status_text}
        #         return new_health
            
        # ---> THE FIX: Save the health state under the deployment ID <---
        if dep_ref:
            current_health = current_health or {}
            existing = current_health.get(dep_ref, {})
            
            if existing.get("health") != health or existing.get("text") != status_text:
                new_health = current_health.copy()
                new_health[dep_ref] = {"health": health, "text": status_text}
                return new_health
        # ----------------------------------------------------------------

    except Exception as e:
        L.error(f"Fleet Health Stream Error: {e}")
        
    return dash.no_update


# --- 1. ACCORDION CALLBACK -> REBUILT AS GRID ---
@callback(
    Output("projects-grid-container", "children"),
    Input("store-projects", "data"),
    Input("store-deployments", "data"),
    Input("store-allocations", "data"), 
    Input("live-health-store", "data"), 
    prevent_initial_call=True
)
def render_fleet_grid(projects, deployments, allocations, health_store):
    if health_store is None: health_store = {}
    if not projects and not deployments: return html.P("No active projects found.", className="text-muted fst-italic px-2")

    platform_to_dep = {d.get("data", {}).get("platform_ref"): d for d in deployments if d.get("data", {}).get("platform_ref")}
    host_deployments = [d for d in deployments if not d.get("data", {}).get("host_platform_ref") or d.get("data", {}).get("host_platform_ref") not in platform_to_dep]
    sub_deployments = [d for d in deployments if d.get("data", {}).get("host_platform_ref") in platform_to_dep]

    hosts_by_project = {}
    now = datetime.now(timezone.utc)
    
    for dep in host_deployments:
        platform_ref = dep.get("data", {}).get("platform_ref")
        proj_ref = "Unallocated Deployments"
        
        if allocations:
            for alloc in allocations:
                data = alloc.get("data", {})
                # THE FIX: Check the allocation's platform_ref!
                if data.get("platform_ref") == platform_ref:
                    start_str = data.get("start_time", "1970-01-01T00:00:00Z")
                    end_str = data.get("end_time", "9999-12-31T23:59:59Z")
                    try:
                        start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                        end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                        if start_dt <= now <= end_dt:
                            proj_ref = data.get("project_ref")
                            break
                    except Exception:
                        proj_ref = data.get("project_ref")
                        break
                        
        dep_name = dep.get("metadata", {}).get("name", "Unknown")
        hosts_by_project.setdefault(proj_ref, {})[dep_name] = {"host": dep, "subs": []}

    for dep in sub_deployments:
        host_pref = dep.get("data", {}).get("host_platform_ref")
        parent_dep = platform_to_dep.get(host_pref)
        if parent_dep:
            parent_name = parent_dep.get("metadata", {}).get("name")
            
            parent_platform_ref = parent_dep.get("data", {}).get("platform_ref")
            parent_proj_ref = "Unallocated Deployments"
            if allocations:
                for alloc in allocations:
                    data = alloc.get("data", {})
                    # THE FIX: Check the allocation's platform_ref against the parent's platform_ref!
                    if data.get("platform_ref") == parent_platform_ref:
                        start_str = data.get("start_time", "1970-01-01T00:00:00Z")
                        end_str = data.get("end_time", "9999-12-31T23:59:59Z")
                        try:
                            start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                            end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                            if start_dt <= now <= end_dt:
                                parent_proj_ref = data.get("project_ref")
                                break
                        except Exception:
                            parent_proj_ref = data.get("project_ref")
                            break
                            
            if parent_name and parent_proj_ref in hosts_by_project and parent_name in hosts_by_project[parent_proj_ref]:
                hosts_by_project[parent_proj_ref][parent_name]["subs"].append(dep)

    project_blocks = []
    
    def get_health_indicator(uid, is_host=False):
        h_data = health_store.get(uid, {"health": "secondary", "text": "UNKNOWN"})
        color = "success" if h_data["health"] == "ok" else h_data["health"]
        icon = "bi-hdd-network" if is_host else "bi-hdd"
        return dbc.Badge([html.I(className=f"bi {icon} me-1"), uid], color=color, className="me-2 mb-2 p-2 shadow-sm rounded-pill font-monospace", style={"fontSize": "0.75rem"})

    for proj in projects:
        proj_name = proj.get("metadata", {}).get("name", "Unknown")
        proj_display = proj.get("data", {}).get("display_name", proj_name)
        proj_hosts = hosts_by_project.get(proj_name, {})
        
        host_cols = []
        proj_health_status = "ok"

        for host_name, group in proj_hosts.items():
            host_data = group["host"]
            subs = group["subs"]
            h_display = host_data.get("data", {}).get("display_name", host_name)
            
            if host_name not in health_store and subs:
                sub_healths = [health_store.get(s.get("metadata", {}).get("name"), {}).get("health", "secondary") for s in subs]
                
                if "danger" in sub_healths: agg_health = "danger"
                elif "warning" in sub_healths: agg_health = "warning"
                elif "ok" in sub_healths: agg_health = "ok"
                else: agg_health = "secondary"
                
                health_store[host_name] = {"health": agg_health, "text": "AGGREGATED"}
            
            host_state = health_store.get(host_name, {}).get("health", "ok")
            if host_state == "danger": proj_health_status = "danger"
            elif host_state == "warning" and proj_health_status != "danger": proj_health_status = "warning"
            
            for s in subs:
                s_state = health_store.get(s.get("metadata", {}).get("name"), {}).get("health", "ok")
                if s_state == "danger": proj_health_status = "danger"
                elif s_state == "warning" and proj_health_status != "danger": proj_health_status = "warning"

            host_badge = get_health_indicator(host_name, is_host=True)
            sub_badges = [get_health_indicator(s.get("metadata", {}).get("name"), is_host=False) for s in subs]

            btn = dbc.Button("Access Flight Deck \u2192", href=dash.get_relative_path(f"/deployment/{host_name}"), color="primary", size="sm", className="mt-auto w-100 fw-bold shadow-sm")
            
            host_card = dbc.Card([
                dbc.CardHeader([
                    html.H6(h_display, className="mb-0 fw-bold text-dark text-truncate"),
                    html.Span(f"{host_data.get('data', {}).get('platform_ref', 'N/A')}", className="font-monospace small text-muted text-truncate d-block")
                ], className="bg-light p-2 border-bottom"),
                dbc.CardBody([
                    html.Div([host_badge] + sub_badges, className="d-flex flex-wrap mb-3"), 
                    btn
                ], className="p-3 d-flex flex-column h-100")
            ], className="border-0 shadow-sm h-100")
            
            host_cols.append(dbc.Col(host_card, lg=4, md=6, sm=12, className="mb-3"))

        if not host_cols: 
            host_cols = [dbc.Col(html.P("No active deployments mapped to this project.", className="text-muted small fst-italic"))]

        title_color = "text-dark"
        if proj_health_status == "danger": title_color = "text-danger"
        elif proj_health_status == "warning": title_color = "text-warning"
        
        project_block = html.Div([
            html.H5([html.I(className="bi bi-folder2-open me-2"), proj_display], className=f"fw-bold mb-3 border-bottom pb-2 {title_color}"),
            dbc.Row(host_cols, className="mb-4")
        ])
        project_blocks.append(project_block)

    return html.Div(project_blocks)

# --- 2. MAP CALLBACK ---
@callback(
    Output("fleet-map", "figure"),
    Input("live-fleet-locations", "data"),
    State("store-deployments", "data"),
    State("store-allocations", "data"),
    prevent_initial_call=True
)
def patch_fleet_map(live_locations, deployments, allocations):
    if live_locations is None: live_locations = {}
    if not deployments: return dash.no_update

    platform_to_dep = {d.get("data", {}).get("platform_ref"): d for d in deployments if d.get("data", {}).get("platform_ref")}
    host_deployments = [d for d in deployments if not d.get("data", {}).get("host_platform_ref") or d.get("data", {}).get("host_platform_ref") not in platform_to_dep]
    sub_deployments = [d for d in deployments if d.get("data", {}).get("host_platform_ref") in platform_to_dep]

    hosts_by_project = {}
    now = datetime.now(timezone.utc)
    
    for dep in host_deployments:
        platform_ref = dep.get("data", {}).get("platform_ref")
        proj_ref = "Unallocated Deployments"
        
        if allocations:
            for alloc in allocations:
                data = alloc.get("data", {})
                # THE FIX: Check the allocation's platform_ref!
                if data.get("platform_ref") == platform_ref:
                    start_str = data.get("start_time", "1970-01-01T00:00:00Z")
                    end_str = data.get("end_time", "9999-12-31T23:59:59Z")
                    try:
                        start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                        end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                        if start_dt <= now <= end_dt:
                            proj_ref = data.get("project_ref")
                            break
                    except Exception:
                        proj_ref = data.get("project_ref")
                        break
                        
        dep_name = dep.get("metadata", {}).get("name", "Unknown")
        hosts_by_project.setdefault(proj_ref, {})[dep_name] = {"host": dep, "subs": []}

    for dep in sub_deployments:
        host_pref = dep.get("data", {}).get("host_platform_ref")
        parent_dep = platform_to_dep.get(host_pref)
        if parent_dep:
            parent_name = parent_dep.get("metadata", {}).get("name")
            
            parent_platform_ref = parent_dep.get("data", {}).get("platform_ref")
            parent_proj_ref = "Unallocated Deployments"
            if allocations:
                for alloc in allocations:
                    data = alloc.get("data", {})
                    # THE FIX: Check the allocation's platform_ref against the parent's platform_ref!
                    if data.get("platform_ref") == parent_platform_ref:
                        start_str = data.get("start_time", "1970-01-01T00:00:00Z")
                        end_str = data.get("end_time", "9999-12-31T23:59:59Z")
                        try:
                            start_dt = datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                            end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                            if start_dt <= now <= end_dt:
                                parent_proj_ref = data.get("project_ref")
                                break
                        except Exception:
                            parent_proj_ref = data.get("project_ref")
                            break
                            
            if parent_name and parent_proj_ref in hosts_by_project and parent_name in hosts_by_project[parent_proj_ref]:
                hosts_by_project[parent_proj_ref][parent_name]["subs"].append(dep)

    planned_lats, planned_lons, planned_text = [], [], []
    live_lats, live_lons, live_text = [], [], []

    for proj_hosts in hosts_by_project.values():
        for host_name, group in proj_hosts.items():
            host_dep = group["host"]
            dep_data = host_dep.get("data", {})
            h_display = dep_data.get('display_name', host_name)
            platform_ref = dep_data.get('platform_ref', '')

            live_loc = live_locations.get(host_name) or live_locations.get(platform_ref)

            if not live_loc:
                for sub in group["subs"]:
                    sub_name = sub.get("metadata", {}).get("name")
                    sub_pref = sub.get("data", {}).get("platform_ref")
                    live_loc = live_locations.get(sub_name) or live_locations.get(sub_pref)
                    if live_loc: break

            if live_loc:
                live_lats.append(live_loc["lat"])
                live_lons.append(live_loc["lon"])
                live_text.append(f"{h_display}<br><b>(Live)</b>")
            else:
                lat_min = dep_data.get("planned_geospatial_lat_min")
                lon_min = dep_data.get("planned_geospatial_lon_min")
                if lat_min is not None and lon_min is not None:
                    planned_lats.append(lat_min)
                    planned_lons.append(lon_min)
                    planned_text.append(f"{h_display}<br><i>(Estimated)</i>")

    map_patch = Patch()
    map_patch["data"][0]["lat"] = planned_lats
    map_patch["data"][0]["lon"] = planned_lons
    map_patch["data"][0]["text"] = planned_text
    
    map_patch["data"][1]["lat"] = live_lats
    map_patch["data"][1]["lon"] = live_lons
    map_patch["data"][1]["text"] = live_text

    return map_patch

@callback(
    Output("live-fleet-locations", "data"),
    Input("ws-fleet-telemetry", "message"),
    State("live-fleet-locations", "data"),
    prevent_initial_call=True
)
def update_live_locations(message, current_locations):
    if not message or "data" not in message: 
        return dash.no_update
        
    try:
        payload = json.loads(message["data"])
        target_id = payload.get("target_id") 
        data = payload.get("data", {})
        variables = data.get("variables", {})
        
        lat = variables.get("latitude", {}).get("data") or variables.get("lat", {}).get("data")
        lon = variables.get("longitude", {}).get("data") or variables.get("lon", {}).get("data")
        
        # ---> ADD THESE TWO LINES TO HANDLE ARRAYS <---
        if isinstance(lat, list): lat = lat[-1] if len(lat) > 0 else None
        if isinstance(lon, list): lon = lon[-1] if len(lon) > 0 else None

        if lat is not None and lon is not None and target_id:
            if current_locations is None: 
                current_locations = {}
            
            lat_val, lon_val = round(float(lat), 5), round(float(lon), 5)
            
            curr_lat = current_locations.get(target_id, {}).get("lat")
            curr_lon = current_locations.get(target_id, {}).get("lon")
                
            if curr_lat != lat_val or curr_lon != lon_val:
                new_locations = current_locations.copy() if current_locations else {}
                new_locations[target_id] = {"lat": lat_val, "lon": lon_val}
                return new_locations
            
    except Exception as e:
        L.error(f"Live Location Parse Error: {e}")
        
    return dash.no_update