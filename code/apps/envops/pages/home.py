import dash
from dash import html, dcc, callback, Input, Output, State, no_update
from dash_extensions import WebSocket
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import httpx
# from cachetools import cached, TTLCache
import logging
import traceback
import json
from datetime import datetime, timezone
from pydantic import BaseSettings

from utils import get_registry_data, config

dash.register_page(__name__, path='/', title="EnvOps - Active Deployments", order=0)

L = logging.getLogger(__name__)

# class Settings(BaseSettings):
#     daq_id: str = "mspbase01"
#     external_hostname: str = "mspbase01.pmel.noaa.gov"
#     ws_port: str = "8080"
#     ws_use_tls: str = "false"
#     class Config:
#         env_prefix = "ENVOPS_"
#         case_sensitive = False

# config = Settings()
# datastore_url = f"datastore.{config.daq_id}-system.svc.cluster.local"

ws_protocol = "wss://" if config.ws_use_tls.lower() == "true" else "ws://"
ws_url = f"{ws_protocol}{config.external_hostname}:{config.ws_port}/envds/envops/ws/system-ops/main"
# ws_url = f"{ws_protocol}{config.external_hostname}:{config.ws_port}/ws/system-ops/main"

# registry_cache = TTLCache(maxsize=128, ttl=300)

# @cached(cache=registry_cache)
# def get_registry_data(endpoint: str):
#     """Safely fetches data using httpx, heavily cached to protect the datastore."""
#     url = f"http://{datastore_url}/{endpoint}"
#     L.debug("Cache miss! Re-fetching registry data from datastore", extra={"fetch_url": url})
    
#     try:
#         with httpx.Client() as client:
#             response = client.get(url, timeout=5.0)
            
#         if response.status_code == 200:
#             data = response.json()
#             results = data.get("results", [])
#             L.debug("Successfully parsed registry items", extra={"endpoint": endpoint, "count": len(results)})
#             return results
#         else:
#             L.error("API returned non-200 code", extra={"status": response.status_code, "body": response.text})
            
#     except httpx.RequestError as e:
#         L.error("CONNECTION ERROR during fetch", extra={"fetch_url": url, "failure_detail": str(e)})
#     except Exception as e:
#         L.error("Unexpected fetch failure", extra={"endpoint": endpoint, "failure_detail": str(e)})
        
#     return []


def determine_deployment_status(dep_data):
    now = datetime.now(timezone.utc)
    start_str = dep_data.get("planned_start_time")
    end_str = dep_data.get("actual_end_time") or dep_data.get("planned_end_time")
    if not start_str or not end_str: return "unknown"
    try:
        start_time = datetime.fromisoformat(str(start_str).replace("Z", "+00:00"))
        end_time = datetime.fromisoformat(str(end_str).replace("Z", "+00:00"))
        if now < start_time: return "planned"
        elif start_time <= now <= end_time: return "active"
        else: return "completed"
    except Exception:
        return "unknown"

def get_status_badge(status):
    status = str(status).lower()
    # "Active" is now primary (blue) to avoid conflicting with the green health badge
    if status == "active": return dbc.Badge("Active", color="primary", className="ms-2 shadow-sm")
    elif status == "planned": return dbc.Badge("Planned", color="info", className="ms-2 shadow-sm")
    elif status == "completed": return dbc.Badge("Completed", color="secondary", className="ms-2 shadow-sm")
    return dbc.Badge(status.capitalize(), color="warning", text_color="dark", className="ms-2 shadow-sm")

def create_deployment_card(deployment, platform_info, host_info, child_cards=None, telemetry_cache=None, is_child=False):
    if telemetry_cache is None: telemetry_cache = {}
    dep_meta = deployment.get("metadata", {})
    dep_data = deployment.get("data", {})
    dep_id = dep_meta.get("name", "Unknown ID")
    status = determine_deployment_status(dep_data) 
    
    start_time_raw = dep_data.get("planned_start_time")
    start_time = str(start_time_raw)[:10] if start_time_raw else "TBD"
    
    p_data = platform_info.get("data") or {}
    platform_name = p_data.get("display_name", dep_data.get("platform_ref", "Unknown Platform"))
    h_data = host_info.get("data") or {}
    host_name = h_data.get("display_name", dep_data.get("host_platform_ref", "Unknown Host"))
    
    # Extract live telemetry for this specific platform
    plat_ref = dep_data.get("platform_ref", "")
    plat_id = plat_ref.split(".")[-1] if "." in plat_ref else plat_ref
    live_data = telemetry_cache.get(plat_id, {})
    
    has_telemetry = bool(live_data.get("last_seen"))
    issues = live_data.get("issues", 0)
    
    # 1. Evaluate specific health status based on telemetry cache
    if not has_telemetry:
        health_badge = dbc.Badge("Unknown", color="secondary", className="ms-2 shadow-sm")
        live_indicator = html.Span("Awaiting Data...", className="small text-muted")
    elif issues > 0:
        health_badge = dbc.Badge(f"{issues} Alarms", color="danger", className="ms-2 shadow-sm")
        live_indicator = html.Span([html.I(className="bi bi-exclamation-triangle-fill text-danger me-1"), "Issues Detected"], className="small text-danger fw-bold")
    else:
        health_badge = dbc.Badge("Healthy", color="success", className="ms-2 shadow-sm")
        live_indicator = html.Span([html.I(className="bi bi-activity text-success me-1"), "Live & Nominal"], className="small text-success")
    
    # 2. Assign buttons based on hierarchical level (Host vs Child)
    if not is_child:
        buttons_row = dbc.Row([
            dbc.Col(dbc.Button("Group Ops Dashboard", color="primary", size="sm", className="w-100 shadow-sm fw-bold", href=f"/envds/envops/deployment/{dep_id}/ops"), width=6, className="pe-1"),
            dbc.Col(dbc.Button("Platform Details", color="outline-secondary", size="sm", className="w-100 shadow-sm", href=f"/envds/envops/platform/{plat_ref}"), width=6, className="ps-1")
        ])
    else:
        buttons_row = dbc.Row([
            dbc.Col(dbc.Button("Platform Details", color="outline-secondary", size="sm", className="w-100 shadow-sm", href=f"/envds/envops/platform/{plat_ref}"), width=12)
        ])
    
    card_body_content = [
        html.H6(f"Hosted on: {host_name}", className="card-subtitle text-muted mb-3 text-truncate"),
        html.P(dep_data.get("description", "No description available."), className="small text-secondary mb-3"),
        
        dbc.Row([
            dbc.Col(html.B("Start Date:", className="small"), width=4),
            dbc.Col(html.Span(start_time, className="text-muted small"))
        ], className="mb-1"),
        
        dbc.Row([
            dbc.Col(html.B("Telemetry:", className="small"), width=4),
            dbc.Col(live_indicator)
        ], className="mb-4"),
        
        buttons_row
    ]

    if child_cards:
        card_body_content.append(html.Hr(className="my-3"))
        card_body_content.append(html.H6("Attached Payloads", className="fw-bold small text-dark mb-2"))
        card_body_content.append(html.Div(child_cards, className="ps-3 border-start border-2 border-primary"))

    return dbc.Card([
        dbc.CardHeader([
            html.H5(platform_name, className="mb-0 d-inline-block text-truncate fw-bold", style={"maxWidth": "65%"}),
            # Inject both badges into the header
            html.Div([get_status_badge(status), health_badge], className="text-end")
        ], className="d-flex justify-content-between align-items-center bg-dark text-white"),
        dbc.CardBody(card_body_content)
    ], className="shadow-sm mb-3 border-0")

# -----------------------------------------------------------------------------
# Main Layout Shell
# -----------------------------------------------------------------------------
layout = html.Div([
    dcc.Store(id="home-telemetry-cache", data={}),
    WebSocket(id="ws-home-telemetry", url=ws_url),
    
    dbc.Row([
        dbc.Col([
            html.H2("Active Deployments", className="fw-bold mb-1"),
            html.P("Real-time fleet tracking and project statuses.", className="text-muted")
        ])
    ], className="mb-4"),
    
    html.Div(id="home-metrics-container"),
    
    # 1. Target the Map Figure directly (no dcc.Loading, prevents flashing)
    dbc.Card(dbc.CardBody(
        dcc.Graph(id="home-map", config={"displayModeBar": False}, style={"height": "350px"})
    ), className="shadow-sm border-0 mb-4"),
    
    # 2. Define the Accordion statically so Dash can track its state
    dbc.Accordion(id="home-projects-accordion", always_open=True, flush=True),
    
    dcc.Interval(id="home-refresh-interval", interval=60000, n_intervals=0)
], className="mt-2 container-fluid")

# -----------------------------------------------------------------------------
# Callbacks
# -----------------------------------------------------------------------------

from dash import callback, Input, Output, State, no_update
import json
from datetime import datetime
import logging

L = logging.getLogger(__name__)

@callback(
    Output("home-telemetry-cache", "data"),
    Input("ws-home-telemetry", "message"),
    State("home-telemetry-cache", "data")
)
def ingest_live_telemetry(msg, current_cache):
    """Parses lightweight fleet location updates from the middleware."""
    if not msg or "data" not in msg: 
        return no_update
    
    try:
        # 1. Log the raw stringified receipt (truncated to prevent log flooding)
        L.debug(f"[HOME WS] Raw data received: {str(msg['data'])[:150]}...")
        
        # Parse the stringified JSON from the WebSocket
        payload = json.loads(msg["data"])
        
        # Only process the micro-payloads we specifically designed for this page
        if payload.get("type") != "fleet.location.update":
            # 2. Log if we are actively dropping a message because of a type mismatch
            L.debug(f"[HOME WS] Dropping ignored payload type: {payload.get('type')}")
            return no_update

        platform_id = payload["platform"]
        L.info(f"[HOME WS] Successfully parsed GPS for platform: {platform_id}")
        
        new_cache = current_cache.copy() if current_cache else {}
        if platform_id not in new_cache:
            new_cache[platform_id] = {"lat": None, "lon": None, "alarms": {}, "last_seen": None}
            
        new_cache[platform_id]["last_seen"] = payload["time"] or datetime.now().isoformat()
        new_cache[platform_id]["lat"] = payload["lat"]
        new_cache[platform_id]["lon"] = payload["lon"]
            
        return new_cache
        
    except Exception as e:
        L.error(f"[HOME WS] Location parse failure: {e}")
        return no_update

@callback(
    Output("home-metrics-container", "children"),
    Output("home-map", "figure"),
    Output("home-projects-accordion", "children"),
    Output("home-projects-accordion", "active_item"),
    Input("home-refresh-interval", "n_intervals"),
    Input("home-telemetry-cache", "data"),
    State("home-projects-accordion", "active_item")
)
def update_home_dashboard(n, telemetry_cache, current_active_items):
    try:
        deployments = get_registry_data("deployment-definition/registry/get/")
        projects = get_registry_data("project-definition/registry/get/")
        platforms = get_registry_data("platform-definition/registry/get/")

        if not deployments:
            return dbc.Alert("No data found.", color="warning"), go.Figure(), [], current_active_items

        project_map = {p.get("metadata", {}).get("name"): p for p in projects}
        platform_map = {p.get("metadata", {}).get("name"): p for p in platforms}

        # --- 1. Metrics ---
        active_deps = sum(1 for d in deployments if determine_deployment_status(d.get("data", {})) == "active")
        total_issues = sum(plat.get("issues", 0) for plat in telemetry_cache.values())
        
        metrics_row = dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody([html.H5("Deployments", className="text-muted"), html.H2(str(len(deployments)), className="fw-bold")]), className="shadow-sm border-0 text-center"), width=4),
            dbc.Col(dbc.Card(dbc.CardBody([html.H5("Active", className="text-muted"), html.H2(str(active_deps), className="text-primary fw-bold")]), className="shadow-sm border-0 text-center"), width=4),
            dbc.Col(dbc.Card(dbc.CardBody([html.H5("Live Issues", className="text-muted"), html.H2(str(total_issues), className="text-danger fw-bold")]), className="shadow-sm border-0 text-center"), width=4),
        ], className="mb-4")

        # --- 2. Map Generation ---
        live_lats, live_lons, live_texts = [], [], []
        est_lats, est_lons, est_texts = [], [], []

        coverage_map = {
            "UAS West Coast / Pacific Ocean Transit (Marjorie C route)": {"lat": 35.0, "lon": -135.0},
            "default": {"lat": 47.6, "lon": -122.3}
        }

        # First, build relationships to aggregate map markers by Host
        platform_to_dep = {d.get("data", {}).get("platform_ref"): d for d in deployments}
        host_to_children = {}
        root_deployments = []
        projects_grouped = {}
        
        for d in deployments:
            # Group by project for the accordions later
            proj_ref = d.get("data", {}).get("project_ref", "unassigned")
            projects_grouped.setdefault(proj_ref, []).append(d)
            
            # Group by host for the map
            plat_ref = d.get("data", {}).get("platform_ref")
            host_ref = d.get("data", {}).get("host_platform_ref")
            
            if host_ref in platform_to_dep and host_ref != plat_ref:
                host_to_children.setdefault(host_ref, []).append(d)
            else:
                root_deployments.append(d)

        # Build Map Markers using ONLY Root (Host) Deployments
        for root in root_deployments:
            root_data = root.get("data", {})
            root_name = root_data.get("display_name", root.get("metadata", {}).get("name", "Unknown"))
            root_plat_ref = root_data.get("platform_ref", "")
            
            # Look for telemetry in the host OR any of its children
            platforms_to_check = [root_plat_ref] + [c.get("data", {}).get("platform_ref") for c in host_to_children.get(root_plat_ref, [])]
            
            live_lat, live_lon = None, None
            for p_ref in platforms_to_check:
                p_id = p_ref.split(".")[-1] if "." in p_ref else p_ref
                lat = telemetry_cache.get(p_id, {}).get("lat")
                lon = telemetry_cache.get(p_id, {}).get("lon")
                if lat is not None and lon is not None:
                    live_lat, live_lon = lat, lon
                    break # Found a GPS lock in this deployment group!
            
            if live_lat is not None and live_lon is not None:
                live_lats.append(live_lat)
                live_lons.append(live_lon)
                # The text now guarantees it is the Host's name!
                live_texts.append(f"<b>{root_name}</b><br>Live: {live_lat}, {live_lon}")
            else:
                cov_str = root_data.get("planned_spatial_coverage", "default")
                est = coverage_map.get(cov_str, coverage_map["default"])
                est_lats.append(est["lat"])
                est_lons.append(est["lon"])
                est_texts.append(f"<b>{root_name}</b><br><i>Est: {cov_str}</i>")

        fig = go.Figure()
        if live_lats:
            fig.add_trace(go.Scattermapbox(lat=live_lats, lon=live_lons, mode='markers', marker=dict(size=12, color='blue'), text=live_texts, hoverinfo="text", name="Live Telemetry"))
        if est_lats:
            fig.add_trace(go.Scattermapbox(lat=est_lats, lon=est_lons, mode='markers', marker=dict(size=20, color='gray', opacity=0.5), text=est_texts, hoverinfo="text", name="Estimated Region"))

        fig.update_layout(
            mapbox_style="carto-positron", 
            margin={"r":0,"t":0,"l":0,"b":0}, 
            showlegend=True, 
            legend=dict(yanchor="top", y=0.95, xanchor="left", x=0.05, bgcolor="rgba(255,255,255,0.8)"),
            uirevision="constant" # Prevents map zoom/pan resetting when data updates
        )
        if live_lats or est_lats:
            all_lats, all_lons = live_lats + est_lats, live_lons + est_lons
            # Only automatically center if there wasn't a previous pan/zoom state
            fig.update_layout(mapbox_center={"lat": sum(all_lats)/len(all_lats), "lon": sum(all_lons)/len(all_lons)})

        # --- 3. Projects & Health Rollups ---
        project_accordions = []
        for proj_ref, deps in projects_grouped.items():
            proj_name = project_map.get(proj_ref, {}).get("data", {}).get("display_name", proj_ref)
            
            platform_to_dep = {d.get("data", {}).get("platform_ref"): d for d in deps}
            root_deployments = [d for d in deps if d.get("data", {}).get("host_platform_ref") not in platform_to_dep]
            child_deployments = {} 
            for d in deps:
                host_ref = d.get("data", {}).get("host_platform_ref")
                if host_ref in platform_to_dep:
                    child_deployments.setdefault(host_ref, []).append(d)

            def build_cards(d_list, is_child=False):
                cards = []
                for d in d_list:
                    plat_ref = d.get("data", {}).get("platform_ref")
                    children = child_deployments.get(plat_ref, [])
                    child_ui = build_cards(children, is_child=True) if children else None
                    card = create_deployment_card(d, platform_map.get(plat_ref, {}), platform_map.get(d.get("data", {}).get("host_platform_ref"), {}), child_ui, telemetry_cache, is_child=is_child)
                    cards.append(html.Div(card, className="mb-2") if is_child else dbc.Col(card, width=12, lg=6, xl=4, className="mb-4"))
                return cards

            root_cards_ui = dbc.Row(build_cards(root_deployments, is_child=False))
            
            proj_issues = sum(telemetry_cache.get(d.get("data", {}).get("platform_ref", "").split(".")[-1], {}).get("issues", 0) for d in deps)
            
            if proj_issues > 0:
                health_badge = dbc.Badge([html.I(className="bi bi-exclamation-triangle-fill me-2"), f"{proj_issues} Issues"], color="danger", className="rounded-pill shadow-sm px-3 py-2")
                title_class = "text-danger"
                bg_class = "bg-soft-danger"
            else:
                health_badge = dbc.Badge([html.I(className="bi bi-check-circle-fill me-2"), "Nominal"], color="success", className="rounded-pill shadow-sm px-3 py-2 opacity-75")
                title_class = "text-dark"
                bg_class = ""

            custom_title = html.Div([
                html.Span([html.I(className="bi bi-folder2-open me-2"), f"Project: {proj_name}"], className=f"fw-bold fs-5 {title_class}"),
                health_badge
            ], className="d-flex justify-content-between align-items-center w-100 pe-3")

            project_accordions.append(dbc.AccordionItem(root_cards_ui, title=custom_title, item_id=proj_ref, class_name=bg_class))

        if current_active_items is None:
            current_active_items = []

        return metrics_row, fig, project_accordions, current_active_items

    except Exception as e:
        L.error(f"Home dashboard crash: {traceback.format_exc()}")
        return dbc.Alert(f"Error: {e}", color="danger"), go.Figure(), [], current_active_items