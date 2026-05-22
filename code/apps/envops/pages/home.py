import dash
from dash import html, dcc, callback, Input, Output
import dash_bootstrap_components as dbc
import requests
import logging
import traceback
from datetime import datetime, timezone
from pydantic import BaseSettings

dash.register_page(__name__, path='/', title="EnvOps - Active Deployments", order=0)

L = logging.getLogger(__name__)

class Settings(BaseSettings):
    daq_id: str = "mspbase01"
    class Config:
        env_prefix = "ENVOPS_"
        case_sensitive = False

config = Settings()
datastore_url = f"datastore.{config.daq_id}-system.svc.cluster.local"

def get_registry_data(endpoint: str):
    url = f"http://{datastore_url}/{endpoint}"
    try:
        response = requests.get(url, timeout=5.0)
        if response.status_code == 200:
            data = response.json()
            return data.get("results", [])
    except Exception as e:
        L.error(f"Failed to fetch {endpoint}: {e}")
    return []

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
    except Exception as e:
        L.error(f"Time parsing error: {e}")
        return "unknown"

def get_status_badge(status):
    status = str(status).lower()
    if status == "active": return dbc.Badge("Active", color="success", className="ms-2 shadow-sm")
    elif status == "planned": return dbc.Badge("Planned", color="info", className="ms-2 shadow-sm")
    elif status == "completed": return dbc.Badge("Completed", color="secondary", className="ms-2 shadow-sm")
    return dbc.Badge(status.capitalize(), color="warning", text_color="dark", className="ms-2 shadow-sm")

def create_deployment_card(deployment, platform_info, host_info, child_cards=None):
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
    
    card_body_content = [
        html.H6(f"Hosted on: {host_name}", className="card-subtitle text-muted mb-3 text-truncate"),
        html.P(dep_data.get("description", "No description available."), className="small text-secondary mb-4"),
        
        dbc.Row([
            dbc.Col(html.B("Deployment ID:", className="small"), width=5),
            dbc.Col(html.Span(dep_id, className="text-muted small"), className="text-truncate")
        ], className="mb-1"),

        dbc.Row([
            dbc.Col(html.B("Start Date:", className="small"), width=5),
            dbc.Col(html.Span(start_time, className="text-muted small"))
        ], className="mb-3"),
        
        dbc.Row([
            dbc.Col(
                dbc.Button("Ops Dashboard", color="primary", size="sm", className="w-100 shadow-sm", href=f"/envds/envops/deployment/{dep_id}/ops"), 
                width=6, className="pe-1"
            ),
            dbc.Col(
                dbc.Button("Platform Details", color="outline-secondary", size="sm", className="w-100 shadow-sm", href=f"/envds/envops/platform/{dep_data.get('platform_ref')}"), 
                width=6, className="ps-1"
            )
        ])
    ]

    if child_cards:
        card_body_content.append(html.Hr(className="my-3"))
        card_body_content.append(html.H6("Attached Payloads / Sub-Systems", className="fw-bold small text-dark mb-2"))
        card_body_content.append(html.Div(child_cards, className="ps-3 border-start border-2 border-primary"))

    return dbc.Card([
        dbc.CardHeader([
            html.H5(platform_name, className="mb-0 d-inline-block text-truncate fw-bold", style={"maxWidth": "75%"}),
            get_status_badge(status)
        ], className="d-flex justify-content-between align-items-center bg-dark text-white"),
        dbc.CardBody(card_body_content)
    ], className="shadow-sm mb-3 border-0")

layout = html.Div([
    dbc.Row([
        dbc.Col([
            html.H2("Active Deployments", className="fw-bold mb-1"),
            html.P("Fleet overview organized by Project and Platform hierarchy.", className="text-muted")
        ])
    ], className="mb-4"),
    
    html.Div(id="home-metrics-container"),
    html.Div(id="home-projects-container"),
    
    dcc.Interval(id="home-refresh-interval", interval=30000, n_intervals=0)
], className="mt-2 container-fluid")

@callback(
    Output("home-metrics-container", "children"),
    Output("home-projects-container", "children"),
    Input("home-refresh-interval", "n_intervals")
)
def update_home_dashboard(n):
    try:
        deployments = get_registry_data("deployment-definition/registry/get/")
        projects = get_registry_data("project-definition/registry/get/")
        platforms = get_registry_data("platform-definition/registry/get/")

        if not deployments:
            return dbc.Alert("No deployments found or Datastore unreachable.", color="warning", className="shadow-sm"), html.Div()

        project_map = {p.get("metadata", {}).get("name"): p for p in projects}
        platform_map = {p.get("metadata", {}).get("name"): p for p in platforms}

        total_deps = len(deployments)
        active_deps = sum(1 for d in deployments if determine_deployment_status(d.get("data", {})) == "active")
        planned_deps = sum(1 for d in deployments if determine_deployment_status(d.get("data", {})) == "planned")

        metrics_row = dbc.Row([
            dbc.Col(dbc.Card(dbc.CardBody([html.H5("Total Deployments", className="text-muted"), html.H2(str(total_deps), className="fw-bold")]), className="shadow-sm border-0 text-center"), width=4),
            dbc.Col(dbc.Card(dbc.CardBody([html.H5("Active", className="text-muted"), html.H2(str(active_deps), className="text-success fw-bold")]), className="shadow-sm border-0 text-center"), width=4),
            dbc.Col(dbc.Card(dbc.CardBody([html.H5("Planned", className="text-muted"), html.H2(str(planned_deps), className="text-info fw-bold")]), className="shadow-sm border-0 text-center"), width=4),
        ], className="mb-4")

        projects_grouped = {}
        for dep in deployments:
            proj_ref = dep.get("data", {}).get("project_ref", "unassigned")
            if proj_ref not in projects_grouped:
                projects_grouped[proj_ref] = []
            projects_grouped[proj_ref].append(dep)

        project_accordions = []
        
        for proj_ref, deps in projects_grouped.items():
            p_data = project_map.get(proj_ref, {}).get("data") or {}
            proj_name = p_data.get("display_name", proj_ref)
            
            platform_to_dep = {d.get("data", {}).get("platform_ref"): d for d in deps}
            root_deployments = []
            child_deployments = {} 

            for d in deps:
                host_ref = d.get("data", {}).get("host_platform_ref")
                if host_ref in platform_to_dep:
                    if host_ref not in child_deployments:
                        child_deployments[host_ref] = []
                    child_deployments[host_ref].append(d)
                else:
                    root_deployments.append(d)

            def build_cards(deployments_list, is_child=False):
                cards = []
                for d in deployments_list:
                    plat_ref = d.get("data", {}).get("platform_ref")
                    children = child_deployments.get(plat_ref, [])
                    child_ui = build_cards(children, is_child=True) if children else None
                    
                    card_component = create_deployment_card(
                        deployment=d,
                        platform_info=platform_map.get(plat_ref, {}),
                        host_info=platform_map.get(d.get("data", {}).get("host_platform_ref"), {}),
                        child_cards=child_ui
                    )
                    
                    if is_child:
                        cards.append(html.Div(card_component, className="mb-2"))
                    else:
                        cards.append(dbc.Col(card_component, width=12, lg=6, xl=4, className="mb-4"))
                        
                return cards

            root_cards_ui = dbc.Row(build_cards(root_deployments, is_child=False))

            project_accordions.append(
                dbc.AccordionItem(root_cards_ui, title=f"Project: {proj_name}", item_id=proj_ref)
            )

        projects_ui = dbc.Accordion(project_accordions, start_collapsed=False, always_open=True, flush=True)

        return metrics_row, projects_ui

    except Exception as e:
        L.error(f"Home dashboard crash: {traceback.format_exc()}")
        return dbc.Alert(f"Internal Dashboard Error: {e}", color="danger"), html.Div()