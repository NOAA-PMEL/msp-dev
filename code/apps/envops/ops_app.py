import dash
from dash import html, dcc, Input, Output, State, no_update, ALL, ctx
from dash_extensions import WebSocket
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import logging
import json
from datetime import datetime

# Import shared tools from our utility layer
from utils import get_registry_data, config, create_unified_shell, register_sidebar_callbacks

L = logging.getLogger(__name__)

# --- Initialize Isolated Dash App ---
app = dash.Dash(__name__, requests_pathname_prefix="/envds/envops/ops/")
register_sidebar_callbacks(app)


def create_empty_dual_plot(title, y1_name, y2_name, y1_color="#1f77b4", y2_color="#d62728"):
    """Generates a highly stylized, empty dual-axis plot skeleton."""
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    
    fig.add_trace(go.Scatter(x=[], y=[], name=y1_name, mode="lines", line=dict(color=y1_color, width=2)), secondary_y=False)
    fig.add_trace(go.Scatter(x=[], y=[], name=y2_name, mode="lines", line=dict(color=y2_color, width=2)), secondary_y=True)
    
    fig.update_layout(
        title=dict(text=title, font=dict(size=14), y=0.95),
        margin=dict(l=40, r=40, t=40, b=20),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="white",
        paper_bgcolor="white",
        uirevision="constant" # Prevents zoom/pan resets when new data arrives
    )
    
    fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor="LightGray", secondary_y=False)
    fig.update_yaxes(showgrid=False, secondary_y=True)
    fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor="LightGray")
    return fig

# --- Core Shell and URL Router ---
app.layout = create_unified_shell(html.Div([
    dcc.Location(id="ops-url", refresh=False),
    html.Div(id="ops-page-content") 
]), active_item="ops")

@app.callback(
    Output("ops-page-content", "children"),
    Input("ops-url", "pathname")
)
def render_deployment_ops(pathname):
    if not pathname or "deployment/" not in pathname:
        return dbc.Alert("Select a deployment from the sidebar.", color="info", className="m-4")
        
    deployment_id = pathname.split("/")[-1]
    return build_ops_layout(deployment_id)


def build_ops_layout(deployment_id):
    all_deployments = get_registry_data("deployment")
    
    host_dep = next((d for d in all_deployments if d.get("metadata", {}).get("name") == deployment_id), None)
    if not host_dep:
        return dbc.Alert(f"Deployment {deployment_id} not found.", color="warning", className="m-4")
        
    host_data = host_dep.get("data", {})
    host_plat_ref = host_data.get("platform_ref", "")
    host_name = host_data.get("display_name", host_plat_ref)
    
    child_deps = [d for d in all_deployments if d.get("data", {}).get("host_platform_ref") == host_plat_ref and d.get("metadata", {}).get("name") != deployment_id]
    group_platforms = [host_plat_ref.split(".")[-1]] + [c.get("data", {}).get("platform_ref", "").split(".")[-1] for c in child_deps]

    header = dbc.Row([
        dbc.Col([
            html.H2([html.I(className="bi bi-hdd-network me-2"), host_name], className="fw-bold mb-0"),
            html.P(f"ID: {deployment_id} | Attached Payloads: {len(child_deps)}", className="text-muted mb-0")
        ]),
        dbc.Col([
            dbc.Badge("Group Health: Pending", id="ops-health-badge", color="secondary", className="fs-5 shadow-sm rounded-pill px-3 py-2")
        ], width="auto", className="text-end align-self-center")
    ], className="mb-4 align-items-center border-bottom pb-3")

    ops_ribbon = dbc.Card(dbc.CardBody(dbc.Row([
        dbc.Col([html.H6("System Mode", className="text-muted mb-1 small text-uppercase"), html.H5("STANDBY", id="ops-sys-mode", className="fw-bold mb-0")], width=3, className="border-end"),
        dbc.Col([html.H6("Sampling State", className="text-muted mb-1 small text-uppercase"), html.H5("IDLE", id="ops-samp-state", className="fw-bold mb-0")], width=3, className="border-end"),
        dbc.Col([html.H6("Active Alarms", className="text-muted mb-1 small text-uppercase"), html.H5("0", id="ops-alarm-count", className="text-success fw-bold mb-0")], width=3),
        dbc.Col([dbc.Button([html.I(className="bi bi-sliders me-2"), "Command & Control"], color="dark", className="w-100 shadow-sm fw-bold h-100")], width=3)
    ])), className="shadow-sm border-0 mb-4 bg-light")
    
    plots_accordion = dbc.Accordion([
        dbc.AccordionItem([
            dbc.Row([
                dbc.Col(dcc.Graph(id="ops-plot-wind", figure=create_empty_dual_plot("Wind", "True Speed (m/s)", "True Dir (°)", "#1f77b4", "#7f7f7f"), style={"height": "300px"}), width=4),
                dbc.Col(dcc.Graph(id="ops-plot-atm", figure=create_empty_dual_plot("Atmosphere", "Temp (°C)", "RH (%)", "#ff7f0e", "#17becf"), style={"height": "300px"}), width=4),
                dbc.Col(dcc.Graph(id="ops-plot-precip", figure=create_empty_dual_plot("Precip & Pressure", "Rain (mm/h)", "Pressure (hPa)", "#2ca02c", "#8c564b"), style={"height": "300px"}), width=4),
            ])
        ], title=html.B([html.I(className="bi bi-cloud-sun me-2"), "Meteorology"]), item_id="met"),

        dbc.AccordionItem([
            dbc.Row([
                dbc.Col(dcc.Graph(id="ops-plot-o3-co", figure=create_empty_dual_plot("Ozone & CO", "O3 (ppb)", "CO (ppb)", "#9467bd", "#e377c2"), style={"height": "300px"}), width=6),
                dbc.Col(dcc.Graph(id="ops-plot-no-no2", figure=create_empty_dual_plot("Nitrogen Oxides", "NO (ppb)", "NO2 (ppb)", "#1f77b4", "#ff7f0e"), style={"height": "300px"}), width=6),
            ])
        ], title=html.B([html.I(className="bi bi-wind me-2"), "Gas Phase Chemistry"]), item_id="gas"),

        dbc.AccordionItem([
            dbc.Row([
                dbc.Col(dcc.Graph(id="ops-plot-rel-wind", figure=create_empty_dual_plot("Platform Relative Wind", "Rel Speed (m/s)", "Rel Dir (°)", "#bcbd22", "#7f7f7f"), style={"height": "300px"}), width=6),
                dbc.Col(dcc.Graph(id="ops-plot-flow", figure=create_empty_dual_plot("Inlet Flow & Particulates", "Inlet Flow (LPM)", "PM 2.5 (µg/m³)", "#17becf", "#d62728"), style={"height": "300px"}), width=6),
            ])
        ], title=html.B([html.I(className="bi bi-sliders me-2"), "Sampling Operations"]), item_id="ops")
    ], always_open=True, active_item=["met", "gas", "ops"], className="mb-4 shadow-sm")

    child_links = [
        dbc.ListGroupItem([
            html.Div([
                html.Span(c.get("data", {}).get("display_name", "Unknown Payload"), className="fw-bold"),
                dbc.Button("Device Details", size="sm", color="outline-primary", href=f"/envds/envops/platform/{c.get('data', {}).get('platform_ref')}", className="float-end")
            ], className="w-100")
        ]) for c in child_deps
    ]
    sub_systems = dbc.Card([
        dbc.CardHeader("Attached Sub-Systems (Level 3 Pathways)", className="fw-bold bg-dark text-white"),
        dbc.ListGroup(child_links, flush=True) if child_links else dbc.CardBody(html.P("No attached payloads.", className="text-muted mb-0"))
    ], className="shadow-sm border-0")

    ws_protocol = "wss://" if config.ws_use_tls.lower() == "true" else "ws://"
    ws_base = f"{ws_protocol}{config.external_hostname}:{config.ws_port}/envds/envops"

    ws_connections = [
        WebSocket(id="ws-ops-system", url=f"{ws_base}/ws/system-ops/main")
    ]
    
    for p_id in group_platforms:
        if p_id: 
            ws_connections.append(WebSocket(id={"type": "ws-ops-platform", "index": p_id}, url=f"{ws_base}/ws/platform/{p_id}"))

    return html.Div([
        dcc.Store(id="ops-group-platforms", data=group_platforms),
        dcc.Store(id="ops-telemetry-cache", data={}),
        html.Div(ws_connections),
        header, ops_ribbon, plots_accordion, sub_systems
    ], className="container-fluid mt-3")


# --- Callbacks ---
@app.callback(
    Output("ops-telemetry-cache", "data"),
    Input({"type": "ws-ops-platform", "index": ALL}, "message"),
    State("ops-telemetry-cache", "data")
)
def ingest_live_telemetry(messages, current_cache):
    if not ctx.triggered:
        return no_update

    msg = ctx.triggered[0].get("value")
    if not msg or "data" not in msg: 
        return no_update

    try:
        ws_wrapper = json.loads(msg["data"])
        cloud_event = ws_wrapper.get("data-update", ws_wrapper.get("data", {}))
        if not cloud_event:
            return no_update
            
        current_topic = ws_wrapper.get("topic") or cloud_event.get("destpath", "") or cloud_event.get("sourcepath", "")
        parts = current_topic.split("/")
        
        payload = cloud_event.get("data", {})
        if not payload:
            return no_update

        attributes = payload.get("attributes", {})
        platform_id = attributes.get("platform", {}).get("data")
        if not platform_id:
            platform_id = parts[3].split("::")[0] if len(parts) > 3 else "unknown"
            
        new_cache = current_cache.copy() if current_cache else {}
        if platform_id not in new_cache:
            new_cache[platform_id] = {"variables": {}, "state": {}}
            
        incoming_vars = payload.get("variables", {})
        current_time = incoming_vars.get("time", {}).get("data") or datetime.now().isoformat()
        
        for var_name, var_data in incoming_vars.items():
            val = var_data.get("data")
            if val is None:
                continue
                
            if var_name not in new_cache[platform_id]["variables"]:
                new_cache[platform_id]["variables"][var_name] = {"x": [], "y": []}
                
            new_cache[platform_id]["variables"][var_name]["x"].append(current_time)
            new_cache[platform_id]["variables"][var_name]["y"].append(val)
            
            if len(new_cache[platform_id]["variables"][var_name]["x"]) > 300:
                new_cache[platform_id]["variables"][var_name]["x"].pop(0)
                new_cache[platform_id]["variables"][var_name]["y"].pop(0)
            
        return new_cache
    except Exception:
        return no_update
    
@app.callback(
    Output("ops-health-badge", "children"),
    Output("ops-health-badge", "color"),
    Output("ops-sys-mode", "children"),
    Output("ops-sys-mode", "className"),
    Output("ops-alarm-count", "children"),
    Input("ops-telemetry-cache", "data")
)
def update_ribbon_ui(cache):
    if not cache: 
        return no_update
    
    total_alarms = 0
    sys_mode = "STANDBY"
    sys_mode_color = "fw-bold text-muted mb-0"
    
    for uid, data in cache.items():
        state = data.get("state", {})
        if "alarm" in str(state).lower() or "error" in str(state).lower():
            total_alarms += 1
            
        if "system_active" in state:
            sys_mode = "ACTIVE" if str(state["system_active"].get("actual", "")).lower() == "true" else "STANDBY"
            sys_mode_color = "fw-bold text-primary mb-0" if sys_mode == "ACTIVE" else "fw-bold text-muted mb-0"
            
    if total_alarms > 0:
        health_badge = f"{total_alarms} Critical Alarms"
        health_color = "danger"
    else:
        health_badge = "Group Nominal"
        health_color = "success"

    return health_badge, health_color, sys_mode, sys_mode_color, str(total_alarms)

@app.callback(
    Output("ops-plot-wind", "extendData"),
    Output("ops-plot-atm", "extendData"),
    Output("ops-plot-precip", "extendData"),
    Output("ops-plot-o3-co", "extendData"),
    Output("ops-plot-no-no2", "extendData"),
    Output("ops-plot-rel-wind", "extendData"),
    Output("ops-plot-flow", "extendData"),
    Input("ops-telemetry-cache", "data"),
    prevent_initial_call=True
)
def update_operational_plots(cache):
    if not cache:
        return [no_update] * 7

    all_vars = {}
    for uid, data in cache.items():
        all_vars.update(data.get("variables", {}))

    current_time = all_vars.get("time", {}).get("data")
    if not current_time:
        current_time = datetime.now().isoformat()

    def get_val(key):
        val = all_vars.get(key, {}).get("data")
        try:
            return float(val) if val is not None else None
        except (ValueError, TypeError):
            return None

    def build_extend_payload(var1_name, var2_name, max_points=300):
        v1 = get_val(var1_name)
        v2 = get_val(var2_name)
        
        x_data, y_data, traces = [], [], []
        
        if v1 is not None:
            x_data.append([current_time])
            y_data.append([v1])
            traces.append(0)
            
        if v2 is not None:
            x_data.append([current_time])
            y_data.append([v2])
            traces.append(1)
            
        if not traces:
            return no_update
            
        return ({"x": x_data, "y": y_data}, traces, max_points)

    return [
        build_extend_payload("true_wind_speed", "true_wind_direction"),
        build_extend_payload("air_temperature", "relative_humidity"),
        build_extend_payload("pressure", "rain_intensity"),
        build_extend_payload("O3", "CO"),
        build_extend_payload("NO", "NO2"),
        build_extend_payload("relative_wind_speed", "relative_wind_direction"),
        build_extend_payload("inlet_flow", "PM2_5") 
    ]