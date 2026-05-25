import dash
from dash import html, dcc, Input, Output, State, no_update, ALL, ctx
from dash_extensions import WebSocket
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import logging
import json
from datetime import datetime, timezone

from utils import get_registry_data, config, create_unified_shell, register_sidebar_callbacks

L = logging.getLogger(__name__)

# --- Initialize Isolated Dash App ---
app = dash.Dash(__name__, requests_pathname_prefix="/envds/envops/plots/", routes_pathname_prefix="/")
register_sidebar_callbacks(app)

app.layout = create_unified_shell(html.Div([
    dcc.Location(id="plot-url", refresh=False),
    html.Div(id="plot-page-content") 
]), active_item="ops")

@app.callback(
    Output("plot-page-content", "children"),
    Input("plot-url", "pathname")
)
def render_deployment_plots(pathname):
    if not pathname or "deployment/" not in pathname:
        return dbc.Alert("Select a deployment from the sidebar.", color="info", className="m-4")
    deployment_id = pathname.split("/")[-1]
    return build_plot_layout(deployment_id)

def build_plot_layout(deployment_id):
    all_deployments = get_registry_data("deployment")
    
    host_dep = next((d for d in all_deployments if d.get("metadata", {}).get("name") == deployment_id), None)
    if not host_dep:
        return dbc.Alert(f"Deployment {deployment_id} not found.", color="warning", className="m-4")
        
    host_data = host_dep.get("data", {})
    host_plat_ref = host_data.get("platform_ref", "")
    host_name = host_data.get("display_name", host_plat_ref)
    
    child_deps = [d for d in all_deployments if d.get("data", {}).get("host_platform_ref") == host_plat_ref and d.get("metadata", {}).get("name") != deployment_id]
    
    raw_targets = [host_plat_ref] + [c.get("data", {}).get("platform_ref", "") for c in child_deps]
    short_targets = [p.split(".")[-1] for p in raw_targets if "." in p]
    group_platforms = list(set(raw_targets + short_targets))
    group_platforms = [p for p in group_platforms if p]

    header = dbc.Row([
        dbc.Col([
            html.H2([html.I(className="bi bi-graph-up me-2"), "Live Analytics"], className="fw-bold mb-0"),
            html.P(f"Deployment: {host_name}", className="text-muted mb-0")
        ]),
        dbc.Col([
            dbc.Button("Return to Ops", href=f"/envds/envops/ops/deployment/{deployment_id}", color="outline-secondary", size="sm", className="shadow-sm")
        ], width="auto")
    ], className="mb-4 align-items-center border-bottom pb-3")

    # --- Analytics Accordion (Adapted from sensor.py logic) ---
    plot_sections = dbc.Accordion([
        dbc.AccordionItem([
            dbc.Row([
                dbc.Col([
                    html.Label("Select Platform & Variable (Y-Axis)", className="fw-bold small text-muted"),
                    dcc.Dropdown(id="dropdown-1d-y", placeholder="Awaiting telemetry...", className="mb-3 shadow-sm"),
                ], width=12, md=6)
            ]),
            dcc.Graph(id="graph-1d", config={"displayModeBar": True}, style={"height": "400px"})
        ], title="1D Time Series", item_id="1d"),
        
        dbc.AccordionItem([
            dbc.Row([
                dbc.Col([
                    html.Label("Select Coordinate / Bin (Y-Axis)", className="fw-bold small text-muted"),
                    dcc.Dropdown(id="dropdown-2d-y", placeholder="Awaiting telemetry...", className="mb-3 shadow-sm"),
                ], width=12, md=6),
                dbc.Col([
                    html.Label("Select Value (Z-Axis / Heatmap Color)", className="fw-bold small text-muted"),
                    dcc.Dropdown(id="dropdown-2d-z", placeholder="Awaiting telemetry...", className="mb-3 shadow-sm"),
                ], width=12, md=6)
            ]),
            dbc.Row([
                dbc.Col(dcc.Graph(id="graph-2d-heatmap", style={"height": "400px"}), width=12, md=6),
                dbc.Col(dcc.Graph(id="graph-2d-scatter", style={"height": "400px"}), width=12, md=6)
            ])
        ], title="2D Profiles & Heatmaps"),
        
        dbc.AccordionItem([
             dbc.Row([
                dbc.Col([
                    html.Label("Select X-Axis Coordinate", className="fw-bold small text-muted"),
                    dcc.Dropdown(id="dropdown-3d-x", placeholder="Awaiting telemetry...", className="mb-3 shadow-sm"),
                ], width=4),
                dbc.Col([
                    html.Label("Select Y-Axis Coordinate", className="fw-bold small text-muted"),
                    dcc.Dropdown(id="dropdown-3d-y", placeholder="Awaiting telemetry...", className="mb-3 shadow-sm"),
                ], width=4),
                dbc.Col([
                    html.Label("Select Z-Axis Value", className="fw-bold small text-muted"),
                    dcc.Dropdown(id="dropdown-3d-z", placeholder="Awaiting telemetry...", className="mb-3 shadow-sm"),
                ], width=4)
            ]),
            dcc.Graph(id="graph-3d-surface", style={"height": "600px"})
        ], title="3D Surface Plots")
    ], always_open=True, active_item="1d")

    ws_protocol = "wss://" if config.ws_use_tls.lower() == "true" else "ws://"
    ws_base = f"{ws_protocol}{config.external_hostname}:{config.ws_port}/envds/envops"

    ws_connections = []
    for p_id in group_platforms:
        if p_id: ws_connections.append(WebSocket(id={"type": "ws-plot-platform", "index": p_id}, url=f"{ws_base}/ws/platform/{p_id}"))

    return html.Div([
        dcc.Store(id="plot-telemetry-cache", data={}),
        html.Div(ws_connections),
        header, plot_sections
    ], className="container-fluid mt-3")


# --- Callbacks ---

@app.callback(
    Output("plot-telemetry-cache", "data"),
    Input({"type": "ws-plot-platform", "index": ALL}, "message"),
    State("plot-telemetry-cache", "data")
)
def ingest_live_telemetry(messages, current_cache):
    """Maintains a high-performance 300-point sliding window for all active variables."""
    if not ctx.triggered: return no_update
    msg = ctx.triggered[0].get("value")
    if not msg or "data" not in msg: return no_update

    try:
        ws_wrapper = json.loads(msg["data"])
        payload = ws_wrapper.get("data-update")
        if not payload: return no_update

        attributes = payload.get("attributes", {})
        platform_id = attributes.get("platform", {}).get("data")
        
        if not platform_id:
            current_topic = ws_wrapper.get("topic") or payload.get("destpath", "") or payload.get("sourcepath", "")
            parts = current_topic.split("/")
            platform_id = parts[3].split("::")[0] if len(parts) > 3 else "unknown"
            
        new_cache = current_cache.copy() if current_cache else {}
        if platform_id not in new_cache: new_cache[platform_id] = {"variables": {}}
            
        incoming_vars = payload.get("variables", {})
        current_time = incoming_vars.get("time", {}).get("data") or datetime.now(timezone.utc).isoformat()
        
        for var_name, var_data in incoming_vars.items():
            if var_name == "time": continue
            val = var_data.get("data")
            if val is not None:
                if var_name not in new_cache[platform_id]["variables"]:
                    new_cache[platform_id]["variables"][var_name] = {"x": [], "y": [], "unit": var_data.get("unit", "")}
                
                new_cache[platform_id]["variables"][var_name]["x"].append(current_time)
                new_cache[platform_id]["variables"][var_name]["y"].append(val)
                new_cache[platform_id]["variables"][var_name]["unit"] = var_data.get("unit", "")
                
                if len(new_cache[platform_id]["variables"][var_name]["x"]) > 300:
                    new_cache[platform_id]["variables"][var_name]["x"].pop(0)
                    new_cache[platform_id]["variables"][var_name]["y"].pop(0)
            
        return new_cache
    except Exception:
        return no_update

@app.callback(
    Output("dropdown-1d-y", "options"),
    Output("dropdown-2d-y", "options"), Output("dropdown-2d-z", "options"),
    Output("dropdown-3d-x", "options"), Output("dropdown-3d-y", "options"), Output("dropdown-3d-z", "options"),
    Input("plot-telemetry-cache", "data")
)
def populate_dropdowns(cache):
    """Dynamically fills the dropdowns with whatever variables arrive in the cache."""
    if not cache: return no_update

    options = []
    for platform_id, p_data in cache.items():
        for var_name in p_data.get("variables", {}).keys():
            options.append({"label": f"{var_name.replace('_', ' ').title()} ({platform_id})", "value": f"{platform_id}::{var_name}"})
            
    # Sort alphabetically to keep it clean
    options = sorted(options, key=lambda d: d['label'])
    return options, options, options, options, options, options

@app.callback(
    Output("graph-1d", "figure"),
    Input("plot-telemetry-cache", "data"),
    State("dropdown-1d-y", "value")
)
def render_1d_plot(cache, selected_var):
    default_fig = go.Figure(layout={"xaxis_title": "Time", "yaxis_title": "Value", "template": "simple_white"})
    if not cache or not selected_var: return default_fig

    try:
        platform_id, var_name = selected_var.split("::")
        var_data = cache[platform_id]["variables"][var_name]
        
        x_data, y_data = var_data["x"], var_data["y"]
        
        fig = go.Figure(go.Scatter(x=x_data, y=y_data, mode="lines+markers", line=dict(color="#1f77b4", width=2)))
        fig.update_layout(
            title=f"{var_name.replace('_', ' ').title()} <br><span style='font-size:10px;color:gray;'>{platform_id}</span>",
            yaxis_title=var_data.get("unit", ""), xaxis_title="Time",
            template="simple_white", margin=dict(t=50, b=30, l=40, r=40),
            uirevision=selected_var # Prevents zoom resetting when new data streams in
        )
        return fig
    except KeyError:
        return default_fig

@app.callback(
    Output("graph-2d-heatmap", "figure"), Output("graph-2d-scatter", "figure"),
    Input("plot-telemetry-cache", "data"),
    State("dropdown-2d-y", "value"), State("dropdown-2d-z", "value")
)
def render_2d_plots(cache, y_sel, z_sel):
    default_fig = go.Figure(layout={"template": "simple_white"})
    if not cache or not y_sel or not z_sel: return default_fig, default_fig

    try:
        y_plat, y_var = y_sel.split("::")
        z_plat, z_var = z_sel.split("::")
        
        x_data = cache[z_plat]["variables"][z_var]["x"]
        y_data_raw = cache[y_plat]["variables"][y_var]["y"][-1] # Grabbing the latest bin/coordinate array
        z_data_raw = cache[z_plat]["variables"][z_var]["y"]     # The history of the value arrays
        
        heatmap = go.Figure(go.Heatmap(x=x_data, y=y_data_raw, z=z_data_raw, colorscale="Rainbow"))
        heatmap.update_layout(title="2D Time Profile", yaxis_title=y_var, xaxis_title="Time", template="simple_white", uirevision=f"{y_sel}-{z_sel}")

        # Slice the very last reading for the scatter profile
        latest_z = z_data_raw[-1] if z_data_raw else []
        scatter = go.Figure(go.Scatter(x=latest_z, y=y_data_raw, mode="lines+markers"))
        scatter.update_layout(title=f"Latest Snapshot: {str(x_data[-1])[11:19]}", xaxis_title=z_var, yaxis_title=y_var, template="simple_white", uirevision=f"{y_sel}-{z_sel}")

        return heatmap, scatter
    except (KeyError, IndexError):
        return default_fig, default_fig

@app.callback(
    Output("graph-3d-surface", "figure"),
    Input("plot-telemetry-cache", "data"),
    State("dropdown-3d-x", "value"), State("dropdown-3d-y", "value"), State("dropdown-3d-z", "value")
)
def render_3d_plots(cache, x_sel, y_sel, z_sel):
    default_fig = go.Figure(layout={"template": "simple_white"})
    if not cache or not x_sel or not y_sel or not z_sel: return default_fig

    try:
        x_plat, x_var = x_sel.split("::")
        y_plat, y_var = y_sel.split("::")
        z_plat, z_var = z_sel.split("::")
        
        x_data = cache[x_plat]["variables"][x_var]["y"][-1]
        y_data = cache[y_plat]["variables"][y_var]["y"][-1]
        z_data = cache[z_plat]["variables"][z_var]["y"][-1] # For a true 3D surface, Z should be a 2D matrix. We pass the latest slice here.
        
        surface = go.Figure(data=go.Surface(z=z_data, x=x_data, y=y_data, colorscale="Viridis"))
        surface.update_layout(
            title="3D Surface Map",
            scene=dict(xaxis_title=x_var, yaxis_title=y_var, zaxis_title=z_var),
            template="simple_white", uirevision=f"{x_sel}-{y_sel}-{z_sel}"
        )
        return surface
    except (KeyError, IndexError):
        return default_fig