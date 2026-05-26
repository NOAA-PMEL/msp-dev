import dash
from dash import html, dcc, Input, Output, State, no_update, ALL, MATCH, ctx, Patch
from dash.exceptions import PreventUpdate
from dash_extensions import WebSocket
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import logging
import json
import traceback
from datetime import datetime, timezone
from collections import deque

from utils import get_registry_data, config, create_unified_shell, register_sidebar_callbacks

L = logging.getLogger(__name__)

# --- Initialize Isolated Dash App ---
app = dash.Dash(__name__, requests_pathname_prefix="/envds/envops/plots/", routes_pathname_prefix="/", suppress_callback_exceptions=True)
register_sidebar_callbacks(app)

# SERVER_PLOT_CACHE strictly holds history so dropdown switches are instant
SERVER_PLOT_CACHE = {}
MAX_POINTS = 300

app.layout = create_unified_shell(html.Div([
    dcc.Location(id="plot-url", refresh=False),
    html.Div(id="plot-page-content") 
]), active_item="plots")

@app.callback(
    Output("plot-page-content", "children"),
    Input("plot-url", "pathname")
)
def render_deployment_plots(pathname):
    try:
        if not pathname or "deployment/" not in pathname:
            return dbc.Alert("Select a deployment from the sidebar.", color="info", className="m-4")
        deployment_id = pathname.split("/")[-1]
        
        all_deployments = get_registry_data("deployment") or []
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

        plot_sections = dbc.Accordion([
            dbc.AccordionItem([
                dbc.Row([
                    dbc.Col([
                        html.Label("Select Platform & Variable", className="fw-bold small text-muted"),
                        dcc.Dropdown(id="dropdown-1d-y", placeholder="Waiting for data...", className="mb-3 shadow-sm"),
                    ], width=12, md=6)
                ]),
                dcc.Graph(id="graph-1d", config={"displayModeBar": True}, style={"height": "400px"})
            ], title="1D Time Series (Scalars)", item_id="1d"),
            
            dbc.AccordionItem([
                dbc.Row([
                    dbc.Col([
                        html.Label("Z-Axis: Measured Distribution (e.g., dNdlogDp)", className="fw-bold small text-muted"),
                        dcc.Dropdown(id="dropdown-2d-z", placeholder="Waiting for data...", className="mb-3 shadow-sm"),
                    ], width=12, md=5),
                    dbc.Col([
                        html.Label("Y/X-Axis: Coordinate (e.g., diameter, wavelength)", className="fw-bold small text-muted"),
                        dcc.Dropdown(id="dropdown-2d-y", placeholder="Waiting for data...", className="mb-3 shadow-sm"),
                    ], width=12, md=5),
                    dbc.Col([
                        html.Label("Plot Settings", className="fw-bold small text-muted d-block"),
                        dbc.Switch(id="toggle-log-scale", label="Logarithmic Scale", value=True, className="mt-2 fw-bold text-primary")
                    ], width=12, md=2)
                ]),
                dbc.Row([
                    dbc.Col(dcc.Graph(id="graph-2d-heatmap", style={"height": "450px"}), width=12, md=6),
                    dbc.Col(dcc.Graph(id="graph-2d-scatter", style={"height": "450px"}), width=12, md=6)
                ])
            ], title="2D Profiles & Distributions (Arrays)", item_id="2d")
        ], always_open=True, active_item=["1d", "2d"])

        ws_protocol = "wss://" if str(config.ws_use_tls).lower() == "true" else "ws://"
        ws_base = f"{ws_protocol}{config.external_hostname}:{config.ws_port}/envds/envops"

        ws_connections = []
        for p_id in group_platforms:
            if p_id: 
                ws_connections.append(WebSocket(id={"type": "ws-plot-platform", "index": p_id}, url=f"{ws_base}/ws/platform/{p_id}"))

        return html.Div([
            html.Div(ws_connections),
            dcc.Store(id="plot-stream-buffer", data={}), # Catches live WS points
            header, plot_sections
        ], className="container-fluid mt-3")
        
    except Exception as e:
        L.error(f"[PLOT UI] Layout Crash: {traceback.format_exc()}")
        return dbc.Alert(f"Fatal Layout Error: {e}", color="danger")


# -----------------------------------------------------------------------------
# WEBSOCKET INGEST & STREAM BUFFER
# -----------------------------------------------------------------------------
@app.callback(
    Output("plot-stream-buffer", "data"),
    Input({"type": "ws-plot-platform", "index": ALL}, "message"),
    prevent_initial_call=True
)
def ingest_and_stream(messages):
    # Only process the socket that actually fired
    trigger = ctx.triggered[0]
    if not trigger["value"] or "data" not in trigger["value"]: raise PreventUpdate
    
    try:
        plat_dict = json.loads(trigger["prop_id"].split(".")[0])
        plat_id = plat_dict.get("index", "Unknown")
        
        ws_wrapper = json.loads(trigger["value"]["data"])
        payload = ws_wrapper.get("data-update")
        if not payload: raise PreventUpdate
            
        incoming_vars = payload.get("variables", {})
        current_time = incoming_vars.get("time", {}).get("data") or datetime.now(timezone.utc).isoformat()
        
        if plat_id not in SERVER_PLOT_CACHE: SERVER_PLOT_CACHE[plat_id] = {}
        
        stream_payload = {"platform": plat_id, "variables": {}}

        for var_name, var_data in incoming_vars.items():
            if var_name == "time": continue
            val = var_data.get("data")
            if val is not None:
                is_2d = isinstance(val, list)
                
                # 1. Update History Cache
                if var_name not in SERVER_PLOT_CACHE[plat_id]:
                    SERVER_PLOT_CACHE[plat_id][var_name] = {"x": deque(maxlen=MAX_POINTS), "y": deque(maxlen=MAX_POINTS), "unit": var_data.get("unit", ""), "is_2d": is_2d}
                
                SERVER_PLOT_CACHE[plat_id][var_name]["x"].append(current_time)
                SERVER_PLOT_CACHE[plat_id][var_name]["y"].append(val)
                
                # 2. Add to live stream buffer
                stream_payload["variables"][var_name] = {"x": current_time, "y": val, "is_2d": is_2d}
                
        # Send live points to the browser via the Store
        return stream_payload
    except Exception as e:
        raise PreventUpdate

# -----------------------------------------------------------------------------
# DYNAMIC UI RENDERERS (extendData & Layout)
# -----------------------------------------------------------------------------
@app.callback(
    Output("dropdown-1d-y", "options"), Output("dropdown-2d-y", "options"), Output("dropdown-2d-z", "options"),
    Input("plot-stream-buffer", "data")
)
def populate_dropdowns(stream_data):
    opts_1d, opts_2d = [], []
    for platform_id, p_vars in SERVER_PLOT_CACHE.items():
        for var_name, var_dict in p_vars.items():
            label = f"{var_name.replace('_', ' ').title()} ({platform_id})"
            val = f"{platform_id}::{var_name}"
            if var_dict.get("is_2d"): opts_2d.append({"label": label, "value": val})
            else:
                opts_1d.append({"label": label, "value": val})
                opts_2d.append({"label": f"[Coord] {label}", "value": val})
            
    opts_1d = sorted(opts_1d, key=lambda d: d['label'])
    opts_2d = sorted(opts_2d, key=lambda d: d['label'])
    return opts_1d, opts_2d, opts_2d

# --- 1D PLOTS (HISTORY LOAD & extendData) ---
@app.callback(
    Output("graph-1d", "figure"), Output("graph-1d", "extendData"),
    Input("dropdown-1d-y", "value"), Input("plot-stream-buffer", "data"),
    State("graph-1d", "figure")
)
def manage_1d_plot(selected_var, stream_data, current_fig):
    trigger_id = ctx.triggered_id
    default_fig = go.Figure(layout={"xaxis_title": "Time", "yaxis_title": "Value", "template": "simple_white"})

    # 1. DROPDOWN CHANGED -> Load Full History
    if trigger_id == "dropdown-1d-y":
        if not selected_var: return default_fig, no_update
        platform_id, var_name = selected_var.split("::")
        if platform_id in SERVER_PLOT_CACHE and var_name in SERVER_PLOT_CACHE[platform_id]:
            var_data = SERVER_PLOT_CACHE[platform_id][var_name]
            fig = go.Figure(go.Scatter(x=list(var_data["x"]), y=list(var_data["y"]), mode="lines+markers", line=dict(color="#1f77b4", width=2)))
            fig.update_layout(title=f"{var_name.replace('_', ' ').title()} <br><span style='font-size:10px;color:gray;'>{platform_id}</span>",
                              yaxis_title=var_data.get("unit", ""), xaxis_title="Time", template="simple_white", margin=dict(t=50, b=30, l=40, r=40))
            return fig, no_update
        return default_fig, no_update

    # 2. STREAM BUFFER UPDATED -> Use extendData
    if trigger_id == "plot-stream-buffer" and selected_var and stream_data:
        platform_id, var_name = selected_var.split("::")
        if stream_data.get("platform") == platform_id and var_name in stream_data.get("variables", {}):
            new_x = stream_data["variables"][var_name]["x"]
            new_y = stream_data["variables"][var_name]["y"]
            # Dash standard format for extendData: ({dict of arrays}, trace_indices, max_points)
            return no_update, ({"x": [[new_x]], "y": [[new_y]]}, [0], MAX_POINTS)

    return no_update, no_update


# --- 2D PLOTS (HISTORY LOAD & LIVE UPDATE) ---
@app.callback(
    Output("graph-2d-heatmap", "figure"), Output("graph-2d-scatter", "figure"), Output("graph-2d-scatter", "extendData"),
    Input("dropdown-2d-y", "value"), Input("dropdown-2d-z", "value"), Input("toggle-log-scale", "value"), Input("plot-stream-buffer", "data"),
    State("graph-2d-heatmap", "figure")
)
def manage_2d_plots(coord_sel, dist_sel, use_log, stream_data, heatmap_fig):
    default_fig = go.Figure(layout={"template": "simple_white"})
    if not coord_sel or not dist_sel: return default_fig, default_fig, no_update
    
    y_plat, y_var = coord_sel.split("::")
    z_plat, z_var = dist_sel.split("::")
    scale_type = "log" if use_log else "linear"
    trigger_id = ctx.triggered_id

    # Extract required history
    if z_plat not in SERVER_PLOT_CACHE or z_var not in SERVER_PLOT_CACHE[z_plat]: return no_update, no_update, no_update
    if y_plat not in SERVER_PLOT_CACHE or y_var not in SERVER_PLOT_CACHE[y_plat]: return no_update, no_update, no_update
    
    time_data = list(SERVER_PLOT_CACHE[z_plat][z_var]["x"])
    z_arrays = list(SERVER_PLOT_CACHE[z_plat][z_var]["y"])
    y_raw = list(SERVER_PLOT_CACHE[y_plat][y_var]["y"])[-1] 
    coord_data = y_raw if isinstance(y_raw, list) else list(SERVER_PLOT_CACHE[y_plat][y_var]["y"])
    
    min_len = min(len(row) for row in z_arrays if isinstance(row, list)) if z_arrays else 0
    if min_len == 0: return no_update, no_update, no_update
    coord_truncated = coord_data[:min_len]

    # 1. DROPDOWN / TOGGLE CHANGED -> Build Full History Figures
    if trigger_id in ["dropdown-2d-y", "dropdown-2d-z", "toggle-log-scale"]:
        z_transposed = [[row[i] for row in z_arrays if isinstance(row, list)] for i in range(min_len)]
        
        heatmap = go.Figure(go.Heatmap(x=time_data, y=coord_truncated, z=z_transposed, colorscale="Rainbow"))
        heatmap.update_layout(title=f"Time Profile: {z_var}", yaxis_title=f"{y_var} ({scale_type})", xaxis_title="Time", yaxis_type=scale_type, template="simple_white")

        latest_z = z_arrays[-1] if isinstance(z_arrays[-1], list) else []
        snapshot = go.Figure(go.Scatter(x=coord_truncated, y=latest_z[:min_len], mode="lines+markers", marker=dict(color="#d62728")))
        snapshot.update_layout(title=f"Latest Snapshot", xaxis_title=f"{y_var} ({scale_type})", yaxis_title=z_var, xaxis_type=scale_type, template="simple_white", yaxis=dict(rangemode="tozero"))
        
        return heatmap, snapshot, no_update

    # 2. STREAM BUFFER UPDATED -> Update Heatmap & extendData Scatter
    if trigger_id == "plot-stream-buffer":
        if stream_data.get("platform") == z_plat and z_var in stream_data.get("variables", {}):
            new_x = stream_data["variables"][z_var]["x"]
            new_z_array = stream_data["variables"][z_var]["y"]
            
            if not isinstance(new_z_array, list): return no_update, no_update, no_update
            
            # Heatmaps don't support extendData natively on Z-arrays. Rebuild with latest cache.
            z_transposed = [[row[i] for row in z_arrays if isinstance(row, list)] for i in range(min_len)]
            new_heatmap = go.Figure(go.Heatmap(x=time_data, y=coord_truncated, z=z_transposed, colorscale="Rainbow"))
            new_heatmap.update_layout(title=f"Time Profile: {z_var}", yaxis_title=f"{y_var} ({scale_type})", xaxis_title="Time", yaxis_type=scale_type, template="simple_white")

            # Snapshot Scatter can use extendData trick: We replace the Y data entirely for the line.
            patched_scatter = Patch()
            patched_scatter["data"][0]["y"] = new_z_array[:min_len]
            patched_scatter["layout"]["title"]["text"] = f"Latest Snapshot ({str(new_x)[11:19]} Z)"

            return new_heatmap, patched_scatter, no_update

    return no_update, no_update, no_update