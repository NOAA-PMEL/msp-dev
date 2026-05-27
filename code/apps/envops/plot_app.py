import dash
from dash import html, dcc, Input, Output, State, no_update, ALL, MATCH, ctx
from dash.exceptions import PreventUpdate
from dash_extensions import WebSocket
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import logging
import json
import httpx
import traceback
import flask

from utils import get_registry_data, config, create_unified_shell, register_sidebar_callbacks

L = logging.getLogger(__name__)

app = dash.Dash(__name__, requests_pathname_prefix="/envds/envops/plots/", routes_pathname_prefix="/", suppress_callback_exceptions=True)
register_sidebar_callbacks(app)

datastore_url = f"datastore.{config.daq_id}-system.svc.cluster.local"

# 🟢 FIX: Corrected the WebSocket Path to match your FastAPI Mount!
ws_protocol = "wss://" if str(config.ws_use_tls).lower() == "true" else "ws://"
ws_base = f"{ws_protocol}{config.external_hostname}:{config.ws_port}/msp/dashboardtest"


def get_short_id(full_id: str) -> str:
    parts = full_id.split("::")
    if len(parts) >= 4: return f"{parts[1]}::{parts[3]}"
    return full_id

# --- Dynamic Builders ---
def build_graph_1d(dropdown_list, xaxis="time"):
    default_fig = go.Figure(layout={"template": "simple_white", "xaxis": {"title": "Time (UTC)"}, "yaxis": {"title": "Value"}})
    return dbc.Card([
        dbc.CardHeader([dcc.Dropdown(id={"type": "plot-graph-1d-dropdown", "index": xaxis}, options=dropdown_list, value="", placeholder="Select variable to plot...")]),
        dcc.Graph(id={"type": "plot-graph-1d", "index": xaxis}, figure=default_fig, style={"height": 400}),
    ], className="shadow-sm border-0")

def build_graph_2d(dropdown_list, xaxis="time", yaxis=""):
    default_heatmap = go.Figure(layout={"template": "simple_white", "xaxis": {"title": "Time"}, "yaxis": {"title": yaxis}})
    default_scatter = go.Figure(layout={"template": "simple_white", "xaxis": {"title": yaxis}, "yaxis": {"title": "Value"}})
    content = dbc.Row([
        dbc.Button("Submit Range", {"type": "plot-graph-2d-z-axis-submit", "index": f"{xaxis}::{yaxis}"}, color="primary", className="mb-2"),
        dbc.Label("z-axis min:", className="small text-muted fw-bold"), dbc.Col(dbc.Input(type="number", id={"type": "plot-graph-2d-z-axis-min", "index": f"{xaxis}::{yaxis}"}, className="mb-2")),
        dbc.Label("z-axis max:", className="small text-muted fw-bold"), dbc.Col(dbc.Input(type="number", id={"type": "plot-graph-2d-z-axis-max", "index": f"{xaxis}::{yaxis}"})),
    ])
    axes_settings = dbc.Accordion([dbc.AccordionItem([dbc.Card(children=[content], className="border-0 shadow-sm p-3")], title="Axes Settings")], start_collapsed=True, className="mb-3")
    return dbc.Card([
        dbc.CardHeader([dcc.Dropdown(id={"type": "plot-graph-2d-dropdown", "index": f"{xaxis}::{yaxis}"}, options=dropdown_list, value="", placeholder="Select variable to plot...")]),
        dbc.CardBody([
            axes_settings,
            dbc.Row([
                dbc.Col(dcc.Graph(id={"type": "plot-graph-2d-heatmap", "index": f"{xaxis}::{yaxis}"}, figure=default_heatmap, style={"height": 500})),
                dbc.Col(dcc.Graph(id={"type": "plot-graph-2d-line", "index": f"{xaxis}::{yaxis}"}, figure=default_scatter, style={"height": 500})),
            ])
        ])
    ], className="shadow-sm border-0")

def build_graph_3d(dropdown_list, xaxis="", yaxis=""):
    default_fig = go.Figure(layout={"template": "simple_white"})
    return dbc.Card([
        dbc.CardHeader([dcc.Dropdown(id={"type": "plot-graph-3d-dropdown", "index": f"{xaxis}::{yaxis}"}, options=dropdown_list, value="", placeholder="Select variable to plot...")]),
        dbc.Row([
            dbc.Col(dcc.Graph(id={"type": "plot-graph-3d-line", "index": f"{xaxis}::{yaxis}"}, figure=default_fig, style={"height": 500})),
            dbc.Col(dcc.Graph(id={"type": "plot-graph-3d-heatmap", "index": f"{xaxis}::{yaxis}"}, figure=default_fig, style={"height": 500})),
        ]),
    ])

def build_graphs(layout_options):
    graph_list = []
    for ltype, dims in layout_options.items():
        for dim, options in dims.items():
            if not options["variable-list"]: continue
            if ltype == "layout-1d":
                graph_list.append(dbc.AccordionItem([build_graph_1d(options["variable-list"], xaxis=dim)], title=f"Plots 1-D ({dim})"))
            elif ltype == "layout-2d":
                graph_list.append(dbc.AccordionItem([build_graph_2d(options["variable-list"], xaxis="time", yaxis=dim)], title=f"Plots 2-D (time, {dim})"))
            elif ltype == "layout-3d":
                axes = dim.split("::")
                graph_list.append(dbc.AccordionItem([build_graph_3d(options["variable-list"], xaxis=axes[0], yaxis=axes[1])], title=f"Plots 3-D ({axes[0]}, {axes[1]})"))
    return graph_list


# --- Core View Renderers (Server Side Rendered) ---
def render_deployment_plots(deployment_id):
    L.info(f"🚨 SSR BUILD: Building plot page for deployment {deployment_id}")
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
    group_platforms = [p for p in list(set(raw_targets + short_targets)) if p]

    vset_defs = {} 
    try:
        ids_url = f"http://{datastore_url}/variableset-definition/registry/ids/get/"
        timeout = httpx.Timeout(10.0)
        ids_response = httpx.get(ids_url, timeout=timeout)
        for full_id in ids_response.json().get("results", []):
            if not full_id: continue
            short_id = get_short_id(full_id)
            if short_id.split("::")[0] in group_platforms:
                def_url = f"http://{datastore_url}/variableset-definition/registry/get/"
                def_response = httpx.get(def_url, params={"variableset_definition_id": full_id}, timeout=timeout)
                if def_response.status_code == 200: vset_defs[short_id] = def_response.json().get("results", [{}])[0]
    except Exception as e: L.error(f"🚨 SSR BUILD: Failed to fetch variablesets: {e}")

    layout_options = {"layout-1d": {"time": {"variable-list": []}}, "layout-2d": {}, "layout-3d": {}}
    for short_id, vset_def in vset_defs.items():
        for v_name, v_def in vset_def.get("variables", {}).items():
            long_name = v_def.get("attributes", {}).get("long_name", {}).get("data", v_name)
            unit = v_def.get("attributes", {}).get("units", {}).get("data", "")
            label = f"{long_name} ({unit}) - {short_id}" if unit else f"{long_name} - {short_id}"
            value = f"{short_id}::{v_name}" 
            
            dtype = str(v_def.get("type", "unknown")).lower()
            if not any(x in dtype for x in ["float", "double", "int", "number"]): continue
                
            shape = v_def.get("shape", ["time"])
            if not shape: shape = ["time"]
            is_multi_dim = len(shape) > 1
            
            option = {"label": label, "value": value}
            
            if is_multi_dim and len(shape) == 2:
                dim_2d = [d for d in shape if d != "time"][0]
                if dim_2d not in layout_options["layout-2d"]: layout_options["layout-2d"][dim_2d] = {"variable-list": []}
                layout_options["layout-2d"][dim_2d]["variable-list"].append(option)
            elif is_multi_dim and len(shape) == 3:
                dims_3d = [d for d in shape if d != "time"]
                dim_3d_key = f"{dims_3d[0]}::{dims_3d[1]}"
                if dim_3d_key not in layout_options["layout-3d"]: layout_options["layout-3d"][dim_3d_key] = {"variable-list": []}
                layout_options["layout-3d"][dim_3d_key]["variable-list"].append(option)
            else:
                layout_options["layout-1d"]["time"]["variable-list"].append(option)
                    
    for ltype in layout_options.values():
        for dim_data in ltype.values():
            dim_data["variable-list"] = sorted(dim_data["variable-list"], key=lambda x: x["label"])

    header = dbc.Row([
        dbc.Col([
            html.H2([html.I(className="bi bi-graph-up me-2"), f"{host_name} Analytics"], className="fw-bold mb-0"),
            html.P(f"ID: {deployment_id} | Multi-Dimensional Explorer", className="text-muted mb-0")
        ]),
        dbc.Col([dbc.Button([html.I(className="bi bi-sliders me-2"), "Back to Ops Dashboard"], href=f"/envds/envops/ops/deployment/{deployment_id}", color="dark", className="fw-bold shadow-sm")], width="auto", className="text-end align-self-center")
    ], className="mb-4 align-items-center border-bottom pb-3")

    ws_connections = [WebSocket(id={"type": "ws-variableset", "index": short_id}, url=f"{ws_base}/ws/variableset/{short_id}") for short_id in vset_defs.keys()]
    
    # 🟢 FIX: Wrap the layout in a Div with a dynamic `key`. This forces React to unmount the old WebSockets cleanly!
    return html.Div(key=deployment_id, children=[
        html.Div(ws_connections),
        dcc.Store(id="plot-vset-definitions", data=vset_defs),
        dcc.Store(id="plot-data-buffer", data={}),
        header, 
        dbc.Accordion(build_graphs(layout_options), id="plot-accordion", className="mb-4", always_open=True)
    ], className="container-fluid mt-3")


# --- Pure SSR Layout Router ---
# 🟢 FIX: Completely removed the dcc.Location and `@app.callback` router. 
# Dash will natively route using flask.request on page load.
def serve_layout():
    """Builds the UI on the server based on the active URL."""
    try:
        pathname = flask.request.path
        if "/deployment/" in pathname:
            deployment_id = pathname.split("/deployment/")[-1]
            return create_unified_shell(render_deployment_plots(deployment_id), active_item="plots")
        return create_unified_shell(dbc.Alert("Select a deployment from the sidebar.", color="info", className="m-4"), active_item="plots")
    except Exception as e:
        L.error(f"Error serving layout: {traceback.format_exc()}")
        return create_unified_shell(dbc.Alert(f"Fatal Error: {e}", color="danger", className="m-4"), active_item="plots")

app.layout = serve_layout


# --- Sub-Callbacks (Isolated per page load) ---
@app.callback(
    Output("plot-data-buffer", "data"),
    Input({"type": "ws-variableset", "index": ALL}, "message"),
    prevent_initial_call=True
)
def buffer_ws_streams(messages):
    if not ctx.triggered or not ctx.triggered_id: raise PreventUpdate
    short_id = str(ctx.triggered_id.get("index"))
    msg = ctx.triggered[0].get("value")
    if not msg or "data" not in msg: raise PreventUpdate
    try:
        event_data = json.loads(msg["data"]).get("data-update")
        if not event_data: raise PreventUpdate
        return {"short_id": short_id, "data-update": event_data}
    except Exception: raise PreventUpdate

@app.callback(
    Output({"type": "plot-graph-1d", "index": MATCH}, "figure"),
    Input({"type": "plot-graph-1d-dropdown", "index": MATCH}, "value")
)
def init_graph_1d(selected_value):
    default_fig = go.Figure(layout={"template": "simple_white", "xaxis": {"title": "Time (UTC)"}, "yaxis": {"title": "Value"}, "margin": {"t": 30}})
    if not selected_value: return default_fig
    try:
        parts = selected_value.split("::")
        short_id = f"{parts[0]}::{parts[1]}"
        var_name = parts[2]
        url = f"http://{datastore_url}/variableset/data/get/"
        x, y = [], []
        response = httpx.get(url, params={"variableset_id": short_id}, timeout=10.0)
        if response.status_code == 200:
            for doc in response.json().get("results", []):
                variables = doc.get("variables", {})
                if "time" in variables and var_name in variables:
                    x.append(variables["time"].get("data"))
                    y.append(variables[var_name].get("data"))
        return go.Figure(data=go.Scatter(x=x, y=y, type="scatter", mode="lines+markers", marker=dict(size=4)), layout={"template": "simple_white", "xaxis": {"title": "Time (UTC)"}, "yaxis": {"title": var_name}, "margin": {"t": 30}, "uirevision": "constant"})
    except Exception: return default_fig

@app.callback(
    Output({"type": "plot-graph-1d", "index": ALL}, "extendData"),
    Input("plot-data-buffer", "data"),
    State({"type": "plot-graph-1d-dropdown", "index": ALL}, "value"),
    prevent_initial_call=True
)
def update_graph_1d(buffer_payload, selected_values):
    if not buffer_payload: raise PreventUpdate
    incoming_short_id = buffer_payload.get("short_id")
    event_data = buffer_payload.get("data-update", {})
    figs_to_update = []
    for selected_value in selected_values:
        if not selected_value: 
            figs_to_update.append(no_update)
            continue
        parts = selected_value.split("::")
        short_id = f"{parts[0]}::{parts[1]}"
        var_name = parts[2]
        if incoming_short_id != short_id:
            figs_to_update.append(no_update)
            continue
        variables = event_data.get("variables", {})
        x_val, y_val = variables.get("time", {}).get("data"), variables.get(var_name, {}).get("data")
        if x_val is None or y_val is None:
            figs_to_update.append(no_update)
            continue
        if isinstance(x_val, list) and len(x_val) > 0: x_val = x_val[-1]
        if isinstance(y_val, list) and len(y_val) > 0: y_val = y_val[-1]
        figs_to_update.append(({"x": [[x_val]], "y": [[y_val]]}, [0], 1000))
    if all(f == no_update for f in figs_to_update): raise PreventUpdate
    return figs_to_update

@app.callback(
    [Output({"type": "plot-graph-2d-heatmap", "index": MATCH}, "figure", allow_duplicate=True), Output({"type": "plot-graph-2d-line", "index": MATCH}, "figure", allow_duplicate=True)],
    Input({"type": "plot-graph-2d-dropdown", "index": MATCH}, "value"),
    [State("plot-vset-definitions", "data"), State({"type": "plot-graph-2d-dropdown", "index": MATCH}, "id")],
    prevent_initial_call=True,
)
def init_graph_2d(selected_value, vset_defs, graph_id):
    y_axis = graph_id["index"].split("::")[1]
    default_heatmap = go.Figure(layout={"template": "simple_white", "xaxis": {"title": "Time"}, "yaxis": {"title": y_axis}})
    default_scatter = go.Figure(layout={"template": "simple_white", "xaxis": {"title": y_axis}, "yaxis": {"title": "Value"}})
    if not selected_value or not vset_defs: return [default_heatmap, default_scatter]
    try:
        parts = selected_value.split("::")
        short_id = f"{parts[0]}::{parts[1]}"
        z_axis = parts[2]
        use_log = (y_axis == "diameter")
        x, y, orig_z = [], [], y_is_coord = False
        vmap = vset_defs.get(short_id, {}).get("variables", {})
        if y_axis in vmap and vmap[y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
            y_is_coord = True
            y = vmap[y_axis].get("data", [])
        response = httpx.get(f"http://{datastore_url}/variableset/data/get/", params={"variableset_id": short_id}, timeout=10.0)
        if response.status_code == 200:
            for doc in response.json().get("results", []):
                t_data = doc.get("variables", {}).get("time", {}).get("data")
                z_data = doc.get("variables", {}).get(z_axis, {}).get("data")
                if t_data is None or z_data is None: continue
                if isinstance(t_data, list):
                    x.extend(t_data)
                    orig_z.extend(z_data)
                    if not y_is_coord: y.extend(doc.get("variables", {}).get(y_axis, {}).get("data", []))
                else:
                    x.append(t_data)
                    orig_z.append(z_data)
                    if not y_is_coord: y.append(doc.get("variables", {}).get(y_axis, {}).get("data"))
        if len(y) > 0 and isinstance(y[-1], list): y = y[-1]
        z = []
        for yi in range(len(y)):
            new_z = []
            for xi in range(len(x)):
                try: new_z.append(orig_z[xi][yi])
                except IndexError: new_z.append(None)
            z.append(new_z)
        heatmap = go.Figure(data=go.Heatmap(x=x, y=y, z=z, type="heatmap", colorscale="Rainbow"), layout={"template": "simple_white", "xaxis": {"title": "Time"}, "yaxis": {"title": y_axis}})
        scatter = go.Figure(data=[{"x": y, "y": orig_z[-1] if len(orig_z) > 0 else [], "type": "scatter"}], layout={"template": "simple_white", "xaxis": {"title": y_axis}, "yaxis": {"title": z_axis}, "title": str(x[-1]) if len(x) > 0 else ""})
        if use_log:
            heatmap.update_yaxes(type="log")
            heatmap.update_layout(coloraxis=dict(cmax=None, cmin=None))
            scatter.update_xaxes(type="log")
        return [heatmap, scatter]
    except Exception: return [default_heatmap, default_scatter]

@app.callback(
    Output({"type": "plot-graph-2d-heatmap", "index": ALL}, "figure", allow_duplicate=True),
    Input("plot-data-buffer", "data"),
    [State({"type": "plot-graph-2d-dropdown", "index": ALL}, "value"), State("plot-vset-definitions", "data"), State({"type": "plot-graph-2d-heatmap", "index": ALL}, "figure"), State({"type": "plot-graph-2d-heatmap", "index": ALL}, "id")],
    prevent_initial_call=True,
)
def update_graph_2d_heatmap(buffer_payload, selected_values, vset_defs, current_figs, graph_ids):
    if not buffer_payload: raise PreventUpdate
    incoming_short_id = buffer_payload.get("short_id")
    event_data = buffer_payload.get("data-update", {})
    heatmaps = []
    for selected_value, graph_id, current_fig in zip(selected_values, graph_ids, current_figs):
        if not current_fig or not selected_value:
            heatmaps.append(no_update)
            continue
        parts = selected_value.split("::")
        short_id = f"{parts[0]}::{parts[1]}"
        z_axis = parts[2]
        if incoming_short_id != short_id:
            heatmaps.append(no_update)
            continue
        y_axis = graph_id["index"].split("::")[1]
        y_is_coord = False
        vmap = vset_defs.get(short_id, {}).get("variables", {})
        if y_axis in vmap and vmap[y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate": y_is_coord = True
        if ("time" not in event_data.get("variables", {}) or (not y_is_coord and y_axis not in event_data.get("variables", {})) or z_axis not in event_data.get("variables", {})):
            heatmaps.append(no_update)
            continue
        x = event_data["variables"]["time"]["data"]
        if x in current_fig["data"][0].get("x", []):
            heatmaps.append(no_update)
            continue
        if not isinstance(x, list): x = [x]
        for nx in x: current_fig["data"][0]["x"].append(nx)
        y = current_fig["data"][0].get("y", [])
        if len(y) == 0: y = vmap[y_axis].get("data", []) if y_is_coord else event_data["variables"][y_axis]["data"]
        orig_z = event_data["variables"][z_axis]["data"]
        if not isinstance(orig_z, list): orig_z = [orig_z]
        if len(x) > 1:
            z = []
            for yi, yval in enumerate(y):
                new_z = []
                for xi, xval in enumerate(x):
                    try: new_z.append(orig_z[xi][yi])
                    except IndexError: new_z.append(None)
                z.append(new_z)
            current_fig["data"][0]["z"] = z
        else:
            for yi, yval in enumerate(y):
                try: current_fig["data"][0]["z"][yi].append(orig_z[yi])
                except IndexError: pass
        heatmaps.append(current_fig)
    if all(h == no_update for h in heatmaps): raise PreventUpdate
    return heatmaps

@app.callback(
    Output({"type": "plot-graph-2d-line", "index": ALL}, "figure"),
    Input("plot-data-buffer", "data"),
    [State({"type": "plot-graph-2d-dropdown", "index": ALL}, "value"), State("plot-vset-definitions", "data"), State({"type": "plot-graph-2d-line", "index": ALL}, "figure"), State({"type": "plot-graph-2d-line", "index": ALL}, "id")],
    prevent_initial_call=True,
)
def update_graph_2d_scatter(buffer_payload, selected_values, vset_defs, current_figs, graph_ids):
    if not buffer_payload: raise PreventUpdate
    incoming_short_id = buffer_payload.get("short_id")
    event_data = buffer_payload.get("data-update", {})
    scatters = []
    for selected_value, graph_id, current_fig in zip(selected_values, graph_ids, current_figs):
        if not current_fig or not selected_value:
            scatters.append(no_update)
            continue
        parts = selected_value.split("::")
        short_id = f"{parts[0]}::{parts[1]}"
        z_axis = parts[2]
        if incoming_short_id != short_id:
            scatters.append(no_update)
            continue
        y_axis = graph_id["index"].split("::")[1]
        y_is_coord = False
        vmap = vset_defs.get(short_id, {}).get("variables", {})
        if y_axis in vmap and vmap[y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate": y_is_coord = True
        if ("time" not in event_data.get("variables", {}) or (not y_is_coord and y_axis not in event_data.get("variables", {})) or z_axis not in event_data.get("variables", {})):
            scatters.append(no_update)
            continue
        x = event_data["variables"]["time"]["data"]
        y = vmap[y_axis].get("data", []) if y_is_coord else event_data["variables"][y_axis]["data"]
        z = event_data["variables"][z_axis]["data"]
        current_fig["data"][0]["x"] = y
        current_fig["data"][0]["y"] = z
        if isinstance(x, list) and len(x) > 0: x = x[-1]
        current_fig["layout"]["title"] = str(x)
        scatters.append(current_fig)
    if all(s == no_update for s in scatters): raise PreventUpdate
    return scatters

@app.callback(
    [Output({"type": "plot-graph-3d-line", "index": MATCH}, "figure", allow_duplicate=True), Output({"type": "plot-graph-3d-heatmap", "index": MATCH}, "figure", allow_duplicate=True)],
    Input({"type": "plot-graph-3d-dropdown", "index": MATCH}, "value"),
    [State("plot-vset-definitions", "data"), State({"type": "plot-graph-3d-dropdown", "index": MATCH}, "id")],
    prevent_initial_call=True,
)
def init_graph_3d(selected_value, vset_defs, graph_id):
    default_fig = go.Figure(layout={"template": "simple_white"})
    if not selected_value or not vset_defs: return [default_fig, default_fig]
    try:
        parts = selected_value.split("::")
        short_id = f"{parts[0]}::{parts[1]}"
        z_axis = parts[2]
        x_axis = graph_id["index"].split("::")[0]
        y_axis = graph_id["index"].split("::")[1]
        x_is_coord, y_is_coord = False, False
        x, y, z_history = [], [], []
        vmap = vset_defs.get(short_id, {}).get("variables", {})
        if x_axis in vmap and vmap[x_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
            x_is_coord = True
            x = vmap[x_axis].get("data", [])
        if y_axis in vmap and vmap[y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate":
            y_is_coord = True
            y = vmap[y_axis].get("data", [])
        response = httpx.get(f"http://{datastore_url}/variableset/data/get/", params={"variableset_id": short_id}, timeout=10.0)
        if response.status_code == 200:
            for doc in response.json().get("results", []):
                z_data = doc.get("variables", {}).get(z_axis, {}).get("data")
                if z_data is None: continue
                if isinstance(z_data, list):
                    if not x_is_coord: x.extend(doc.get("variables", {}).get(x_axis, {}).get("data", []))
                    if not y_is_coord: y.extend(doc.get("variables", {}).get(y_axis, {}).get("data", []))
                    z_history.extend(z_data)
                else:
                    if not x_is_coord: x.append(doc.get("variables", {}).get(x_axis, {}).get("data"))
                    if not y_is_coord: y.append(doc.get("variables", {}).get(y_axis, {}).get("data"))
                    z_history.append(z_data)
        if len(x) > 0 and isinstance(x[-1], list): x = x[-1]
        if len(y) > 0 and isinstance(y[-1], list): y = y[-1]
        if not z_history: return [default_fig, default_fig]
        latest_z = z_history[-1] 
        z = []
        for yi in range(len(y)):
            new_row = []
            for xi in range(len(x)):
                try: new_row.append(latest_z[xi][yi])
                except IndexError: new_row.append(None)
            z.append(new_row)
        scatter = go.Figure(data=go.Surface(z=z, x=x, y=y))
        scatter.update_scenes(xaxis_title_text=x_axis, yaxis_title_text=y_axis, zaxis_title_text=z_axis)
        heatmap = go.Figure(data=go.Heatmap(z=z, x=x, y=y, type="heatmap", colorscale="Rainbow"))
        heatmap.update_layout(xaxis={"title": x_axis}, yaxis={"title": y_axis})
        if x_axis == "diameter": heatmap.update_xaxes(type="log")
        return [scatter, heatmap]
    except Exception: return [default_fig, default_fig]

@app.callback(
    [Output({"type": "plot-graph-3d-line", "index": ALL}, "figure"), Output({"type": "plot-graph-3d-heatmap", "index": ALL}, "figure")],
    Input("plot-data-buffer", "data"),
    [State({"type": "plot-graph-3d-dropdown", "index": ALL}, "value"), State("plot-vset-definitions", "data"), State({"type": "plot-graph-3d-line", "index": ALL}, "figure"), State({"type": "plot-graph-3d-heatmap", "index": ALL}, "figure"), State({"type": "plot-graph-3d-dropdown", "index": ALL}, "id")],
    prevent_initial_call=True,
)
def update_graph_3d_plots(buffer_payload, selected_values, vset_defs, line_figs, heatmap_figs, graph_ids):
    if not buffer_payload: raise PreventUpdate
    incoming_short_id = buffer_payload.get("short_id")
    event_data = buffer_payload.get("data-update", {})
    updated_lines, updated_heatmaps = [], []
    for selected_value, graph_id, line_fig, heatmap_fig in zip(selected_values, graph_ids, line_figs, heatmap_figs):
        if not selected_value or not line_fig or not heatmap_fig:
            updated_lines.append(no_update)
            updated_heatmaps.append(no_update)
            continue
        parts = selected_value.split("::")
        short_id = f"{parts[0]}::{parts[1]}"
        z_axis = parts[2]
        if incoming_short_id != short_id:
            updated_lines.append(no_update)
            updated_heatmaps.append(no_update)
            continue
        x_axis = graph_id["index"].split("::")[0]
        y_axis = graph_id["index"].split("::")[1]
        x_is_coord, y_is_coord = False, False
        vmap = vset_defs.get(short_id, {}).get("variables", {})
        if x_axis in vmap and vmap[x_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate": x_is_coord = True
        if y_axis in vmap and vmap[y_axis].get("attributes", {}).get("variable_type", {}).get("data") == "coordinate": y_is_coord = True
        if ((not x_is_coord and x_axis not in event_data.get("variables", {})) or (not y_is_coord and y_axis not in event_data.get("variables", {})) or z_axis not in event_data.get("variables", {})):
            updated_lines.append(no_update)
            updated_heatmaps.append(no_update)
            continue
        x = vmap[x_axis].get("data", []) if x_is_coord else event_data["variables"][x_axis]["data"]
        y = vmap[y_axis].get("data", []) if y_is_coord else event_data["variables"][y_axis]["data"]
        latest_z = event_data["variables"][z_axis]["data"]
        z = []
        for yi in range(len(y)):
            new_row = []
            for xi in range(len(x)):
                try: new_row.append(latest_z[xi][yi])
                except IndexError: new_row.append(None)
            z.append(new_row)
        line_fig["data"][0]["z"] = z
        heatmap_fig["data"][0]["z"] = z
        if isinstance(x, list) and len(x) > 0: x_title = x[-1]
        else: x_title = x
        line_fig["layout"]["title"] = str(x_title)
        updated_lines.append(line_fig)
        updated_heatmaps.append(heatmap_fig)
    if all(l == no_update for l in updated_lines): raise PreventUpdate
    return updated_lines, updated_heatmaps

@app.callback(
    Output({"type": "plot-graph-2d-heatmap", "index": MATCH}, "figure", allow_duplicate=True),
    [Input({"type": "plot-graph-2d-z-axis-submit", "index": MATCH}, "n_clicks")],
    [State({"type": "plot-graph-2d-z-axis-min", "index": MATCH}, "value"), State({"type": "plot-graph-2d-z-axis-max", "index": MATCH}, "value"), State({"type": "plot-graph-2d-heatmap", "index": MATCH}, "figure")],
    prevent_initial_call=True
)
def set_2d_z_axis_range(n, axis_min, axis_max, heatmap):
    return go.Figure(heatmap).update_layout(coloraxis=dict(cauto=False, cmax=axis_max, cmin=axis_min))