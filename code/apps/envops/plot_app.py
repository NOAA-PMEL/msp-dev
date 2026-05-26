import dash
from dash import html, dcc, Input, Output, State, no_update, ALL, MATCH, ctx
from dash.exceptions import PreventUpdate
from dash_extensions import WebSocket
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import logging
import json
import traceback
from datetime import datetime, timezone
import httpx

from utils import get_registry_data, config, create_unified_shell, register_sidebar_callbacks

L = logging.getLogger(__name__)

# --- Initialize Isolated Dash App ---
app = dash.Dash(__name__, requests_pathname_prefix="/envds/envops/plots/", routes_pathname_prefix="/", suppress_callback_exceptions=True)
register_sidebar_callbacks(app)

datastore_url = f"datastore.{config.daq_id}-system.svc.cluster.local"

app.layout = create_unified_shell(html.Div([
    dcc.Location(id="plot-url", refresh=False),
    html.Div(id="plot-page-content") 
]), active_item="plots")


@app.callback(
    Output("plot-page-content", "children"),
    Input("plot-url", "pathname")
)
def render_deployment_plots(pathname):
    L.info(f"[[DEBUG PLOTS]] 🚦 Callback triggered. Pathname: {pathname}")
    try:
        if not pathname or "deployment/" not in pathname:
            return dbc.Alert("Select a deployment from the sidebar.", color="info", className="m-4")
        
        deployment_id = pathname.split("/")[-1]
        L.info(f"[[DEBUG PLOTS]] ⚙️ Extracted ID: {deployment_id}. Building layout...")
        
        return build_plot_layout(deployment_id)
        
    except Exception as e:
        L.error(f"[[DEBUG PLOTS]] 💥 CRASH: {e}", exc_info=True) 
        return dbc.Alert(f"Fatal Error: {str(e)}", color="danger", className="m-4")


def build_plot_layout(deployment_id):
    # 1. Resolve Host Deployment Context
    all_deployments = get_registry_data("deployment") or []
    host_dep = next((d for d in all_deployments if d.get("metadata", {}).get("name") == deployment_id), None)
    if not host_dep:
        return dbc.Alert(f"Deployment {deployment_id} not found.", color="warning", className="m-4")
        
    host_data = host_dep.get("data", {})
    host_plat_ref = host_data.get("platform_ref", "")
    host_name = host_data.get("display_name", host_plat_ref)
    
    # 2. Resolve Child Sub-systems
    child_deps = [d for d in all_deployments if d.get("data", {}).get("host_platform_ref") == host_plat_ref and d.get("metadata", {}).get("name") != deployment_id]

    raw_targets = [host_plat_ref] + [c.get("data", {}).get("platform_ref", "") for c in child_deps]
    short_targets = [p.split(".")[-1] for p in raw_targets if "." in p]
    group_platforms = list(set(raw_targets + short_targets))
    group_platforms = [p for p in group_platforms if p]

    # 3. 🟢 VARIABLE DISCOVERY: Find Variables strictly for this Deployment
    variablemaps = get_registry_data("variablemap") or []
    master_dropdown_options = []
    
    for vmap in variablemaps:
        p_name = vmap.get("metadata", {}).get("name")
        if p_name in group_platforms:
            variables = vmap.get("data", {}).get("variables", {})
            for v_name, v_def in variables.items():
                vset = v_def.get("variableset", "main")
                long_name = v_def.get("attributes", {}).get("long_name", {}).get("data", v_name)
                unit = v_def.get("attributes", {}).get("units", {}).get("data", "")
                
                label = f"{long_name} ({unit}) - {p_name}" if unit else f"{long_name} - {p_name}"
                # Value packs the routing info: platform::variableset::variable
                value = f"{p_name}::{vset}::{v_name}" 
                
                # Only plot numbers
                dtype = v_def.get("type", "unknown")
                if dtype in ["float", "double", "int", "number"]:
                    master_dropdown_options.append({"label": label, "value": value})

    master_dropdown_options = sorted(master_dropdown_options, key=lambda x: x["label"])

    # 4. Build the UI
    header = dbc.Row([
        dbc.Col([
            html.H2([html.I(className="bi bi-graph-up me-2"), f"{host_name} Analytics"], className="fw-bold mb-0"),
            html.P(f"ID: {deployment_id} | Scoped Telemetry Explorer", className="text-muted mb-0")
        ]),
        dbc.Col([
            dbc.Button([html.I(className="bi bi-sliders me-2"), "Back to Ops Dashboard"], 
                       href=f"/envds/envops/ops/deployment/{deployment_id}", 
                       color="dark", className="fw-bold shadow-sm")
        ], width="auto", className="text-end align-self-center")
    ], className="mb-4 align-items-center border-bottom pb-3")

    ws_protocol = "wss://" if str(config.ws_use_tls).lower() == "true" else "ws://"
    ws_base = f"{ws_protocol}{config.external_hostname}:{config.ws_port}/envds/envops"

    # Listen to the exact same platform feeds as ops_app
    ws_connections = [
        WebSocket(id={"type": "ws-plot-platform", "index": p_id}, url=f"{ws_base}/ws/platform/{p_id}")
        for p_id in group_platforms if p_id
    ]

    graph_card = dbc.Card([
        dbc.CardHeader([
            dbc.Row([
                dbc.Col([
                    html.H6("Filter by Platform:", className="small text-muted fw-bold mb-1 text-uppercase"),
                    dbc.Checklist(
                        id="plot-platform-filter", 
                        options=[{"label": f" {p}", "value": p} for p in group_platforms],
                        value=group_platforms,
                        inline=True,
                        className="fw-bold text-secondary"
                    )
                ], width=5),
                dbc.Col([
                    html.H6("Select Variable:", className="small text-muted fw-bold mb-1 text-uppercase"),
                    dcc.Dropdown(
                        id="plot-variable-dropdown",
                        options=master_dropdown_options,
                        value=master_dropdown_options[0]["value"] if master_dropdown_options else None,
                        clearable=False
                    )
                ], width=7)
            ])
        ], className="bg-light pb-3"),
        dbc.CardBody([
            dcc.Graph(
                id="plot-graph-1d",
                figure=go.Figure(data=go.Scatter(x=[], y=[], type="scatter")),
                style={"height": "65vh"}
            )
        ])
    ], className="shadow-sm border-0")

    return html.Div([
        html.Div(ws_connections),
        dcc.Store(id="plot-master-options", data=master_dropdown_options),
        header, 
        graph_card
    ], className="container-fluid mt-3")


# --- CALLBACKS ---

@app.callback(
    Output("plot-variable-dropdown", "options"),
    Input("plot-platform-filter", "value"),
    State("plot-master-options", "data"),
    prevent_initial_call=False
)
def filter_dropdown(selected_platforms, master_options):
    """Filters the dropdown so it only shows variables for checked platforms."""
    if not master_options: return dash.no_update
    if not selected_platforms: return [] 
        
    filtered = []
    for opt in master_options:
        try: 
            plat_id = opt["value"].split("::")[0]
            if plat_id in selected_platforms:
                filtered.append(opt)
        except Exception: continue
            
    return filtered


@app.callback(
    Output("plot-graph-1d", "figure"),
    Input("plot-variable-dropdown", "value")
)
def init_graph_historical(selected_value):
    """Fires when dropdown changes. Fetches history and draws initial line."""
    default_fig = go.Figure(
        data=go.Scatter(x=[], y=[], type="scatter", mode="lines"),
        layout={"xaxis": {"title": "Time (UTC)"}, "yaxis": {"title": "Value"}, "margin": {"t": 30}}
    )
    if not selected_value: return default_fig
    
    try:
        # Unpack the routing info: platform::variableset::variable
        plat_id, vset, var_name = selected_value.split("::")
        short_id = f"{plat_id}::{vset}"

        # 🟢 Fetch historical data from Datastore
        url = f"http://{datastore_url}/variableset/data/get/"
        query = {"variableset_id": short_id}
        
        x, y = [], []
        try:
            timeout = httpx.Timeout(10.0)
            response = httpx.get(url, params=query, timeout=timeout)
            if response.status_code == 200:
                results = response.json().get("results", [])
                for doc in results:
                    variables = doc.get("variables", {})
                    if "time" in variables and var_name in variables:
                        x.append(variables["time"].get("data"))
                        y.append(variables[var_name].get("data"))
        except Exception as e:
            L.warning(f"[[DEBUG PLOTS]] History fetch failed: {e}")

        # Draw the graph
        fig = go.Figure(
            data=go.Scatter(x=x, y=y, type="scatter", mode="lines+markers", marker=dict(size=4)),
            layout={
                "xaxis": {"title": "Time (UTC)"},
                "yaxis": {"title": var_name},
                "margin": {"t": 30},
                "uirevision": "constant" # Prevents zoom from resetting on extendData!
            }
        )
        return fig 

    except Exception as e:
        L.error(f"[[DEBUG PLOTS]] Graph init error: {e}", exc_info=True)
        return default_fig


@app.callback(
    Output("plot-graph-1d", "extendData"),
    Input({"type": "ws-plot-platform", "index": ALL}, "message"),
    State("plot-variable-dropdown", "value"),
    prevent_initial_call=True
)
def stream_graph_telemetry(ws_messages, selected_value):
    """Appends live points to the graph seamlessly without redrawing."""
    if not ctx.triggered or not selected_value: 
        raise PreventUpdate

    try:
        # Get the ID of the WebSocket that actually fired
        triggered_id = ctx.triggered_id
        if not triggered_id or not isinstance(triggered_id, dict): 
            raise PreventUpdate
            
        incoming_plat_id = str(triggered_id.get("index"))
        plat_id, vset, var_name = selected_value.split("::")

        # 🟢 Only update if the incoming WebSocket matches the currently selected platform!
        if incoming_plat_id != plat_id:
            raise PreventUpdate

        # Parse payload
        ws_msg = ctx.triggered[0].get("value")
        if not ws_msg or "data" not in ws_msg:
            raise PreventUpdate
            
        event_data = json.loads(ws_msg["data"]).get("data-update", {})
        variables = event_data.get("variables", {})

        x_val = variables.get("time", {}).get("data") or event_data.get("time", {}).get("data")
        y_val = variables.get(var_name, {}).get("data")

        if not x_val or y_val is None:
            raise PreventUpdate

        # Format exactly as Dash extendData requires: ( {x: [[new_x]], y: [[new_y]]}, [trace_index], max_points )
        return ( {"x": [[x_val]], "y": [[y_val]]}, [0], 1000 )

    except Exception as e:
        # Silent fail on parse errors to avoid console spam during high-freq streams
        raise PreventUpdate