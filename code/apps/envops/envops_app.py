import dash
from dash import html, dcc, Input, Output, State
import dash_bootstrap_components as dbc

# Because we put bootstrap.min.css in the assets/ folder, Dash will automatically 
# load it. We no longer need to pass external_stylesheets!
app = dash.Dash(
    __name__,
    use_pages=True,
    requests_pathname_prefix="/envds/envops/", 
)

# 1. Define the Sidebar Header
sidebar_header = dbc.Row([
    dbc.Col(html.H4("EnvOps", className="display-6 fw-bold")),
    dbc.Col(
        html.Button(
            html.Span(className="navbar-toggler-icon"),
            className="navbar-toggler",
            id="sidebar-toggle",
        ),
        width="auto",
        align="center",
    ),
], className="mb-4")

# 2. Define the Sidebar Layout
sidebar = html.Div([
    sidebar_header,
    dbc.Collapse(
        dbc.Nav(
            [
                # Dynamically generate links based on pages registered in the pages/ folder
                dbc.NavLink(
                    [html.I(className="bi bi-layout-text-sidebar-reverse me-2"), page["title"]],
                    href=page["relative_path"],
                    active="exact",
                    className="mb-2 rounded"
                )
                for page in dash.page_registry.values()
                if page.get("nav_bar", True) # Allow hiding pages from nav by setting nav_bar=False
            ],
            vertical=True,
            pills=True,
        ),
        id="sidebar-collapse",
    ),
], id="sidebar")

# 3. Define the Main Application Shell
app.layout = html.Div([
    dcc.Location(id="url"),
    sidebar,
    html.Div([
        dash.page_container
    ], id="page-content")
])

# 4. Callback for mobile sidebar toggling
@app.callback(
    Output("sidebar-collapse", "is_open"),
    Input("sidebar-toggle", "n_clicks"),
    State("sidebar-collapse", "is_open"),
)
def toggle_collapse(n, is_open):
    if n:
        return not is_open
    return is_open