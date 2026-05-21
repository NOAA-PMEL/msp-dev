import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

# Initialize the EnvOps Dash app
dash_app = dash.Dash(
    __name__,
    use_pages=True,
    pages_folder="pages",
    external_stylesheets=[
        dbc.themes.SPACELAB, # Clean, modern Bootstrap theme
        dbc.icons.BOOTSTRAP, 
        "/assets/custom.css" # Assuming you place your responsive-sidebar.css here
    ],
    suppress_callback_exceptions=True
)

# Define the Sidebar Navigation
sidebar = html.Div(
    [
        html.H2("EnvOps", className="display-6 text-primary fw-bold mb-0"),
        html.P("ACG Sampling Systems", className="text-muted small mb-4", id="blurb"),
        html.Hr(),
        dbc.Nav(
            [
                dbc.NavLink([html.I(className="bi bi-grid-1x2-fill me-2"), "Fleet Overview"], href="/", active="exact"),
                dbc.NavLink([html.I(className="bi bi-activity me-2"), "System Ops"], href="/system-ops", active="exact"),
                dbc.NavLink([html.I(className="bi bi-sliders me-2"), "Controllers"], href="/controllers", active="exact"),
                dbc.NavLink([html.I(className="bi bi-hdd-network me-2"), "Registry"], href="/registry", active="exact"),
            ],
            vertical=True,
            pills=True,
            className="fs-6"
        ),
    ],
    id="sidebar",
    className="bg-light shadow-sm"
)

# Define the Main Layout Wrapper
dash_app.layout = html.Div([
    dcc.Location(id="url"),
    sidebar,
    html.Div(
        [
            dash.page_container
        ],
        id="page-content"
    )
])