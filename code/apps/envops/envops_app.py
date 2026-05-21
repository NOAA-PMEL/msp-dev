import dash
from dash import html, dcc
import dash_bootstrap_components as dbc

# Initialize the Dash app with Pages support and Bootstrap styling
app = dash.Dash(
    __name__,
    use_pages=True,
    requests_pathname_prefix="/msp/envops/", 
    external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP],
)

# A sleek, modern shell layout
app.layout = html.Div([
    dbc.NavbarSimple(
        brand="EnvOps Dashboard",
        brand_href="/msp/envops/",
        color="dark",
        dark=True,
        className="mb-4 shadow-sm"
    ),
    dbc.Container([
        dash.page_container
    ], fluid=True)
])