import dash
from dash import html
import dash_bootstrap_components as dbc

# By removing requests_pathname_prefix, we allow Uvicorn to dynamically 
# tell Dash what the base URL is. This is much more robust behind Traefik.
app = dash.Dash(
    __name__,
    use_pages=True,
    external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP],
)

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