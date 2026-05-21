import dash
from dash import html
import dash_bootstrap_components as dbc

# Tell Dash how to bridge the gap between the browser's URL and Traefik's stripped URL
app = dash.Dash(
    __name__,
    use_pages=True,
    requests_pathname_prefix="/msp/envops/",  # What the browser requests
    routes_pathname_prefix="/",               # What Dash sees after Traefik strips it
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