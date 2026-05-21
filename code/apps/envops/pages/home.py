import dash
from dash import html
import dash_bootstrap_components as dbc

dash.register_page(__name__, path='/', title="EnvOps - Home", order=0)

layout = html.Div([
    dbc.Row([
        dbc.Col([
            html.H1("Welcome to EnvOps", className="display-4 fw-bold text-primary"),
            html.P("Environmental Operations Baseline Deployment", className="lead text-muted"),
            html.Hr(className="my-4"),
            dbc.Alert([
                html.I(className="bi bi-check-circle-fill me-2"),
                "System Status: Online and Routing Successfully!"
            ], color="success", className="d-flex align-items-center fs-5 shadow-sm")
        ], width=12, md=8, lg=6)
    ], className="justify-content-center mt-5 text-center")
])