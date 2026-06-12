import os
import dash
from dash import html
import dash_bootstrap_components as dbc

# Register the page in the Dash routing system
dash.register_page(
    __name__, 
    path="/docs", 
    name="Documentation", 
    title="Documentation | EnvOps",
    order=10 # Adjust this to change its position in your auto-generated sidebar
)

def layout():
    # 1. Resolve the absolute path to the assets/docs folder
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    docs_dir = os.path.join(base_dir, "assets", "docs")
    
    # Safely create the directory if it doesn't exist yet
    os.makedirs(docs_dir, exist_ok=True)
    
    # 2. Dynamically scan the folder for files (ignoring hidden OS files)
    files = [f for f in os.listdir(docs_dir) if os.path.isfile(os.path.join(docs_dir, f)) and not f.startswith('.')]
    files.sort()
    
    # 3. Build the UI
    if not files:
        content = html.Div(
            "No documentation files found. Please add PDFs, MDs, or TXTs to the 'assets/docs' folder.", 
            className="alert alert-info m-3"
        )
    else:
        list_items = []
        for f in files:
            # Assign helpful Bootstrap icons based on file extension
            ext = f.split('.')[-1].lower()
            if ext == 'pdf':
                icon = "bi bi-file-earmark-pdf text-danger"
            elif ext in ['md', 'txt']:
                icon = "bi bi-file-earmark-text text-primary"
            else:
                icon = "bi bi-file-earmark text-secondary"

            # Dash automatically serves files in the 'assets' folder
            file_url = f"/assets/docs/{f}"
            
            list_items.append(
                dbc.ListGroupItem(
                    html.A(
                        [
                            html.I(className=f"{icon} me-3", style={"fontSize": "1.5rem"}),
                            html.Span(f, className="fw-bold text-dark")
                        ],
                        href=file_url,
                        target="_blank", # Forces the document to open in a new tab
                        className="text-decoration-none d-flex align-items-center"
                    ),
                    className="py-3" 
                )
            )
        content = dbc.ListGroup(list_items, flush=True)

    return dbc.Container([
        dbc.Row([
            dbc.Col([
                html.H2([html.I(className="bi bi-journal-bookmark me-2"), "Documentation & Manuals"], className="mb-2 mt-4"),
                html.P("Quick reference files, manuals, and procedures. Click a file to open it in a new tab.", className="text-muted mb-4"),
            ])
        ]),
        dbc.Row([
            dbc.Col([
                dbc.Card(content, className="shadow-sm border-0")
            ], width=12, md=8, lg=6)
        ])
    ], fluid=True)