import os
import zipfile
import io
from pathlib import Path
from datetime import datetime
import dash
from dash import html, dcc, Input, Output, State, callback
import dash_bootstrap_components as dbc
import dash_ag_grid as dag
from pydantic import BaseSettings

class FileBrowserSettings(BaseSettings):
    # Default to the path used in chunked-filemanager deployment
    storage_base_path: str = "/data/pco2" 

    class Config:
        env_prefix = "CHUNKED_FILEMANAGER_"
        case_sensitive = False

settings = FileBrowserSettings()

dash.register_page(
    __name__,
    path="/pco2-file-browser",
    title="pCO2 File Browser",
)

def get_file_list():
    """Scans the mounted K8s folder for files."""
    files_data = []
    base = Path(settings.storage_base_path)
    
    if not base.exists():
        return []

    for root, _, files in os.walk(base):
        for name in files:
            full_path = Path(root) / name
            rel_path = full_path.relative_to(base)
            stats = full_path.stat()
            
            files_data.append({
                "filename": name,
                "relative_path": str(rel_path),
                "size": f"{stats.st_size / 1024:.2f} KB",
                "modified": datetime.fromtimestamp(stats.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
            })
    return files_data

def layout():
    return html.Div([
        html.H1("pCO2 File Manager Browser"),
        html.Hr(),
        dbc.Row([
            dbc.Col(dbc.Button("Refresh", id="btn-refresh", color="secondary"), width="auto"),
            dbc.Col(dbc.Button("Download Selected (ZIP)", id="btn-download", color="primary"), width="auto"),
        ], className="mb-3 g-2"),
        
        dag.AgGrid(
            id="file-grid",
            columnDefs=[
                {"headerName": "", "checkboxSelection": True, "headerCheckboxSelection": True, "width": 50},
                {"field": "filename", "headerName": "File Name", "filter": True},
                {"field": "relative_path", "headerName": "Path", "filter": True},
                {"field": "size", "headerName": "Size"},
                {"field": "modified", "headerName": "Modified"},
            ],
            rowData=get_file_list(),
            dashGridOptions={"rowSelection": "multiple", "suppressRowClickSelection": True},
            columnSizeOptions="autoSize",
            defaultColDef={"resizable": True, "sortable": True},
        ),
        dcc.Download(id="download-component")
    ], className="p-4")

@callback(
    Output("file-grid", "rowData"),
    Input("btn-refresh", "n_clicks"),
    prevent_initial_call=True
)
def refresh_list(n):
    return get_file_list()

@callback(
    Output("download-component", "data"),
    Input("btn-download", "n_clicks"),
    State("file-grid", "selectedRows"),
    prevent_initial_call=True
)
def handle_download(n, selected_rows):
    if not selected_rows:
        return dash.no_update

    # Create ZIP in memory
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for row in selected_rows:
            file_path = Path(settings.storage_base_path) / row["relative_path"]
            if file_path.exists():
                zf.write(file_path, arcname=row["relative_path"])
    
    buf.seek(0)
    return dcc.send_bytes(buf.getvalue(), f"pco2_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip")