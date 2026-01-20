import json
import datetime
import pandas as pd
from dash import Dash, dcc, html, Input, Output, State
import plotly.express as px
from google.cloud import storage

# -----------------------
# CONFIG
# -----------------------
GCS_BUCKET = "dash-query-history"

ICEBERG_ENVS = {
    "prod": "thrift://prod-host:9083",
    "uat": "thrift://uat-host:9083",
    "test": "thrift://test-host:9083",
}

ALLOWED_USERS = {"demo.user@company.com"}

# -----------------------
# SSO (mock)
# -----------------------
def get_user():
    return "demo.user@company.com"

def authorize():
    if get_user() not in ALLOWED_USERS:
        raise PermissionError("Access denied")

# -----------------------
# QUERY HISTORY (GCS)
# -----------------------
gcs_client = storage.Client()

def _history_blob(system, user):
    today = datetime.date.today().isoformat()
    return f"{system}/{user}/{today}.jsonl"

def log_query(system, sql):
    user = get_user()
    blob = gcs_client.bucket(GCS_BUCKET).blob(
        _history_blob(system, user)
    )

    record = {
        "user": user,
        "system": system,
        "sql": sql,
        "timestamp": datetime.datetime.utcnow().isoformat()
    }

    existing = blob.download_as_text() if blob.exists() else ""
    blob.upload_from_string(existing + json.dumps(record) + "\n")

def search_history(system, keyword=""):
    user = get_user()
    blob = gcs_client.bucket(GCS_BUCKET).blob(
        _history_blob(system, user)
    )
    if not blob.exists():
        return []

    return [
        json.loads(l)
        for l in blob.download_as_text().splitlines()
        if keyword.lower() in l.lower()
    ]

# -----------------------
# MOCK QUERY EXECUTION
# -----------------------
def run_iceberg_query(sql):
    log_query("iceberg", sql)
    return pd.DataFrame({
        "date": pd.date_range("2026-01-01", periods=5),
        "value": [10, 20, 15, 30, 25]
    })

def run_pg_query(sql):
    log_query("postgres", sql)
    return pd.DataFrame({
        "category": ["A", "B", "C"],
        "count": [12, 7, 19]
    })

# -----------------------
# DASH APP
# -----------------------
app = Dash(__name__, suppress_callback_exceptions=True)
server = app.server

@server.before_request
def secure():
    authorize()

# -----------------------
# LAYOUT
# -----------------------
app.layout = html.Div([
    html.H2(f"DataDelights – Insights | Hi {get_user()}"),

    dcc.Tabs(id="tabs", value="iceberg", children=[
        dcc.Tab(label="Iceberg", value="iceberg"),
        dcc.Tab(label="Postgres", value="postgres"),
        dcc.Tab(label="Pipeline Analytics", value="analytics"),
        dcc.Tab(label="Query History", value="history"),
    ]),

    html.Div(id="tab-content")
])

# -----------------------
# TAB ROUTER
# -----------------------
@app.callback(Output("tab-content", "children"), Input("tabs", "value"))
def render_tab(tab):

    if tab == "iceberg":
        return html.Div([
            dcc.Dropdown(
                id="iceberg-env",
                options=[{"label": k, "value": k} for k in ICEBERG_ENVS],
                value="prod"
            ),
            dcc.Textarea(id="iceberg-sql", style={"width": "100%", "height": 80}),
            html.Button("Run", id="run-iceberg"),
            dcc.Graph(id="iceberg-chart"),
            html.Div(id="iceberg-table")
        ])

    if tab == "postgres":
        return html.Div([
            dcc.Textarea(id="pg-sql", style={"width": "100%", "height": 80}),
            html.Button("Run", id="run-pg"),
            dcc.Graph(id="pg-chart"),
            html.Div(id="pg-table")
        ])

    if tab == "analytics":
        return html.Div([
            html.H4("Pipeline Analytics (mock)"),
            html.Ul([
                html.Li("Which pipeline took maximum time"),
                html.Li("Pipeline failure counts"),
                html.Li("Most common errors"),
            ])
        ])

    if tab == "history":
        return html.Div([
            dcc.Dropdown(
                id="history-system",
                options=[
                    {"label": "Iceberg", "value": "iceberg"},
                    {"label": "Postgres", "value": "postgres"},
                ],
                value="iceberg"
            ),
            dcc.Input(
                id="history-search",
                placeholder="Search query text...",
                style={"width": "300px"}
            ),
            html.Div(id="history-results")
        ])

# -----------------------
# ICEBERG CALLBACK
# -----------------------
@app.callback(
    Output("iceberg-table", "children"),
    Output("iceberg-chart", "figure"),
    Input("run-iceberg", "n_clicks"),
    State("iceberg-sql", "value"),
)
def iceberg_query(_, sql):
    if not sql:
        return None, {}
    df = run_iceberg_query(sql)
    fig = px.line(df, x=df.columns[0], y=df.columns[1])
    return df.to_html(), fig

# -----------------------
# POSTGRES CALLBACK
# -----------------------
@app.callback(
    Output("pg-table", "children"),
    Output("pg-chart", "figure"),
    Input("run-pg", "n_clicks"),
    State("pg-sql", "value"),
)
def pg_query(_, sql):
    if not sql:
        return None, {}
    df = run_pg_query(sql)
    fig = px.bar(df, x=df.columns[0], y=df.columns[1])
    return df.to_html(), fig

# -----------------------
# QUERY HISTORY CALLBACK
# -----------------------
@app.callback(
    Output("history-results", "children"),
    Input("history-system", "value"),
    Input("history-search", "value"),
)
def load_history(system, keyword):
    records = search_history(system, keyword or "")
    if not records:
        return "No history found"

    return [
        html.Div([
            html.Pre(r["sql"]),
            html.Small(f'{r["user"]} | {r["timestamp"]}')
        ], style={"borderLeft": "4px solid #4f46e5", "padding": "8px", "margin": "8px"})
        for r in reversed(records)
    ]

# -----------------------
# MAIN
# -----------------------
if __name__ == "__main__":
    app.run_server(debug=True)
