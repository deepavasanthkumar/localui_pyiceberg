import json
import datetime
import pandas as pd
from dash import Dash, dcc, html, Input, Output, State
import plotly.express as px
from google.cloud import storage

# =====================
# CONFIG
# =====================
GCS_BUCKET = "dash-query-history"

ICEBERG_ENVS = {
    "prod": "thrift://prod-host:9083",
    "uat": "thrift://uat-host:9083",
    "test": "thrift://test-host:9083",
}

# =====================
# MOCK SSO
# =====================
def get_user():
    return "demo.user@company.com"

# =====================
# GCS QUERY HISTORY
# =====================
gcs = storage.Client()

def _blob(system, user):
    d = datetime.date.today().isoformat()
    return f"{system}/{user}/{d}.jsonl"

def log_query(system, sql):
    user = get_user()
    blob = gcs.bucket(GCS_BUCKET).blob(_blob(system, user))
    record = {
        "user": user,
        "system": system,
        "sql": sql,
        "ts": datetime.datetime.utcnow().isoformat()
    }
    existing = blob.download_as_text() if blob.exists() else ""
    blob.upload_from_string(existing + json.dumps(record) + "\n")

# =====================
# MOCK ICEBERG METADATA
# =====================
def list_schemas(env):
    return ["sales", "finance", "ops"]

def list_tables(env, schema):
    return {
        "sales": ["orders", "customers"],
        "finance": ["payments"],
        "ops": ["jobs"]
    }.get(schema, [])

def get_table_schema(env, schema, table):
    return pd.DataFrame({
        "column": ["id", "created_at", "amount"],
        "type": ["int", "timestamp", "decimal"]
    })

# =====================
# MOCK DUCKDB QUERY
# =====================
def run_duckdb_sql(sql):
    log_query("iceberg", sql)
    return pd.DataFrame({
        "date": pd.date_range("2026-01-01", periods=6),
        "value": [10, 12, 18, 14, 22, 25]
    })

# =====================
# DASH APP
# =====================
app = Dash(__name__, suppress_callback_exceptions=True)

app.layout = html.Div([
    html.H2(f"DataDelights – Insights | Hi {get_user()}"),

    dcc.Tabs(value="iceberg", id="tabs", children=[
        dcc.Tab(label="Iceberg", value="iceberg"),
        dcc.Tab(label="Postgres", value="postgres"),
        dcc.Tab(label="Analytics", value="analytics"),
        dcc.Tab(label="Query History", value="history"),
    ]),

    html.Div(id="content")
])

# =====================
# TAB ROUTER
# =====================
@app.callback(Output("content", "children"), Input("tabs", "value"))
def render(tab):
    if tab != "iceberg":
        return html.Div("Other tabs unchanged")

    return html.Div([

        # ---- Environment ----
        dcc.Dropdown(
            id="env",
            options=[{"label": k, "value": k} for k in ICEBERG_ENVS],
            value="prod",
            placeholder="Select Environment"
        ),

        # ---- Schema ----
        dcc.Dropdown(
            id="schema",
            placeholder="Select Schema"
        ),

        # ---- Table ----
        dcc.Dropdown(
            id="table",
            placeholder="Select Table"
        ),

        # ---- Side by Side Panels ----
        html.Div(style={"display": "flex", "gap": "16px"}, children=[

            html.Div([
                html.H4("Table Schema"),
                html.Div(id="table-schema")
            ], style={"width": "50%"}),

            html.Div([
                html.H4("Preview (SELECT *)"),
                html.Div(id="table-preview")
            ], style={"width": "50%"}),

        ]),

        html.Hr(),

        # ---- DuckDB SQL ----
        html.H4("DuckDB SQL on Iceberg"),
        dcc.Textarea(
            id="duckdb-sql",
            placeholder="SELECT date, sum(value) FROM iceberg_table GROUP BY date",
            style={"width": "100%", "height": 90}
        ),
        html.Button("Run DuckDB SQL", id="run-duckdb"),
        dcc.Graph(id="duckdb-chart"),
        html.Div(id="duckdb-table")

    ])

# =====================
# LOAD SCHEMAS
# =====================
@app.callback(
    Output("schema", "options"),
    Input("env", "value")
)
def load_schemas(env):
    return [{"label": s, "value": s} for s in list_schemas(env)]

# =====================
# LOAD TABLES
# =====================
@app.callback(
    Output("table", "options"),
    Input("env", "value"),
    Input("schema", "value")
)
def load_tables(env, schema):
    if not schema:
        return []
    return [{"label": t, "value": t} for t in list_tables(env, schema)]

# =====================
# TABLE SELECTED
# =====================
@app.callback(
    Output("table-schema", "children"),
    Output("table-preview", "children"),
    Input("env", "value"),
    Input("schema", "value"),
    Input("table", "value"),
)
def table_selected(env, schema, table):
    if not table:
        return None, None

    schema_df = get_table_schema(env, schema, table)
    preview_sql = f"SELECT * FROM {schema}.{table} LIMIT 100"

    log_query("iceberg", preview_sql)

    preview_df = pd.DataFrame({
        "id": [1, 2],
        "created_at": ["2026-01-01", "2026-01-02"],
        "amount": [100, 200]
    })

    return (
        schema_df.to_html(index=False),
        preview_df.to_html(index=False)
    )

# =====================
# DUCKDB QUERY
# =====================
@app.callback(
    Output("duckdb-table", "children"),
    Output("duckdb-chart", "figure"),
    Input("run-duckdb", "n_clicks"),
    State("duckdb-sql", "value")
)
def run_duckdb(_, sql):
    if not sql:
        return None, {}

    df = run_duckdb_sql(sql)
    fig = px.line(df, x=df.columns[0], y=df.columns[1])
    return df.to_html(index=False), fig

# =====================
# MAIN
# =====================
if __name__ == "__main__":
    app.run_server(debug=True)
