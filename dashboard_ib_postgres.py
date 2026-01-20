"""
Single-file Dash Web Application
--------------------------------
Tech stack:
- Dash
- PyIceberg
- DuckDB
- psycopg2

Features:
1. Iceberg SQL Query UI (Prod/UAT/Test)
2. Postgres SQL Query UI (SELECT only)
3. Pipeline Analytics (Postgres-backed)
4. Query History stored in GCS (logical placeholder)
5. CSV Download
6. Professional CSS styling

NOTE:
- This file is intentionally self-contained.
- Replace connection details, bucket paths, and Iceberg catalog configs
  with real values before deployment.
"""

import os
import io
import json
import duckdb
import psycopg2
import pandas as pd
from datetime import datetime

import dash
from dash import Dash, dcc, html, dash_table, Input, Output, State
import dash_bootstrap_components as dbc

from pyiceberg.catalog import load_catalog

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------

ICEBERG_ENVIRONMENTS = {
    "prod": "thrift://prod-iceberg:9083",
    "uat": "thrift://uat-iceberg:9083",
    "test": "thrift://test-iceberg:9083",
}

QUERY_HISTORY_BUCKET = "gs://my-query-history-bucket/history/"

POSTGRES_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "database": "pipelines",
    "user": "postgres",
    "password": "postgres",
}

# ------------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------------

def get_postgres_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)


def load_iceberg_catalog(env: str):
    return load_catalog(
        name=f"{env}_catalog",
        type="hive",
        uri=ICEBERG_ENVIRONMENTS[env],
    )


def save_query_history(engine: str, query: str, env: str):
    record = {
        "engine": engine,
        "query": query,
        "environment": env,
        "timestamp": datetime.utcnow().isoformat(),
    }
    # Placeholder: write to GCS / local file
    fname = f"history_{engine}_{datetime.utcnow().timestamp()}.json"
    with open(fname, "w") as f:
        json.dump(record, f)


def run_iceberg_query(env: str, sql: str) -> pd.DataFrame:
    catalog = load_iceberg_catalog(env)
    con = duckdb.connect()
    for ns in catalog.list_namespaces():
        for tbl in catalog.list_tables(ns):
            iceberg_table = catalog.load_table(tbl)
            con.register(tbl[-1], iceberg_table.scan().to_arrow())
    return con.execute(sql).df()


def run_postgres_query(sql: str) -> pd.DataFrame:
    with get_postgres_connection() as conn:
        return pd.read_sql(sql, conn)

# ------------------------------------------------------------------
# Dash App Setup
# ------------------------------------------------------------------

app = Dash(__name__, external_stylesheets=[dbc.themes.FLATLY])
app.title = "Data Query & Pipeline Analytics"

# ------------------------------------------------------------------
# Layout Components
# ------------------------------------------------------------------

iceberg_tab = dbc.Container([
    html.H4("Iceberg Query Console"),
    dbc.Row([
        dbc.Col([
            dbc.Label("Environment"),
            dcc.Dropdown(list(ICEBERG_ENVIRONMENTS.keys()), id="iceberg-env"),
            dbc.Label("SQL Query"),
            dcc.Textarea(id="iceberg-sql", style={"width": "100%", "height": "120px"}),
            dbc.Button("Run Query", id="run-iceberg", color="primary", className="mt-2"),
            dbc.Button("Download CSV", id="download-iceberg", className="mt-2 ms-2"),
            dcc.Download(id="download-iceberg-csv"),
        ], width=4),
        dbc.Col([
            html.H6("Query Result"),
            dash_table.DataTable(id="iceberg-result", page_size=10, style_table={"overflowX": "auto"})
        ], width=8)
    ])
], fluid=True)

postgres_tab = dbc.Container([
    html.H4("Postgres SQL Console (SELECT only)"),
    dbc.Label("SQL Query"),
    dcc.Textarea(id="pg-sql", style={"width": "100%", "height": "120px"}),
    dbc.Button("Run Query", id="run-pg", color="primary", className="mt-2"),
    dbc.Button("Download CSV", id="download-pg", className="mt-2 ms-2"),
    dcc.Download(id="download-pg-csv"),
    html.Hr(),
    dash_table.DataTable(id="pg-result", page_size=10, style_table={"overflowX": "auto"})
], fluid=True)

analytics_tab = dbc.Container([
    html.H4("Pipeline Analytics"),
    dbc.Row([
        dbc.Col([
            dbc.Label("Environment"),
            dcc.Dropdown(["prod", "uat", "test"], id="an-env"),
            dbc.Label("Pipeline ID"),
            dcc.Input(id="an-pipeline", type="text"),
            dbc.Label("From Date"),
            dcc.DatePickerSingle(id="an-from"),
            dbc.Label("To Date"),
            dcc.DatePickerSingle(id="an-to"),
            dbc.Button("Run Analysis", id="run-analytics", color="primary", className="mt-2")
        ], width=3),
        dbc.Col([
            dash_table.DataTable(id="analytics-result", page_size=10)
        ], width=9)
    ])
], fluid=True)

app.layout = dbc.Container([
    html.H2("Enterprise Data Query & Analytics Platform", className="text-center my-3"),
    dcc.Tabs([
        dcc.Tab(label="Iceberg Query", children=iceberg_tab),
        dcc.Tab(label="Postgres Query", children=postgres_tab),
        dcc.Tab(label="Pipeline Analytics", children=analytics_tab),
    ])
], fluid=True)

# ------------------------------------------------------------------
# Callbacks
# ------------------------------------------------------------------

@app.callback(
    Output("iceberg-result", "data"),
    Output("iceberg-result", "columns"),
    Input("run-iceberg", "n_clicks"),
    State("iceberg-env", "value"),
    State("iceberg-sql", "value"),
)
def execute_iceberg(n, env, sql):
    if not n or not env or not sql:
        return [], []
    df = run_iceberg_query(env, sql)
    save_query_history("iceberg", sql, env)
    return df.to_dict("records"), [{"name": c, "id": c} for c in df.columns]


@app.callback(
    Output("download-iceberg-csv", "data"),
    Input("download-iceberg", "n_clicks"),
    State("iceberg-result", "data"),
    prevent_initial_call=True
)
def download_iceberg(n, data):
    return dcc.send_data_frame(pd.DataFrame(data).to_csv, "iceberg_result.csv")


@app.callback(
    Output("pg-result", "data"),
    Output("pg-result", "columns"),
    Input("run-pg", "n_clicks"),
    State("pg-sql", "value"),
)
def execute_pg(n, sql):
    if not n or not sql or not sql.strip().lower().startswith("select"):
        return [], []
    df = run_postgres_query(sql)
    save_query_history("postgres", sql, "na")
    return df.to_dict("records"), [{"name": c, "id": c} for c in df.columns]


@app.callback(
    Output("download-pg-csv", "data"),
    Input("download-pg", "n_clicks"),
    State("pg-result", "data"),
    prevent_initial_call=True
)
def download_pg(n, data):
    return dcc.send_data_frame(pd.DataFrame(data).to_csv, "postgres_result.csv")


@app.callback(
    Output("analytics-result", "data"),
    Output("analytics-result", "columns"),
    Input("run-analytics", "n_clicks"),
    State("an-env", "value"),
    State("an-pipeline", "value"),
    State("an-from", "date"),
    State("an-to", "date"),
)
def run_analytics(n, env, pipeline, dfrom, dto):
    if not n:
        return [], []

    sql = f"""
    SELECT pipeline_id,
           COUNT(*) FILTER (WHERE status='FAILED') AS failures,
           MAX(duration) AS max_runtime,
           STRING_AGG(DISTINCT error_message, '; ') AS common_errors
    FROM pipeline_runs
    WHERE environment = '{env}'
      AND start_time BETWEEN '{dfrom}' AND '{dto}'
    GROUP BY pipeline_id
    """

    df = run_postgres_query(sql)
    return df.to_dict("records"), [{"name": c, "id": c} for c in df.columns]


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True)
