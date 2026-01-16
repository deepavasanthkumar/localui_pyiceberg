import re
import duckdb
import pandas as pd

import dash
from dash import html, dcc, dash_table
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

from pyiceberg.catalog import load_catalog

# ------------------------------------------------
# Iceberg environment configs
# ------------------------------------------------
ICEBERG_CATALOGS = {
    "dev": {
        "type": "hive",
        "uri": "thrift://dev-hms:9083",
        "warehouse": "s3://dev-warehouse/iceberg"
    },
    "test": {
        "type": "hive",
        "uri": "thrift://test-hms:9083",
        "warehouse": "s3://test-warehouse/iceberg"
    },
    "prod": {
        "type": "hive",
        "uri": "thrift://prod-hms:9083",
        "warehouse": "s3://prod-warehouse/iceberg"
    }
}

MAX_ROWS = 1000

# ------------------------------------------------
# Helpers
# ------------------------------------------------
def get_catalog(env):
    return load_catalog(f"iceberg_{env}", ICEBERG_CATALOGS[env])

def enforce_limit(sql):
    if re.search(r"\blimit\b", sql, re.IGNORECASE):
        return sql
    return f"{sql.rstrip(';')} LIMIT {MAX_ROWS}"

def extract_table_name(sql):
    return sql.lower().split("from")[1].strip().split()[0]

def read_table(env, table_name):
    catalog = get_catalog(env)
    table = catalog.load_table(table_name)
    arrow = table.scan().to_arrow()
    return arrow.to_pandas().head(MAX_ROWS)

def run_sql(env, sql):
    catalog = get_catalog(env)
    table_name = extract_table_name(sql)

    table = catalog.load_table(table_name)
    arrow = table.scan().to_arrow()

    con = duckdb.connect()
    con.register("iceberg_table", arrow)

    rewritten = sql.replace(table_name, "iceberg_table")
    rewritten = enforce_limit(rewritten)

    return con.execute(rewritten).df()

def get_schema(env, table_name):
    catalog = get_catalog(env)
    table = catalog.load_table(table_name)

    rows = []
    for field in table.schema().fields:
        rows.append({
            "name": field.name,
            "type": str(field.field_type),
            "nullable": field.optional
        })
    return pd.DataFrame(rows)

# ------------------------------------------------
# Dash app
# ------------------------------------------------
app = dash.Dash(__name__)
server = app.server

app.layout = html.Div(
    style={"padding": "30px"},
    children=[
        html.H2("Iceberg Dashboard (pyiceberg)"),

        html.Label("Environment"),
        dcc.Dropdown(
            id="env",
            options=[{"label": k.upper(), "value": k} for k in ICEBERG_CATALOGS],
            value="dev",
            clearable=False
        ),

        html.Br(),
        html.Label("Table name or SELECT query"),
        dcc.Textarea(
            id="query",
            placeholder="db.table  OR  SELECT col1, col2 FROM db.table WHERE col1 > 10",
            style={"width": "100%", "height": "100px"}
        ),

        html.Br(),
        html.Button("Run Query", id="run"),
        html.Button("Show Schema", id="schema", style={"marginLeft": "10px"}),
        html.Button("Download CSV", id="download", style={"marginLeft": "10px"}),

        html.Br(), html.Br(),
        html.Div(id="error", style={"color": "red"}),

        dcc.Store(id="result-store"),

        html.H4("Query Results"),
        dash_table.DataTable(
            id="table",
            page_size=15,
            filter_action="native",
            sort_action="native",
            sort_mode="multi",
            style_table={"overflowX": "auto"}
        ),

        html.H4("Table Schema"),
        dash_table.DataTable(
            id="schema-table",
            style_table={"overflowX": "auto"}
        ),

        dcc.Download(id="download-csv")
    ]
)

# ------------------------------------------------
# Run query
# ------------------------------------------------
@app.callback(
    [
        Output("table", "data"),
        Output("table", "columns"),
        Output("result-store", "data"),
        Output("error", "children")
    ],
    Input("run", "n_clicks"),
    State("env", "value"),
    State("query", "value")
)
def run_query(n, env, query):
    if not n or not query:
        raise PreventUpdate

    try:
        query = query.strip()

        if query.lower().startswith("select"):
            df = run_sql(env, query)
        else:
            df = read_table(env, query)

        return (
            df.to_dict("records"),
            [{"name": c, "id": c} for c in df.columns],
            df.to_json(orient="split"),
            ""
        )
    except Exception as e:
        return [], [], None, str(e)

# ------------------------------------------------
# Schema callback
# ------------------------------------------------
@app.callback(
    [
        Output("schema-table", "data"),
        Output("schema-table", "columns")
    ],
    Input("schema", "n_clicks"),
    State("env", "value"),
    State("query", "value")
)
def show_schema(n, env, query):
    if not n or not query:
        raise PreventUpdate

    table = extract_table_name(query) if query.lower().startswith("select") else query
    df = get_schema(env, table)

    return (
        df.to_dict("records"),
        [{"name": c, "id": c} for c in df.columns]
    )

# ------------------------------------------------
# Download CSV
# ------------------------------------------------
@app.callback(
    Output("download-csv", "data"),
    Input("download", "n_clicks"),
    State("result-store", "data"),
    prevent_initial_call=True
)
def download_csv(n, data):
    if not data:
        raise PreventUpdate

    df = pd.read_json(data, orient="split")
    return dcc.send_data_frame(df.to_csv, "iceberg_query_result.csv", index=False)

# ------------------------------------------------
# Main
# ------------------------------------------------
if __name__ == "__main__":
    app.run_server(host="0.0.0.0", port=8050, debug=False)
