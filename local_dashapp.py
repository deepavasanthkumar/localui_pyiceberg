import re
import psycopg2
import pandas as pd

import dash
from dash import html, dcc, dash_table
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

# -------------------------
# PostgreSQL configuration
# -------------------------
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "mydb",
    "user": "myuser",
    "password": "mypassword"
}

MAX_LIMIT = 1000  # enforced max rows

# -------------------------
# Helpers
# -------------------------
def is_select_only(query: str) -> bool:
    query = query.strip().lower()
    forbidden = r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke)\b"
    return query.startswith("select") and not re.search(forbidden, query)

def enforce_limit(query: str, limit: int) -> str:
    if re.search(r"\blimit\b", query, re.IGNORECASE):
        return query
    return f"{query.rstrip(';')} LIMIT {limit}"

def run_query(query: str) -> pd.DataFrame:
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        df = pd.read_sql(query, conn)
        return df
    finally:
        conn.close()

# -------------------------
# Dash app
# -------------------------
app = dash.Dash(__name__)
server = app.server

app.layout = html.Div(
    style={"padding": "30px"},
    children=[
        html.H2("PostgreSQL Read-Only SQL Console"),

        html.Label("SELECT query only"),
        dcc.Textarea(
            id="sql-input",
            placeholder="SELECT * FROM public.my_table WHERE col > 10",
            style={"width": "100%", "height": "120px"}
        ),

        html.Br(),
        html.Label("Row limit"),
        dcc.Input(
            id="limit-input",
            type="number",
            value=100,
            min=1,
            max=MAX_LIMIT
        ),

        html.Br(), html.Br(),
        html.Button("Run Query", id="run-btn"),
        html.Button("Download CSV", id="download-btn", style={"marginLeft": "10px"}),

        html.Br(), html.Br(),
        html.Div(id="error-msg", style={"color": "red"}),

        dcc.Store(id="query-result"),

        dash_table.DataTable(
            id="result-table",
            page_size=15,
            filter_action="native",
            sort_action="native",
            sort_mode="multi",
            page_action="native",
            style_table={"overflowX": "auto"},
            style_cell={
                "textAlign": "left",
                "padding": "6px",
                "whiteSpace": "normal"
            }
        ),

        dcc.Download(id="download-dataframe-csv")
    ]
)

# -------------------------
# Run query callback
# -------------------------
@app.callback(
    [
        Output("result-table", "data"),
        Output("result-table", "columns"),
        Output("query-result", "data"),
        Output("error-msg", "children"),
    ],
    Input("run-btn", "n_clicks"),
    State("sql-input", "value"),
    State("limit-input", "value")
)
def execute_query(n_clicks, query, limit):
    if not n_clicks:
        raise PreventUpdate

    if not query:
        return [], [], None, "Query cannot be empty"

    if not is_select_only(query):
        return [], [], None, "Only SELECT queries are allowed"

    limit = min(limit or 100, MAX_LIMIT)
    final_query = enforce_limit(query, limit)

    try:
        df = run_query(final_query)

        return (
            df.to_dict("records"),
            [{"name": c, "id": c} for c in df.columns],
            df.to_json(date_format="iso", orient="split"),
            ""
        )
    except Exception as e:
        return [], [], None, str(e)

# -------------------------
# Download callback
# -------------------------
@app.callback(
    Output("download-dataframe-csv", "data"),
    Input("download-btn", "n_clicks"),
    State("query-result", "data"),
    prevent_initial_call=True
)
def download_csv(n_clicks, stored_data):
    if not stored_data:
        raise PreventUpdate

    df = pd.read_json(stored_data, orient="split")
    return dcc.send_data_frame(df.to_csv, "query_result.csv", index=False)

# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    app.run_server(
        host="0.0.0.0",
        port=8050,
        debug=False
    )
