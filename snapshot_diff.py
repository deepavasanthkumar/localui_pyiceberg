# Dash app: Snapshot-wise change tracking using PyIceberg + DuckDB
# ---------------------------------------------------------------
# Features
# - Select environment, database, table
# - Choose primary key columns (composite supported)
# - Choose attribute columns
# - Compare two snapshot versions
# - Show row-level changes (INSERT / UPDATE / DELETE)

import dash
from dash import dcc, html, Input, Output, State, dash_table
import pandas as pd
import duckdb
from pyiceberg.catalog import load_catalog

# ---------------------- CONFIG ----------------------
ENV_CATALOGS = {
    "dev": {
        "type": "hadoop",
        "warehouse": "s3://dev-warehouse/"
    },
    "test": {
        "type": "hadoop",
        "warehouse": "s3://test-warehouse/"
    },
    "prod": {
        "type": "hadoop",
        "warehouse": "s3://prod-warehouse/"
    }
}

# ---------------------- HELPERS ----------------------

def list_snapshots(env, database, table):
    catalog = get_catalog(env)
    tbl = catalog.load_table(f"{database}.{table}")
    return [
        {
            "label": f"{s.snapshot_id} | {s.timestamp_ms}",
            "value": s.snapshot_id,
        }
        for s in tbl.snapshots
    ]


def get_catalog(env):
    cfg = ENV_CATALOGS[env]
    return load_catalog("iceberg", **cfg)


def list_tables(env, database):
    catalog = get_catalog(env)
    return [t[1] for t in catalog.list_tables(database)]


def load_snapshot_to_duckdb(env, database, table, snapshot_id):
    catalog = get_catalog(env)
    tbl = catalog.load_table(f"{database}.{table}")

    scan = tbl.scan(snapshot_id=snapshot_id)
    df = scan.to_pandas()

    con = duckdb.connect(database=":memory:")
    con.register("snap", df)
    return con


def diff_snapshots(con_old, con_new, pk_cols, attr_cols):
    pk_join = ",".join(pk_cols)

    # Build column-level diff expressions
    diff_exprs = []
    for c in attr_cols:
        diff_exprs.append(f"CASE WHEN o.{c} <> n.{c} OR (o.{c} IS NULL AND n.{c} IS NOT NULL) OR (o.{c} IS NOT NULL AND n.{c} IS NULL) THEN TRUE ELSE FALSE END AS {c}_changed")
        diff_exprs.append(f"o.{c} AS old_{c}")
        diff_exprs.append(f"n.{c} AS new_{c}")

    diff_sql = ",
        ".join(diff_exprs)

    query = f"""
    SELECT
        COALESCE({', '.join([f'n.{c}' for c in pk_cols])}, {', '.join([f'o.{c}' for c in pk_cols])}) AS pk,
        CASE
            WHEN o.{pk_cols[0]} IS NULL THEN 'INSERT'
            WHEN n.{pk_cols[0]} IS NULL THEN 'DELETE'
            ELSE 'UPDATE'
        END AS change_type,
        {diff_sql}
    FROM snap o
    FULL OUTER JOIN snap n
    USING ({pk_join})
    """

    return con_new.execute(query).df()

# ---------------------- DASH APP ----------------------
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H2("Iceberg Snapshot Change Tracker"),

    html.Div([
        dcc.Dropdown(id="env", options=[{"label": e, "value": e} for e in ENV_CATALOGS], placeholder="Environment"),
        dcc.Input(id="database", placeholder="Database"),
        dcc.Input(id="table", placeholder="Table")
    ], style={"display": "flex", "gap": "10px"}),

    html.Br(),

    html.Div([
        dcc.Dropdown(id="snapshot_old", placeholder="Old Snapshot"),
        dcc.Dropdown(id="snapshot_new", placeholder="New Snapshot")
    ], style={"display": "flex", "gap": "10px"}),

    html.Br(),

    html.Div([
        dcc.Dropdown(id="pk_cols", multi=True, placeholder="Primary Key Columns"),
        dcc.Dropdown(id="attr_cols", multi=True, placeholder="Attribute Columns")
    ], style={"display": "flex", "gap": "10px"}),

    html.Br(),

    html.Button("Compare Snapshots", id="compare", n_clicks=0),

    html.Hr(),

    html.Div([
        dcc.Checklist(
            id="view_toggles",
            options=[
                {"label": "Hide unchanged columns", "value": "hide_cols"},
                {"label": "Hide unchanged values", "value": "hide_vals"}
            ],
            value=[],
            inline=True
        )
    ]),

    html.Hr(),

    html.Div(id="change_summary", style={"margin": "10px 0", "fontWeight": "bold"}),

    html.Button("Download Result", id="download_btn"),
    dcc.Download(id="download_cmp"),

    dash_table.DataTable(
        id="result_table",
        page_size=20,
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "left", "padding": "6px"},
        style_data_conditional=[]
    )
        id="result_table",
        page_size=20,
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "left", "padding": "6px"},
        style_data_conditional=[]
    )
        id="result_table",
        page_size=20,
        style_table={"overflowX": "auto"},
        style_cell={"textAlign": "left", "padding": "6px"}
    )
])

# ---------------------- CALLBACKS ----------------------

@app.callback(
    Output("result_table", "data"),
    Output("result_table", "columns"),
    Output("change_summary", "children"),
    Input("compare", "n_clicks"),
    State("env", "value"),
    State("database", "value"),
    State("table", "value"),
    State("snapshot_old", "value"),
    State("snapshot_new", "value"),
    State("pk_cols", "value"),
    State("attr_cols", "value"),
    State("view_toggles", "value"),
)
def compare(n, env, database, table, s_old, s_new, pk_cols, attr_cols):
    if n == 0:
        return [], []

    con_old = load_snapshot_to_duckdb(env, database, table, int(s_old))
    con_new = load_snapshot_to_duckdb(env, database, table, int(s_new))

    df = diff_snapshots(con_old, con_new, pk_cols, attr_cols)

    # ---------------- Toggle: Hide unchanged values ----------------
    if view_toggles and "hide_vals" in view_toggles:
        for c in attr_cols:
            mask = df[f"{c}_changed"] == False
            df.loc[mask, f"old_{c}"] = None
            df.loc[mask, f"new_{c}"] = None

    # ---------------- Toggle: Hide unchanged columns ----------------
    if view_toggles and "hide_cols" in view_toggles:
        drop_cols = []
        for c in attr_cols:
            if df[f"{c}_changed"].sum() == 0:
                drop_cols.extend([f"old_{c}", f"new_{c}", f"{c}_changed"])
        df = df.drop(columns=drop_cols)
    styles = []
    for c in df.columns:
        if c.endswith('_changed'):
            styles.append({
                'if': {'column_id': c, 'filter_query': '{%s} = true' % c},
                'backgroundColor': '#ffcccc'
            })

    app._last_df = df

        # ---------------- Text Summary ----------------
    summaries = []
    for c in attr_cols:
        if f"{c}_changed" in df.columns:
            changed_rows = df[df[f"{c}_changed"] == True]
            if not changed_rows.empty:
                summaries.append(f"{c} changed for {len(changed_rows)} row(s)")

    summary_text = " | ".join(summaries) if summaries else "No attribute values changed between selected snapshots"

    app._last_df = df

    return (
        df.to_dict("records"),("records"),
        [{"name": c, "id": c} for c in df.columns]
    )


@app.callback(
    Output("download_cmp", "data"),
    Input("download_btn", "n_clicks"),
    prevent_initial_call=True
)
def download_result(n):
    if not hasattr(app, "_last_df"):
        return None
    return dcc.send_data_frame(app._last_df.to_csv, "snapshot_comparison.csv", index=False)


if __name__ == "__main__":
    app.run_server(debug=True)
