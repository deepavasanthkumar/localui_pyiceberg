dcc.Tab(label="Column Lineage (PK Timeline)", children=[

    html.H3("Column Lineage by Primary Key"),

    html.Div([
        dcc.Input(
            id="lineage_pk_value",
            placeholder="Primary key value",
            style={"width": "200px"}
        ),
        dcc.Dropdown(
            id="lineage_column",
            placeholder="Select column",
            style={"width": "200px"}
        ),
        dcc.Input(
            id="lineage_limit",
            type="number",
            value=10,
            placeholder="No. of snapshots",
            style={"width": "150px"}
        ),
        html.Button("Load Lineage", id="load_lineage")
    ], style={"display": "flex", "gap": "10px"}),

    html.Br(),

    dash_table.DataTable(
        id="lineage_table",
        style_table={"overflowX": "auto"},
        style_cell={"padding": "6px"},
        style_data_conditional=[
            {
                "if": {
                    "filter_query": "{status} = 'LATEST'",
                    "column_id": "value"
                },
                "backgroundColor": "#d4f8d4",
                "fontWeight": "bold"
            },
            {
                "if": {
                    "filter_query": "{status} = 'CHANGED'",
                    "column_id": "value"
                },
                "backgroundColor": "#ffd6d6",
                "fontWeight": "bold"
            }
        ]
    ),

    html.Br(),
    html.Button("Download Lineage", id="download_lineage_btn"),
    dcc.Download(id="download_lineage")
])

@app.callback(
    Output("lineage_column", "options"),
    Input("env", "value"),
    Input("database", "value"),
    Input("table", "value"),
)
def load_lineage_columns(env, database, table):
    if not all([env, database, table]):
        return []
    catalog = get_catalog(env)
    tbl = catalog.load_table(f"{database}.{table}")
    return [{"label": c.name, "value": c.name} for c in tbl.schema().columns]

@app.callback(
    Output("lineage_table", "data"),
    Output("lineage_table", "columns"),
    Input("load_lineage", "n_clicks"),
    State("env", "value"),
    State("database", "value"),
    State("table", "value"),
    State("pk_cols", "value"),
    State("lineage_pk_value", "value"),
    State("lineage_column", "value"),
    State("lineage_limit", "value"),
)
def load_lineage(n, env, database, table, pk_cols, pk_value, col_name, limit):
    if not n:
        return [], []

    catalog = get_catalog(env)
    tbl = catalog.load_table(f"{database}.{table}")

    snapshots = sorted(
        tbl.snapshots, key=lambda s: s.timestamp_ms, reverse=True
    )[:limit]

    rows = []
    latest_value = None

    for i, s in enumerate(snapshots):
        df = tbl.scan(snapshot_id=s.snapshot_id).to_pandas()
        row = df[df[pk_cols[0]] == pk_value]

        val = row.iloc[0][col_name] if not row.empty else None

        if i == 0:
            latest_value = val
            status = "LATEST"
        else:
            status = "SAME" if val == latest_value else "CHANGED"

        rows.append({
            "snapshot_time": s.timestamp_ms,
            "snapshot_id": s.snapshot_id,
            "value": val,
            "status": status
        })@app.callback(
    Output("download_lineage", "data"),
    Input("download_lineage_btn", "n_clicks"),
    prevent_initial_call=True
)
def download_lineage(n):
    return dcc.send_data_frame(
        app._lineage_df.to_csv,
        "column_lineage.csv",
        index=False
    )


    out = pd.DataFrame(rows)
    app._lineage_df = out

    return out.to_dict("records"), [{"name": c, "id": c} for c in out.columns]
