import dash
from dash import dcc, html, Input, Output, State, dash_table
import pandas as pd
import psycopg2
from datetime import datetime
import plotly.express as px

# ---------------- CONFIG ---------------- #

DB_CONFIG = {
    "host": "localhost",
    "dbname": "pipelines",
    "user": "pipeline_user",
    "password": "secret",
    "port": 5432
}

# ---------------- DATA ACCESS ---------------- #

def fetch_pipeline_data(env, pipeline_id, status, date_from, date_to):
    conn = psycopg2.connect(**DB_CONFIG)

    query = """
        SELECT
            pipeline_id,
            description,
            job_id,
            start_time,
            end_time,
            status,
            COALESCE(error_message, 'SUCCESS') AS message,
            EXTRACT(EPOCH FROM (end_time - start_time)) AS duration_sec
        FROM pipeline_runs
        WHERE environment = %s
          AND start_time BETWEEN %s AND %s
    """

    params = [env, date_from, date_to]

    if pipeline_id:
        query += " AND pipeline_id ILIKE %s"
        params.append(f"%{pipeline_id}%")

    if status and status != "ALL":
        query += " AND status = %s"
        params.append(status)

    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df


# ---------------- Q&A ENGINE (RULE BASED) ---------------- #

def answer_question(df, question):
    q = question.lower()

    if df.empty:
        return "No data available for the selected filters."

    if "longest" in q or "maximum" in q:
        row = df.sort_values("duration_sec", ascending=False).iloc[0]
        return f"Pipeline **{row.pipeline_id}** ran the longest ({int(row.duration_sec)} seconds)."

    if "failure" in q:
        count = df[df.status == "FAILED"].shape[0]
        return f"Total pipeline failures: **{count}**."

    if "success" in q:
        count = df[df.status == "SUCCESS"].shape[0]
        return f"Total successful runs: **{count}**."

    if "success vs error" in q:
        summary = df.groupby("status").size().to_dict()
        return f"Run summary: {summary}"

    return "Sorry, I couldn’t understand the question. Try asking about failures, success, or longest running pipeline."


# ---------------- DASH APP ---------------- #

app = dash.Dash(__name__)
app.title = "Insights | Pipeline Analytics"

# ---------------- STYLING ---------------- #

CARD_STYLE = {
    "background": "#ffffff",
    "padding": "16px",
    "borderRadius": "12px",
    "boxShadow": "0 4px 14px rgba(0,0,0,0.08)"
}

LABEL_STYLE = {"fontWeight": "600", "marginTop": "8px"}

# ---------------- LAYOUT ---------------- #

app.layout = html.Div(
    style={"fontFamily": "Inter, Arial", "background": "#f5f7fb", "padding": "24px"},
    children=[

        html.H2("📊 Pipeline Analytics – Insights", style={"marginBottom": "16px"}),

        # -------- FILTER PANEL -------- #
        html.Div(style=CARD_STYLE, children=[

            html.Div(style={"display": "grid", "gridTemplateColumns": "repeat(4, 1fr)", "gap": "16px"}, children=[

                html.Div([
                    html.Label("Environment", style=LABEL_STYLE),
                    dcc.Dropdown(
                        ["prod", "uat", "test"],
                        "prod",
                        id="env"
                    )
                ]),

                html.Div([
                    html.Label("Pipeline ID", style=LABEL_STYLE),
                    dcc.Input(id="pipeline_id", placeholder="pipeline_x", style={"width": "100%"})
                ]),

                html.Div([
                    html.Label("Status", style=LABEL_STYLE),
                    dcc.Dropdown(
                        ["ALL", "SUCCESS", "FAILED", "RUNNING"],
                        "ALL",
                        id="status"
                    )
                ]),

                html.Div([
                    html.Label("Date Range", style=LABEL_STYLE),
                    dcc.DatePickerRange(
                        id="date_range",
                        start_date=datetime.now().date(),
                        end_date=datetime.now().date()
                    )
                ])
            ]),

            html.Button("Run Analysis", id="run", style={
                "marginTop": "16px",
                "background": "#4f46e5",
                "color": "white",
                "border": "none",
                "padding": "10px 16px",
                "borderRadius": "8px",
                "cursor": "pointer"
            })
        ]),

        html.Br(),

        # -------- TABLE + CHART -------- #
        html.Div(style=CARD_STYLE, children=[

            dcc.Input(
                id="text_search",
                placeholder="Search pipelines, job id, errors...",
                style={"width": "100%", "marginBottom": "12px"}
            ),

            dash_table.DataTable(
                id="table",
                page_size=10,
                style_table={"overflowX": "auto"},
                style_header={"background": "#eef2ff", "fontWeight": "700"},
                style_cell={"padding": "8px", "fontSize": "13px"}
            ),

            html.Br(),
            dcc.Graph(id="status_chart")
        ]),

        html.Br(),

        # -------- Q&A BOT -------- #
        html.Div(style=CARD_STYLE, children=[
            html.H4("🤖 Ask Insights"),
            dcc.Input(
                id="question",
                placeholder="e.g. Which pipeline ran the longest?",
                style={"width": "100%"}
            ),
            html.Br(), html.Br(),
            html.Div(id="answer", style={"fontWeight": "600"})
        ])
    ]
)

# ---------------- CALLBACKS ---------------- #

@app.callback(
    Output("table", "data"),
    Output("table", "columns"),
    Output("status_chart", "figure"),
    Input("run", "n_clicks"),
    State("env", "value"),
    State("pipeline_id", "value"),
    State("status", "value"),
    State("date_range", "start_date"),
    State("date_range", "end_date"),
    State("text_search", "value")
)
def update_table(_, env, pipeline_id, status, start, end, search):
    df = fetch_pipeline_data(env, pipeline_id, status, start, end)

    if search:
        df = df[df.apply(lambda r: search.lower() in str(r).lower(), axis=1)]

    fig = px.bar(
        df.groupby("status").size().reset_index(name="count"),
        x="status",
        y="count",
        title="Pipeline Run Status Distribution"
    )

    return (
        df.to_dict("records"),
        [{"name": c, "id": c} for c in df.columns],
        fig
    )


@app.callback(
    Output("answer", "children"),
    Input("question", "value"),
    State("table", "data")
)
def qa_bot(question, table_data):
    if not question or not table_data:
        return ""

    df = pd.DataFrame(table_data)
    return answer_question(df, question)


# ---------------- MAIN ---------------- #

if __name__ == "__main__":
    app.run(debug=True)
