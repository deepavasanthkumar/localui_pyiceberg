# -------- ASK INSIGHTS (LLM Q&A) -------- #
html.Div(
    style={
        "background": "#ffffff",
        "padding": "20px",
        "borderRadius": "14px",
        "boxShadow": "0 6px 20px rgba(0,0,0,0.08)",
        "marginTop": "24px"
    },
    children=[

        # Header
        html.Div(
            style={
                "display": "flex",
                "alignItems": "center",
                "gap": "10px",
                "marginBottom": "12px"
            },
            children=[
                html.Div("🤖", style={"fontSize": "20px"}),
                html.H4(
                    "Ask Insights",
                    style={
                        "margin": 0,
                        "fontWeight": "700",
                        "color": "#1f2937"
                    }
                ),
                html.Span(
                    "LLM-powered analytics",
                    style={
                        "fontSize": "12px",
                        "color": "#6b7280",
                        "marginLeft": "6px"
                    }
                )
            ]
        ),

        # Input row
        html.Div(
            style={
                "display": "flex",
                "gap": "12px",
                "alignItems": "center"
            },
            children=[
                dcc.Input(
                    id="question",
                    placeholder="Ask questions like: Which pipeline ran the longest?",
                    style={
                        "flex": "1",
                        "padding": "12px 14px",
                        "borderRadius": "10px",
                        "border": "1px solid #d1d5db",
                        "fontSize": "14px"
                    }
                ),
                html.Button(
                    "Ask",
                    id="ask_btn",
                    style={
                        "background": "#2563eb",
                        "color": "#ffffff",
                        "border": "none",
                        "padding": "10px 18px",
                        "borderRadius": "10px",
                        "fontWeight": "600",
                        "cursor": "pointer"
                    }
                )
            ]
        ),

        # LLM Answer
        html.Div(
            id="answer",
            style={
                "marginTop": "18px"
            }
        )
    ]
)

@app.callback(
    Output("answer", "children"),
    Input("ask_btn", "n_clicks"),
    State("question", "value"),
    State("table", "data"),
    prevent_initial_call=True
)
def llm_qa(_, question, table_data):
    if not question or not table_data:
        return ""

    df = pd.DataFrame(table_data)
    response = ask_llm(question, df)

    return html.Div(
        style={
            "display": "flex",
            "gap": "12px",
            "alignItems": "flex-start",
            "background": "#f9fafb",
            "padding": "14px",
            "borderRadius": "12px",
            "border": "1px solid #e5e7eb"
        },
        children=[
            # Bot Icon
            html.Div(
                "🧠",
                style={
                    "fontSize": "22px",
                    "marginTop": "2px"
                }
            ),

            # Answer Content
            html.Div(
                children=[
                    html.Div(
                        "Insights Bot",
                        style={
                            "fontWeight": "700",
                            "fontSize": "13px",
                            "color": "#111827",
                            "marginBottom": "4px"
                        }
                    ),
                    html.Div(
                        response,
                        style={
                            "fontSize": "14px",
                            "lineHeight": "1.6",
                            "color": "#374151"
                        }
                    )
                ]
            )
        ]
    )

html.Div(
    style={
        "marginTop": "10px",
        "fontSize": "12px",
        "color": "#6b7280"
    },
    children=[
        "Try asking: ",
        html.Span("Which pipeline failed most?", style={"fontWeight": "600"}),
        " • ",
        html.Span("Success vs failure summary", style={"fontWeight": "600"}),
        " • ",
        html.Span("Longest running pipeline", style={"fontWeight": "600"}),
    ]
)


