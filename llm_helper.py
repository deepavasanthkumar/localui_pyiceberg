import requests
import json

OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "llama3"


def ask_llm(question, df):
    if df.empty:
        return "No data available for the selected filters."

    summary = {
        "total_runs": len(df),
        "success": int((df["status"] == "SUCCESS").sum()),
        "failed": int((df["status"] == "FAILED").sum()),
        "max_duration_sec": int(df["duration_sec"].max()),
        "avg_duration_sec": int(df["duration_sec"].mean()),
        "top_longest_pipeline": df.sort_values(
            "duration_sec", ascending=False
        ).iloc[0]["pipeline_id"],
        "pipeline_run_counts": df["pipeline_id"].value_counts().to_dict()
    }

    prompt = f"""
You are a data analytics assistant.

Given this pipeline execution summary:
{json.dumps(summary, indent=2)}

Answer the following question in clear business language:
"{question}"

Rules:
- Do not invent data
- Use only the provided summary
- Keep the answer concise
"""

    response = requests.post(
        OLLAMA_URL,
        json={
            "model": MODEL,
            "prompt": prompt,
            "stream": False
        },
        timeout=60
    )

    return response.json()["response"]

@app.callback(
    Output("answer", "children"),
    Input("question", "value"),
    State("table", "data")
)
def llm_qa(question, table_data):
    if not question or not table_data:
        return ""

    df = pd.DataFrame(table_data)
    return ask_llm(question, df)
