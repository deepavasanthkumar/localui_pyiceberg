import streamlit as st
import psycopg2
import pandas as pd

# =====================
# HARD-CODED DB CONFIG
# =====================
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "your_database",
    "user": "your_user",
    "password": "your_password"
}

st.set_page_config(page_title="Postgres Table Browser", layout="wide")

st.title("🐘 PostgreSQL Table Browser (Streamlit)")
st.caption("Credentials are configured in code | Query, preview, and download tables")

# Session state
if "conn" not in st.session_state:
    st.session_state.conn = None

# ---------------------
# CONNECT TO DATABASE
# ---------------------
if st.session_state.conn is None:
    try:
        st.session_state.conn = psycopg2.connect(**DB_CONFIG)
        st.success("✅ Connected to PostgreSQL")
    except Exception as e:
        st.error(f"❌ Failed to connect: {e}")
        st.stop()

conn = st.session_state.conn

# ---------------------
# FETCH TABLE LIST
# ---------------------
@st.cache_data(show_spinner=False)
def get_tables():
    query = """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name;
    """
    return pd.read_sql(query, conn)

tables_df = get_tables()

# ---------------------
# SIDEBAR – TABLE BROWSER
# ---------------------
st.sidebar.header("📂 Table Browser")
schema = st.sidebar.selectbox("Schema", tables_df["table_schema"].unique())

filtered_tables = tables_df[tables_df["table_schema"] == schema]
table = st.sidebar.selectbox("Table", filtered_tables["table_name"].unique())

limit = st.sidebar.number_input("Preview rows", min_value=10, max_value=10000, value=100, step=10)

# ---------------------
# LOAD TABLE DATA
# ---------------------
if table:
    st.subheader(f"Preview: {schema}.{table}")

    query = f"SELECT * FROM {schema}.{table} LIMIT {limit};"

    try:
        df = pd.read_sql(query, conn)
        st.dataframe(df, use_container_width=True)
        st.caption(f"Showing {len(df)} rows")

        # ---------------------
        # DOWNLOAD CSV
        # ---------------------
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️ Download as CSV",
            data=csv,
            file_name=f"{schema}_{table}.csv",
            mime="text/csv"
        )

    except Exception as e:
        st.error(f"Failed to load table: {e}")

# ---------------------
# QUERY CACHE LAYER
# ---------------------
if "query_cache" not in st.session_state:
    # each entry: {name: str, sql: str}
    st.session_state.query_cache = []

if "query_counter" not in st.session_state:
    st.session_state.query_counter = 1

# ---------------------
# CUSTOM SQL (OPTIONAL)

# ---------------------
st.markdown("---")
st.subheader("📝 Custom SQL (SELECT only)")

# Click-to-load cached query
if st.session_state.query_cache:
    st.markdown("**📜 Saved Queries (click to reuse)**")
    for idx, q in enumerate(reversed(st.session_state.query_cache)):
        label = f"{q['name']}"
        if st.button(label, key=f"saved_{idx}"):
            st.session_state.selected_query = q["sql"]

# Main query editor
sql_query = st.text_area(
    "SQL Editor",
    value=st.session_state.get("selected_query", ""),
    height=140
)

run_query = st.button("Run Query")

# Optional query name
query_name = st.text_input(
    "Query name (optional)",
    placeholder="Leave empty for auto-name (query_1 / query_YYYYMMDD_HHMMSS)"
)

if run_query:
    if not sql_query.strip().lower().startswith("select"):
        st.warning("Only SELECT queries are allowed")
    else:
        try:
            df_sql = pd.read_sql(sql_query, conn)
            st.dataframe(df_sql, use_container_width=True)

            # Auto-name logic
            if query_name.strip():
                name = query_name.strip()
            else:
                ts = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
                name = f"query_{st.session_state.query_counter}_{ts}"
                st.session_state.query_counter += 1

            # Save query (avoid exact duplicates by SQL)
            if not any(q["sql"] == sql_query for q in st.session_state.query_cache):
                st.session_state.query_cache.append({
                    "name": name,
                    "sql": sql_query
                })
                st.session_state.query_cache = st.session_state.query_cache[-20:]

            csv = df_sql.to_csv(index=False).encode("utf-8")
            st.download_button(
                "⬇️ Download Result as CSV",
                csv,
                "query_result.csv",
                "text/csv"
            )
        except Exception as e:
            st.error(f"Query failed: {e}")

# ---------------------
# DISCONNECT
# ---------------------
if st.sidebar.button("Disconnect"):
    conn.close()
    st.session_state.conn = None
    st.cache_data.clear()
    st.session_state.query_cache = []
    st.info("Disconnected from database")

st.markdown("---")
st.code("pip install streamlit psycopg2-binary pandas")
