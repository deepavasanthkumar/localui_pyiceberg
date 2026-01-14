import streamlit as st
import duckdb
import pandas as pd
from pyiceberg.catalog import load_catalog

# ---------------------------
# MinIO / Iceberg Config
# ---------------------------
CATALOG_NAME = "minio"
WAREHOUSE = "s3://iceberg/warehouse"

S3_CONFIG = {
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "minioadmin",
    "s3.secret-access-key": "minioadmin",
    "s3.region": "us-east-1",
    "s3.path-style-access": "true",
}

# ---------------------------
# Load Iceberg Catalog
# ---------------------------
@st.cache_resource
def get_catalog():
    return load_catalog(
        CATALOG_NAME,
        **{
            "type": "hadoop",
            "warehouse": WAREHOUSE,
            **S3_CONFIG
        }
    )

catalog = get_catalog()

st.title("🧊 Iceberg Query UI (MinIO)")

# ---------------------------
# List Namespaces & Tables
# ---------------------------
st.sidebar.header("📂 Iceberg Tables")

namespaces = catalog.list_namespaces()
namespace = st.sidebar.selectbox("Namespace", namespaces)

tables = catalog.list_tables(namespace)
table_name = st.sidebar.selectbox("Table", tables)

full_table_name = f"{namespace}.{table_name}"

st.write(f"### Selected Table: `{full_table_name}`")

# ---------------------------
# DuckDB Connection
# ---------------------------
@st.cache_resource
def get_duckdb():
    con = duckdb.connect()
    con.execute("INSTALL iceberg; LOAD iceberg;")
    con.execute("INSTALL httpfs; LOAD httpfs;")

    con.execute(f"""
        SET s3_endpoint='{S3_CONFIG["s3.endpoint"]}';
        SET s3_access_key_id='{S3_CONFIG["s3.access-key-id"]}';
        SET s3_secret_access_key='{S3_CONFIG["s3.secret-access-key"]}';
        SET s3_region='{S3_CONFIG["s3.region"]}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return con

con = get_duckdb()

# ---------------------------
# Basic SELECT UI
# ---------------------------
limit = st.number_input("Limit", min_value=1, max_value=10000, value=100)

query = f"""
SELECT *
FROM iceberg_scan('{WAREHOUSE}/{namespace}/{table_name}')
LIMIT {limit}
"""

if st.button("▶ Run Query"):
    with st.spinner("Running query..."):
        df = con.execute(query).fetchdf()
        st.success(f"Returned {len(df)} rows")
        st.dataframe(df)
