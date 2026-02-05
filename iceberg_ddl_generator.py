from pyiceberg.catalog import load_catalog
from pyiceberg.types import (
    BooleanType,
    IntegerType,
    LongType,
    FloatType,
    DoubleType,
    DecimalType,
    StringType,
    BinaryType,
    DateType,
    TimestampType,
    TimestamptzType,
    ListType,
    MapType,
    StructType,
)


# ----------------------------
# Iceberg type → SQL type
# ----------------------------
def _iceberg_type_to_sql(t):
    if isinstance(t, BooleanType):
        return "BOOLEAN"
    if isinstance(t, IntegerType):
        return "INT"
    if isinstance(t, LongType):
        return "BIGINT"
    if isinstance(t, FloatType):
        return "FLOAT"
    if isinstance(t, DoubleType):
        return "DOUBLE"
    if isinstance(t, DecimalType):
        return f"DECIMAL({t.precision},{t.scale})"
    if isinstance(t, StringType):
        return "STRING"
    if isinstance(t, BinaryType):
        return "BINARY"
    if isinstance(t, DateType):
        return "DATE"
    if isinstance(t, TimestampType):
        return "TIMESTAMP"
    if isinstance(t, TimestamptzType):
        return "TIMESTAMP WITH TIME ZONE"
    if isinstance(t, ListType):
        return f"ARRAY<{_iceberg_type_to_sql(t.element_type)}>"
    if isinstance(t, MapType):
        return (
            f"MAP<{_iceberg_type_to_sql(t.key_type)}, "
            f"{_iceberg_type_to_sql(t.value_type)}>"
        )
    if isinstance(t, StructType):
        fields = ", ".join(
            f"{f.name}: {_iceberg_type_to_sql(f.field_type)}"
            for f in t.fields
        )
        return f"STRUCT<{fields}>"

    return str(t)


# -------------------------------------------------
# Public API: Generate CREATE TABLE DDL
# -------------------------------------------------
def generate_create_table_ddl(
    catalog_name: str,
    table_name: str,
    warehouse: str = None,
    catalog_type: str = "hadoop",
    catalog_props: dict | None = None,
) -> str:
    """
    Generate CREATE TABLE DDL for an existing Iceberg table.

    Args:
        catalog_name: Name of the Iceberg catalog
        table_name: Fully qualified table name (db.table)
        warehouse: Warehouse path (required for hadoop catalog)
        catalog_type: hadoop | rest | hive | glue
        catalog_props: Extra catalog properties (optional)

    Returns:
        CREATE TABLE statement as string
    """

    props = {"type": catalog_type}
    if warehouse:
        props["warehouse"] = warehouse
    if catalog_props:
        props.update(catalog_props)

    catalog = load_catalog(catalog_name, **props)
    table = catalog.load_table(table_name)

    schema = table.schema()

    # ----------------------------
    # Columns
    # ----------------------------
    columns = []
    for field in schema.fields:
        col = f"{field.name} {_iceberg_type_to_sql(field.field_type)}"
        if field.required:
            col += " NOT NULL"
        columns.append(col)

    # ----------------------------
    # Partition spec
    # ----------------------------
    partition_cols = []
    spec = table.spec()

    for field in spec.fields:
        src_col = schema.find_field(field.source_id).name
        transform = field.transform

        if transform == "identity":
            partition_cols.append(src_col)
        else:
            partition_cols.append(f"{transform}({src_col})")

    # ----------------------------
    # Table properties
    # ----------------------------
    IGNORED_PROPS = {
        "uuid",
        "metadata_location",
        "previous_metadata_location",
    }

    tbl_props = {
        k: v for k, v in table.properties.items()
        if k not in IGNORED_PROPS
    }

    # ----------------------------
    # Assemble CREATE TABLE
    # ----------------------------
    ddl = f"""
CREATE TABLE {table_name} (
    {",\n    ".join(columns)}
)
USING ICEBERG
""".strip()

    if partition_cols:
        ddl += f"\nPARTITIONED BY ({', '.join(partition_cols)})"

    location = table.location()
    if location:
        ddl += f"\nLOCATION '{location}'"

    if tbl_props:
        props_sql = ",\n    ".join(
            f"'{k}'='{v}'" for k, v in sorted(tbl_props.items())
        )
        ddl += f"\nTBLPROPERTIES (\n    {props_sql}\n)"

    return ddl
  

ddl = generate_create_table_ddl(
    catalog_name="my_catalog",
    warehouse="s3://warehouse",
    table_name="db.table"
)
print(ddl)

