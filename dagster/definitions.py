from dagster import AssetSpec, Definitions


source = AssetSpec(
    key=["nyc_open_data", "complaints_311"],
    description="311 Restaurant data jrsc-cabt in NYC Open Data",
    group_name="sources",
)

staging = AssetSpec(
    key=["staging", "complaints_311"],
    deps=[source],
    description="NYC 311 restaurant complaints loaded into PostgreSQL staging",
    group_name="staging",
    metadata={"table": "staging.complaints_311"},
)

defs = Definitions(assets=[source, staging])
