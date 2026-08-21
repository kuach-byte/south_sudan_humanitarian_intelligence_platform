import dagster as dg


@dg.asset
def hello_dagster():
    return "Dagster works"


defs = dg.Definitions(
    assets=[hello_dagster],
)