import snowflake.snowpark.functions as F

def model(dbt, session):
    dbt.config(materialized = "incremental")

    df = dbt.ref("inserttime")
    df2 = dbt.ref("inserttime")

    df.collect()
    df2.collect()

    return df