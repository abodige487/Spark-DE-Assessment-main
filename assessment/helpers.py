from pyspark.sql import SparkSession

def get_spark():
    return (
        SparkSession
        .builder
        .remote("sc://localhost:15002")
        .getOrCreate()
    )
