from pyspark import SparkContext
from awsglue.context import GlueContext
import pytest


@pytest.fixture(scope="session")
def glue_context():
    """
    Function to setup test environment for PySpark and Glue
    """
    spark_context = SparkContext()
    glue_context = GlueContext(spark_context)
    yield glue_context
    spark_context.stop()
