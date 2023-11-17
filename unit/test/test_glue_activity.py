import boto3
from decimal import Decimal

from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from awsglue.dynamicframe import DynamicFrame

from unittest.mock import patch
import pytest
from moto import mock_secretsmanager, mock_s3

OUTPUT_BUCKET = "data-s3-csv"
OUTPUT_PATH = "subdata/data"
ENDPOINT_URL = "http://localhost:5000/"
TODAY_DATE = datetime.utcnow()


@pytest.fixture(scope="function", autouse=True)
def mock_args(monkeypatch):
    monkeypatch.setattr("sys.argv", ['',
                                     '--JOB_NAME=test',
                                     '--AWS_REGION=us-east-1',
                                     '--SNOWFLAKE_CONNECTION=SNOWFLAKE_CONNECTION',
                                     '--SERVICE_AM_MAPPING_PATH=SERVICE_AM_MAPPING_PATH',
                                     f'--VRS_HISTORY_DATA_BUCKET={OUTPUT_BUCKET}',
                                     f'--VRS_HISTORY_DATA_PATH={OUTPUT_PATH}',
                                     f'--OUTPUT_PATH={OUTPUT_PATH}',
                                     '--VRS_VEHICLES_LIMIT=VRS_VEHICLES_LIMIT',
                                     '--VRS_RPS=111',
                                     '--SNOWFLAKE_DB_NAME=SNOWFLAKE_DB_NAME',
                                     '--VRS_PARALLELISM=1',
                                     '--VRS_API_SECRET_NAME=VRS_API_SECRET_NAME',
                                     f'--OUTPUT_BUCKET={OUTPUT_BUCKET}'])


@pytest.fixture(scope="function")
def mock_data_frame(glue_context):
    spark = glue_context.spark_session

    mock_data = [
        [Decimal(1), 'Audi', 'A4', Decimal(2000), 'str', 'AP', Decimal(1.000000)],
        [Decimal(2), 'Audi', 'A6', Decimal(2000), 'str', 'VSC', Decimal(1.000000)],
        [Decimal(3), 'Audi', 'A8', Decimal(2000), 'str', 'PPM', Decimal(1.000000)]
    ]

    mock_schema = ["VEHICLE_ID", "MAKE", "MODEL", "YEAR", "REPAIR_SERVICE_LEVEL_5", "AM_PRODUCT", "AVR_COST"]

    mock_data_frame = spark.createDataFrame(mock_data, schema=mock_schema)

    return mock_data_frame


@pytest.fixture(scope="function")
def mock_dynamic_frame(glue_context, mock_data_frame):
    mock_dynamic_frame = DynamicFrame.fromDF(mock_data_frame, glue_context, "mock_dynamic_frame")

    return mock_dynamic_frame


@pytest.fixture(scope="function")
def mock_data_frame_after_mapping(glue_context, mock_data_frame):
    mock_data = [
        ['1', 'Audi', 'A4', '2000', 'str', 'AP', '1.00'],
        ['2', 'Audi', 'A6', '2000', 'str', 'VSC', '1.00'],
        ['3', 'Audi', 'A8', '2000', 'str', 'PPM', '1.00']
    ]
    mock_schema = ["vehicleId", "Make", "Model", "Year", "ServiceType", "AMProducts", "AverageCost"]

    spark = glue_context.spark_session

    mock_data_frame_after_mapping = spark.createDataFrame(mock_data, schema=mock_schema)

    return mock_data_frame_after_mapping


@pytest.fixture(scope="function")
def mock_final_expected_data_frame(glue_context, mock_data_frame):
    mock_data = [
        ['Audi', 'A4', '4', 'AP', 'str', '1.000000', '2000'],
        ['Audi', 'A6', '5', 'VSC', 'str', '1.000000', '2000'],
        ['Audi', 'A8', '6', 'PPM', 'str', '1.000000', '2000']
    ]
    mock_schema = ["Make", "Model", "Generation", "AMProducts", "ServiceType", "AverageCosts", "Year"]

    spark = glue_context.spark_session

    mock_final_expected_data_frame = spark.createDataFrame(mock_data, schema=mock_schema)

    return mock_final_expected_data_frame


@pytest.fixture(scope="function")
def mock_data_frame_after_vrs(glue_context, mock_data_frame):
    mock_data = [[1, 4], [2, 5], [3, 6]]
    mock_schema = ["vehicleId", "generationSequence"]

    spark = glue_context.spark_session

    mock_data_frame_after_vrs = spark.createDataFrame(mock_data, schema=mock_schema)

    return mock_data_frame_after_vrs


def initialize_test(spark: SparkSession, df: DataFrame = None):
    """
    Function to setup and initialize test case execution
    :param spark: PySpark session object
    :param df: PySpark DataFrame to save as test parquet file on S3
    :return: server: object for the moto server that was started
    """

    from moto.server import ThreadedMotoServer
    server = ThreadedMotoServer()
    server.start()

    client = boto3.client(
        'secretsmanager',
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="FakeKey",
        aws_secret_access_key="FakeSecretKey",
        aws_session_token="FakeSessionToken",
        region_name="us-east-1", )

    response = client.create_secret(
        Name='VRS_API_SECRET_NAME',
        SecretString='{"url": "test_url", "api_key": "test_key"}'
    )

    s3 = boto3.resource(
        "s3",
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="FakeKey",
        aws_secret_access_key="FakeSecretKey",
        aws_session_token="FakeSessionToken",
        region_name="us-east-1",
    )

    s3.create_bucket(
        Bucket=OUTPUT_BUCKET,
    )

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", "dummy-value")
    hadoop_conf.set("fs.s3a.secret.key", "dummy-value")
    hadoop_conf.set("fs.s3a.endpoint", ENDPOINT_URL)
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    if df:
        df.write.mode('overwrite').parquet(f"s3://{OUTPUT_BUCKET}/{OUTPUT_PATH}")

    return server


@mock_secretsmanager
@mock_s3
def test_main_all_vehicles_exist(glue_context, mock_dynamic_frame, mock_final_expected_data_frame):
    spark = glue_context.spark_session
    # prepared vrs_history_data for get_vrs_data where all vehicles exist
    df = spark.createDataFrame([[1, 4], [2, 5], [3, 6]], schema=["vehicleId", "generationSequence"])

    server = initialize_test(spark, df)

    try:
        import src.am_get_repair_data as glue_module
        with patch.object(glue_module, "generate_query"), \
                patch("awsglue.dynamicframe.DynamicFrameReader.from_options", return_value=mock_dynamic_frame), \
                patch.object(glue_module, "get_data_for_missed_vehicle_ids"):

            glue_module.main()

        df_result_csv = spark.read.option('header', True).csv(
            f"s3://{OUTPUT_BUCKET}/{OUTPUT_PATH}/{TODAY_DATE.strftime('%m%d%Y')}"
        )

        assert len(df_result_csv.schema) == len(mock_final_expected_data_frame.schema) \
               and all(df_result.name == df_expected.name
                       for df_result, df_expected in zip(df_result_csv.schema, mock_final_expected_data_frame.schema)) \
               and (df_result_csv.subtract(mock_final_expected_data_frame).count() == 0)\
               and (mock_final_expected_data_frame.subtract(df_result_csv).count() == 0)

    finally:
        server.stop()


@mock_secretsmanager
@mock_s3
def test_main_vehicle_not_exist(glue_context, mock_dynamic_frame, mock_final_expected_data_frame):
    spark = glue_context.spark_session
    # prepared vrs_history_data for get_vrs_data where one vehicleId does not exist
    df = spark.createDataFrame([[1, 4], [2, 5]], schema=["vehicleId", "generationSequence"])

    server = initialize_test(spark, df)

    try:
        import src.am_get_repair_data as glue_module
        with patch.object(glue_module, "generate_query"), \
                patch("awsglue.dynamicframe.DynamicFrameReader.from_options", return_value=mock_dynamic_frame), \
                patch.object(glue_module, "get_data_for_missed_vehicle_ids") as get_data_for_missed_vehicle_ids_mock:

            get_data_for_missed_vehicle_ids_mock.return_value = iter([{"vehicleId": 3, "generationSequence": 6}])
            glue_module.main()

        df_result_csv = spark.read.option('header', True).csv(
            f"s3://{OUTPUT_BUCKET}/{OUTPUT_PATH}/{TODAY_DATE.strftime('%m%d%Y')}"
        )

        assert len(df_result_csv.schema) == len(mock_final_expected_data_frame.schema) \
               and all(df_result.name == df_expected.name
                       for df_result, df_expected in zip(df_result_csv.schema, mock_final_expected_data_frame.schema)) \
               and (df_result_csv.subtract(mock_final_expected_data_frame).count() == 0)\
               and (mock_final_expected_data_frame.subtract(df_result_csv).count() == 0)

    finally:
        server.stop()


# function specific tests

@mock_secretsmanager
@mock_s3
def test_export_data(glue_context, mock_data_frame_after_mapping):
    spark = glue_context.spark_session
    server = initialize_test(spark)
    try:
        from src.am_get_repair_data import export_data
        export_data(mock_data_frame_after_mapping, OUTPUT_BUCKET, OUTPUT_PATH, True)

        df_result = spark.read.parquet(
            f"s3://{OUTPUT_BUCKET}/{OUTPUT_PATH}"
        )

        assert len(df_result.schema) == len(mock_data_frame_after_mapping.schema) \
               and all(df_res.name == df_expected.name
                       for df_res, df_expected in zip(df_result.schema, mock_data_frame_after_mapping.schema)) \
               and (df_result.subtract(mock_data_frame_after_mapping).count() == 0) \
               and (mock_data_frame_after_mapping.subtract(df_result).count() == 0)

    finally:
        server.stop()


@mock_secretsmanager
@mock_s3
def test_join_sf_data_with_vrs_data(mock_data_frame_after_mapping, mock_data_frame_after_vrs):
    from src.am_get_repair_data import join_sf_data_with_vrs_data

    result_df = join_sf_data_with_vrs_data(mock_data_frame_after_mapping, mock_data_frame_after_vrs)

    assert result_df.count() == mock_data_frame_after_mapping.count()
    assert not("vehicleId" in result_df.schema.fieldNames())
    assert not("generationSequence" in result_df.schema.fieldNames())
    assert "Generation" in result_df.schema.fieldNames()


@mock_secretsmanager
@mock_s3
def test_agg_avg_costs(mock_data_frame_after_mapping):
    from src.am_get_repair_data import agg_avg_costs
    result_df = agg_avg_costs(mock_data_frame_after_mapping, ['Year'])

    assert result_df.count() == 1
    assert not("AverageCost" in result_df.schema.fieldNames())
    assert "AverageCosts" in result_df.schema.fieldNames()