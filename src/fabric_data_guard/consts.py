from pyspark.sql.types import *

# Define the schema for the SucceededDetailResults struct
succeeded_detail_results_schema = StructType(
    [
        StructField("TestId", StringType(), True),
        StructField("ColumnName", StringType(), True),
        StructField("TestStatus", StringType(), True),
        StructField("ExpectationType", StringType(), True),
    ]
)

# Define the schema for the FailedDetailResults struct
failed_detail_results_schema = StructType(
    [
        StructField("TestId", StringType(), True),
        StructField("ColumnName", StringType(), True),
        StructField("TestStatus", StringType(), True),
        StructField("ElementCount", IntegerType(), True),
        StructField("UnexpectedCount", IntegerType(), True),
        StructField("UnexpectedPercent", FloatType(), True),
        StructField("ExpectationType", StringType(), True),
        StructField("UnexpectedIndexQuery", StringType(), True),
        StructField(
            "UnexpectedIndexList", ArrayType(MapType(StringType(), StringType())), True
        ),
    ]
)

# Define the schema for the main DataFrame
main_schema = StructType(
    [
        StructField("DatasourceName", StringType(), True),
        StructField("DataAssetName", StringType(), True),
        StructField("ValidationId", StringType(), True),
        StructField("CheckpointId", StringType(), True),
        StructField("RunName", StringType(), True),
        StructField("RunTime", StringType(), True),
        StructField("ValidationTime", StringType(), True),
        StructField("TestStatus", StringType(), True),
        StructField("SuiteName", StringType(), True),
        StructField("EvaluatedExpectations", IntegerType(), True),
        StructField("SuccessfulExpectations", IntegerType(), True),
        StructField("UnsuccessfulExpectations", IntegerType(), True),
        StructField("SuccessPercent", FloatType(), True),
        StructField(
            "SucceededDetailResults", ArrayType(succeeded_detail_results_schema), True
        ),
        StructField(
            "FailedDetailResults", ArrayType(failed_detail_results_schema), True
        ),
    ]
)
