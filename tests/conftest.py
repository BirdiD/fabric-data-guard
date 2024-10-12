from unittest.mock import MagicMock, Mock, patch

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("local-tests")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


class MockFabricDataGuard:
    def __init__(self):
        self.datasource_name = "test_datasource"
        self.context = Mock()
        self.validation_definition = Mock()
        self.context.checkpoints = MagicMock()
        self.context.checkpoints.all.return_value = []  # Default to an empty list


@pytest.fixture
def mock_fabric_data_guard():
    return MockFabricDataGuard()
