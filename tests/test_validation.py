from unittest.mock import Mock, patch

import great_expectations as gx
import pytest
from pyspark.sql import DataFrame

from src.fabric_data_guard.validation import Validator


@pytest.fixture
def mock_fabric_data_guard():
    fdg = Mock()
    fdg.datasource_name = "test_datasource"
    fdg.data_asset_name = "test_asset"
    fdg.expectation_suite = Mock()
    fdg.expectation_suite.name = "test_suite"
    fdg.batch_definition = Mock()
    fdg.project_root_dir = "/test/path"
    return fdg


@pytest.fixture
def mock_dataframe():
    return Mock(spec=DataFrame)


def test_validate_success(mock_fabric_data_guard, mock_dataframe):
    with patch(
        "src.fabric_data_guard.validation.create_checkpoint"
    ) as mock_create_checkpoint, patch(
        "src.fabric_data_guard.validation.parse_validation_results"
    ) as mock_parse_results, patch(
        "src.fabric_data_guard.validation.create_dataframe"
    ) as mock_create_dataframe, patch(
        "src.fabric_data_guard.validation.append_logs_to_table"
    ) as mock_append_logs, patch(
        "src.fabric_data_guard.validation.show_great_expectations_html"
    ) as mock_show_html:

        mock_checkpoint = Mock()
        mock_create_checkpoint.return_value = mock_checkpoint
        mock_checkpoint.run.return_value = Mock(run_results={"test_results": "data"})
        mock_parse_results.return_value = {"parsed": "results"}
        mock_create_dataframe.return_value = mock_dataframe

        validator = Validator(mock_fabric_data_guard)
        result = validator.validate(mock_dataframe)

        assert result == {"parsed": "results"}
        mock_create_checkpoint.assert_called_once()
        mock_checkpoint.run.assert_called_once()
        mock_parse_results.assert_called_once_with({"test_results": "data"})
        mock_create_dataframe.assert_called_once()
        mock_append_logs.assert_called_once()
        mock_show_html.assert_called_once()


def test_validate_missing_required_attributes(mock_fabric_data_guard, mock_dataframe):
    mock_fabric_data_guard.datasource_name = None
    validator = Validator(mock_fabric_data_guard)

    with pytest.raises(
        ValueError,
        match="Datasource, data asset, expectation suite, and batch definition must be set before running validation",
    ):
        validator.validate(mock_dataframe)
