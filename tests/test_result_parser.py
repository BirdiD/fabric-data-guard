from datetime import datetime, timezone

import pytest

from src.fabric_data_guard.result_parser import parse_validation_results


def test_parse_validation_results_basic(spark):
    validation_results = {
        "test_validation": {
            "success": True,
            "meta": {
                "active_batch_definition": {
                    "datasource_name": "test_datasource",
                    "data_asset_name": "test_asset",
                },
                "validation_id": "test_id",
                "checkpoint_id": "checkpoint_1",
                "validation_time": datetime(2023, 1, 1, tzinfo=timezone.utc),
                "run_id": {
                    "run_name": "test_run",
                    "run_time": datetime(2023, 1, 1, tzinfo=timezone.utc),
                },
            },
            "statistics": {
                "evaluated_expectations": 1,
                "successful_expectations": 1,
                "unsuccessful_expectations": 0,
                "success_percent": 100.0,
            },
            "results": [],
        }
    }

    parsed_data = parse_validation_results(validation_results)
    assert len(parsed_data) == 1
    assert parsed_data[0]["DatasourceName"] == "test_datasource"
    assert parsed_data[0]["DataAssetName"] == "test_asset"
    assert parsed_data[0]["TestStatus"] == "Success"
    assert parsed_data[0]["SuccessPercent"] == 100.0


def test_parse_validation_results_with_failed_expectation(spark):
    validation_results = {
        "test_validation": {
            "success": False,
            "meta": {
                "active_batch_definition": {},
                "validation_time": datetime(2023, 1, 1, tzinfo=timezone.utc),
                "run_id": {"run_time": datetime(2023, 1, 1, tzinfo=timezone.utc)},
            },
            "statistics": {},
            "results": [
                {
                    "success": False,
                    "expectation_config": {
                        "id": "test_id",
                        "kwargs": {"column": "test_column"},
                        "type": "expect_column_values_to_not_be_null",
                    },
                    "result": {
                        "element_count": 100,
                        "unexpected_count": 5,
                        "unexpected_percent": 5.0,
                        "partial_unexpected_index_list": [
                            {"id": 1, "value": None},
                            {"id": 2, "value": None},
                        ],
                    },
                }
            ],
        }
    }

    parsed_data = parse_validation_results(validation_results)
    assert len(parsed_data) == 1
    assert parsed_data[0]["TestStatus"] == "Failure"
    assert len(parsed_data[0]["FailedDetailResults"]) == 1
    assert parsed_data[0]["FailedDetailResults"][0]["UnexpectedCount"] == 5
    assert len(parsed_data[0]["FailedDetailResults"][0]["UnexpectedIndexList"]) == 2


def test_parse_validation_results_with_succeeded_expectation(spark):
    validation_results = {
        "test_validation": {
            "success": True,
            "meta": {
                "active_batch_definition": {},
                "validation_time": datetime(2023, 1, 1, tzinfo=timezone.utc),
                "run_id": {"run_time": datetime(2023, 1, 1, tzinfo=timezone.utc)},
            },
            "statistics": {},
            "results": [
                {
                    "success": True,
                    "expectation_config": {
                        "id": "test_id",
                        "kwargs": {"column": "test_column"},
                        "type": "expect_column_values_to_not_be_null",
                    },
                    "result": {},
                }
            ],
        }
    }

    parsed_data = parse_validation_results(validation_results)
    assert len(parsed_data) == 1
    assert len(parsed_data[0]["SucceededDetailResults"]) == 1
    assert parsed_data[0]["SucceededDetailResults"][0]["TestStatus"] == "Success"


def test_parse_validation_results_empty(spark):
    validation_results = {}
    parsed_data = parse_validation_results(validation_results)
    assert len(parsed_data) == 0


def test_parse_validation_results_missing_fields(spark):
    validation_results = {
        "test_validation": {"meta": {}, "statistics": {}, "results": []}
    }

    parsed_data = parse_validation_results(validation_results)
    assert len(parsed_data) == 1
    assert (
        parsed_data[0]["TestStatus"] == "Failure"
    )  # Default when 'success' is missing
    assert parsed_data[0]["RunTime"] is None
    assert parsed_data[0]["ValidationTime"] is None
    assert parsed_data[0]["DatasourceName"] is None
    assert parsed_data[0]["DataAssetName"] is None
    assert parsed_data[0]["ValidationId"] is None
    assert parsed_data[0]["CheckpointId"] is None
    assert parsed_data[0]["RunName"] is None
    assert parsed_data[0]["SuiteName"] is None
    assert parsed_data[0]["EvaluatedExpectations"] is None
    assert parsed_data[0]["SuccessfulExpectations"] is None
    assert parsed_data[0]["UnsuccessfulExpectations"] is None
    assert parsed_data[0]["SuccessPercent"] is None


def test_parse_validation_results_multiple_validations(spark):
    validation_results = {
        "validation1": {
            "success": True,
            "meta": {
                "active_batch_definition": {"datasource_name": "source1"},
                "validation_time": datetime(2023, 1, 1, tzinfo=timezone.utc),
                "run_id": {"run_time": datetime(2023, 1, 1, tzinfo=timezone.utc)},
            },
            "statistics": {},
            "results": [],
        },
        "validation2": {
            "success": False,
            "meta": {
                "active_batch_definition": {"datasource_name": "source2"},
                "validation_time": datetime(2023, 1, 2, tzinfo=timezone.utc),
                "run_id": {"run_time": datetime(2023, 1, 2, tzinfo=timezone.utc)},
            },
            "statistics": {},
            "results": [],
        },
    }

    parsed_data = parse_validation_results(validation_results)
    assert len(parsed_data) == 2
    assert parsed_data[0]["DatasourceName"] == "source1"
    assert parsed_data[1]["DatasourceName"] == "source2"
    assert parsed_data[0]["TestStatus"] == "Success"
    assert parsed_data[1]["TestStatus"] == "Failure"
