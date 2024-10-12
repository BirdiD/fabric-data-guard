from pyspark.sql.types import *


def parse_validation_results(validation_results):
    """
    Parses validation results and creates a PySpark DataFrame with extracted attributes and details
    of individual test results.
    Args:
        - validation_results (dict): A dictionary containing validation results, where each entry includes
        metadata, statistics, and test outcomes.

    Returns:
        pyspark.sql.DataFrame: A PySpark DataFrame with parsed validation results. The main attributes
        are stored as separate columns in Pascal Case
    """

    parsed_data = []
    for validation_name, v in validation_results.items():
        meta = v.get("meta", {})
        statistics = v.get("statistics", {})
        active_batch_definition = meta.get("active_batch_definition", {})
        run_id = meta.get("run_id", {})

        main_row = {
            "DatasourceName": active_batch_definition.get("datasource_name"),
            "DataAssetName": active_batch_definition.get("data_asset_name"),
            "ValidationId": meta.get("validation_id"),
            "CheckpointId": meta.get("checkpoint_id"),
            "RunName": run_id.get("run_name"),
            "RunTime": run_id.get("run_time").isoformat(),
            "ValidationTime": meta.get("validation_time").isoformat(),
            "TestStatus": "Success" if v.get("success", False) else "Failure",
            "SuiteName": v.get("suite_name"),
            "EvaluatedExpectations": statistics.get("evaluated_expectations"),
            "SuccessfulExpectations": statistics.get("successful_expectations"),
            "UnsuccessfulExpectations": statistics.get("unsuccessful_expectations"),
            "SuccessPercent": statistics.get("success_percent"),
        }

        succeeded_detail_results = []
        failed_detail_results = []
        for result in v.get("results", []):
            expectation_config = result.get("expectation_config", {})
            result_data = result.get("result", {})

            detail_result = {
                "TestId": expectation_config.get("id"),
                "ColumnName": expectation_config.get("kwargs", {}).get("column"),
                "TestStatus": "Success" if result.get("success", False) else "Failure",
                "ElementCount": result_data.get("element_count"),
                "UnexpectedCount": result_data.get("unexpected_count"),
                "UnexpectedPercent": result_data.get("unexpected_percent"),
                "ExpectationType": expectation_config.get("type"),
                "UnexpectedIndexQuery": result_data.get("unexpected_index_query"),
            }

            if result.get("success", False):
                succeeded_detail_results.append(
                    {
                        "TestId": detail_result["TestId"],
                        "ColumnName": detail_result["ColumnName"],
                        "TestStatus": detail_result["TestStatus"],
                        "ExpectationType": detail_result["ExpectationType"],
                    }
                )
            else:
                unexpected_index_list = result_data.get(
                    "partial_unexpected_index_list", []
                )
                detail_result["UnexpectedIndexList"] = [
                    {str(k): str(v) for k, v in item.items()}
                    for item in unexpected_index_list
                ]
                failed_detail_results.append(detail_result)

        main_row["SucceededDetailResults"] = succeeded_detail_results
        main_row["FailedDetailResults"] = failed_detail_results
        parsed_data.append(main_row)

    return parsed_data
