import great_expectations as gx
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import *

from .checkpoint import create_checkpoint
from .consts import main_schema
from .display import show_great_expectations_html
from .result_parser import parse_validation_results
from .utils import append_logs_to_table, create_dataframe


class Validator:
    def __init__(self, fabric_data_guard):
        self.fdg = fabric_data_guard

    def validate(
        self, dataframe: DataFrame, checkpoint: gx.Checkpoint = None, **kwargs
    ):
        """
        Runs validation on the provided dataframe using the configured expectations.

        Args:
            dataframe (DataFrame): The Spark DataFrame to validate.
            checkpoint (gx.Checkpoint, optional): A pre-configured checkpoint. If None, a new one will be created.
            **kwargs: Additional arguments to pass to the checkpoint run.

        Returns:
            dict: The validation results.
        """
        if not all(
            [
                self.fdg.datasource_name,
                self.fdg.data_asset_name,
                self.fdg.expectation_suite,
                self.fdg.batch_definition,
            ]
        ):
            raise ValueError(
                "Datasource, data asset, expectation suite, and batch definition must be set before running validation"
            )

        if checkpoint is None:
            checkpoint = create_checkpoint(self.fdg, **kwargs)

        batch_parameters = {"dataframe": dataframe}

        run_id = gx.core.RunIdentifier(
            run_name=f"{self.fdg.expectation_suite.name}_run"
        )
        results = checkpoint.run(run_id=run_id, batch_parameters=batch_parameters)

        table_name = kwargs.get(
            "table_name", f"{self.fdg.datasource_name}DataQualityResults"
        )
        workspace_name = kwargs.get("workspace_name", None)
        lakehouse_name = kwargs.get("lakehouse_name", None)
        display_html = kwargs.get("display_html", True)

        parsed_results = self._process_results(
            results.run_results,
            table_name,
            workspace_name,
            lakehouse_name,
            display_html,
        )

        return parsed_results

    def _process_results(
        self, results, table_name, workspace_name, lakehouse_name, display_html
    ):
        """
        Processes the validation results by parsing them, logging to a table, and displaying the HTML output.

        Args:
            results (dict): The validation results from the checkpoint run.
            table_name (str): Great expectations log results table name
            workspace_name (str) : The name of the workspace where the log table will be saved
            lakehouse_name (str) : The name of the lakehouse containing the log results table
            display_html (bool) : Whether or not to display the HTNL after validation is done (default is True)
        """
        parsed_results = parse_validation_results(results)
        df_result = create_dataframe(parsed_results, main_schema)
        for column in ["RunTime", "ValidationTime"]:
            df_result = df_result.withColumn(
                column, F.col(column).cast(TimestampType())
            )

        append_logs_to_table(df_result, table_name, workspace_name, lakehouse_name)

        if display_html:
            show_great_expectations_html(
                self.fdg.project_root_dir, self.fdg.expectation_suite.name
            )

        return parsed_results
