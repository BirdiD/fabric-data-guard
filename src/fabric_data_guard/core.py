import logging

import great_expectations as gx
from pyspark.sql import DataFrame
from pyspark.sql.types import *

from .consts import project_root_dir
from .validation import Validator


class FabricDataGuard:
    def __init__(
        self,
        datasource_name: str,
        data_asset_name: str,
        project_root_dir: str = project_root_dir,
    ):
        # User parameters to be provided
        self.project_root_dir = project_root_dir
        self.datasource_name = datasource_name
        self.data_asset_name = data_asset_name
        self.suite_name = f"{self.data_asset_name}Suite"

        # Initialize great expectation objects
        self.context = gx.get_context(
            mode="file", project_root_dir=self.project_root_dir
        )
        self.datasource = self._set_datasource(self.datasource_name)
        self.data_asset = self._set_data_asset(self.data_asset_name)
        self.expectation_suite = self._create_expectation_suite(self.suite_name)
        self.batch_definition = self._create_batch_definition()
        self.validation_definition = self._create_validation_definition()

    def _set_datasource(self, datasource_name: str):
        datasource = (
            self.context.data_sources.add_or_update_spark(name=datasource_name)
            if not any(ds == datasource_name for ds in self.context.data_sources.all())
            else self.context.data_sources.get(datasource_name)
        )
        return datasource

    def _set_data_asset(self, data_asset_name: str):
        try:
            data_asset = self.context.data_sources.get(self.datasource_name).get_asset(
                data_asset_name
            )
        except:
            data_asset = self.datasource.add_dataframe_asset(name=data_asset_name)
        return data_asset

    def _create_expectation_suite(self, suite_name: str):
        expectation_suite = (
            self.context.suites.add(gx.ExpectationSuite(name=suite_name))
            if not any(cs["name"] == suite_name for cs in self.context.suites.all())
            else self.context.suites.get(suite_name)
        )
        return expectation_suite

    def _create_batch_definition(self):
        batch_definition_name = f"{self.data_asset_name}BatchDefinition"
        try:
            batch_definition = self.data_asset.get_batch_definition(
                batch_definition_name
            )
        except:
            batch_definition = self.data_asset.add_batch_definition_whole_dataframe(
                batch_definition_name
            )
        return batch_definition

    def _create_validation_definition(self):
        definition_name = f"{self.data_asset_name}ValidationDefinition"
        validation_definition = (
            self.context.validation_definitions.add(
                gx.ValidationDefinition(
                    data=self.batch_definition,
                    suite=self.expectation_suite,
                    name=definition_name,
                )
            )
            if not any(
                cs.name == definition_name
                for cs in self.context.validation_definitions.all()
            )
            else self.context.validation_definitions.get(definition_name)
        )
        return validation_definition

    def add_expectation(self, expectations):
        """
        Add one or more expectations to the expectation suite.

        Args:
            expectations: A single expectation or a list of expectations to add.
        """
        if not isinstance(expectations, list):
            expectations = [expectations]

        for expectation in expectations:
            try:
                self.expectation_suite.add_expectation(expectation)
                logging.info(f"Added expectation: {expectation}")
            except Exception as e:
                logging.info(f"Expectation already exists in the suite. Updating it.")
                self.expectation_suite.remove_expectation(
                    expectation.configuration, remove_multiple_matches=True
                )
                self.expectation_suite.add_expectation(expectation)

    def run_validation(self, dataframe: DataFrame, **kwargs):
        validator = Validator(self)
        return validator.validate(dataframe, **kwargs)
