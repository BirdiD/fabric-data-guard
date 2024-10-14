from unittest.mock import Mock, patch

import great_expectations as gx
import pytest
from pyspark.sql import DataFrame

from src.fabric_data_guard.core import FabricDataGuard


@pytest.fixture(scope="module")
def gx_context():
    project_root_dir = "/lakehouse/default/Files/BDA"
    context = gx.get_context(mode="file", project_root_dir=project_root_dir)

    # Initialize datasource
    datasource_name = "Product"
    datasource = (
        context.data_sources.add_or_update_spark(name=datasource_name)
        if not any(ds == datasource_name for ds in context.data_sources.all())
        else context.data_sources.get(datasource_name)
    )

    # Initialize data asset
    data_asset_name = "Subscription"
    try:
        data_asset = context.data_sources.get(datasource_name).get_asset(
            data_asset_name
        )
    except:
        data_asset = datasource.add_dataframe_asset(name=data_asset_name)

    # Initialize expectation suite
    suite_name = "SubscriptionSuite"
    expectation_suite = (
        context.suites.add(gx.ExpectationSuite(name=suite_name))
        if not any(cs["name"] == suite_name for cs in context.suites.all())
        else context.suites.get(suite_name)
    )

    # Initialize batch definition
    batch_definition_name = f"{data_asset_name}BatchDefinition"
    try:
        batch_definition = (
            context.data_sources.get(datasource_name)
            .get_asset(data_asset_name)
            .get_batch_definition(batch_definition_name)
        )
    except:
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            batch_definition_name
        )

    # Initialize validation definition
    validation_definition_name = f"{data_asset_name}ValidationDefinition"
    validation_definition = (
        context.validation_definitions.add(
            gx.ValidationDefinition(
                data=batch_definition,
                suite=expectation_suite,
                name=validation_definition_name,
            )
        )
        if not any(
            cs.name == validation_definition_name
            for cs in context.validation_definitions.all()
        )
        else context.validation_definitions.get(validation_definition_name)
    )
    expectation = gx.expectations.ExpectColumnValuesToNotBeNull(column="UserId")
    expectation_suite.add_expectation(expectation)

    return context


@pytest.fixture
def mock_dataframe():
    return Mock(spec=DataFrame)


def test_fabric_data_guard_initialization(gx_context):
    fdg = FabricDataGuard("Product", "Subscription")

    assert fdg.datasource_name == "Product"
    assert fdg.data_asset_name == "Subscription"
    assert fdg.suite_name == "SubscriptionSuite"

    assert "Product" in [ds for ds in gx_context.data_sources.all()]
    assert "Subscription" in gx_context.data_sources.get("Product").get_asset_names()
    assert "SubscriptionSuite" in [s.name for s in gx_context.suites.all()]
    assert (
        "SubscriptionBatchDefinition"
        == gx_context.data_sources.get("Product")
        .get_asset("Subscription")
        .get_batch_definition("SubscriptionBatchDefinition")
        .name
    )
    assert "SubscriptionValidationDefinition" in [
        v.name for v in gx_context.validation_definitions.all()
    ]


def test_add_expectation(gx_context):
    fdg = FabricDataGuard("Product", "Subscription")
    expectation = gx.expectations.ExpectColumnValuesToNotBeNull(column="UserId")

    fdg.add_expectation(expectation)

    suite = gx_context.suites.get("SubscriptionSuite")
    assert fdg.expectation_suite.to_json_dict()["name"] == suite.to_json_dict()["name"]
    assert fdg.expectation_suite.to_json_dict()["expectations"][0].get(
        "type"
    ) == suite.to_json_dict()["expectations"][0].get("type")
    assert fdg.expectation_suite.to_json_dict()["expectations"][0].get("kwargs").get(
        "column"
    ) == suite.to_json_dict()["expectations"][0].get("kwargs").get("column")


@patch("src.fabric_data_guard.core.Validator")
def test_run_validation(mock_validator_class, gx_context, mock_dataframe):
    mock_validator = Mock()
    mock_validator_class.return_value = mock_validator
    mock_validator.validate.return_value = {"validation": "results"}

    fdg = FabricDataGuard("Product", "Subscription")
    result = fdg.run_validation(mock_dataframe, custom_param="value")

    mock_validator_class.assert_called_once_with(fdg)
    mock_validator.validate.assert_called_once_with(
        mock_dataframe, custom_param="value"
    )
    assert result == {"validation": "results"}
