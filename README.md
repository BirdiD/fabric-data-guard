# FabricDataGuard

FabricDataGuard is a Python library that simplifies data quality checks in Microsoft Fabric using Great Expectations. It provides an easy-to-use interface for data scientists and engineers to perform data quality checks without the need for extensive Great Expectations setup.

## Purpose

The main purpose of FabricDataGuard is to:
- Streamline the process of setting up and running data quality checks in Microsoft Fabric
- Provide a wrapper around Great Expectations for easier integration with Fabric workflows
- Enable quick and efficient data validation with minimal setup

## Installation

To install FabricDataGuard, use pip:

```bash
pip install fabric-data-guard
```

## Usage
Here's a basic example of how to use FabricDataGuard:

```python
from fabric_data_guard import FabricDataGuard
import great_expectations as gx

# Initialize FabricDataGuard
fdg = FabricDataGuard(
    datasource_name="MyDataSourceName",
    data_asset_name="MyDataAssetName",
    #project_root_dir="/lakehouse/default/Files" # This is an optional parameter. Default is set yo your lakehouse filestore
)

# Define data quality checks
fdg.add_expectation([
    gx.expectations.ExpectColumnValuesToNotBeNull(column="UserId"),
    gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
        column_A="UpdateDatime", 
        column_B="CreationDatetime"
    ),
    gx.expectations.ExpectColumnValueLengthsToEqual(
        column="PostalCode", 
        value=5
    ),
])

# Read your data from your lake is a pysaprk dataframe
df = spark.sql("SELECT * FROM MyLakehouseName.MyDataAssetName")

# Run validation
results = fdg.run_validation(df, unexpected_identifiers=['UserId'])

```

## Customizing Validation Run

The `run_validation` function accepts several keyword arguments that allow you to customize its behavior:

#### 1. Display HTML Results:

```python
results = fdg.run_validation(df, display_html=True)
```
Set **`display_html=False`** to suppress the HTML output (default is True).

#### 2. Custom Target Table:

```python
results = fdg.run_validation(df, table_name="MyCustomResultsTable")
```
Specify a custom name for the table where results will be stored.
#### 3. Custom Workspace and Lakehouse:

```python
results = fdg.run_validation(df, workspace_name="MyWorkspace", lakehouse_name="MyLakehouse")
```

By default, it uses the workspace and lakehouse attached to the running notebook. Use these parameters to specify different locations.

#### 4. Notification Settings::
Below an example usage. See `checkpoint.py` to check all required arguments for your use case (Microsoft Teams, Slack or Email)

```python
results = fdg.run_validation(df, 
                             slack_notification=True, 
                             slack_webhook="https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK",
                             email_notification=True,
                             email_to="user@example.com",
                             teams_notification=True,
                             teams_webhook="https://outlook.office.com/webhook/YOUR/TEAMS/WEBHOOK")
```

You can combine these options as needed:


```python
results = fdg.run_validation(df, 
                             display_html=True,
                             table_name="MyCustomResultsTable",
                             workspace_name="MyWorkspace",
                             lakehouse_name="MyLakehouse",
                             slack_notification=True,
                             slack_webhook="https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK",
                             unexpected_identifiers=['UserId', 'TransactionId'])
```
This flexibility allows you to tailor the validation process to your specific needs and integrate it seamlessly with your existing data quality workflows.
## Contributing
Contributions to FabricDataGuard are welcome! If you'd like to contribute:

1. Fork the repository
2. Create a new branch for your feature
3. Implement your changes
4. Write or update tests as necessary
5. Submit a pull request

Please ensure your code adheres to the project's coding standards and includes appropriate tests.