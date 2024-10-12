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

## Contributing
Contributions to FabricDataGuard are welcome! If you'd like to contribute:

1. Fork the repository
2. Create a new branch for your feature
3. Implement your changes
4. Write or update tests as necessary
5. Submit a pull request

Please ensure your code adheres to the project's coding standards and includes appropriate tests.