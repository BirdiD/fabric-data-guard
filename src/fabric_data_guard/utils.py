import sempy
import sempy.fabric as fabric
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.getOrCreate()


def get_lakehouse_name():
    """
    Function to retrieve the lakehouse name attached to notebook and workspace in Fabric
    """
    try:
        lakehouse_name = spark.conf.get("trident.lakehouse.name")
    except:
        lakehouse_name = fabric.resolve_item_name(fabric.get_lakehouse_id())
    return lakehouse_name


def get_workspace_name():
    """
    Function to retrieve the workspace name of notebook running data quality checks
    """
    try:
        workspace_name = fabric.resolve_workspace_name()
    except:
        workspace_name = mssparkutils.env.getWorkspaceName()
    return workspace_name


def build_table_path(table_name, workspace_name=None, lakehouse_name=None):
    """
    Build the path where to save the logs table in the Lakehouse.

    Args:
        table_name (str): Name of the great expectation log result table.
        workspace_name (str, optional): Name of the workspace. Defaults to None, in which case it is retrieved internally.
        lakehouse_name (str, optional): Name of the lakehouse. Defaults to None, in which case it is retrieved internally.

    Returns:
        str: Full path where to store the table in the lakehouse.
    """
    if lakehouse_name is None:
        lakehouse_name = get_lakehouse_name()
    if workspace_name is None:
        workspace_name = get_workspace_name()
    return f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/{lakehouse_name}.Lakehouse/Tables/{table_name}"


def append_logs_to_table(
    df: DataFrame,
    table_name: str,
    workspace_name: str = None,
    lakehouse_name: str = None,
):
    """
    Function to add the results of great expectations tests in a table in the lake house.
    If a lakehouse is not provided, it creates the table in the lakehouse attached to the notebook

    Args:
        df (DataFrame): Dataframe to be saved in the lakehouse
        table_name (str): The name of the target table
        workspace_name (str, optional): Name of the workspace. Defaults to None, in which case it is retrieved internally.
        lakehouse_name (str, optional): Name of the lakehouse. Defaults to None, in which case it is retrieved internally.
    """
    target_path = build_table_path(table_name, workspace_name, lakehouse_name)
    df.write.mode("append").format("delta").option("overwriteSchema", "true").save(
        target_path
    )


def create_dataframe(parsed_data, main_schema):
    """
    Create a dataframe from the parsed results of great expectation output
    """
    df = spark.createDataFrame(parsed_data, schema=main_schema)
    return df
