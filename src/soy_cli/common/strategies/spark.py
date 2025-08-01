from soy_cli import logging
from soy_cli.common.models import InputModel, OutputModel
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

logger = logging.getLogger(__name__)


def spark_reader(spark: SparkSession, input_config: InputModel, columns: list[str], **kwargs) -> DataFrame:
    """Read data using Spark with specified API and arguments.

    Note that this 'api' parameter should be one of the methods available in the Spark DataFrameReader,
    such as "read", "table", or "load". The 'args' parameter should be a dictionary containing the necessary arguments
    for the specified API method. The 'columns' parameter is a list of columns to select from the DataFrame.
    If 'columns' is None, all columns will be selected.

    To see the pyspark DataFrameReader API, refer to:
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameReader.table.html

    :param spark: The Spark session.
    :param input_config: The InputModel containing the API and arguments for reading data.
    :param kwargs: Additional keyword arguments for the reader.
    If None, all columns will be selected.
    :return: A Spark DataFrame reader configured with the provided options.
    """
    static_args = input_config.args.copy()
    if "options" in static_args:
        if static_args["options"]:
            options = static_args["options"]
            static_args.pop("options")
            reader = getattr(spark.read.options(
                **options), input_config.api)
        else:
            reader = getattr(spark.read, input_config.api)
    else:
        reader = getattr(spark.read, input_config.api)

    df: DataFrame = reader(**static_args)
    logger.info(
        "Read input data on {name}", name=input_config.name, config=static_args, columns=columns
    )

    if columns:
        if isinstance(columns, str):
            columns = [columns]
        # Select only the specified columns
        df = df.select(*columns)
        logger.info("Filter on columns: {columns}", columns=columns)

    return df


def spark_writer(output_config: OutputModel, df: DataFrame, columns: list[str] | None = None, **kwargs) -> None:
    """Write data using Spark with specified API and arguments.

    Note that this 'api' parameter should be one of the methods available in the Spark DataFrameWriter,
    such as "save", "saveAsTable", or "insertInto". The 'args' parameter should be a dictionary containing the necessary arguments
    for the specified API method.

    To see the pyspark DataFrameWriter API, refer to:
    https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameWriter.saveAsTable.html

    :param output_config: The OutputModel containing the API and arguments for writing data.
    :param df: The DataFrame to write.
    :param columns: List of columns to select before writing. If None, all columns will be written.
    :param kwargs: Additional keyword arguments for the writer.
    """
    # Filter columns if specified
    if columns:
        if isinstance(columns, str):
            columns = [columns]
        # Select only the specified columns
        df = df.select(*columns)
        logger.info("Filter on columns: {columns}", columns=columns)

    static_args = output_config.args.copy()

    # Handle options configuration
    if "options" in static_args:
        options = static_args.pop("options")
        if options:
            writer = getattr(df.write.options(**options), output_config.api)
        else:
            writer = getattr(df.write, output_config.api)
    else:
        writer = getattr(df.write, output_config.api)

    # Execute the write operation
    writer(**static_args)
