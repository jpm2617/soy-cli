from typing import Any

import pandas as pd
from soy_cli import logging
from soy_cli.common.models import InputModel, OutputModel

logger = logging.getLogger(__name__)


def pandas_reader(input_config: InputModel, columns: list[str] | None = None, **kwargs) -> pd.DataFrame:
    """Read data using pandas and return a pandas DataFrame.

    This function supports reading from various file formats supported by pandas
    (CSV, Excel, JSON, Parquet, etc.) and returns a native pandas DataFrame.

    :param input_config: The InputModel containing the API and arguments for reading data.
    :param columns: List of columns to select after reading. If None, all columns will be selected.
    :param kwargs: Additional keyword arguments for the pandas reader.
    :return: A pandas DataFrame containing the loaded data.
    """
    static_args = input_config.args.copy()

    # Extract pandas-specific options
    pandas_options = static_args.pop("pandas_options", {})

    # Map API to pandas read function
    api_mapping = {
        "read_csv": pd.read_csv,
        "read_excel": pd.read_excel,
        "read_json": pd.read_json,
        "read_parquet": pd.read_parquet,
        "read_feather": pd.read_feather,
        "read_pickle": pd.read_pickle,
        "read_hdf": pd.read_hdf,
        "read_sql": pd.read_sql,
        "read_table": pd.read_table,
        # Shorter aliases for convenience
        "csv": pd.read_csv,
        "excel": pd.read_excel,
        "json": pd.read_json,
        "parquet": pd.read_parquet,
        "feather": pd.read_feather,
        "pickle": pd.read_pickle,
        "hdf": pd.read_hdf,
        "sql": pd.read_sql,
        "table": pd.read_table
    }

    if input_config.api not in api_mapping:
        raise ValueError(f"Unsupported pandas API: {input_config.api}. "
                         f"Supported APIs: {list(api_mapping.keys())}")

    pandas_reader_func = api_mapping[input_config.api]

    try:
        # Merge static_args with pandas_options and kwargs
        read_params = {**static_args, **pandas_options, **kwargs}

        logger.debug(
            "Reading data with pandas for input '{name}' using {api}",
            name=input_config.name,
            api=input_config.api
        )

        # Read data with pandas
        pandas_df = pandas_reader_func(**read_params)

        # Filter columns if specified
        if columns:
            if isinstance(columns, str):
                columns = [columns]
            # Check if all requested columns exist
            missing_cols = set(columns) - set(pandas_df.columns)
            if missing_cols:
                logger.warning(
                    "Columns not found in data: {missing_cols}",
                    missing_cols=list(missing_cols)
                )
            available_cols = [
                col for col in columns if col in pandas_df.columns]
            if available_cols:
                pandas_df = pandas_df[available_cols]
                logger.info(
                    "Filtered on columns: {columns}", columns=available_cols)

        logger.info(
            "Successfully read data with pandas for input '{name}': {rows} rows, {cols} columns",
            name=input_config.name,
            rows=len(pandas_df),
            cols=len(pandas_df.columns)
        )

        return pandas_df

    except Exception as e:
        logger.exception(
            "Failed to read data with pandas for input '{name}': {error}",
            name=input_config.name,
            error=str(e)
        )
        raise


def pandas_writer(output_config: OutputModel, data: Any, columns: list[str] | None = None, **kwargs) -> None:
    """Write data using pandas to various file formats.

    This function accepts various data types (pandas DataFrame, Spark DataFrame, etc.)
    and writes them to file formats supported by pandas (CSV, Excel, JSON, Parquet, etc.).

    :param output_config: The OutputModel containing the API and arguments for writing data.
    :param data: The data to write (pandas DataFrame, Spark DataFrame, etc.).
    :param columns: List of columns to select before writing. If None, all columns will be written.
    :param kwargs: Additional keyword arguments for the pandas writer.
    """
    static_args = output_config.args.copy()

    # Extract pandas-specific options
    pandas_options = static_args.pop("pandas_options", {})

    try:
        # Convert data to pandas DataFrame if it's not already
        if hasattr(data, 'toPandas'):
            # Spark DataFrame
            pandas_df = data.toPandas()
            logger.debug("Converted Spark DataFrame to pandas DataFrame")
        elif isinstance(data, pd.DataFrame):
            # Already a pandas DataFrame
            pandas_df = data.copy()
        else:
            # Try to convert to pandas DataFrame
            try:
                pandas_df = pd.DataFrame(data)
                logger.debug("Converted data to pandas DataFrame")
            except Exception as e:
                raise ValueError(
                    f"Cannot convert data of type {type(data)} to pandas DataFrame: {e}") from e

        # Filter columns if specified
        if columns:
            if isinstance(columns, str):
                columns = [columns]
            # Select only the specified columns that exist
            available_columns = [
                col for col in columns if col in pandas_df.columns]
            if available_columns:
                pandas_df = pandas_df[available_columns]
                logger.info(
                    "Filtered to columns: {columns}", columns=available_columns)
            else:
                logger.warning(
                    "None of the specified columns {columns} found in DataFrame", columns=columns)

        # Map API to pandas write method
        api_mapping = {
            "to_csv": pandas_df.to_csv,
            "to_excel": pandas_df.to_excel,
            "to_json": pandas_df.to_json,
            "to_parquet": pandas_df.to_parquet,
            "to_feather": pandas_df.to_feather,
            "to_pickle": pandas_df.to_pickle,
            "to_hdf": pandas_df.to_hdf,
            "to_html": pandas_df.to_html,
            "to_latex": pandas_df.to_latex,
            # Shorter aliases for convenience
            "csv": pandas_df.to_csv,
            "excel": pandas_df.to_excel,
            "json": pandas_df.to_json,
            "parquet": pandas_df.to_parquet,
            "feather": pandas_df.to_feather,
            "pickle": pandas_df.to_pickle,
            "hdf": pandas_df.to_hdf,
            "html": pandas_df.to_html,
            "latex": pandas_df.to_latex
        }

        if output_config.api not in api_mapping:
            raise ValueError(f"Unsupported pandas API: {output_config.api}. "
                             f"Supported APIs: {list(api_mapping.keys())}")

        pandas_writer_func = api_mapping[output_config.api]

        # Merge static_args with pandas_options and kwargs
        write_params = {**static_args, **pandas_options, **kwargs}

        logger.debug(
            "Writing data with pandas for output '{name}' using {api}",
            name=output_config.name,
            api=output_config.api
        )

        # Write data with pandas
        pandas_writer_func(**write_params)

        logger.info(
            "Successfully wrote data with pandas for output '{name}': {rows} rows, {cols} columns",
            name=output_config.name,
            rows=len(pandas_df),
            cols=len(pandas_df.columns)
        )

    except Exception as e:
        logger.exception(
            "Failed to write data with pandas for output '{name}': {error}",
            name=output_config.name,
            error=str(e)
        )
        raise
