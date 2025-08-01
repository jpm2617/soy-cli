import pandas as pd
from pyspark.sql import SparkSession

from soy_cli import logging

logger = logging.getLogger(__name__)


def get_table_details(
    spark_session: SparkSession,
    catalog: str,
    schema: str,
    table_name: str
) -> dict:
    """Get detailed information about a table including count, size, and metadata.

    :param spark_session: Spark session to execute SQL queries
    :param catalog: Catalog name of the table
    :param schema: Schema name of the table
    :param table_name: Name of the table
    :return: Dictionary containing table details such as format, number of files, size in bytes,
             size in MB, row count, creation date, properties, and location
    """
    full_table_name = ".".join([catalog, schema, table_name])

    logger.info(
        "Getting details for table: {full_table_name}", full_table_name=full_table_name
    )

    try:
        # Get basic table info
        table_info = spark_session.sql(
            f"DESCRIBE DETAIL {full_table_name}").collect()[0]

        # Get row count (this can be expensive for large tables)
        row_count = spark_session.table(full_table_name).count()

        # Validate table name to prevent SQL injection
        columns = spark_session.sql(f"SELECT * FROM {full_table_name} LIMIT 1").columns  # noqa: S608

        return {
            "catalog": catalog,
            "schema": schema,
            'table_name': table_name,
            'full_table_name': full_table_name,
            'format': table_info['format'],
            'num_files': table_info['numFiles'],
            'size_in_bytes': table_info['sizeInBytes'],
            'size_in_mb': round(table_info['sizeInBytes'] / (1024 * 1024), 2) if table_info['sizeInBytes'] else 0,
            'row_count': row_count,
            'created_at': str(table_info['createdAt']),
            'properties': table_info['properties'],
            'location': table_info['location'],
            'columns': columns
        }
    except Exception as e:
        return {
            "catalog": catalog,
            "schema": schema,
            'table_name': table_name,
            'full_table_name': full_table_name,
            'error': str(e)
        }


def get_all_tables_summary(spark_session: SparkSession, table_list: list[str]) -> pd.DataFrame:
    """Get summary information for all tables in the provided list.

    List must have table names in the format 'catalog.schema.table_name'.

    :param table_list: List of table names to summarize
    :return: List of dictionaries containing summary information for each table
    """
    summary_data = []

    for table_full_name in table_list:
        catalog, schema, table_name = table_full_name.split('.')
        summary_data += [get_table_details(spark_session,
                                           catalog, schema, table_name)]

    summary_df = pd.DataFrame(summary_data)
    summary_df.sort_values(by="row_count", ascending=False, inplace=True)

    return summary_df
