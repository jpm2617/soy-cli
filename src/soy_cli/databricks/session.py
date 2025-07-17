import time

import requests
from databricks.connect import DatabricksSession
from databricks.sdk.core import Config
from pyspark.sql import SparkSession

from soy_cli import logging
from soy_cli.config.env import env

logger = logging.getLogger(__name__)

# Suppress gRPC warnings
# os.environ['GRPC_VERBOSITY'] = 'ERROR'
# os.environ['GRPC_TRACE'] = ''


def check_cluster_state(host_id: str, cluster_id: str, headers: dict[str, str]) -> str:
    """Check the state of the Databricks cluster."""
    url = f"{host_id}/api/2.0/clusters/get"
    response = requests.get(url, headers=headers, timeout=10, params={
                            "cluster_id": cluster_id})
    cluster_state = response.json().get("state", None)
    if cluster_state is None:
        raise ValueError("Cluster state not found in response.")
    return cluster_state


def start_spark_session() -> SparkSession | None:
    """Get or create a Databricks session.

    :param app_name: The name of the Spark application.
    :param spark_properties: Additional Spark properties to set.
    :param enable_hive_support: Whether to enable Hive support.
    """
    config = Config(
        profile=env.DATABRICKS_PROFILE_ID,
        cluster_id=env.DATABRICKS_CLUSTER_ID,
    )
    headers = config.authenticate()
    logger.info(
        "Starting databricks-connect spark session", config=config
    )
    result = 0
    timeout = 180  # 3 minutes in seconds
    start_time = time.time()
    while time.time() - start_time < timeout:
        cluster_state = check_cluster_state(
            host_id=config.host,
            cluster_id=config.cluster_id,
            headers=headers
        )
        if cluster_state != "RUNNING":
            logger.warning(
                "Databricks cluster is not running. Waiting until it starts.",
                cluster_state=cluster_state
            )
            time.sleep(10)
            continue

        spark_session = DatabricksSession.builder.sdkConfig(
            config
        ).getOrCreate()
        result = spark_session.sql("SELECT 1").collect()[0][0]

        if result == 1:
            logger.info(
                "Databricks cluster is running.",
                host=env.DATABRICKS_PROFILE_ID,
                cluster_id=env.DATABRICKS_CLUSTER_ID,
                cluster_state=cluster_state
            )
            break
    else:
        logger.exception(
            "Databricks cluster did not start within the timeout period.",
            host=env.DATABRICKS_PROFILE_ID,
            cluster_id=env.DATABRICKS_CLUSTER_ID,
            cluster_state=cluster_state
        )
        return None

    return spark_session
