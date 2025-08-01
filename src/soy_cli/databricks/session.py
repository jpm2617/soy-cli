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


def get_config() -> Config:
    """Get the Databricks configuration from environment settings.

    :return Config: A Config object containing the Databricks profile ID and cluster ID.
    """
    return Config(
        profile=env.DATABRICKS_PROFILE_ID,
        cluster_id=env.DATABRICKS_CLUSTER_ID,
    )


def get_cluster_state(config: Config | None = None) -> str:
    """Get the state of the Databricks cluster.

    :param config: Optional configuration for the Databricks connection.
    If not provided, it will use the environment variables from `env`.
    :return: The state of the cluster as a string.
    :raises ValueError: If the cluster state is not found in the response.
    """
    if config is None:
        config = get_config()

    headers = config.authenticate()
    url = f"{config.host}/api/2.0/clusters/get"
    response = requests.get(
        url,
        headers=headers,
        timeout=10,
        params={
            "cluster_id": config.cluster_id
        }
    )
    if response.status_code != 200:
        logger.error(
            "Failed to get Databricks cluster state.",
            status_code=response.status_code,
            cluster_id=config.cluster_id,
            host=config.host
        )
        raise RuntimeError(
            f"Failed to get Databricks cluster state: {response.text}"
        )

    cluster_state = response.json().get("state", None)
    if cluster_state is None:
        raise ValueError("Cluster state not found in response.")

    logger.debug(
        "Databricks cluster state retrieved successfully.",
        cluster_id=config.cluster_id,
        host=config.host,
        cluster_state=cluster_state
    )
    return cluster_state


def start_cluster(config: Config | None = None) -> bool:
    """Start the Databricks cluster.

    :param config: Optional configuration for the Databricks connection.
    If not provided, it will use the environment variables from `env`.
    :raises RuntimeError: If the cluster start request fails.
    """
    if config is None:
        config = get_config()

    headers = config.authenticate()
    url = f"{config.host}/api/2.0/clusters/start"
    response = requests.post(
        url,
        headers=headers,
        json={
            "cluster_id": config.cluster_id
        },
        timeout=10
    )

    if response.status_code != 200:
        logger.error(
            "Failed to start Databricks cluster.",
            status_code=response.status_code,
            cluster_id=config.cluster_id,
            host=config.host
        )
        raise RuntimeError(
            f"Failed to start Databricks cluster: {response.text}"
        )
    logger.info(
        "Databricks cluster is starting...",
        cluster_id=config.cluster_id,
        host=config.host
    )
    return True


def stop_cluster(config: Config | None = None) -> bool:
    """Stop the Databricks cluster.

    :param config: Optional configuration for the Databricks connection.
    If not provided, it will use the environment variables from `env`.
    :raises RuntimeError: If the cluster stop request fails.
    """
    if config is None:
        config = get_config()

    headers = config.authenticate()
    url = f"{config.host}/api/2.0/clusters/delete"
    response = requests.post(
        url,
        headers=headers,
        json={
            "cluster_id": config.cluster_id
        },
        timeout=10
    )

    if response.status_code != 200:
        logger.error(
            "Failed to stop Databricks cluster.",
            status_code=response.status_code,
            cluster_id=config.cluster_id,
            host=config.host
        )
        raise RuntimeError(
            f"Failed to stop Databricks cluster: {response.text}"
        )
    logger.info(
        "Databricks cluster stopped successfully.",
        cluster_id=config.cluster_id,
        host=config.host
    )
    return True


def wait_for_cluster() -> str:
    """Wait for the Databricks cluster to be running."""
    delta_t = 30
    while True:
        cluster_state = get_cluster_state()
        if cluster_state == "RUNNING":
            break
        logger.warning(
            f"Waiting for Databricks cluster to start... Retrying in {delta_t} seconds...",
            cluster_state=cluster_state,
        )
        time.sleep(delta_t)
    return cluster_state


def create_databricks_session() -> SparkSession:
    """Create a Databricks session if it does not already exist."""
    spark_session = SparkSession.getActiveSession()

    if spark_session:
        return spark_session

    config = Config(
        profile=env.DATABRICKS_PROFILE_ID,
        cluster_id=env.DATABRICKS_CLUSTER_ID,
    )
    return DatabricksSession.builder.sdkConfig(config).getOrCreate()


def get_databricks_session() -> SparkSession | None:
    """Get or create a Databricks session."""
    try:
        cluster_state = get_cluster_state()
        if cluster_state == "TERMINATED":
            start_cluster()
        if cluster_state != "RUNNING":
            cluster_state = wait_for_cluster()

        session = create_databricks_session()
        session.sql("SELECT 1").collect()
    except Exception as e:
        logger.exception(
            "Failed to create Databricks session.",
            host=env.DATABRICKS_PROFILE_ID,
            cluster_id=env.DATABRICKS_CLUSTER_ID,
            error=str(e)
        )
        raise

    logger.info(
        "Databricks session created successfully.",
        profile_id=env.DATABRICKS_PROFILE_ID,
        cluster_id=env.DATABRICKS_CLUSTER_ID,
        cluster_state=cluster_state,
    )
    return session
