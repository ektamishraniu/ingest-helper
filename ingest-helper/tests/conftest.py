"""
pytest fixtures that can be reused across tests. the filename needs to be conftest.py
"""

# make sure env variables are set correctly
import findspark  # this needs to be the first import

findspark.init()

import logging  # noqa
import pytest  # noqa

from pyspark.sql import SparkSession  # noqa


def quiet_py4j():
    """ turn down spark logging for the test context """
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)


@pytest.fixture(scope='session')
def spark_session():
    """
    Fixture for creating a shared SparkSession to use when testing code that primarily
    utilizes Spark's Structured API (DataFrames, SQL tables and views).
    """
    spark = (
        SparkSession.builder.master('local[2]')
        .appName('SparkSession Transformations Tests')
        .getOrCreate()
    )
    spark.conf.set("spark.sql.execution.arrow.enabled", True)
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    yield spark
    spark.stop()


@pytest.fixture
def spark_context(spark_session):
    return spark_session.sparkContext


@pytest.fixture(autouse=True)
def no_spark_stop(monkeypatch):
    """
    Disable calls to `spark.stop()` on the shared spark session during integration tests.
    """
    def nop(*args, **kwargs):
        print('Prevented spark.stop for improved test performance.')
    monkeypatch.setattr('pyspark.sql.SparkSession.stop', nop)
