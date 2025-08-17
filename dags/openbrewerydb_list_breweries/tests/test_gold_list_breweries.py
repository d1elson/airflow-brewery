import os
import sys
import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))

plugin_path = os.path.join(project_root, 'plugins')
if plugin_path not in sys.path:
    sys.path.append(plugin_path)

from brewery_plugins.gcs_buckets import GcsBuckets
from brewery_plugins.spark_config import SparkConfig

from openbrewerydb_list_breweries.tasks.gold_list_breweries_task import (
    GoldListBreweriesTask,
)


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("pytest").getOrCreate()


@pytest.fixture(scope="module")
def gold():
    return GoldListBreweriesTask()


def test_aggregate_data(gold):
    """Tests the aggregation of data by country, state, and brewery type"""

    spark_config = SparkConfig('business_test')

    spark = spark_config.create_spark_session()
    data = [
        Row(id='1', country="Brazil", state="Acre", brewery_type="micro"),
        Row(id='2', country="Brazil", state="Acre", brewery_type="micro"),
        Row(id='3', country="Brazil", state="Pernambuco", brewery_type="brewpub"),
        Row(id='4', country="Canada", state="Ontario", brewery_type="regional"),
    ]
    df = spark.createDataFrame(data)

    df_aggregated = gold._groupby(df)

    result = df_aggregated.collect()

    assert len(result) == 3, "The number of rows is incorrect"

    expected_counts = {
        ("Brazil", "Acre", "micro"): 2,
        ("Brazil", "Pernambuco", "brewpub"): 1,
        ("Canada", "Ontario", "regional"): 1,
    }

    for row in result:
        key = (row["country"], row["state"], row["brewery_type"])
        assert row["brewery_count"] == expected_counts[key], f"Contagem incorreta para {key}"
