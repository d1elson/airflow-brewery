import os
import sys
import pytest
from pyspark.sql import SparkSession


project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))

plugin_path = os.path.join(project_root, "plugins")
if plugin_path not in sys.path:
    sys.path.append(plugin_path)

from brewery_plugins.gcs_buckets import GcsBuckets

from openbrewerydb_list_breweries.tasks.silver_list_breweries_task import (
    SilverListBreweriesTask,
)

from openbrewerydb_list_breweries.tools.schema_tables import SchemaTables


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("pytest").getOrCreate()


@pytest.fixture(scope="module")
def silver():
    return SilverListBreweriesTask()


def _get_sample_data(silver):
    """Reads sample brewery data from disk using the provided 'silver' task object."""

    data = silver._read_from_disk()

    return data


def test_no_nulls_in_key_columns(silver):
    """Checks that key columns do not have null values"""

    sample_data = _get_sample_data(silver)

    null_count = sample_data.filter(sample_data["id"].isNull()).count()

    assert null_count == 0, "Existem IDs nulos no DataFrame"


def test_missing_columns(silver):
    """Checks that no columns are missing"""

    sample_data = _get_sample_data(silver)

    expected_columns = SchemaTables.EXPECTED_COLUMNS

    assert all(col in sample_data.columns for col in expected_columns)


def test_row_count_after_transformation(silver):
    """Checks if the number of rows is preserved after transformation"""

    sample_data = _get_sample_data(silver)
    row_count_before = sample_data.count()

    sample_after_transform = silver._transform_columns(sample_data)

    row_count_after = sample_after_transform.count()
    assert row_count_before == row_count_after
