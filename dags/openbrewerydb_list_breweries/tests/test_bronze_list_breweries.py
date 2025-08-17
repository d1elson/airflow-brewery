import os
import sys
import pytest
import requests

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))

plugin_path = os.path.join(project_root, "plugins")
if plugin_path not in sys.path:
    sys.path.append(plugin_path)

from brewery_plugins.gcs_buckets import GcsBuckets

from openbrewerydb_list_breweries.tasks.bronze_list_breweries_task import (
    BronzeListBreweriesTask,
)

from openbrewerydb_list_breweries.tools.openbrewerydb_data_package import (
    BreweriesdbDataPackage,
)


@pytest.fixture(scope="module")
def bronze():
    return BronzeListBreweriesTask()


def test_extract_data_from_api(bronze):
    """Test if the API data extraction returns a list with data"""

    data = bronze._extract()

    assert isinstance(data, list)

    if data:
        assert isinstance(data[0], dict)


def test_extract_multiple_pages(bronze):
    """Test if extracting multiple pages returns a list"""

    data = bronze._extract()

    assert len(data) > 200, "Expected more than 200 records, indicating multiple pages"


def test_api_status_code():
    """Test if the API responds with status 200"""

    url = BreweriesdbDataPackage.URL_LIST_BREWERIES
    formatted_url = url.format(1, 200)  # formatting url

    response = requests.get(formatted_url)

    assert (
        response.status_code == 200
    ), f"API request failed with status {response.status_code}"
