import os
import json
import requests
from datetime import datetime

from openbrewerydb_list_breweries.tools.openbrewerydb_data_package import (
    BreweriesdbDataPackage,
)

from brewery_plugins.gcs_buckets import GcsBuckets
from brewery_plugins.logging_config import LoggingConfig


class BronzeListBreweriesTask:
    """Class responsible for extracting data
    from openbrewerydb API and saving it to GCS Buckets
    """

    def __init__(self):

        logger = LoggingConfig()
        self.log = logger.configure_logging()

        self.execution_date = datetime.today().strftime("%Y-%m-%d")

        self.bucket_name = "storage-open-brewery"
        self.bronze_layer = "bronze"
        self.bronze_storage = (
            f"/opt/airflow/.storage/{self.bucket_name}/{self.bronze_layer}"
        )

    def execute(self) -> None:
        """Execute the flow order of functions"""

        data = self._extract()

        self._save_on_disk(data)
        self._save_on_gcs(data)

    def _extract(self) -> list:
        """Extracts data from openbrewerydb API

        Returns:
            data (list): The extracted data
        """

        print("Starting data extraction...")

        url = BreweriesdbDataPackage.URL_LIST_BREWERIES
        session = requests.Session()  # session init
        page = 1
        all_data = []

        while True:
            # NOTE maxing out number of breweries per page (200)
            formatted_url = url.format(page, 200)  # formatting url

            response = session.get(formatted_url)
            response.raise_for_status()

            data = response.json()  # parsing json response

            # checking for empty response
            if not data:
                break

            all_data.extend(data)

            print(page)
            page += 1

        return all_data

    def _save_on_disk(self, data: dict) -> None:
        """Saves the extracted data to a local disk file

        Args:
            data (dict): The extracted data
        """
        # checking if the dir exists
        os.makedirs(f"{self.bronze_storage}/{self.execution_date}", exist_ok=True)

        print(f"Saving data to disk...")
        with open(
            f"{self.bronze_storage}/{self.execution_date}/list_breweries.json", "w"
        ) as f:
            f.write(json.dumps(data))

    def _save_on_gcs(self, data: json) -> None:
        """Saves the extracted data to a GCS bucket

        Args:
            data (json): The extracted data
        """
        file_name = f"{self.execution_date}/list_breweries.json"

        gcs_buckets = GcsBuckets()
        gcs_buckets.save_json_to_gcs(
            data, self.bucket_name, self.bronze_layer, file_name
        )

        self.log.info("Data extraction completed successfully")
