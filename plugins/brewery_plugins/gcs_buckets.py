import os

from google.cloud import storage
import json


class GcsBuckets:
    """A class to interact with Google Cloud Storage buckets.
    Initializes the GcsBuckets instance with a storage client.
    """

    def __init__(self):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
            "/opt/airflow/config/gcs_config.json"
        )
        self.storage_client = storage.Client()

    def get_path_in_layer(
        self, bucket_name: str, layer_name: str, file_name: str = None
    ) -> str:
        """Generates a path string for a given layer and timestamp"""

        return f"gs://{bucket_name}/{layer_name}/" + (file_name or "")

    def get_file_list_in_layer(self, bucket_name: str, layer_name: str) -> list:
        """
        Retrieves a list of file paths from a specified layer within a Google
        Cloud Storage bucket.

        Args:
            bucket_name (str): The name of the GCS bucket to search in.
            layer_name (str): The name of the layer (prefix) to filter files.

        Returns:
            list: A list of file paths in the format 'gs://{bucket_name}/{blob.name}'
            found within the specified layer.
        """
        bucket = self.storage_client.bucket(bucket_name)

        blobs = bucket.list_blobs(prefix=f"{layer_name}")

        blobs_list = [f"gs://{bucket_name}/{blob.name}" for blob in blobs]

        return blobs_list

    def save_json_to_gcs(
        self,
        data: dict,
        bucket_name: str,
        layer_name: str,
        file_name: str,
    ) -> None:
        """
        Saves a JSON-serializable dictionary to a specified Google Cloud Storage
        (GCS) bucket.

        Args:
            data (dict): The data to be saved as a JSON object.
            bucket_name (str): The name of the GCS bucket where the file will be stored.
            layer_name (str): The logical layer name used to organize files within the bucket.
            file_name (str): The name of the file to be created in the bucket.

        Returns:
            None

        Raises:
            google.cloud.exceptions.GoogleCloudError: If there is an error
            uploading the file to GCS.
        """

        blob_path = f"{layer_name}/{file_name}"

        bucket = self.storage_client.bucket(bucket_name)

        blob = bucket.blob(blob_path)

        blob.upload_from_string(json.dumps(data), content_type="application/json")

    def clear_bucket(self, bucket_name: str, layer_name: str) -> None:
        """
        Deletes all blobs in the specified Google Cloud Storage bucket that
        match the given layer name prefix.

        Args:
            bucket_name (str): The name of the GCS bucket to clear.
            layer_name (str): The prefix used to identify blobs belonging to a
            specific layer. All blobs with names starting with '{layer_name}'
            will be deleted.

        Returns:
            None
        """
        bucket = self.storage_client.bucket(bucket_name)

        blobs = bucket.list_blobs(prefix=f"{layer_name}")

        for blob in blobs:
            blob.delete()
