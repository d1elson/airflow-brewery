from airflow.plugins_manager import AirflowPlugin
from brewery_plugins.gcs_buckets import GcsBuckets

class GcsPlugin(AirflowPlugin):
    name = "brewery_plugins"
    helpers = [GcsBuckets]
