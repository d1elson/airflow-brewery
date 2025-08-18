from datetime import datetime

from pyspark.sql.functions import count
from pyspark.sql import DataFrame

from brewery_plugins.gcs_buckets import GcsBuckets
from brewery_plugins.logging_config import LoggingConfig
from brewery_plugins.spark_config import SparkConfig


class GoldListBreweriesTask:
    """Class responsible for ingesting business rules
    and saving it to GCS Buckets
    """

    def __init__(self):

        logger = LoggingConfig()
        self.log = logger.configure_logging()

        self.execution_date = datetime.today().strftime("%Y-%m-%d")

        self.bucket_name = "storage-open-brewery"

        # GCS variables
        self.silver_layer = "silver"
        self.gold_layer = "gold"

        # Disk variables
        self.silver_storage = (
            f"/opt/airflow/.storage/{self.bucket_name}/{self.silver_layer}"
        )
        self.gold_storage = (
            f"/opt/airflow/.storage/{self.bucket_name}/{self.gold_layer}"
        )

        # Initialize Spark session
        spark_config = SparkConfig("gold_list_breweries_task")
        self.spark = spark_config.create_spark_session()

    def execute(self) -> None:
        """Execute the flow order of functions"""

        df_breweries = self._read_from_disk()
        df_breweries = self._read_from_gcs()

        df_breweries_per_location_type = self._groupby(df_breweries)

        self._save_on_disk(df_breweries_per_location_type)
        self._save_on_gcs(df_breweries_per_location_type)

    def _read_from_disk(self) -> DataFrame:
        """Reads the silver data from the disk

        Returns:
            DataFrame: The silver data in a DataFrame
        """
        df = self.spark.read.parquet(f"{self.silver_storage}/breweries_per_location")

        return df

    def _read_from_gcs(self) -> DataFrame:
        """Reads all data from the current month

        Returns:
            df (DataFrame): Data in a pyspark DataFrame
        """
        self.log.info("Reading data from GCS...")

        self.gcs_buckets = GcsBuckets()

        blob_path = self.gcs_buckets.get_path_in_layer(
            self.bucket_name, self.silver_layer, "breweries_per_location"
        )

        df = self.spark.read.parquet(blob_path)

        return df

    def _groupby(self, df: DataFrame) -> DataFrame:
        """Groups and aggregates brewery data by country, state, and brewery type.

        This method takes a PySpark DataFrame containing brewery information
        and performs a group-by aggregation
        on the columns 'country', 'state', and 'brewery_type'. It computes the
        count of breweries for each unique combination of these fields.

        df (DataFrame): Input PySpark DataFrame with brewery data. Must contain
            the columns 'country', 'state', and 'brewery_type'.

        Returns:
            DataFrame: Aggregated PySpark DataFrame with columns 'country',
            'state', 'brewery_type', and 'brewery_count', where 'brewery_count'
            represents the number of breweries for each group.
        """
        df_groupby = df.groupBy("country", "state", "brewery_type").agg(
            count("id").alias("brewery_count")
        )

        df_groupby.show(truncate=False)  # remove show for release

        return df_groupby

    def _save_on_disk(self, df: DataFrame) -> None:
        """Saves the transformed data to a local disk file

        Args:
            df (DataFrame): The transformed data
        """
        df.write.mode("overwrite").parquet(
            f"{self.gold_storage}/breweries_per_type_location"
        )

    def _save_on_gcs(self, df: DataFrame) -> None:
        """Saves the transformed data to a GCS bucket in the gold layer
        at the current month path

        Args:
            df (DataFrame): The transformed data to be saved
        """
        blob_path = self.gcs_buckets.get_path_in_layer(
            self.bucket_name, self.gold_layer, "breweries_per_type_location"
        )

        self.log.info(f"Saving data to GCS: {blob_path}")

        df.write.mode("overwrite").parquet(blob_path)
