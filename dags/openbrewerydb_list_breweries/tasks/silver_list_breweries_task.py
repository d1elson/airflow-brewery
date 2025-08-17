from functools import reduce
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit, col, trim, regexp_replace

from brewery_plugins.gcs_buckets import GcsBuckets
from brewery_plugins.logging_config import LoggingConfig
from brewery_plugins.spark_config import SparkConfig

from openbrewerydb_list_breweries.tools.schema_tables import SchemaTables


class SilverListBreweriesTask:
    """Class responsible for transforming raw data
    from openbrewerydb API and saving it to GCS Buckets
    in a format ready for analysis
    """

    def __init__(self):

        logger = LoggingConfig()
        self.log = logger.configure_logging()

        self.execution_date = datetime.today().strftime("%Y-%m-%d")

        self.bucket_name = "storage-open-brewery"
        self.bronze_layer = "bronze"
        self.silver_layer = "silver"

        self.bronze_storage = (
            f"/opt/airflow/.storage/{self.bucket_name}/{self.bronze_layer}"
        )
        self.silver_storage = (
            f"/opt/airflow/.storage/{self.bucket_name}/{self.silver_layer}"
        )

        # Initialize Spark session
        spark_config = SparkConfig("silver_list_breweries_task")
        self.spark = spark_config.create_spark_session()

    def execute(self) -> None:
        """Execute the flow order of functions"""

        df_breweries = self._read_from_disk()
        df_breweries = self._read_from_gcs()

        df_breweries = self._apply_schema(
            df_breweries, SchemaTables.SILVER_SCHEMA_LIST_BREWERIES
        )

        df_breweries = self._transform_columns(df_breweries)

        self._save_on_disk(df_breweries)
        self._save_on_gcs(df_breweries)

        # self._post_processing()

    def _read_from_disk(self) -> DataFrame:
        """Reads the raw data from the disk

        Returns:
            DataFrame: The raw data in a DataFrame
        """
        df = self.spark.read.json(
            f"{self.bronze_storage}/{self.execution_date}/list_breweries.json"
        )

        return df

    def _read_from_gcs(self) -> DataFrame:
        """Reads all files from the current month

        Returns:
            df (DataFrame): The extracted data in a DataFrame, concatenate all
            files of the current month
        """
        self.log.info("Reading data from GCS...")

        self.gcs_buckets = GcsBuckets()

        df = self.spark.read.option("multiline", "true").json(
            f"gs://{self.bucket_name}/{self.bronze_layer}/{self.execution_date}/list_breweries.json"
        )

        return df

    def _apply_schema(self, df: DataFrame, schema: SchemaTables) -> DataFrame:
        """Applies the provided schema to the given PySpark DataFrame, ensuring
        that each column matches the specified data types.
        If a column from the schema does not exist in the DataFrame, it is
        added with empty string values and cast to the appropriate type.
        All missing values in the DataFrame are filled with empty strings.

            df (DataFrame): The PySpark DataFrame to have its column types adjusted.
            schema (SchemaTables): The schema definition containing column names
            and data types.

            DataFrame: A PySpark DataFrame with columns cast to the specified
            types and missing columns added as empty strings.
        """
        self.log.info("Applying schema...")

        df = df.select(
            [
                (
                    col(field.name).cast(field.dataType)
                    if field.name in df.columns
                    else lit("").cast(field.dataType).alias(field.name)
                )
                for field in schema
            ]
        )

        df = df.fillna("")

        return df

    def _transform_columns(self, df: DataFrame) -> DataFrame:
        """Remove leading and trailing spaces from the 'country' column"""

        df = df.withColumn("country", trim(col("country"))).withColumn(
            "state", regexp_replace("state", "[^a-zA-Z0-9 ]", "")
        )

        return df

    # def _test_transform_columns(self, df: DataFrame) -> DataFrame:

    def _save_on_disk(self, df: DataFrame) -> None:
        """Saves the transformed data to a local disk file

        Args:
            df (DataFrame): The transformed data
        """
        df.write.mode("overwrite").partitionBy("country", "state").parquet(
            f"{self.silver_storage}/breweries_per_location"
        )

    def _save_on_gcs(self, df: DataFrame) -> None:
        """Saves the transformed data to a GCS bucket partitioned by country,
        state and city

        Args:
            df (DataFrame): The transformed data to be saved
        """
        blob_path = self.gcs_buckets.get_path_in_layer(
            self.bucket_name, self.silver_layer, 'breweries_per_location'
        )

        self.log.info(f"Saving data to GCS: {blob_path}")
        df.write.mode("overwrite").partitionBy("country", "state").parquet(blob_path)

    def _post_processing(self) -> None:
        """Post processing step to be executed after the transformation"""

        self.spark.stop()

        self.log.info("Transform task completed successfully")
