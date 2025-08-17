from pyspark.sql.types import StructType, StructField, StringType, DoubleType


class SchemaTables:
    """SchemaTables class is used to define the ideal schema of the table"""

    SILVER_SCHEMA_LIST_BREWERIES = StructType(
        [
            StructField("id", StringType(), False), # NOTE must be not-null
            StructField("name", StringType(), True),
            StructField("brewery_type", StringType(), True),
            StructField("address_1", StringType(), True),
            StructField("address_2", StringType(), True),
            StructField("address_3", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state_province", StringType(), True),
            StructField("postal_code", StringType(), True),
            StructField("country", StringType(), False), # NOTE must be not-null
            StructField("longitude", DoubleType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("phone", StringType(), True),
            StructField("website_url", StringType(), True),
            StructField("state", StringType(), False), # NOTE must be not-null
            StructField("street", StringType(), True),
        ]
    )

    EXPECTED_COLUMNS = [
        "id",
        "name",
        "brewery_type",
        "address_1",
        "address_2",
        "address_3",
        "city",
        "state_province",
        "postal_code",
        "country",
        "longitude",
        "latitude",
        "phone",
        "website_url",
        "state",
        "street",
    ]
