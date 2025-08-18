class Pipeline:
    """Pipeline for the openbrewerydb_list_breweries ETL process"""

    PIPELINE_NAME = "openbrewerydb_list_breweries"

    SOURCE = "https://www.openbrewerydb.org/"
    ENDPOINT = "v1/breweries"
