class BreweriesdbDataPackage:
    """Class containing the API endpoints for the breweriesdb"""

    URL_LIST_BREWERIES = (
        "https://api.openbrewerydb.org/v1/breweries?page={}&per_page={}"
    )
