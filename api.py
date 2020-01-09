from os import getenv

import pandas as pd
import requests

DOMAIN = getenv("DOMAIN")


class API:
    def __init__(self, version, api_key):
        """Initialize the class with the API key (school-specific) and URL.
        
        Certain endpoints are only available on the beta version of the API
        (eg. behaviors, communications).
        """
        self.api_key = api_key
        self.version = version

    def get(self, entity, addl_params=None):
        """Get the response for this endpoint.
        
        entity: the keyword for the endpoint (defined by DeansList API documentation)
        addl_params: dictionary of additional parameters, such as start/end date
            eg. {"sdt": "2019-12-01, "edt": "2019-12-31"}
        """
        params = {"apikey": self.api_key}
        if addl_params:
            params.update(addl_params)
        if self.version == "v1":
            url = f"{DOMAIN}/api/v1/{entity}"
        elif self.version == "beta":
            url = f"{DOMAIN}/api/beta/export/{entity}.php"
        response = requests.get(url, params=params)
        return response.json()
