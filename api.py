from os import getenv

import pandas as pd
import requests

DOMAIN = getenv("DOMAIN")


class API:
    def __init__(self, version, api_key):
        self.api_key = api_key
        self.version = version

    def get(self, entity, addl_params=None):
        params = {"apikey": self.api_key}
        if addl_params:
            params.update(addl_params)
        if self.version == "v1":
            url = f"{DOMAIN}/api/v1/{entity}"
        elif self.version == "beta":
            url = f"{DOMAIN}/api/beta/export/{entity}.php"
        response = requests.get(url, params=params)
        return response.json()
