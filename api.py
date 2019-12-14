from os import getenv

import pandas as pd
import requests

DOMAIN = getenv("DOMAIN")


class API:
    def __init__(self, version, api_key):
        self.api_key = api_key
        self.version = version

    def get(self, entity):
        if self.version == "v1":
            url = f"{DOMAIN}/api/v1/{entity}?apikey={self.api_key}"
        elif self.version == "beta":
            url = f"{DOMAIN}/api/beta/export/{entity}.php?apikey={self.api_key}"
        response = requests.get(url)
        return response.json()
