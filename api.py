from os import getenv

import pandas as pd
import requests

from datamap import api_key_map

DOMAIN = getenv("DOMAIN")


class API:
    def __init__(self, school):
        domain = getenv("DOMAIN")
        self.api_key = api_key_map.get(school)
        self.url = f"{domain}/api/v1/"

    def get(self, entity):
        response = requests.get(f"{self.url}{entity}?apikey={self.api_key}")
        return response.json()
