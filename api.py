from os import getenv

import pandas as pd
import requests


DOMAIN = getenv("DOMAIN")


class API:
    def __init__(self, api_key):
        domain = getenv("DOMAIN")
        self.api_key = api_key
        self.url = f"{domain}/api/v1/"

    def get(self, entity):
        response = requests.get(f"{self.url}{entity}?apikey={self.api_key}")
        return response.json()
