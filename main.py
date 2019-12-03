import argparse

import pandas as pd
from pandas.io.json import json_normalize
import requests

from datamap import api_key_map
from api import API


parser = argparse.ArgumentParser()
parser.add_argument('-s',
    help='Run for the specified schools. List separated by spaces (eg. -s BayviewMS King)',
    dest='schools',
    nargs='+'
)
args = parser.parse_args()
SCHOOLS = args.schools


def main():
    schools = SCHOOLS if SCHOOLS else api_key_map.keys()

    df = pd.DataFrame()
    for school in schools:
        incidents_data = API(school).get("incidents")
        incidents_df = json_normalize(incidents_data['data'])
        print(f"Retrieved {len(incidents_df)} records for {school}.")
        df = df.append(incidents_df, sort=False)

if __name__ == "__main__":
    main()