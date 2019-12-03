import argparse
import logging
import traceback

import pandas as pd
from pandas.io.json import json_normalize
import requests
from sqlsorcery import MSSQL

from api import API
from datamap import api_key_map
from mailer import Mailer


parser = argparse.ArgumentParser()
parser.add_argument(
    "-s",
    help="Run for the specified schools. List separated by spaces (eg. -s BayviewMS King)",
    dest="schools",
    nargs="+",
)
args = parser.parse_args()
SCHOOLS = args.schools


logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S%p",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_raw_incidents_data(incidents, school):
    """Get the raw data and add additional columns."""
    df = json_normalize(incidents["data"])
    df.columns = df.columns.str.replace(".", "_")
    df["SPACE"] = " "
    df["SchoolAPIKey"] = api_key_map.get(school)
    df = df.astype({"Actions": str, "Penalties": str})
    logging.info(f"Retrieved {len(df)} Incident records.")
    return df


def get_nested_column_data(incidents, column):
    """Get column data that is stored as a list of JSON objects."""
    data = []
    for record in incidents["data"]:
        if record[column]:
            data.extend(record[column])
    df = pd.DataFrame(data)
    logging.info(f"Retrieved {len(df)} {column} records.")
    return df


def main():
    try:
        mailer = Mailer()
        schools = SCHOOLS if SCHOOLS else api_key_map.keys()
        sql = MSSQL()

        all_raw = pd.DataFrame()
        all_actions = pd.DataFrame()
        all_penalties = pd.DataFrame()

        for school in schools:
            logging.info(f"Getting data for {school}.")
            incidents = API(school).get("incidents")

            raw = get_raw_incidents_data(incidents, school)
            all_raw = all_raw.append(raw, sort=False)

            actions = get_nested_column_data(incidents, "Actions")
            all_actions = all_actions.append(actions, sort=False)

            penalties = get_nested_column_data(incidents, "Penalties")
            all_penalties = all_penalties.append(penalties, sort=False)

        sql.insert_into("DeansList_zdev_Raw", all_raw, if_exists="replace")
        logging.info(f"Inserted {len(raw)} records into DeansList_zdev_Raw.")
        sql.insert_into("DeansList_zdev_Actions", all_actions, if_exists="replace")
        logging.info(
            f"Inserted {len(all_actions)} records into DeansList_zdev_Actions."
        )
        sql.insert_into("DeansList_zdev_Penalties", all_penalties, if_exists="replace")
        logging.info(
            f"Inserted {len(all_penalties)} records into DeansList_zdev_Penalties."
        )

        mailer.notify()
    except Exception as e:
        stack_trace = traceback.format_exc()
        mailer.notify(success=False, error_message=stack_trace)


if __name__ == "__main__":
    main()
