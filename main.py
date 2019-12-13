import argparse
import logging
import sys
import traceback

import pandas as pd
from pandas.io.json import json_normalize
from sqlsorcery import MSSQL

from api import API
from mailer import Mailer


# This argparse is currently only useful for testing since the job does a truncate and reload.
parser = argparse.ArgumentParser()
parser.add_argument(
    "--schools",
    help='Run for the specified schools. List separated by spaces (eg. --schools "KIPP Bayview Academy" "KIPP Bridge Academy (Upper)")',
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
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_schools_and_keys(sql):
    """Retrieve schools and API keys from the data warehouse."""
    df = sql.query(f"SELECT * FROM custom.DeansList_APIConnection")
    df = df[df["Active"] == True]
    school_key_map = dict(zip(df["SchoolName"], df["APIKey"]))
    if SCHOOLS:
        school_key_map = {school: school_key_map[school] for school in SCHOOLS}
    return school_key_map


def get_raw_incidents_data(incidents, api_key):
    """Get the raw data and add additional columns."""
    incident_fields = [
        "Actions",
        "AddlReqs",
        "AdminSummary",
        "Category",
        "CategoryID",
        "Context",
        "CreateBy",
        "CreateFirst",
        "CreateLast",
        "CreateTS_date",
        "FamilyMeetingNotes",
        "GradeLevelShort",
        "HomeroomName",
        "IncidentID",
        "Infraction",
        "InfractionTypeID",
        "IsActive",
        "IsReferral",
        "IssueTS_date",
        "Penalties",
        "ReportedDetails",
        "SchoolID",
        "SendAlert",
        "Status",
        "StatusID",
        "StudentID",
        "StudentSchoolID",
    ]
    df = json_normalize(incidents["data"])
    df.columns = df.columns.str.replace(".", "_")
    df = df[incident_fields]
    df["SchoolAPIKey"] = api_key
    df = df.astype({"Actions": str, "Penalties": str})
    logging.info(f"Retrieved {len(df)} Incident records.")
    return df


def delete_current_incidents(sql, api_key):
    table = sql.table("DeansList_Raw")
    d = table.delete().where(table.c.SchoolAPIKey == api_key)
    sql.engine.execute(d)


def insert_new_incidents(sql, incidents, api_key):
    """Insert records into the Raw Incidents table."""
    df = get_raw_incidents_data(incidents, api_key)
    sql.insert_into("DeansList_Raw", df, if_exists="append")
    count = len(df)
    logging.info(f"Inserted {count} records into DeansList_Raw.")
    return count


def delete_current_records(sql, api_key, table_name, incident_column):
    """Delete records for the given table (Actions or Penalties) that have a corresponding incident in the Raw table."""
    incident_ids = sql.query(
        f"""SELECT DISTINCT t.{incident_column}
        FROM custom.{table_name} t
        LEFT JOIN custom.DeansList_Raw r
            ON r.IncidentID = t.{incident_column}
        WHERE r.SchoolAPIKey='{api_key}'"""
    )
    incident_ids = incident_ids[incident_column].tolist()
    table = sql.table(table_name)
    for incident_id in incident_ids:
        d = table.delete().where(table.c[incident_column] == incident_id)
        sql.engine.execute(d)


def insert_new_records(sql, incidents, record_type):
    """Insert records into the table by the record_type (Actions or Penalties)."""
    df = get_nested_column_data(incidents, record_type)
    sql.insert_into(f"DeansList_{record_type}", df, if_exists="append")
    count = len(df)
    logging.info(f"Inserted {count} records into DeansList_{record_type}.")
    return count


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
        sql = MSSQL()
        school_key_map = get_schools_and_keys(sql)

        total_incidents = 0
        total_actions = 0
        total_penalties = 0

        for school, api_key in school_key_map.items():
            logging.info(f"Getting data for {school}.")
            incidents = API(api_key).get("incidents")

            delete_current_incidents(sql, api_key)
            count_incidents = insert_new_incidents(sql, incidents, api_key)
            total_incidents += count_incidents

            delete_current_records(sql, api_key, "DeansList_Actions", "SourceID")
            count_actions = insert_new_records(sql, incidents, "Actions")
            total_actions += count_actions

            delete_current_records(sql, api_key, "DeansList_Penalties", "IncidentID")
            count_penalties = insert_new_records(sql, incidents, "Penalties")
            total_penalties += count_penalties

        logging.info(f"Updated {total_incidents} total records in DeansList_Raw.")
        logging.info(f"Updated {total_actions} total records in DeansList_Actions.")
        logging.info(f"Updated {total_penalties} total records in DeansList_Penalties.")

        mailer.notify()
    except Exception as e:
        stack_trace = traceback.format_exc()
        logging.error(stack_trace)
        mailer.notify(success=False)


if __name__ == "__main__":
    main()
