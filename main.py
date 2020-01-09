import argparse
import calendar
import datetime
import logging
import sys
import traceback

import pandas as pd
from pandas.io.json import json_normalize
from sqlsorcery import MSSQL

from api import API
from mailer import Mailer


parser = argparse.ArgumentParser()
parser.add_argument(
    "--schools",
    help='Run for the specified schools. List separated by spaces (eg. --schools "KIPP Bayview Academy" "KIPP Bridge Academy (Upper)")',
    dest="schools",
    nargs="+",
)
parser.add_argument(
    "--behavior-backfill",
    help='Backfill behavior data only for the given date range. Recommended: 1 month max. Example: --behavior-backfill "2019-12-01" "2019-12-31"',
    dest="behavior_backfill",
    nargs=2,
)
args = parser.parse_args()
SCHOOLS = args.schools
BEHAVIOR_BACKFILL = args.behavior_backfill

logging.basicConfig(
    filename="app.log",
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %I:%M:%S%p",
)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def get_schools_and_apikeys(sql):
    """Retrieve schools and API keys from the data warehouse."""
    df = sql.query(f"SELECT * FROM custom.DeansList_APIConnection")
    df = df[df["Active"] == True]
    school_key_map = dict(zip(df["SchoolName"], df["APIKey"]))
    if SCHOOLS:
        school_key_map = {school: school_key_map[school] for school in SCHOOLS}
    return school_key_map


def refresh_incident_data(sql, api_key):
    """Get incidents from the API and refresh all incidents data."""
    incidents = API("v1", api_key).get("incidents")
    delete_matching_records(sql, "DeansList_Incidents", api_key)
    count = insert_new_incidents(sql, incidents, api_key)
    return incidents, count


def refresh_actions_data(sql, api_key, incidents):
    """Refresh all actions data using incidents raw data."""
    delete_current_nested_records(sql, api_key, "DeansList_Actions", "SourceID")
    count = insert_new_nested_records(sql, incidents, "Actions")
    return count


def refresh_penalties_data(sql, api_key, incidents):
    """Refresh all penalties data using incidents raw data."""
    delete_current_nested_records(sql, api_key, "DeansList_Penalties", "IncidentID")
    count = insert_new_nested_records(sql, incidents, "Penalties")
    return count


def delete_matching_records(sql, table_name, api_key):
    """Delete table rows that match the given API key."""
    table = sql.table(table_name)
    d = table.delete().where(table.c.SchoolAPIKey == api_key)
    sql.engine.execute(d)


def insert_new_incidents(sql, incidents, api_key):
    """Insert records into the Incidents table."""
    df = get_incidents_data(incidents, api_key)
    sql.insert_into("DeansList_Incidents", df, if_exists="append")
    count = len(df)
    logging.info(f"Inserted {count} records into DeansList_Incidents.")
    return count


def get_incidents_data(incidents, api_key):
    """Get the incidents data and add additional columns."""
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
        "Location",
        "LocationID",
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
    return df


def delete_current_nested_records(sql, api_key, table_name, incident_column):
    """Delete records for the given table (Actions or Penalties) that have a corresponding incident in the Incidents table."""
    incident_ids = sql.query(
        f"""SELECT DISTINCT t.{incident_column}
        FROM custom.{table_name} t
        LEFT JOIN custom.DeansList_Incidents r
            ON r.IncidentID = t.{incident_column}
        WHERE r.SchoolAPIKey='{api_key}'"""
    )
    incident_ids = incident_ids[incident_column].tolist()
    table = sql.table(table_name)
    for incident_id in incident_ids:
        d = table.delete().where(table.c[incident_column] == incident_id)
        sql.engine.execute(d)


def insert_new_nested_records(sql, incidents, record_type):
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
    return df


def refresh_behavior_data(sql, api_key):
    """Refresh behavior data for the current month.
    
    We limit to one month by default because the behaviors data is relatively large.
    If start & end date are passed in, then get refresh for this date range.
    """
    start_date = (
        BEHAVIOR_BACKFILL[0] if BEHAVIOR_BACKFILL else get_current_month_start()
    )
    end_date = BEHAVIOR_BACKFILL[1] if BEHAVIOR_BACKFILL else get_current_month_end()
    params = {"sdt": start_date, "edt": end_date}
    behaviors = API("beta", api_key).get("get-behavior-data", params)
    delete_behavior_records(sql, "DeansList_Behaviors", start_date, end_date, api_key)
    count_behaviors = insert_new_behaviors(sql, behaviors, api_key)
    return count_behaviors


def delete_behavior_records(sql, table_name, start_date, end_date, api_key):
    """Delete behavior table rows that match the date range and given API key."""
    table = sql.table(table_name)
    d = table.delete().where(
        (table.c.SchoolAPIKey == api_key)
        & (table.c.BehaviorDate >= start_date)
        & (table.c.BehaviorDate <= end_date)
    )
    sql.engine.execute(d)


def get_current_month_start():
    """Get the first day of the current month."""
    date = datetime.date.today().replace(day=1).strftime("%Y-%m-%d")
    return date


def get_current_month_end():
    """Get the last day of the current month."""
    today = datetime.date.today()
    month_length = calendar.monthrange(today.year, today.month)[1]
    date = today.replace(day=month_length).strftime("%Y-%m-%d")
    return date


def insert_new_behaviors(sql, behaviors, api_key):
    """Insert records into the Behaviors table."""
    df = parse_json_data(behaviors, api_key)
    sql.insert_into("DeansList_Behaviors", df, if_exists="append")
    count = len(df)
    logging.info(f"Inserted {count} records into DeansList_Behaviors.")
    return count


def parse_json_data(json, api_key):
    """Get the behavior data and add additional columns."""
    df = json_normalize(json["data"])
    df.columns = df.columns.str.replace(".", "_")
    df["SchoolAPIKey"] = api_key
    return df


def refresh_communications_data(sql, api_key):
    """Refresh communications data."""
    comms = API("beta", api_key).get("get-comm-data")
    delete_matching_records(sql, "DeansList_Communications", api_key)
    count_comms = insert_new_comunications(sql, comms, api_key)
    return count_comms


def insert_new_comunications(sql, comms, api_key):
    """Insert records into the Communications table."""
    df = parse_json_data(comms, api_key)
    sql.insert_into("DeansList_Communications", df, if_exists="append")
    count = len(df)
    logging.info(f"Inserted {count} records into DeansList_Communications.")
    return count


def main():
    try:
        mailer = Mailer()
        sql = MSSQL()
        school_apikey_map = get_schools_and_apikeys(sql)

        total_incidents = 0
        total_actions = 0
        total_penalties = 0
        total_comms = 0
        total_behaviors = 0

        for school, api_key in school_apikey_map.items():
            logging.info(f"Getting data for {school}.")

            if not BEHAVIOR_BACKFILL:
                incidents, count = refresh_incident_data(sql, api_key)
                total_incidents += count

                count = refresh_actions_data(sql, api_key, incidents)
                total_actions += count

                count = refresh_penalties_data(sql, api_key, incidents)
                total_penalties += count

                count = refresh_communications_data(sql, api_key)
                total_comms += count

            count = refresh_behavior_data(sql, api_key)
            total_behaviors += count

        logging.info(f"Updated {total_incidents} total records in DeansList_Incidents.")
        logging.info(f"Updated {total_actions} total records in DeansList_Actions.")
        logging.info(f"Updated {total_penalties} total records in DeansList_Penalties.")
        logging.info(
            f"Updated {total_comms} total records in DeansList_Communications."
        )
        logging.info(f"Updated {total_behaviors} total records in DeansList_Behaviors.")

        mailer.notify()
    except Exception as e:
        stack_trace = traceback.format_exc()
        logging.error(stack_trace)
        mailer.notify(success=False)


if __name__ == "__main__":
    main()
