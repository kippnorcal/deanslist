import argparse
import calendar
from collections import Counter
import datetime
import logging
import sys
import traceback

import pandas as pd
from pandas.io.json import json_normalize
from sqlsorcery import MSSQL

from api import API
from datamap import incidents_fields, comms_fields, behaviors_fields
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


def count_and_log(df, entity):
    """Count the length of the dataframe and add a logger line."""
    count = len(df)
    logging.info(f"--Inserted {count} {entity} records.")
    return count


def get_schools_and_apikeys(sql):
    """Retrieve schools and API keys from the data warehouse."""
    df = sql.query(f"SELECT * FROM custom.DeansList_APIConnection")
    df = df[df["Active"] == True]
    school_key_map = dict(zip(df["SchoolName"], df["APIKey"]))
    if SCHOOLS:
        school_key_map = {school: school_key_map[school] for school in SCHOOLS}
    return school_key_map


def get_data_from_api(api_key, version, endpoint, params=None):
    """API get request and return the json response."""
    json_response = API(version, api_key).get(endpoint, params)
    return json_response


def refresh_db_table_data(
    sql, json, entity, fields, api_key, date_column=None, start=None, end=None
):
    """Truncate and reload the data in the database table for this school.
    
    date_column, start, and end are used only by Behavior endpoint."""
    delete_current_records(
        sql,
        f"DeansList_{entity}",
        api_key,
        date_column=date_column,
        start=start,
        end=end,
    )
    df = parse_json_data(json, api_key, fields)
    count = insert_df_into_table(sql, entity, df)
    return count


def delete_current_records(
    sql, table_name, api_key, date_column=None, start=None, end=None
):
    """Delete table rows that match the given API key."""
    table = sql.table(table_name)
    if not date_column:
        d = table.delete().where(table.c.SchoolAPIKey == api_key)
    else:
        # Only for Behaviors
        d = table.delete().where(
            (table.c.SchoolAPIKey == api_key)
            & (table.c[date_column] >= start)
            & (table.c[date_column] <= end)
        )
    sql.engine.execute(d)


def parse_json_data(json, api_key, fields):
    """Parse the json data and format columns."""
    df = json_normalize(json["data"])
    if len(df) > 0:
        df.columns = df.columns.str.replace(".", "_")
        df = df[fields]
        df["SchoolAPIKey"] = api_key
        # The following condition applies only to the Incidents table
        if "Actions" in df and "Penalties" in df:
            df = df.astype({"Actions": str, "Penalties": str})
    return df


def insert_df_into_table(sql, entity, df):
    """Insert data into the corresponding data warehouse table."""
    sql.insert_into(f"DeansList_{entity}", df, if_exists="append")
    count = count_and_log(df, entity)
    return count


def refresh_nested_table_data(sql, json, data_column, source_column_name, api_key):
    """Truncate and reload the data in the database table for this school.
    
    This is for nested records only. Currently only applies to Actions and
    Penalties, which are nested within the Incidents data."""
    delete_current_nested_records(
        sql, api_key, f"DeansList_{data_column}", source_column_name
    )
    df = parse_nested_json_data(json, data_column)
    count = insert_df_into_table(sql, data_column, df)
    return count


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


def parse_nested_json_data(incidents, column):
    """Get column data that is stored as a list of JSON objects."""
    data = []
    for record in incidents["data"]:
        if record[column]:
            data.extend(record[column])
    df = pd.DataFrame(data)
    return df


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


def main():
    try:
        mailer = Mailer()
        sql = MSSQL()
        school_apikey_map = get_schools_and_apikeys(sql)

        objects = ["Incidents", "Actions", "Penalties", "Communications", "Behaviors"]
        counter = Counter({obj: 0 for obj in objects})

        for school, api_key in school_apikey_map.items():
            logging.info(f"Getting data for {school}.")

            if not BEHAVIOR_BACKFILL:
                # INCIDENTS
                incidents = get_data_from_api(api_key, "v1", "incidents")
                count = refresh_db_table_data(
                    sql, incidents, "Incidents", incidents_fields, api_key
                )
                counter.update({"Incidents": count})

                # ACTIONS -- NESTED
                count = refresh_nested_table_data(
                    sql, incidents, "Actions", "SourceID", api_key
                )
                counter.update({"Actions": count})

                # PENALTIES -- NESTED
                count = refresh_nested_table_data(
                    sql, incidents, "Penalties", "IncidentID", api_key
                )
                counter.update({"Penalties": count})

                # COMMUNICATIONS
                comms = get_data_from_api(api_key, "beta", "get-comm-data")
                count = refresh_db_table_data(
                    sql, comms, "Communications", comms_fields, api_key
                )
                counter.update({"Communications": count})

            # BEHAVIORS
            start = (
                BEHAVIOR_BACKFILL[0] if BEHAVIOR_BACKFILL else get_current_month_start()
            )
            end = BEHAVIOR_BACKFILL[1] if BEHAVIOR_BACKFILL else get_current_month_end()
            params = {"sdt": start, "edt": end}
            behaviors = get_data_from_api(api_key, "beta", "get-behavior-data", params)
            count = refresh_db_table_data(
                sql,
                behaviors,
                "Behaviors",
                behaviors_fields,
                api_key,
                date_column="BehaviorDate",
                start=start,
                end=end,
            )
            counter.update({"Behaviors": count})

        for obj, count in counter.items():
            logging.info(f"Total {obj}: {count}")

        mailer.notify()
    except Exception as e:
        stack_trace = traceback.format_exc()
        logging.error(stack_trace)
        mailer.notify(success=False)


if __name__ == "__main__":
    main()
