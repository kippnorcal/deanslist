import argparse
import calendar
from collections import Counter
import datetime
import logging
import sys
import traceback

from sqlsorcery import MSSQL

from datamap import incidents_columns, comms_columns, behaviors_columns
from mailer import Mailer
from school import School


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
        start = BEHAVIOR_BACKFILL[0] if BEHAVIOR_BACKFILL else get_current_month_start()
        end = BEHAVIOR_BACKFILL[1] if BEHAVIOR_BACKFILL else get_current_month_end()

        for school, api_key in school_apikey_map.items():
            logging.info(f"Getting data for {school}.")
            school = School(api_key, sql, counter, start, end)
            if not BEHAVIOR_BACKFILL:
                # Incidents
                incidents = school.get_data_from_api("v1", "incidents")
                school.refresh_data(incidents, "Incidents", incidents_columns)
                # Actions
                school.refresh_nested_table_data(incidents, "Actions", "SourceID")
                # Penalties
                school.refresh_nested_table_data(incidents, "Penalties", "IncidentID")
                # Comms
                comms = school.get_data_from_api("beta", "get-comm-data")
                school.refresh_data(comms, "Communications", comms_columns)
            # Behaviors
            behaviors = school.get_data_from_api("beta", "get-behavior-data")
            school.refresh_data(behaviors, "Behaviors", behaviors_columns)
            counter = school.counter

        for obj, count in counter.items():
            logging.info(f"Total {obj}: {count}")

        mailer.notify()
    except Exception as e:
        stack_trace = traceback.format_exc()
        logging.error(stack_trace)
        mailer.notify(success=False)


if __name__ == "__main__":
    main()
