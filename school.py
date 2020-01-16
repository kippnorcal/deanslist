import logging

from pandas.io.json import json_normalize
import pandas as pd

from api import API


class School:
    """School object which executes the updates for each data table."""

    def __init__(self, api_key, sql, counter, start_date, end_date):
        self.api_key = api_key
        self.sql = sql
        self.counter = counter
        self.start_date = start_date
        self.end_date = end_date

    def get_data_from_api(self, version, endpoint):
        """API get request and return the json response.
        
        Only the behavior endpoint takes start/end date params."""
        params = None
        if endpoint == "get-behavior-data":
            params = {"sdt": self.start_date, "edt": self.end_date}
        json_response = API(version, self.api_key).get(endpoint, params)
        return json_response

    def _delete_current_records(self, entity):
        """Delete table rows that match the given API key."""
        table = self.sql.table(f"DeansList_{entity}")
        if entity != "Behaviors":
            d = table.delete().where(table.c.SchoolAPIKey == self.api_key)
        else:
            d = table.delete().where(
                (table.c.SchoolAPIKey == self.api_key)
                & (table.c["BehaviorDate"] >= self.start_date)
                & (table.c["BehaviorDate"] <= self.end_date)
            )
        self.sql.engine.execute(d)

    def _parse_json_data(self, json, columns):
        """Parse the json data and format columns."""
        df = json_normalize(json["data"])
        if len(df) > 0:
            df.columns = df.columns.str.replace(".", "_")
            df = df[columns]
            df["SchoolAPIKey"] = self.api_key
            # The following condition applies only to the Incidents table
            if ("Actions" in df) and ("Penalties" in df):
                df = df.astype({"Actions": str, "Penalties": str})
        return df

    def _count_and_log(self, df, entity):
        """Count the length of the dataframe and add a logger line."""
        count = len(df)
        logging.info(f"--Inserted {count} {entity} records.")
        return count

    def _insert_df_into_table(self, entity, df):
        """Insert data into the corresponding data warehouse table."""
        self.sql.insert_into(f"DeansList_{entity}", df, if_exists="append")
        count = self._count_and_log(df, entity)
        return count

    def refresh_data(self, json, entity, columns):
        """Truncate and reload the data in the database table for this school."""
        self._delete_current_records(entity)
        df = self._parse_json_data(json, columns)
        count = self._insert_df_into_table(entity, df)
        self.counter.update({entity: count})

    def _delete_current_nested_records(self, entity, incident_column):
        """Delete records for the given table (Actions or Penalties) 
        that have a corresponding incident in the Incidents table. """
        incident_ids = self.sql.query(
            f"""SELECT DISTINCT t.{incident_column}
            FROM custom.DeansList_{entity} t
            LEFT JOIN custom.DeansList_Incidents r
                ON r.IncidentID = t.{incident_column}
            WHERE r.SchoolAPIKey='{self.api_key}'"""
        )
        incident_ids = incident_ids[incident_column].tolist()
        table = self.sql.table(f"DeansList_{entity}")
        for incident_id in incident_ids:
            d = table.delete().where(table.c[incident_column] == incident_id)
            self.sql.engine.execute(d)

    def _parse_nested_json_data(self, incidents, column):
        """Get column data that is stored as a list of JSON objects."""
        data = []
        for record in incidents["data"]:
            if record[column]:
                data.extend(record[column])
        df = pd.DataFrame(data)
        return df

    def refresh_nested_table_data(self, json, entity, source_column_name):
        """Truncate and reload the data in the database table for this school.
        
        This is for nested records only. Currently only applies to Actions and
        Penalties, which are nested within the Incidents data."""
        self._delete_current_nested_records(entity, source_column_name)
        df = self._parse_nested_json_data(json, entity)
        count = self._insert_df_into_table(entity, df)
        self.counter.update({entity: count})
