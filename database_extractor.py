from ald.database_interface import OpenDatabaseConnection, DatabaseInterface
import argparse
import configparser
from datetime import datetime
import dateutil.parser
import os
import sys


DB_CREDENTIALS_PATH = "credentials.conf"


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("-s",
                            "--start",
                            required=True,
                            help="Start time as isoformat string",
                            type=str)
    arg_parser.add_argument("-e",
                            "--end",
                            required=True,
                            help="End time as isoformat string",
                            type=str)
    arg_parser.add_argument("-f",
                            "--filebasename",
                            required=True,
                            help="Base name of the output files. E.g. 'test_' will generate files called 'test_flow.dat' etc.",
                            type=str)
    arg_parser.add_argument("-c",
                            "--credentials-file-path",
                            required=True,
                            help="Path to credentials file. This file (with python configparser format) "
                            "should contain a section called 'Credentials' with keys 'Hostname', "
                            "'Username', 'Password' and 'DatabaseName'.",
                            type=str)
    args = arg_parser.parse_args()

    start_date = dateutil.parser.parse(args.start)
    end_date = dateutil.parser.parse(args.end)
    if end_date < start_date:
        print("End date is before start date. Aborting...")
        return 1


    if not os.path.isfile(DB_CREDENTIALS_PATH):
        print("ERROR: No database credentials file found at expected path '{}'".format(DB_CREDENTIALS_PATH))
        return 2

    db_credentials = configparser.ConfigParser()
    try:
        db_credentials.read(DB_CREDENTIALS_PATH)
        section = db_credentials["Credentials"]
        db_hostname = section["Hostname"]
        db_username = section["Username"]
        db_password = section["Password"]
        db_name = section["DatabaseName"]
    except:
        print("ERROR: Failed to parse credentials file.\n"
              "Expected format:\n\n"
              "[Credentials]\n"
              "Hostname = myhost\n"
              "Username = mydbuser\n"
              "Password = mydbpassword\n"
              "DatabaseName = mydbname\n")
        return 3

    with OpenDatabaseConnection(db_hostname, db_username, db_password, db_name) as db:
        table_names_and_getters = [("flow", db.get_flow),
                                   ("pressure", db.get_pressure),
                                   ("sample_temperature", db.get_sample_temperature),
                                   ("temperature", db.get_temperature),
                                   ("valves", db.get_valves),
                                   ("process_log", db.get_process_log)]

        for (table_name, getter) in table_names_and_getters:
            full_path = "{}{}.dat".format(args.filebasename, table_name)
            dataframe = getter(start_date, end_date)
            dataframe.to_csv(full_path, sep=" ", index=False)


    return 0


if __name__ == "__main__":
    exit(main())

