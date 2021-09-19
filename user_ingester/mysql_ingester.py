import json
import traceback

import jaydebeapi
import mysql
import os
from jaydebeapi import Error
from user_ingester import populate_users
from csv_parse_user import csv_to_users
from database_helper import clear_table
from database_helper import count_rows
from database_helper import execute_scripts_from_file

script_dir = os.path.dirname(__file__)

csv_path = "C:/Users/meeha/OneDrive/Desktop/SmoothStack/Data/onethousand_users.csv"
key_path = "C:/Users/meeha/OneDrive/Desktop/SmoothStack/Data/jdba_dbkey.json"
h2_path = 'C:/Users/meeha/OneDrive/Desktop/SmoothStack/Data/h2_univ.json'

# def connect_mysql():
#     con_try = None
#     try:
#         f = open('C:/Users/meeha/OneDrive/Desktop/SmoothStack/Data/dbkey.json', 'r')
#         key = json.load(f)
#         con_try = mysql.connector.connect(user=key["user"], password=key["password"], host=key["host"],
#                                           database=key["database"])
#     except Error:
#         print("There was a problem connecting to the database, please make sure the database information is correct!")
#     if con_try.is_connected():
#         return con_try
#     else:
#         print("There was a problem connecting to the database, please make sure the database information is correct!")
#         print(Error)


def connect(path):
    con_try = None
    try:
        f = open(path, 'r')
        key = json.load(f)
        con_try = jaydebeapi.connect(key["driver"], key["location"], key["login"], key["jar"])
    except Exception:
        traceback.print_exc()
        print("There was a problem connecting to the database, please make sure the database information is correct!")
    return con_try


if __name__ == '__main__':
    connection = connect(h2_path)
    execute_scripts_from_file(os.path.join(script_dir, "SQL/H2DropUsers.sql"), connection)
    execute_scripts_from_file(os.path.join(script_dir, "SQL/H2DropBranches.sql"), connection)
    execute_scripts_from_file(os.path.join(script_dir, "SQL/H2DropSchema.sql"), connection)
    table = "users"
    populate_users(csv_to_users(csv_path), connection, table)
    print(count_rows(table, connection))
    clear_table(table, connection)
    connection.commit()
    connection.close()

