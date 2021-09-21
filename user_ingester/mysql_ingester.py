import json
import os
import traceback

import jaydebeapi
from jaydebeapi import Error

from database_helper import execute_scripts_from_file
from user_ingester import populate_users, csv_to_users

script_dir = os.path.dirname(__file__)

csv_path = "C:/Users/meeha/OneDrive/Desktop/SmoothStack/Data/onethousand_users.csv"
h2_path = 'C:/Users/meeha/OneDrive/Desktop/SmoothStack/Data/h2_univ.json'
sql_path = 'C:/Users/meeha/OneDrive/Desktop/SmoothStack/Data/mysql_key.json'


def connect(path):
    con_try = None
    try:
        f = open(path, 'r')
        key = json.load(f)
        con_try = jaydebeapi.connect(key["driver"], key["location"], key["login"], key["jar"])
    except Error:
        traceback.print_exc()
        print("There was a problem connecting to the database, please make sure the database information is correct!")
    return con_try


if __name__ == '__main__':
    sql_conn = connect(sql_path)
    sql_conn.jconn.setAutoCommit(False)
    execute_scripts_from_file(os.path.join(script_dir, "../sql/schema_mysql.sql"),
                              sql_conn)
    populate_users(csv_to_users("../dummy_data/onethousand_users.csv"), sql_conn)
    sql_conn.commit()
