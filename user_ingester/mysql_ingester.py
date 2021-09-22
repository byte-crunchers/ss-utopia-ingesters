import json
import os
import traceback

import jaydebeapi
from jaydebeapi import Error

from database_helper import execute_scripts_from_file
from user_ingester import populate_users, csv_to_users, parse_file_json, parse_file_xml

script_dir = os.path.dirname(__file__)

sql_path = 'C:/Users/meeha/OneDrive/Desktop/SmoothStack/Data/mysql_key.json'
schema_path = os.path.join(script_dir, "../sql/schema_mysql.sql")
csv_path = os.path.join(script_dir, "../dummy_data/onethousand_users.csv")
json_path = os.path.join(script_dir, "../dummy_data/onethousand_users.json")
xml_path = os.path.join(script_dir, "../dummy_data/onethousand_users.xml")


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
    execute_scripts_from_file(schema_path, sql_conn)
    populate_users(csv_to_users(csv_path), sql_conn)
    populate_users(parse_file_json(json_path), sql_conn)
    populate_users(parse_file_xml(xml_path), sql_conn)
    sql_conn.commit()
    sql_conn.close()
