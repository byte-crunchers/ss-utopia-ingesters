import os
import traceback

import jaydebeapi
from jaydebeapi import Error

import user_ingester as ui

# Environment Variables
mysql_pass = os.environ.get("MYSQL_PASS")
mysql_user = os.environ.get("MYSQL_USER")
mysql_jar = os.environ.get("MYSQL_JAR")
mysql_loc = os.environ.get("MYSQL_LOC")
# Relative Paths
script_dir = os.path.dirname(__file__)
schema_path = os.path.join(script_dir, "../sql/schema_mysql.sql")
csv_path = os.path.join(script_dir, "../dummy_data/onethousand_users.csv")
json_path = os.path.join(script_dir, "../dummy_data/onethousand_users.json")
xml_path = os.path.join(script_dir, "../dummy_data/onethousand_users.xml")
xlsx_path = os.path.join(script_dir, "../dummy_data/onethousand_users.xlsx")


def connect():
    con_try = None
    try:
        con_try = jaydebeapi.connect("com.mysql.cj.jdbc.Driver", mysql_loc,
                                     [mysql_user, mysql_pass], mysql_jar)
        con_try.jconn.setAutoCommit(False)
    except Error:
        traceback.print_exc()
        print("There was a problem connecting to the database, please make sure the database information is correct!")
    return con_try


if __name__ == '__main__':
    sql_conn = connect()
    ui.execute_scripts_from_file(schema_path, sql_conn)
    # ui.read_file(csv_path, sql_conn)
    # ui.read_file(json_path, sql_conn)
    # ui.read_file(xml_path, sql_conn)
    ui.read_file(xlsx_path, sql_conn)
    sql_conn.commit()
    sql_conn.close()
