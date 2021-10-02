import os
import traceback

import jaydebeapi
from jaydebeapi import Error

from branch_ingester import read_file, parse_file_xlsx, execute_scripts_from_file


# Environment Variables
mysql_pass = os.environ.get("MYSQL_PASS")
mysql_user = os.environ.get("MYSQL_USER")
mysql_jar = os.environ.get("MYSQL_JAR")
mysql_loc = os.environ.get("MYSQL_LOC")
# Relative Paths
script_dir = os.path.dirname(__file__)
schema_path = os.path.join(script_dir, "../sql/schema_mysql.sql")
csv_path = os.path.join(script_dir, "../dummy_data/onethousand_branches.csv")
json_path = os.path.join(script_dir, "../dummy_data/onethousand_branches.json")
xml_path = os.path.join(script_dir, "../dummy_data/onethousand_branches.xml")
xlsx_path = os.path.join(script_dir, "../dummy_data/onethousand_branches.xlsx")


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
    execute_scripts_from_file(schema_path, sql_conn)
    read_file(csv_path, sql_conn)
    # read_file(json_path, sql_conn)
    # read_file(xml_path, sql_conn)
    # read_file(xlsx_path, sql_conn)
    # branches = parse_file_xlsx(xlsx_path)
    sql_conn.commit()
    sql_conn.close()
