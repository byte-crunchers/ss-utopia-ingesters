import os
import traceback

import jaydebeapi
from jaydebeapi import Error

import loan_ingester as li

# Environment Variables
mysql_pass = os.environ.get("MYSQL_PASS")
mysql_user = os.environ.get("MYSQL_USER")
mysql_jar = os.environ.get("MYSQL_JAR")
mysql_loc = os.environ.get("MYSQL_LOC")
# Relative Paths
script_dir = os.path.dirname(__file__)
schema_path = os.path.join(script_dir, "../sql/schema_mysql.sql")
csv_path = os.path.join(script_dir, "../dummy_data/loan_folder/fifty_loans.csv")
json_path = os.path.join(script_dir, "../dummy_data/loan_folder/fifty_loans.json")
xml_path = os.path.join(script_dir, "../dummy_data/loan_folder/fifty_loans.xml")
xlsx_path = os.path.join(script_dir, "../dummy_data/loan_folder/fifty_loans.xlsx")
xlsx_no_pk = os.path.join(script_dir, "../dummy_data/loan_folder/fifty_loans_nopk.xlsx")
xlsx_shifted = os.path.join(script_dir, "../dummy_data/loan_folder/fifty_loans_shifted.xlsx")


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
    # li.execute_scripts_from_file(schema_path, sql_conn)
    # li.read_file(csv_path, sql_conn)
    # li.read_file(json_path, sql_conn)
    # li.read_file(xml_path, sql_conn)
    # li.read_file(xlsx_path, sql_conn)
    # li.read_file(xlsx_shifted, sql_conn)
    li.read_file(xlsx_no_pk, sql_conn)
    sql_conn.commit()
    sql_conn.close()
