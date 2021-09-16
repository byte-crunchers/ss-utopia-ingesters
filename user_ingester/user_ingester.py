# !!!!!!IMPORTANT!!!!!!! You must enter the path of a json file on your local machine that contains the database info
# This method connects to a local MySQL data base
import traceback

import jaydebeapi
import mysql
from mysql.connector import Error


def populate_users(user_data, ing_conn, ing_table):
    curs = ing_conn.cursor()
    for user in user_data:
        query = "INSERT INTO {}(username, email, password, first_name, last_name, is_admin) VALUES('{}', '{}', '{}', " \
                "'{}', '{}', {}) ".format(ing_table, user.user_name, user.email, user.password, user.f_name, user.l_name,
                                          user.is_admin)
        try:
            curs.execute(query)
        except (mysql.connector.errors.IntegrityError, jaydebeapi.DatabaseError):  # Check for Duplicates
            print("Duplicate user: ", user.to_string())
            print("Skipping addition..\n")
