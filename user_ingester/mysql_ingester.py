import json

import mysql
from mysql.connector import Error
from user_ingester import populate_users
from csv_parse_user import csv_to_users
from database_helper import clear_table
from database_helper import count_rows

csv_path = "C:/Users/meeha/OneDrive/Desktop/SmoothStack/Data/onethousand_users.csv"


def connect_mysql():
    con_try = None
    try:
        f = open('C:/Users/meeha/OneDrive/Desktop/SmoothStack/Data/dbkey.json', 'r')
        key = json.load(f)
        con_try = mysql.connector.connect(user=key["user"], password=key["password"], host=key["host"],
                                          database=key["database"])
    except Error:
        print("There was a problem connecting to the database, please make sure the database information is correct!")
    if con_try.is_connected():
        return con_try
    else:
        print("There was a problem connecting to the database, please make sure the database information is correct!")
        print(Error)


if __name__ == '__main__':
    connection = connect_mysql()
    table = "users"
    populate_users(csv_to_users(csv_path), connection, table)
    print(count_rows(table, connection))
    clear_table(table, connection)
    connection.commit()
    connection.close()

