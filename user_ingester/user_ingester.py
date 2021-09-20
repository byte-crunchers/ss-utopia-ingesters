import csv

import jaydebeapi
import mysql
from mysql.connector import Error

path = "C:/Users/meeha/OneDrive/Desktop/SmoothStack/Data/onethousand_users.csv"


class User:
    def __init__(self, user_id, user_name, email, password, f_name, l_name, is_admin):
        self.user_id = user_id
        self.user_name = user_name
        self.email = email
        self.password = password
        self.f_name = f_name
        self.l_name = l_name
        self.is_admin = is_admin

    def to_string(self):
        return str(
            str(self.user_id) + ", " + self.user_name + ", " + self.email + ", " + self.password + ", " + self.f_name +
            ", " + self.l_name + ", " + self.is_admin)


def populate_users(user_data, ing_conn, ing_table):
    curs = ing_conn.cursor()
    for usr in user_data:
        query = "INSERT INTO {}(username, email, password, first_name, last_name, is_admin) VALUES('{}', '{}', '{}', " \
                "'{}', '{}', {}) ".format(ing_table, usr.user_name, usr.email, usr.password, usr.f_name,
                                          usr.l_name,
                                          usr.is_admin)
        try:
            curs.execute(query)
        except (mysql.connector.errors.IntegrityError, jaydebeapi.DatabaseError):  # Check for Duplicates
            print("Duplicate user: ", usr.to_string())
            print("Skipping addition..\n")


# Parse csv into users
def csv_to_users(file):
    user_list = []
    with open(file, newline="") as user_file:
        reader = csv.reader(user_file, delimiter=',')
        row_count = 0
        for row in reader:
            # Functionality for ingesting users
            if len(row) == 7:
                try:
                    user_list.append(
                        User(int(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]), str(row[5]),
                             str(row[6])))
                except ValueError:
                    print("Could not add user on line " + str(row_count) + ": " + str(row))
                    print("Skipping line...\n")
                    continue
    return user_list


if __name__ == '__main__':
    users = csv_to_users("DummyData/onethousand_users.csv")
    for user in users:
        print(user.user_name)
