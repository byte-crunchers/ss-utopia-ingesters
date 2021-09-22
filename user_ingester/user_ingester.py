import csv
import json
import traceback
import xml.etree.ElementTree
import xml.etree.ElementTree as eTree
from typing import List

import jaydebeapi
import mysql
from mysql.connector import Error


class User:
    def __init__(self, user_name, email, password, f_name, l_name, is_admin, is_active):
        self.user_name = user_name
        self.email = email
        self.password = password
        self.f_name = f_name
        self.l_name = l_name
        self.is_admin = is_admin
        self.is_active = is_active

    def to_string(self):
        return self.user_name, self.email, self.password, self.f_name, self.l_name, str(
            self.is_admin), str(self.is_active)


def populate_users(user_data, ing_conn):
    curs = ing_conn.cursor()
    query = "INSERT INTO users(username, email, password, first_name, last_name, is_admin, active) VALUES(?,?,?,?,?," \
            "?,?) "
    for usr in user_data:
        vals = (usr.user_name, usr.email, usr.password, usr.f_name, usr.l_name, usr.is_admin, usr.is_active)
        try:
            curs.execute(query, vals)
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
            try:
                user_list.append(
                    User(str(row[1]), str(row[2]), str(row[3]), str(row[4]), str(row[5]),
                         str(row[6]), str(row[7])))
            except ValueError:
                print("Could not add user on line " + str(row_count) + ": " + str(row))
                print("Skipping line...\n")
                continue
    return user_list


def parse_json_dict(json_dict: dict) -> User:
    json_user = User(json_dict["username"], json_dict["email"], json_dict["password"],
                     json_dict["first_name"], json_dict["last_name"], json_dict["is_admin"], json_dict["active"])
    return json_user


def parse_file_json(path) -> List:
    f = None
    try:
        f = open(path, "r")
    except IOError:
        print("error opening file")
    json_list = json.load(f)
    return_list = []
    for json_dict in json_list:
        return_list.append(parse_json_dict(json_dict))
    return return_list


def parse_file_xml(path):
    xml_user_list = []
    try:
        tree = eTree.parse(path)
        root = tree.getroot()
        for child in root:
            try:
                xml_user_list.append(
                    User(child.find('username').text, child.find('email').text,
                         child.find('password').text, child.find('first_name').text, child.find('last_name').text,
                         child.find('is_admin').text, child.find('active').text))
            except ValueError:
                print("Could not add user:" + child.find('id').text)
                print("Skipping line...\n")
    except xml.etree.ElementTree.ParseError:
        traceback.print_exc()
    return xml_user_list
