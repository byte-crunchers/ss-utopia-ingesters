import csv
import json
import os
import traceback
import xml.etree.ElementTree
import xml.etree.ElementTree as eTree
from typing import List

import jaydebeapi
import mysql
from mysql.connector import Error
from openpyxl import load_workbook
from openpyxl import worksheet

mysql_pass = os.environ.get("MYSQL_PASS")
mysql_user = os.environ.get("MYSQL_USER")
mysql_jar = os.environ.get("MYSQL_JAR")


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
        try:
            # Checking if any of the incoming values are null or duplicate, if they are skip addition
            if usr.user_name and usr.user_name.strip() and usr.email and usr.email.strip() and usr.password \
                    and usr.password.strip() and usr.f_name and usr.f_name.strip() and usr.l_name and \
                    usr.l_name.strip() and str(usr.is_admin) and str(usr.is_admin).strip() and \
                    str(usr.is_active) and str(usr.is_active).strip():
                vals = (usr.user_name, usr.email, usr.password, usr.f_name, usr.l_name, usr.is_admin, usr.is_active)
                curs.execute(query, vals)
            else:
                raise Exception
            # Check for Duplicates and Nulls
        except (mysql.connector.errors.IntegrityError, jaydebeapi.DatabaseError, Exception):
            print("Duplicate User or Illegal Null: ", usr.to_string())
            print("Skipping addition..\n")


# Parse csv into users
def parse_file_csv(file):
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
                row_count += 1
            except (ValueError, IndexError):
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


def parse_table_xlsx(ws: worksheet, bounds: tuple) -> List:
    ret_list = []
    for row in ws.iter_rows(min_row=bounds[0], min_col=bounds[1], max_col=bounds[1] + 7):
        try:  # test to see if we've reached the end of our data
            if row[0].value is None:
                return ret_list
        except IndexError:
            return ret_list
        user = User(row[0].value, row[1].value, row[2].value, row[3].value, row[4].value, row[5].value, row[6].value)
        ret_list.append(user)
    return ret_list


def find_xlsx_bounds(ws: worksheet):
    num_of_fields = 7
    row_num = 0
    for row in ws.iter_rows(min_row=1, max_row=20):
        row_num += 1  # yes this is an odd pattern, but it lets me use the iterator and report back the row number
        for i in range(0, 10):
            try:
                if row[i].value == "id":  # header and primary key
                    return (row_num + 1,
                            i + 2)  # row_num is already 1 indexed, but we want to add one because we hit id. For
                    # column we add one for 0-index and one to get from id to accounts_id
                elif row[i].value == "username":
                    return row_num + 1, i + 1  # adjust row_num for headers and i for 0-index
                elif row[i].value != '' and row[i].value is not None:  # if we hit data
                    try:
                        # if the 9th col is empty we assume
                        if row[i + num_of_fields].value == '' or row[i + num_of_fields].value is None:
                            # it doesn't have primary key
                            return row_num, i + 1  # adjust i for 0-index
                        else:
                            return row_num, i + 2  # adjust i for 0-index and primary key
                    except:  # means there's probably not anything there
                        return row_num, i + 1  # adjust i for 0-index
            except:
                pass  # we don't care about an out of range here
    return None


def parse_file_xlsx(path: str) -> List:
    wb = load_workbook(filename=path, read_only=True)
    ws = None
    for sheet in wb:  # tries to find a sheet named accounts
        if sheet.title.lower() == "accounts":
            ws = sheet
    if ws is None:
        ws = wb.active  # gets the first sheet
    bounds = find_xlsx_bounds(ws)
    return parse_table_xlsx(ws, bounds)


# ENTRY POINT
def read_file(path, conn):
    try:
        ending = path.split('.')[-1].lower()
    except:
        print("Error finding file ending")
        return
    if ending == "json":
        acc = parse_file_json(path)
    elif ending == "xlsx":
        acc = parse_file_xlsx(path)
    elif ending == "csv":
        acc = parse_file_csv(path)
    elif ending == "xml":
        acc = parse_file_xml(path)
    else:
        print("Invalid file format")
        return
    populate_users(acc, conn)
