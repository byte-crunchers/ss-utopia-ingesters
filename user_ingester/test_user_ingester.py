import os

import jaydebeapi
import pytest
from jaydebeapi import Error

from database_helper import execute_scripts_from_file, count_rows, clear_table
from user_ingester import populate_users, csv_to_users, parse_file_json, parse_file_xml

script_dir = os.path.dirname(__file__)
schema_path = os.path.join(script_dir, "../sql/schema_h2.sql")
csv_path = os.path.join(script_dir, "../dummy_data/onethousand_users.csv")
json_path = os.path.join(script_dir, "../dummy_data/onethousand_users.json")
xml_path = os.path.join(script_dir, "../dummy_data/onethousand_users.xml")
table = "users"


@pytest.fixture(scope="module", autouse=True)
def connect_h2():
    con_try = None
    try:
        con_try = jaydebeapi.connect("org.h2.Driver", "jdbc:h2:file:~/test;MODE=MySQL;database_to_upper=false",
                                     ["sa", ""],
                                     os.environ.get('H2'))
        con_try.jconn.setAutoCommit(False)
        con_try.cursor().execute("set schema bytecrunchers")
    except Error:
        print("There was a problem connecting to the database, please make sure the database information is correct!")
    if not isinstance(con_try, jaydebeapi.Connection):
        print("There was a problem connecting to the database, please make sure the database information is correct!")
    else:
        return con_try


# Test connection and create the schema
def test_create_schema(connect_h2):
    execute_scripts_from_file(schema_path, connect_h2)
    assert connect_h2


# Test parsing csv file and adding to database
def test_csv_ingest(connect_h2):
    assert (0 == count_rows(table, connect_h2))
    populate_users(csv_to_users(csv_path), connect_h2)
    assert (1000 == count_rows(table, connect_h2))
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    connect_h2.rollback()


# Test parsing json file and adding to database
def test_json_ingest(connect_h2):
    assert (0 == count_rows(table, connect_h2))
    populate_users(parse_file_json(json_path), connect_h2)
    assert (1000 == count_rows(table, connect_h2))
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    connect_h2.rollback()


def test_xml_ingest(connect_h2):
    assert (0 == count_rows(table, connect_h2))
    populate_users(parse_file_xml(xml_path), connect_h2)
    assert (1000 == count_rows(table, connect_h2))
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    connect_h2.rollback()


if __name__ == '__main__':
    pytest.main()
