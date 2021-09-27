import os

import jaydebeapi
import pytest
from jaydebeapi import Error

import user_ingester as ui
from database_helper import execute_scripts_from_file, count_rows, clear_table
from user_ingester import read_file

script_dir = os.path.dirname(__file__)
schema_path = os.path.join(script_dir, "../sql/schema_h2.sql")
csv_path = os.path.join(script_dir, "../dummy_data/onethousand_users.csv")
json_path = os.path.join(script_dir, "../dummy_data/onethousand_users.json")
xml_path = os.path.join(script_dir, "../dummy_data/onethousand_users.xml")
xlsx_path = os.path.join(script_dir, "../dummy_data/onethousand_users.xlsx")
xlsx_no_pk = os.path.join(script_dir, "../dummy_data/onethousand_users_no_pk.xlsx")
xlsx_shifted = os.path.join(script_dir, "../dummy_data/onethousand_users_shifted.xlsx")

table = "users"


@pytest.fixture(scope="module", autouse=True)
def connect_h2():
    con_try = None
    try:
        con_try = jaydebeapi.connect("org.h2.Driver", "jdbc:h2:tcp://localhost/~/test;MODE=MySQL;database_to_upper=false",
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


def test_invalid_sql(connect_h2):
    execute_scripts_from_file("test", connect_h2)
    connect_h2.rollback()

# Test behavior on invalid file
def test_invalid_file(connect_h2):
    read_file("test", connect_h2)
    read_file(1, connect_h2)
    connect_h2.rollback()

# Test connection and create the schema
def test_create_schema(connect_h2):
    execute_scripts_from_file(schema_path, connect_h2)
    assert connect_h2
    connect_h2.rollback()

# Test parsing csv file and adding to database
def test_csv_ingest(connect_h2):
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    read_file(csv_path, connect_h2)
    assert (1000 == count_rows(table, connect_h2))
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    connect_h2.rollback()


# Test parsing json file and adding to database
def test_json_ingest(connect_h2):
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    read_file(json_path, connect_h2)
    assert (1000 == count_rows(table, connect_h2))
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    json_list = ui.parse_file_json(json_path)
    assert ("Bradley" == json_list[1].l_name)
    connect_h2.rollback()


def test_xml_ingest(connect_h2):
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    read_file(xml_path, connect_h2)
    assert (1000 == count_rows(table, connect_h2))
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    xml_list = ui.parse_file_xml(xml_path)
    assert ("mjimmy2015@dickson.net" == xml_list[1].email)
    connect_h2.rollback()


def test_xlsx_ingest(connect_h2):
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    read_file(xlsx_path, connect_h2)
    assert (1000 == count_rows(table, connect_h2))
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    read_file(xlsx_no_pk, connect_h2)
    assert (1000 == count_rows(table, connect_h2))
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    read_file(xlsx_shifted, connect_h2)
    assert (1000 == count_rows(table, connect_h2))
    norm_list = ui.parse_file_xlsx(xlsx_path)
    assert ("mildSwift2" == norm_list[0].user_name)
    nopk_list = ui.parse_file_xlsx(xlsx_no_pk)
    assert nopk_list[0].is_admin
    shifted_list = ui.parse_file_xlsx(xlsx_shifted)
    assert shifted_list[0].is_active
    connect_h2.rollback()


def test_bad_clear(connect_h2):
    clear_table("test", connect_h2)
    connect_h2.rollback()


if __name__ == '__main__':
    pytest.main()
