import os

import jaydebeapi
import pytest

import branch_ingester as bi
from branch_ingester import read_file
from database_helper import execute_scripts_from_file, count_rows, clear_table

script_dir = os.path.dirname(__file__)
schema_path = os.path.join(script_dir, "../sql/schema_h2.sql")
csv_path = os.path.join(script_dir, "../dummy_data/onethousand_branches.csv")
json_path = os.path.join(script_dir, "../dummy_data/onethousand_branches.json")
xml_path = os.path.join(script_dir, "../dummy_data/onethousand_branches.xml")
xlsx_path = os.path.join(script_dir, "../dummy_data/onethousand_branches.xlsx")
xlsx_shifted = os.path.join(script_dir, "../dummy_data/onethousand_branches_shifted.xlsx")
xlsx_no_pk = os.path.join(script_dir, "../dummy_data/onethousand_branches_no_pk.xlsx")
table = "branches"


@pytest.fixture(scope="module", autouse=True)
def connect_h2():
    con_try = None
    try:
        con_try = jaydebeapi.connect("org.h2.Driver",
                                     "jdbc:h2:tcp://localhost/~/test;MODE=MySQL;database_to_upper=false",
                                     ["sa", ""],
                                     os.environ.get('H2'))
        con_try.jconn.setAutoCommit(False)
        con_try.cursor().execute("set schema bytecrunchers")
    except jaydebeapi.Error:
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
    assert (count_rows(table, connect_h2) > 500)
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    csv_list = bi.parse_file_csv(csv_path)
    #assert ("3904 Cannon Ways,\nHawkinsfort, KS 66839" == csv_list[0].location)
    connect_h2.rollback()


# Test parsing json file and adding to database
def test_json_ingest(connect_h2):
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    read_file(json_path, connect_h2)
    assert (count_rows(table, connect_h2) > 500)
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    json_list = bi.parse_file_json(json_path)
    assert ("46132 Young Cape,\nWest Alexander, NJ 07206" == json_list[0].location)
    connect_h2.rollback()


def test_xml_ingest(connect_h2):
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    read_file(xml_path, connect_h2)
    assert (count_rows(table, connect_h2) > 500)
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    xml_list = bi.parse_file_xml(xml_path)
    assert ("3218 Brady Divide,\nNicholston, MD 21610" == xml_list[0].location)
    connect_h2.rollback()


def test_xlsx_ingest(connect_h2):
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    read_file(xlsx_path, connect_h2)
    assert (count_rows(table, connect_h2) > 500)
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    read_file(xlsx_no_pk, connect_h2)
    assert (count_rows(table, connect_h2) > 500)
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    read_file(xlsx_shifted, connect_h2)
    assert (count_rows(table, connect_h2) > 500)
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))


if __name__ == '__main__':
    pytest.main()
