import os

import jaydebeapi
import pytest

from branch_ingester import read_file, execute_scripts_from_file

script_dir = os.path.dirname(__file__)
schema_path = os.path.join(script_dir, "../sql/schema_h2.sql")
csv_path = os.path.join(script_dir, "../dummy_data/onethousand_branches.csv")
json_path = os.path.join(script_dir, "../dummy_data/onethousand_branches.json")
xml_path = os.path.join(script_dir, "../dummy_data/onethousand_branches.xml")
xlsx_path = os.path.join(script_dir, "../dummy_data/onethousand_branches.xlsx")
xlsx_shifted = os.path.join(script_dir, "../dummy_data/onethousand_branches_shifted.xlsx")
xlsx_no_pk = os.path.join(script_dir, "../dummy_data/onethousand_branches_no_pk.xlsx")


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
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM branches")
    curs.execute("SELECT COUNT(*) FROM branches")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty
    read_file(csv_path, connect_h2)
    curs.execute("SELECT COUNT(*) FROM branches")
    assert (1000 == curs.fetchmany(size=1)[0][0])  # Test that there are now 1000 users
    curs.execute("SELECT * FROM branches")
    branches = curs.fetchall()
    assert ("78625 Aaron Rapids\r\n" == branches[0][1])
    curs.execute("DELETE FROM branches")
    curs.execute("SELECT COUNT(*) FROM branches")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty
    connect_h2.rollback()


# Test parsing json file and adding to database
def test_json_ingest(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM branches")
    curs.execute("SELECT COUNT(*) FROM branches")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty
    read_file(json_path, connect_h2)
    curs.execute("SELECT COUNT(*) FROM branches")
    assert (1000 == curs.fetchmany(size=1)[0][0])  # Test that there are now 1000 users
    curs.execute("SELECT * FROM branches")
    branches = curs.fetchall()
    assert ("304 Lewis Island\n" == branches[0][1])
    curs.execute("DELETE FROM branches")
    curs.execute("SELECT COUNT(*) FROM branches")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty
    connect_h2.rollback()


def test_xml_ingest(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM branches")
    curs.execute("SELECT COUNT(*) FROM branches")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty
    read_file(xml_path, connect_h2)
    curs.execute("SELECT COUNT(*) FROM branches")
    assert (1000 == curs.fetchmany(size=1)[0][0])  # Test that there are now 1000 users
    curs.execute("SELECT * FROM branches")
    branches = curs.fetchall()
    assert ("3598 Williams Mills\n" == branches[0][1])
    curs.execute("DELETE FROM branches")
    curs.execute("SELECT COUNT(*) FROM branches")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty
    connect_h2.rollback()


def test_xlsx_ingest(connect_h2):
    # Test reading normal xlsx
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM branches")
    curs.execute("SELECT COUNT(*) FROM branches")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty
    read_file(xlsx_path, connect_h2)  # Ingest branches
    curs.execute("SELECT COUNT(*) FROM branches")
    assert (1000 == curs.fetchmany(size=1)[0][0])  # Test that there are now 1000 users
    curs.execute("SELECT * FROM branches")
    branches = curs.fetchall()
    assert ("304 Lewis Island\n" == branches[0][1])
    curs.execute("DELETE FROM branches")
    curs.execute("SELECT COUNT(*) FROM branches")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty
    # Test reading shifted xlsx
    read_file(xlsx_shifted, connect_h2)  # Ingest branches
    curs.execute("SELECT COUNT(*) FROM branches")
    assert (1000 == curs.fetchmany(size=1)[0][0])  # Test that there are now 1000 users
    curs.execute("SELECT * FROM branches")
    branches = curs.fetchall()
    assert ("89360 Brent Circle\n" == branches[0][1])
    curs.execute("DELETE FROM branches")
    curs.execute("SELECT COUNT(*) FROM branches")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty
    # Test reading xlsx with no primary key
    read_file(xlsx_no_pk, connect_h2)  # Ingest branches
    curs.execute("SELECT COUNT(*) FROM branches")
    assert (1000 == curs.fetchmany(size=1)[0][0])  # Test that there are now 1000 users
    curs.execute("SELECT * FROM branches")
    branches = curs.fetchall()
    assert ("68645 Teresa Wells\n" == branches[0][1])
    curs.execute("DELETE FROM branches")
    curs.execute("SELECT COUNT(*) FROM branches")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty
    connect_h2.rollback()


if __name__ == '__main__':
    pytest.main()
