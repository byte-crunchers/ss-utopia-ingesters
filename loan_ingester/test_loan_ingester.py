import os

import jaydebeapi
import pytest

import loan_ingester as li

script_dir = os.path.dirname(__file__)
schema_path = os.path.join(script_dir, "../sql/schema_h2.sql")
user_path = os.path.join(script_dir, "../sql/Insert_Users.sql")
type_path = os.path.join(script_dir, "../sql/Insert_LoanTypes.sql")
csv_path = os.path.join(script_dir, "../dummy_data/loan_folder/fifty_loans.csv")
json_path = os.path.join(script_dir, "../dummy_data/loan_folder/fifty_loans.json")
xml_path = os.path.join(script_dir, "../dummy_data/loan_folder/fifty_loans.xml")
xlsx_path = os.path.join(script_dir, "../dummy_data/loan_folder/fifty_loans.xlsx")
xlsx_no_pk = os.path.join(script_dir, "../dummy_data/loan_folder/fifty_loans_nopk.xlsx")
xlsx_shifted = os.path.join(script_dir, "../dummy_data/loan_folder/fifty_loans_shifted.xlsx")


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
    li.execute_scripts_from_file("test", connect_h2)
    connect_h2.rollback()


# Test behavior on invalid file
def test_invalid_file(connect_h2):
    li.read_file("test", connect_h2)
    li.read_file(1, connect_h2)
    connect_h2.rollback()


# Test connection and create the schema and load dependencies
def test_create_schema(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM users")
    curs.execute("DELETE FROM loan_types")
    li.execute_scripts_from_file(schema_path, connect_h2)
    assert connect_h2


# Test the ingesters behavior when
def test_bad_ingest(connect_h2):
    curs = connect_h2.cursor()
    li.read_file(csv_path, connect_h2)
    curs.execute("SELECT COUNT(*) FROM loans")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty


def test_populate_dependencies(connect_h2):
    li.execute_scripts_from_file(user_path, connect_h2)
    li.execute_scripts_from_file(type_path, connect_h2)
    curs = connect_h2.cursor()
    curs.execute("SELECT count(*) FROM users")
    assert (60 == curs.fetchall()[0][0])
    curs.execute("SELECT count(*) FROM loan_types")
    assert (3 == curs.fetchall()[0][0])

# Test parsing csv file and adding to database
def test_csv_ingest(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM loans")
    curs.execute("SELECT COUNT(*) FROM loans")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty
    li.read_file(csv_path, connect_h2)
    curs.execute("SELECT COUNT(*) FROM loans")
    assert (50 == curs.fetchmany(size=1)[0][0])  # Test that there are now 50 loans
    curs.execute("SELECT * FROM loans")
    loans = curs.fetchall()
    assert (0.07270 == loans[0][3])
    curs.execute("DELETE FROM loans")
    curs.execute("SELECT COUNT(*) FROM loans")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty


# Test parsing json file and adding to database
def test_json_ingest(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM loans")
    curs.execute("SELECT COUNT(*) FROM loans")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty
    li.read_file(json_path, connect_h2)
    curs.execute("SELECT COUNT(*) FROM loans")
    assert (50 == curs.fetchmany(size=1)[0][0])  # Test that there are now 50 loans
    curs.execute("SELECT * FROM loans")
    loans = curs.fetchall()
    assert (12 == loans[0][1])  # Test if users_id is functioning correctly
    curs.execute("DELETE FROM loans")
    curs.execute("SELECT COUNT(*) FROM loans")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty


# Test parsing xml file and adding to database
def test_xml_ingest(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM loans")
    curs.execute("SELECT COUNT(*) FROM loans")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty
    li.read_file(xml_path, connect_h2)
    curs.execute("SELECT COUNT(*) FROM loans")
    assert (50 == curs.fetchmany(size=1)[0][0])  # Test that there are now 50 loans
    curs.execute("SELECT * FROM loans")
    loans = curs.fetchall()
    assert (-344266.42 == loans[0][2])  # Test if balance is functioning correctly
    curs.execute("DELETE FROM loans")
    curs.execute("SELECT COUNT(*) FROM loans")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty


# Test parsing xlsx file and adding to database
def test_xlsx_ingest(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM loans")
    curs.execute("SELECT COUNT(*) FROM loans")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty
    li.read_file(xlsx_path, connect_h2)
    curs.execute("SELECT COUNT(*) FROM loans")
    assert (50 == curs.fetchmany(size=1)[0][0])  # Test that there are now 50 loans
    curs.execute("SELECT * FROM loans")
    loans = curs.fetchall()
    assert ("Morgage" == loans[0][6])  # Test if loan_type is functioning correctly
    curs.execute("DELETE FROM loans")
    curs.execute("SELECT COUNT(*) FROM loans")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty


# Test parsing shifted xlsx (start of table is not 0,0) file and adding to database
def test_xlsx_shifted_ingest(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM loans")
    curs.execute("SELECT COUNT(*) FROM loans")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty
    li.read_file(xlsx_shifted, connect_h2)
    curs.execute("SELECT COUNT(*) FROM loans")
    assert (50 == curs.fetchmany(size=1)[0][0])  # Test that there are now 50 loans
    curs.execute("SELECT * FROM loans")
    loans = curs.fetchall()
    assert ("2021-10-20 00:00:00" == loans[0][4])  # Test if loan_type is functioning correctly
    curs.execute("DELETE FROM loans")
    curs.execute("SELECT COUNT(*) FROM loans")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty


# Test parsing xlsx file with no primary key and adding to database
def test_xlsx_no_pk_ingest(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM loans")
    curs.execute("SELECT COUNT(*) FROM loans")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty
    li.read_file(xlsx_no_pk, connect_h2)
    curs.execute("SELECT COUNT(*) FROM loans")
    assert (50 == curs.fetchmany(size=1)[0][0])  # Test that there are now 50 loans
    curs.execute("SELECT * FROM loans")
    loans = curs.fetchall()
    assert (6151.03 == loans[0][5])  # Test if payment_due is functioning correctly
    curs.execute("DELETE FROM loans")
    curs.execute("SELECT COUNT(*) FROM loans")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty
    connect_h2.rollback()


if __name__ == '__main__':
    pytest.main()
