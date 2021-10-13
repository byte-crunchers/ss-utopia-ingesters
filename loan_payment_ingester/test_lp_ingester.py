import os

import jaydebeapi
import pytest

import loan_payment_ingester as lpi

# Environment Variables
mysql_pass = os.environ.get("MYSQL_PASS")
mysql_user = os.environ.get("MYSQL_USER")
mysql_jar = os.environ.get("MYSQL_JAR")
mysql_loc = os.environ.get("MYSQL_LOC")
# Relative Paths
script_dir = os.path.dirname(__file__)
schema_path = os.path.join(script_dir, "../sql/schema_h2.sql")
loan_path = os.path.join(script_dir, "../sql/Insert_Loans.sql")
account_path = os.path.join(script_dir, "../sql/Insert_Accounts.sql")
csv_path = os.path.join(script_dir, "../dummy_data/loan_payment_folder/one_hundred_loan_payments.csv")
json_path = os.path.join(script_dir, "../dummy_data/loan_payment_folder/one_hundred_loan_payments.json")
xml_path = os.path.join(script_dir, "../dummy_data/loan_payment_folder/one_hundred_loan_payments.xml")
xlsx_path = os.path.join(script_dir, "../dummy_data/loan_payment_folder/one_hundred_loan_payments.xlsx")
xlsx_shifted = os.path.join(script_dir, "../dummy_data/loan_payment_folder/one_hundred_loan_payments_shifted.xlsx")
xlsx_nopk = os.path.join(script_dir, "../dummy_data/loan_payment_folder/one_hundred_loan_payments_nopk.xlsx")


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
    lpi.execute_scripts_from_file("test", connect_h2)
    connect_h2.rollback()


# Test behavior on invalid file
def test_invalid_file(connect_h2):
    lpi.read_file("test", connect_h2)
    lpi.read_file(1, connect_h2)
    connect_h2.rollback()


# Test connection and create the schema
def test_create_schema(connect_h2):
    lpi.execute_scripts_from_file(schema_path, connect_h2)
    assert connect_h2


# Test populate dependencies
def test_populate_dependencies(connect_h2):
    lpi.execute_scripts_from_file(loan_path, connect_h2)
    lpi.execute_scripts_from_file(account_path, connect_h2)
    assert connect_h2


# Test parsing csv file and adding to database
def test_csv_ingest(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM loan_payments")
    lpi.read_file(csv_path, connect_h2)
    curs.execute("SELECT COUNT(*) FROM loan_payments")
    assert (100 == curs.fetchmany(size=1)[0][0])  # Test that there are now 1000 users
    curs.execute("SELECT * FROM loan_payments")
    users = curs.fetchall()
    assert (142 == users[0][1])
    curs.execute("DELETE FROM loan_payments")
    curs.execute("SELECT COUNT(*) FROM loan_payments")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty


# Test parsing json file and adding to database
def test_json_ingest(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM loan_payments")
    lpi.read_file(json_path, connect_h2)
    curs.execute("SELECT COUNT(*) FROM loan_payments")
    assert (100 == curs.fetchmany(size=1)[0][0])  # Test that there are now 1000 users
    curs.execute("SELECT * FROM loan_payments")
    users = curs.fetchall()
    assert (14 == users[0][2])
    curs.execute("DELETE FROM loan_payments")
    curs.execute("SELECT COUNT(*) FROM loan_payments")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty


# Test parsing xml file and adding to database
def test_xml_ingest(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM loan_payments")
    lpi.read_file(xml_path, connect_h2)
    curs.execute("SELECT COUNT(*) FROM loan_payments")
    assert (100 == curs.fetchmany(size=1)[0][0])  # Test that there are now 1000 users
    curs.execute("SELECT * FROM loan_payments")
    users = curs.fetchall()
    assert (950.45 == users[0][3])
    curs.execute("DELETE FROM loan_payments")
    curs.execute("SELECT COUNT(*) FROM loan_payments")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty


# Test parsing xlsx file and adding to database
def test_xlsx_ingest(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM loan_payments")
    lpi.read_file(xlsx_path, connect_h2)
    curs.execute("SELECT COUNT(*) FROM loan_payments")
    assert (100 == curs.fetchmany(size=1)[0][0])  # Test that there are now 1000 users
    curs.execute("SELECT * FROM loan_payments")
    users = curs.fetchall()
    assert (0 == users[0][5])
    curs.execute("DELETE FROM loan_payments")
    curs.execute("SELECT COUNT(*) FROM loan_payments")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty
    # Test Shifted xlsx file
    curs.execute("DELETE FROM loan_payments")
    lpi.read_file(xlsx_shifted, connect_h2)
    curs.execute("SELECT COUNT(*) FROM loan_payments")
    assert (100 == curs.fetchmany(size=1)[0][0])  # Test that there are now 1000 users
    curs.execute("SELECT * FROM loan_payments")
    users = curs.fetchall()
    assert (1340.13 == users[0][3])
    curs.execute("DELETE FROM loan_payments")
    curs.execute("SELECT COUNT(*) FROM loan_payments")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty
    # Test xlsx file with no pk
    curs.execute("DELETE FROM loan_payments")
    lpi.read_file(xlsx_nopk, connect_h2)
    curs.execute("SELECT COUNT(*) FROM loan_payments")
    assert (100 == curs.fetchmany(size=1)[0][0])  # Test that there are now 1000 users
    curs.execute("SELECT * FROM loan_payments")
    users = curs.fetchall()
    assert (52 == users[0][1])
    curs.execute("DELETE FROM loan_payments")
    curs.execute("SELECT COUNT(*) FROM loan_payments")
    assert (0 == curs.fetchmany(size=1)[0][0])  # Test that the table is empty


if __name__ == '__main__':
    pytest.main()
