import json
import os
import traceback

import sys
import jaydebeapi
import pytest
from jaydebeapi import Error

from user_ingester import populate_users, csv_to_users
from database_helper import execute_scripts_from_file, count_rows, clear_table

sys.path.insert(0, 'ByteCrunchers/ss-utopia-ingesters/helpers')

h2_path = 'C:/Users/meeha/OneDrive/Desktop/SmoothStack/Data/h2_univ.json'
table = "users"
script_dir = os.path.dirname(__file__)


@pytest.fixture(scope="module", autouse=True)
def connect_h2():
    con_try = None
    try:
        con_try = jaydebeapi.connect("org.h2.Driver", "jdbc:h2:file:~/test;MODE=MySQL;database_to_upper=false", ["sa", ""],
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
    execute_scripts_from_file(os.path.join(script_dir, "../sql/schema_h2.sql"),
                              connect_h2)
    assert connect_h2


def test_csv_ingest(connect_h2):
    assert (0 == count_rows(table, connect_h2))
    populate_users(csv_to_users(os.path.join(script_dir, "../dummy_data/onethousand_users.csv")), connect_h2)
    assert (1000 == count_rows(table, connect_h2))
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    connect_h2.rollback()


if __name__ == '__main__':
    pytest.main()
