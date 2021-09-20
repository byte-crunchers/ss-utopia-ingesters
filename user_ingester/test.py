import json
import os
import traceback

import jaydebeapi
import pytest
from jaydebeapi import Error

from database_helper import execute_scripts_from_file, count_rows, clear_table
from user_ingester import populate_users, csv_to_users

h2_path = 'C:/Users/meeha/OneDrive/Desktop/SmoothStack/Data/h2_univ.json'
table = "users"
script_dir = os.path.dirname(__file__)


@pytest.fixture(scope="module", autouse=True)
def connect_h2():
    con_try = None
    try:
        f = open(h2_path, 'r')
        key = json.load(f)
        con_try = jaydebeapi.connect(key["driver"], key["location"], key["login"], key["jar"])
    except Error:
        traceback.print_exc()
        print("There was a problem connecting to the database, please make sure the database information is correct!")
    return con_try


# Test connection and create the schema
def test_create_schema(connect_h2):
    execute_scripts_from_file(os.path.join(script_dir, "SQL/schema_h2.sql"),
                              connect_h2)
    assert connect_h2


def test_csv_ingest(connect_h2):
    assert (0 == count_rows(table, connect_h2))
    populate_users(csv_to_users(os.path.join(script_dir, "DummyData/onethousand_users.csv")), connect_h2, table)
    assert (1000 == count_rows(table, connect_h2))
    clear_table(table, connect_h2)
    assert (0 == count_rows(table, connect_h2))
    connect_h2.rollback()


if __name__ == '__main__':
    pytest.main()
