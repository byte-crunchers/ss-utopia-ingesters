import json
import os
import traceback

import jaydebeapi
import pytest
from jaydebeapi import Error

from database_helper import execute_scripts_from_file

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


def test_create_schema(connect_h2):
    execute_scripts_from_file(os.path.join(script_dir, "SQL/schema_h2.sql"),
                              connect_h2)


if __name__ == '__main__':
    pytest.main()
