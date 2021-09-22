import pytest
import json
import jaydebeapi
import account_ingester as ai


def connect_h2():
    con = jaydebeapi.connect("org.h2.Driver", "jdbc:h2:tcp://localhost/~/test;MODE=MySQL", ["sa", ""], os.environ.get("H2") )
    con.jconn.setAutoCommit(False)
    return con

def test_parse_json_dict():
    accj = json.loads('{"users_id":1, "account_type":"Basic Credit", "balance":-420.00, "payment_due":44.26, "due_date":"2021-09-19", "limit":-1498, "debt_interest":0.089, "active":1 }')
    acc : ai.Account = ai.parse_json_dict(accj)
    assert acc.limit == -1498

def test_parse_file_json_bad_path():
    ret = ai.parse_file_json("dummy_data/nonexistant.json")
    assert ret == None

def test_parse_file_json():
    ret = ai.parse_file_json("dummy_data/accounts_array.json")
    assert ret[1]
    assert ret[1].limit == None #checking account so no limit
    assert ret[0].limit == -1498

def test_xml():
    ret = ai.parse_file_xlsx("dummy_data/accounts.xlsx")
    assert ret[1]
    assert ret[1].limit == None #checking account so no limit
    assert ret[0].limit == -1365

#def test_parse_file_json():
