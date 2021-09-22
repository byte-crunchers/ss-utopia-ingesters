#from account_ingester.account_ingester import Account
import pytest
import json
import jaydebeapi
import os
import account_ingester.account_ingester as ai

@pytest.fixture(scope="module", autouse=True)
def connect_h2():
    con = jaydebeapi.connect("org.h2.Driver", "jdbc:h2:tcp://localhost/~/test;MODE=MySQL", ["sa", ""], os.environ.get("H2") )
    con.cursor().execute("set schema bytecrunchers")
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

def test_parse_file_xml():
    ret = ai.parse_file_xml("dummy_data/accounts.xml")
    assert ret[1]
    assert ret[1].limit == None #checking account so no limit
    assert float(ret[0].limit) == -1498

def test_xlsx():
    ret = ai.parse_file_xlsx("dummy_data/accounts.xlsx")
    assert ret[1]
    assert ret[1].limit == None #checking account so no limit
    assert ret[0].limit == -1365

def test_csv():
    ret = ai.parse_file_csv("dummy_data/accounts.csv")
    assert ret[10]
    assert ret[10].balance

def test_write_accounts(connect_h2):
    acc = ai.Account()
    acc.user=1
    acc.account_type="Basic Credit"
    acc.active = 1
    acc.balance = -420
    acc.limit = -1498
    acc.payment_due = 44.26
    acc.due_date = "2021-09-19"
    acc.interest = 0.089
    accs = [acc]
    curs = connect_h2.cursor()
    curs.execute("SELECT * FROM accounts WHERE users_id = 1 AND balance = -420")
    ret = curs.fetchall()
    assert len(ret) == 0 #Make sure it's not already in the DB
    ai.write_accounts(accs, connect_h2)
    curs.execute("SELECT * FROM accounts WHERE users_id = 1 AND balance = -420")
    ret =  curs.fetchall()
    assert ret[0]
    assert abs(ret[0][4] - 44.26) < 0.001 #fp compare on the payment
    connect_h2.rollback()

def test_read_file(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM accounts")
    ai.read_file("dummy_data/accounts.xlsx", connect_h2)
    curs.execute("SELECT * FROM accounts")
    ret =  curs.fetchall()
    assert ret[1] #asserts more than one was added
    print (ret[0])
    assert ret[0][3] #make sure there is a balance
    connect_h2.rollback()
    