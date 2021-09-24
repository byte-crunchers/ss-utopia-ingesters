import pytest
import json
import jaydebeapi
import os
import transaction_ingester.transaction_ingester as ti

@pytest.fixture(scope="module", autouse=True)
def connect_h2():
    con = jaydebeapi.connect("org.h2.Driver", "jdbc:h2:tcp://localhost/~/test;MODE=MySQL", ["sa", ""], os.environ.get("H2") )
    con.cursor().execute("set schema bytecrunchers")
    con.jconn.setAutoCommit(False)
    return con


def test_parse_json_dict():
    transj = json.loads('{"id":1,"origin_account":85,"destination_account":89,"memo":"Religious interesting without majority.","transfer_value":1127.38,"time_stamp":"2021-09-23 10:44:51"}')
    trans : ti.Transaction = ti.parse_json_dict(transj)
    assert trans.destination == 89

def test_parse_file_json_bad_path():
    ret = ti.parse_file_json("dummy_data/nonexistant.json")
    assert ret == None

def test_parse_file_json():
    ret = ti.parse_file_json("dummy_data/transactions.json")
    assert ret[10]
    assert ret[0].destination == 89 

def test_parse_file_xml():
    ret = ti.parse_file_xml("dummy_data/transactions.xml")
    assert ret[10]
    assert int(ret[0].destination) == 89

def test_xlsx():
    ret = ti.parse_file_xlsx("dummy_data/transactions.xlsx")
    assert ret[10]
    assert ret[0].destination == 89

def test_csv():
    ret = ti.parse_file_csv("dummy_data/transactions.csv")
    assert ret[10]
    assert int(ret[0].destination) == 89

def test_write_transactions(connect_h2):
    trans = ti.Transaction()
    trans.origin=85
    trans.destination=89
    trans.memo="Religious interesting without majority."
    trans.value=1127.38
    trans.time_stamp="2021-09-23 10:44:51"
    trans_list = [trans]
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM transactions")
    ti.write_transactions(trans_list, connect_h2)
    curs.execute("SELECT * FROM transactions")
    ret = curs.fetchall()
    assert ret[0]
    assert ret[0][2] == 89 #destination_account
    connect_h2.rollback()

def test_read_file(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM transactions")
    ti.read_file("dummy_data/transactions.csv", connect_h2)
    curs.execute("SELECT * FROM transactions")
    ret =  curs.fetchall()
    assert ret[10] #asserts we have at least 10
    assert ret[0][2] == 89 #checks destination account for number 1
    connect_h2.rollback()
    




def test_parse_json_dict_cards():
    transj = json.loads('{"merchant_account_id":80,"card_num":4,"memo":"Religious interesting without majority.","transfer_value":1127.38,"time_stamp":"2021-09-23 10:44:51", "cvc1":123, "pin":1234, "cvc2":123, "location":"TX"}')
    trans : ti.Card_Transaction = ti.parse_json_dict_cards(transj)
    assert trans.acc == 80

def test_parse_file_json_bad_path_cards():
    ret = ti.parse_file_json_cards("dummy_data/nonexistant.json")
    assert ret == None

def test_parse_file_json_cards():
    ret = ti.parse_file_json_cards("dummy_data/card_transactions.json")
    assert ret[10]
    assert int(ret[0].acc) == 80

def test_parse_file_xml_cards():
    ret = ti.parse_file_xml_cards("dummy_data/card_transactions.xml")
    assert ret[10]
    assert int(ret[0].acc) == 80

def test_xlsx_cards():
    ret = ti.parse_file_xlsx_cards("dummy_data/card_transactions.xlsx")
    assert ret[10]
    assert int(ret[0].acc) == 80

def test_csv_cards():
    ret = ti.parse_file_csv_cards("dummy_data/card_transactions.csv")
    assert ret[10]
    assert int(ret[0].acc) == 80

def test_write_transactions_cards(connect_h2):
    trans = ti.Card_Transaction()
    trans.acc = 80
    trans.card=4319234901896186
    trans.memo="Religious interesting without majority."
    trans.value=1127.38
    trans.cvc2 = 123
    trans.time_stamp="2021-09-23 10:44:51"
    trans_list = [trans]
    curs = connect_h2.cursor()
    curs.execute("SET REFERENTIAL_INTEGRITY FALSE")
    curs.execute("DELETE FROM card_transactions")
    ti.write_transactions_cards(trans_list, connect_h2)
    curs.execute("SELECT * FROM card_transactions")
    ret = curs.fetchall()
    assert ret[0]
    assert ret[0][2] == 80 #merchant_account
    curs.execute("SET REFERENTIAL_INTEGRITY TRUE")
    connect_h2.rollback()

def test_read_file_cards(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM card_transactions")
    curs.execute("SET REFERENTIAL_INTEGRITY FALSE")
    ti.read_file_cards("dummy_data/card_transactions.csv", connect_h2)
    curs.execute("SELECT * FROM card_transactions")
    ret =  curs.fetchall()
    assert ret[10] #asserts we have at least 10
    assert ret[0][2] == 80 #checks destination account for number 1
    curs.execute("SET REFERENTIAL_INTEGRITY TRUE")
    connect_h2.rollback()
    