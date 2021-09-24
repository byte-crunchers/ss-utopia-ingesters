import pytest
import json
import jaydebeapi
import os
import card_ingester.card_ingester as ci

@pytest.fixture(scope="module", autouse=True)
def connect_h2():
    con = jaydebeapi.connect("org.h2.Driver", "jdbc:h2:tcp://localhost/~/test;MODE=MySQL", ["sa", ""], os.environ.get("H2") )
    con.cursor().execute("set schema bytecrunchers")
    con.jconn.setAutoCommit(False)
    return con


def test_parse_json_dict():
    cardj = json.loads('{"accounts_id":1,"card_num":1234567890123456,"pin":1234,"cvc1":420,"cvc2":421,"exp_date":"2021-09-19"}')
    card : ci.Card = ci.parse_json_dict(cardj)
    assert int(card.pin) == 1234

def test_parse_file_json_bad_path():
    ret = ci.parse_file_json("dummy_data/nonexistant.json")
    assert ret == None

def test_parse_file_json():
    ret = ci.parse_file_json("dummy_data/cards.json")
    assert ret[3]
    assert ret[0].pin == None or ret[0].pin == '' #this card is known to have no pin
    assert int(ret[3].pin) == 5178

def test_parse_file_xml():
    ret = ci.parse_file_xml("dummy_data/cards.xml")
    assert ret[3]
    assert ret[0].pin == None or ret[0].pin == '' #this card is known to have no pin
    assert int(ret[3].pin) == 5178

def test_xlsx():
    ret = ci.parse_file_xlsx("dummy_data/cards.xlsx")
    assert ret[3]
    assert ret[0].pin == None or ret[0].pin == '' #this card is known to have no pin
    assert ret[3].pin == 5178

def test_csv():
    ret = ci.parse_file_csv("dummy_data/cards.csv")
    assert ret[3]
    assert ret[0].pin == None or ret[0].pin == '' #this card is known to have no pin
    assert int(ret[3].pin) == 5178

def test_write_cards(connect_h2):
    card = ci.Card()

    card.acc=1
    card.num=1234567890123456
    card.pin = None
    card.cvc1 = 420
    card.cvc2 = 421
    card.exp = "2021-09-19"
    cards = [card]
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM cards")
    ci.write_cards(cards, connect_h2)
    curs.execute("SELECT * FROM cards WHERE card_num = 1234567890123456")
    ret =  curs.fetchall()
    assert ret[0]
    assert ret[0][3] == 420 #cvc1
    connect_h2.rollback()

def test_read_file(connect_h2):
    curs = connect_h2.cursor()
    curs.execute("DELETE FROM cards")
    ci.read_file("dummy_data/cards.xlsx", connect_h2)
    curs.execute("SELECT * FROM cards")
    ret =  curs.fetchall()
    assert ret[1] #asserts more than one was added
    print (ret[0])
    assert ret[0][3] #make sure there is a cvc1
    connect_h2.rollback()
    