#script for demoing. Connects to the mysql db and uses the CSVs from dummy_data
import account_ingester.account_ingester as ai
import user_ingester.user_ingester as ui
import branch_ingester.branch_ingester as bi
import card_ingester.card_ingester as ci
import stock_ingester.stock_ingester as si
import transaction_ingester.transaction_ingester as ti
from sys import argv


if __name__ == "__main__":
    if not len(argv) > 1: #must have an argument
        print ("Please specify at least one script to demo\n\
            Available ingesters are 'user', 'branch', 'account', 'card',\n\
            'transaction', and 'card_transaction'\n")
    else:
        conn = ti.connect_mysql('dbkey.json')
        for i in range(1, len(argv)):
            if argv[i] == 'user':
                print("demoing user ingester...")
                ui.read_file('dummy_data/onethousand_users.csv', conn)
            elif argv[i] == 'branch':
                print("demoing branch ingester...")
                bi.read_file('dummy_data/onethousand_branches.csv', conn)
            elif argv[i] == 'account':
                print("demoing account ingester...")
                bi.read_file('dummy_data/accounts.csv', conn)
            elif argv[i] == 'card':
                print("demoing card ingester...")
                ci.read_file('dummy_data/cards.csv', conn)
            elif argv[i] == 'transaction':
                print("demoing transaction ingester...")
                ti.read_file('dummy_data/transactions.csv', conn)
            elif argv[i] == 'card_transaction':
                print("demoing card transaction ingester...")
                ti.read_file_cards('dummy_data/card_transactions.csv', conn)
            else:
                print("Invalid argument {}\n\
                    Available ingesters are 'user', 'branch', 'account', 'card',\n\
                    'transaction', and 'card_transaction'\n".format(argv[i]))
        conn.commit()
        conn.close()
        print("All done :)")
