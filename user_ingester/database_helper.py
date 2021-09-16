import traceback

import jaydebeapi
from mysql.connector import Error
from mysql.connector import MySQLConnection


def clear_table(table, clear_conn):
    queries = []
    if isinstance(clear_conn, MySQLConnection):
        queries.append("SET SQL_SAFE_UPDATES = 0;")
        queries.append("delete from {} where 1 = 1;".format(table))
        queries.append("SET SQL_SAFE_UPDATES = 1;")
        queries.append("ALTER TABLE {} AUTO_INCREMENT = 0;".format(table))
    else:
        h2_query = "DELETE FROM {};".format(table)
        queries.append(h2_query)
    try:
        clear_curs = clear_conn.cursor()
        for q in queries:
            clear_curs.execute(q)
    except Error:
        traceback.print_exc()
        print("There was a problem clearing the user table!")
    clear_conn.commit()


# This returns the count of all rows in the table
def count_rows(table, count_conn):
    count_curs = count_conn.cursor()
    count_query = "select count(*) from {}".format(table)
    row_count = None
    try:
        count_curs.execute(count_query)
        row_count = count_curs.fetchall()[0][0]
    except Error:
        traceback.print_exc(
            print("There was a problem counting the rows")
        )
    return row_count


def execute_scripts_from_file(filename, conn):
    # Open and read the file as a single buffer
    try:
        fd = open(filename, 'r')
        sql_file = fd.read()
        fd.close()
    except IOError:
        traceback.print_exc()
    # all SQL commands (split on ';')
    sql_commands = sql_file.split(';')
    # Execute every command from the input file
    curs = conn.cursor()
    for command in sql_commands:
        # This will skip and report errors
        # For example, if the tables do not yet exist, this will skip over
        # the DROP TABLE commands
        try:
            curs.execute(command)
        except (jaydebeapi.OperationalError, jaydebeapi.DatabaseError, Exception):
            traceback.print_exc()
    conn.commit()
