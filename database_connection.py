import time
from datetime import date

import mysql.connector

#import option_greeks
from constants import DbIndex

host = 'localhost'
user = 'root'
password = ''

db_name = 'options'
table_name = 'greeks_data'
interest = 0.1

columns = ['instrument', 'symbol', 'expiry', 'strike', 'option_typ', 'open', 'high', 'low', 'close', 'settle_pr',
           'contracts', 'val', 'open_int', 'chg_in_oi', 'timestamp', 'spot', 'iv', 'theta', 'gamma', 'delta', 'vega', 'rho']


def _check_database():
    """
    This checks for the for the database present in the MySQL server. If not then it creates the database.
    :return: None
    """
    conn = mysql.connector.connect(host=host, user=user, password=password)
    cursor = conn.cursor()
    query = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '%s'" % db_name
    cursor.execute(query)
    result = cursor.fetchall()
    if len(result) == 0:
        query = "CREATE DATABASE IF NOT EXISTS %s" % db_name
        cursor.execute(query)
        print("Database created: %s" % db_name)
    else:
        print("Database already present")

    cursor.close()
    conn.close()


def _check_table(truncate: bool):
    """
    This checks for the table in the database. If not present then it creates the table.
    :param truncate:
    :return: None
    """
    db_conn = mysql.connector.connect(host=host, user=user, password=password, database=db_name)
    cursor = db_conn.cursor()
    query = "SELECT table_name FROM information_schema.tables WHERE table_name = '%s'" % table_name
    cursor.execute(query)
    result = cursor.fetchall()
    # print(result)
    if len(result) == 0:
        print("Table not found")
        query = "CREATE TABLE %s (INSTRUMENT varchar(8) ,SYMBOL varchar(30) ," \
                "EXPIRY_DT date,STRIKE_PR int, OPTION_TYP varchar(5), OPEN float, HIGH float, LOW float, CLOSE double, " \
                "SETTLE_PR float, CONTRACTS int, VAL_INLAKH float, OPEN_INT int, CHG_IN_OI int, TIMESTAMP date, " \
                " spot float, IV float, Theta float, Gamma float, Delta float, Vega float, Rho float)" % table_name

        cursor.execute(query)
        print("Table created: %s" % table_name)
    else:
        print("Table already present")
        if truncate:
            truncate = 'TRUNCATE TABLE %s' % table_name
            cursor.execute(truncate)
            print('Table Truncated')
    cursor.close()
    db_conn.close()


def insert_data(queries):
    """
    This is used to enter multiple queries into the database.
    :param queries: List[str]
            It should the a list of queries in str format which can be inserted directly in database.
    :return: None
    """
    db_conn = mysql.connector.connect(host=host, user=user, password=password, database=db_name)
    cursor = db_conn.cursor()
    for query in queries:
        cursor.execute(query)
    db_conn.commit()
    cursor.close()
    db_conn.close()


def bulk_entries(truncate: bool):
    """
    This is used to check resources before inserting bulk entries into the database
    :param truncate: bool
            If table is already present then truncate if True.
    :return: None
    """
    _check_database()
    _check_table(truncate)

def execute_simple_query(query):
    """
    This used to execute a MySQL query in the database.
    :param query: str
            MySQL query to be executed
    :return: None
    """
    start_time = time.time()
    db_conn = mysql.connector.connect(host=host, user=user, password=password, database=db_name)
    cursor = db_conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    db_conn.close()
    print("Query executed in: %s secs" % (time.time() - start_time))
    return result
