import pandas as pd
import sqlite3
from sqlite3 import Error



statement = """SELECT id_account AS id_person, 
strftime('%m.%Y', transaction_date) AS month, 
ifnull(count(1),0) AS sum_of_transactions
FROM transactions
WHERE id_account in (345, 1234)
AND transaction_date between '2020-02-15' AND '2020-06-06'
GROUP BY id_account, month"""


def sql_query(statement): 
    """function to run a query and generate a report"""
    conn = sqlite3.connect('/home/kla/Documents/enote/enote_tech_test.db')
    df = pd.read_sql_query(sql=statement, con=conn)
    print (df)
    
if __name__ == '__main__':
    sql_query(statement)
