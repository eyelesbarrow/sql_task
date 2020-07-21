import pandas as pd
import sqlite3
from sqlite3 import Error
import logging



#filepaths

db = "/home/kla/Documents/enote/enote_tech_test.db"
account_file = "/home/kla/Documents/enote/BI_assignment_account.csv"
person_file = "/home/kla/Documents/enote/BI_assignment_person.csv"
transaction_file = "/home/kla/Documents/enote/BI_assignment_transaction.csv"

def create_connection(db):
    """ function that creates a database and a connection to it"""
    conn = None
    
    try:
        conn = sqlite3.connect(db, timeout=30)
        return conn
        
    except Error as e:
        print(e)

    return conn

def create_table(conn, create_table_person_sql): 
    """function to create sql table"""
    try: 
        cur = conn.cursor()
        cur.executescript(create_table_person_sql)
     
    except Error as e: 
        print(e)
              
def main(): 
    """function to create schema and upload tables to database"""
    
    #sql statements to create tables
    create_table_person_sql = """CREATE TABLE IF NOT EXISTS 'persons' (
    id_person INTEGER PRIMARY KEY,
    name TEXT,
    surname TEXT,
    zipcode INTEGER,
    city TEXT,
    country TEXT,
    email TEXT,
    phone_number INTEGER,
    birth_date TEXT);"""

    create_table_account_sql = """CREATE TABLE IF NOT EXISTS 'accounts' (
    id_account INTEGER, 
    id_person INTEGER,
    account_type TEXT,
    FOREIGN KEY (id_person) REFERENCES persons (id_person));"""
    
    create_table_transaction_sql = """CREATE TABLE IF NOT EXISTS 'transactions' (
    id_transaction INTEGER,
    id_account INTEGER, 
    transaction_type TEXT, 
    transaction_date TEXT,
    transaction_amount REAL,
    FOREIGN KEY (id_account) REFERENCES accounts (id_account));"""

    #calling the create connection function 
    conn = create_connection(db)
    
    if conn is not None: 
        
        #create person table
        create_table(conn, create_table_person_sql)
        person_df = pd.read_csv(person_file,  sep=",", dtype='unicode', encoding='utf-8')
        person_df.rename(columns={'zip': 'zipcode'}, inplace=True) #renames zip column to separate it from zip function
        person_df.to_sql(name='persons', con=conn, if_exists='replace', index=False)
        logging.warning('Query statement:' + create_table_person_sql)
        
        #create account table 
        create_table(conn, create_table_account_sql)
        account_df = pd.read_csv(account_file, sep=",", dtype='unicode', encoding='utf-8')
        account_df.to_sql(name='accounts', con=conn, if_exists='replace', index=False)
        logging.warning('Query statement:' + create_table_account_sql)
        
        #create transaction table 
        create_table(conn, create_table_transaction_sql)
        transaction_df = pd.read_csv(transaction_file, sep=",", dtype='unicode', encoding='utf-8')
        transaction_df['transaction_date'] = pd.to_datetime(transaction_df['transaction_date']) #changes date column from str to datetime format
        transaction_df.to_sql(name='transactions', con=conn, if_exists='replace', index=False)
        logging.warning('Query statement:' + create_table_transaction_sql)
        
        
        logging.warning("Successfully created a database")
        
        #sql statement to check if tables were created, if one does not use the logging module
        #pd.set_option('display.max_colwidth', 1)
        #table = pd.read_sql_query("SELECT * FROM sqlite_master WHERE type='table';", con=conn)
        #print(table)

        conn.commit()
        conn.close()

    else: 
        logging.warning("No database created")
        conn.close()
    
if __name__ == '__main__':
    main()
    
    
