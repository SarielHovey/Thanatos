import sqlite3
from datetime import datetime as dt
from datetime import time


def main(path:str ="~/Thanatos/Data/securities_master.db"):
    """
    Used to initial SQLite db for Thanatos under python.
    """
    con = sqlite3.connect(path)
    cur = con.cursor()
    
    cur.execute( """
        CREATE TABLE exchange ( 
        id INT PRIMARY KEY, 
        abbrev VARCHAR(32) NOT NULL, 
        name VARCHAR(255) NOT NULL, 
        city VARCHAR(255), 
        country VARCHAR(255), 
        currency VARCHAR(64), 
        timezone_offset DATETIME, 
        created_date DATETIME NOT NULL, 
        last_updated_date DATETIME NOT NULL
        );
        """ )        
    cur.execute( """
        CREATE TABLE data_vendor ( 
        id INT PRIMARY KEY, 
        name varchar(64) NOT NULL, 
        website_url varchar(255), 
        support_email varchar(255), 
        created_date datetime, 
        last_updated_date datetime
        );
        """ )
    cur.execute( """
        CREATE TABLE symbol ( 
        id int PRIMARY KEY, 
        exchange_id int KEY, 
        ticker varchar(32) NOT NULL, 
        instrument varchar(64) NOT NULL, 
        name varchar(255), 
        sector varchar(255), 
        currency varchar(32), 
        created_date datetime NOT NULL, 
        last_updated_date datetime NOT NULL
        );
        """ )
    cur.execute( """
        CREATE TABLE daily_price ( 
        id int PRIMARY KEY, 
        data_vendor_id int KEY NOT NULL, 
        symbol_id int KEY NOT NULL, 
        price_date datetime NOT NULL, 
        created_date datetime NOT NULL, 
        last_updated_date datetime NOT NULL, 
        open_price decimal(19,4), 
        high_price decimal(19,4), 
        low_price decimal(19,4), 
        close_price decimal(19,4), 
        adj_factor decimal(19,10), 
        volume bigint NULL 
        );
        """ )
        
    con.commit()
    

def enrich(path:str ="~/Thanatos/Data/securities_master.db"):
    """
    Enrich some startup message into db.
    :return: void
    """
    con = sqlite3.connect(path)
    cur = con.cursor()
    # Enrich table data_vendor
    column_str = "id, name, website_url, support_email, created_date, last_updated_date"
    insert_str = ("?, " * 6)[:-2]
    final_str = ("INSERT INTO data_vendor (%s) VALUES (%s)" % (column_str, insert_str))
    DATA = []
    DATA.append(tuple(["1","AlphaVantage","https://www.alphavantage.co/","support@alphavantage.co",dt.utcnow(),dt.utcnow()]))
    DATA.append(tuple(["2","Quandl","https://www.quandl.com/","support@quandl.com",dt.utcnow(),dt.utcnow()]))
    DATA.append(tuple(["3","TuShare","https://www.tushare.pro/","waditu",dt.utcnow(),dt.utcnow()]))
    cur.executemany(final_str,DATA)
    con.commit()
    # Enrich table exchange
    column_str = "id, abbrev, name, city, country, currency, timezone_offset, created_date, last_updated_date"
    insert_str = ("?, " * 9)[:-2]
    final_str = ("INSERT INTO exchange (%s) VALUES (%s)" % (column_str, insert_str))
    DATA = []
    DATA.append(tuple(["1","NYSE","The New York Stock Exchange","New York","USA","USD",None,dt.utcnow(),dt.utcnow()]))
    DATA.append(tuple(["2","Nasdaq","Nasdaq","New York",'USA',"USD",None,dt.utcnow(),dt.utcnow()]))
    DATA.append(tuple(["3","SSE","Shanghai Stock Exchange","Shanghai","China","CNY",time(8,0,0),dt.utcnow(),dt.utcnow()]))
    DATA.append(tuple(["4","SZSE","Shenzhen Stock Exchange Stock Exchange","Shenzhen","China","CNY",time(8,0,0),dt.utcnow(),dt.utcnow()]))
    cur.executemany(final_str,DATA)
    con.commit()

    
if __name__ == "__main__":
    main(path="./securities_master.db")
    print("SQLite db initialized successuflly in ./Data/ !")
