from datetime import datetime as dt
import pandas as pd
import MySQLdb as mdb

db_host = 'localhost'
db_user = 'sec_user'
db_pass = 'Your_Password'
db_name = 'securities_master'

now = dt.utcnow()
con = mdb.connect( host=db_host, user=db_user, passwd=db_pass, db=db_name)
SP500 = pd.read_csv('SP500.csv',encoding='UTF-8',dtype={'ticker':str})
symbols = []
for i, symbol in enumerate(SP500.Symbol):
    symbols.append(
        (
        1,
        SP500.ix[i,'Symbol'],
        SP500.ix[i,'Security'],
        'stock', 
        SP500.iloc[i,3], # Sector
        'USD', now, now
        ))

column_str = ( "exchange_id, ticker, name, instrument, sector, currency, created_date, last_updated_date")
insert_str = ("%s, " * 8)[:-2]
final_str = "INSERT INTO symbol (%s) VALUES (%s)" % (column_str, insert_str)
cur = con.cursor()
cur.executemany(final_str, symbols)
con.commit()

column_str = ("abbrev, name, currency, created_date, last_updated_date")
insert_str = ("%s, " * 5)[:-2]
final_str = "INSERT INTO exchange (%s) VALUES (%s)" % (column_str, insert_str)
cur.executemany(final_str, [('NYSE','New York Stock Exchange','USD',dt.utcnow(),dt.utcnow())])
con.commit()
cur.executemany(final_str, [('NASDAQ','National Association of Securities Dealers Automated Quotations','USD',dt.utcnow(),dt.utcnow())])
con.commit()
cur.executemany(final_str, [('SSE','Shanghai Stock Exchange','CNY',dt.utcnow(),dt.utcnow()),('SZSE','Shenzhen Stock Exchange','CNY',dt.utcnow(),dt.utcnow())])
con.commit()

HS300 = pd.read_csv('HS300.csv',encoding='UTF-8',dtype={'ticker':str})
symbols = []
for i, symbol in enumerate(HS300.ticker):
    symbols.append(
        (
        HS300.ix[i,'exchange_id'],
        HS300.ix[i,'ticker'],
        HS300.ix[i,'name'],
        'stock', 
        HS300.ix[i,'sector'], # Sector
        'CNY', dt.utcnow(), dt.utcnow()
        ))
column_str = ( "exchange_id, ticker, name, instrument, sector, currency, created_date, last_updated_date")
insert_str = ("%s, " * 8)[:-2]
final_str = "INSERT INTO symbol (%s) VALUES (%s)" % (column_str, insert_str)
cur.executemany(final_str, symbols)
con.commit()


'''
CREATE TABLE data_vendor ( id int NOT NULL AUTO_INCREMENT, name varchar(64) NOT NULL, website_url varchar(255) NULL, support_email varchar(255) NULL, created_date datetime NOT NULL, last_updated_date datetime NOT NULL, PRIMARY KEY (id)) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;
'''
column_str = ("name, created_date, last_updated_date")
insert_str = ("%s, " * 3)[:-2]
final_str = "INSERT INTO data_vendor (%s) VALUES (%s)" % (column_str, insert_str)
cur.executemany(final_str, [('AlphaVantage',dt.utcnow(),dt.utcnow()),('Tushare',dt.utcnow(),dt.utcnow())])
con.commit()
