from datetime import datetime as dt
import time
try:
    import MySQLdb as mdb
except ImportError:
    raise ImportWarning("Caution: Library MySQLdb import fails!")
import sqlite3
import tushare as ts
import pandas as pd


def obtain_db_connection(source="MySQL", path="Z:/DB/securities_master.db"):
    """
    Obtain connection to db.
    :param source: db type used, could be "MySQL" or "SQLite"
    :param path: path for SQLite *.db file
    :return: SQL connection object
    """
    if source == "MySQL":
        DB_HOST = 'localhost'
        DB_USER = 'sec_user'
        DB_PASS = 'Your_Password_Here'
        DB_NAME = 'securities_master'
        con = mdb.connect(DB_HOST, DB_USER, DB_PASS, DB_NAME)
        return con
    elif source == "SQLite":
        con = sqlite3.connect(path)
        return con


def obtain_list_of_db_tickers(connection):
    """
    Obtains a list of the ticker symbols of HS300 in the database.
    This function works the same on mdb or sqlite3 cursor
    """
    cur = connection.cursor()
    cur.execute("SELECT id, ticker FROM symbol where exchange_id = 3 or exchange_id = 4")
    connection.commit()
    data = cur.fetchall()  # sqlite3 cursor also supports this method
    return [[d[0], d[1]] for d in data]


def tushare_ticker(ticker_list):
    """
    Input should be output from obtain_list_of_db_tickers(),
    Output will be a modified ticker_list for Tushare API with same shape.
    """
    for i, tick in enumerate(ticker_list):
        if tick[1][:2] == '60':
            tick[1] += '.SH'
        elif tick[1][:2] == '00':
            tick[1] += '.SZ'
        elif tick[1][:2] == '30':
            tick[1] += '.SZ'
    return ticker_list


def tushare_data(tick, start_date, end_date):
    """
    Download daily data via tushare API for one tick.
    Price are non-adjusted.
    Symbols with download error will be appended to global errList.
    """
    try:
        data0 = ts.pro_bar(ts_code=tick, start_date=start_date, end_date=end_date,adj=None)
        data1 = ts.pro_api().adj_factor(ts_code=tick, start_date=start_date, end_date=end_date)
    except Exception as e:
        print(
            "Could not download Tushare data for %s ticker "
            "(%s)...skipping." % (tick, e))
        errList.append(tick)
        return []
    else:
        data = pd.merge(data0[['ts_code','trade_date','open','high','low','close','vol']],
                        data1[['trade_date','adj_factor']], how='left', left_on='trade_date', right_on='trade_date')
        data.vol = data.vol.apply(lambda x: int(x * 100))
        data.trade_date = pd.to_datetime(data.trade_date)
        data['adj_factor'].fillna(method='pad',inplace=True)
        prices = []
        for i, day in enumerate(data.trade_date):
            prices.append(
                (
                    data.iat[i,1].to_pydatetime(), # d0, trade_date, change into datetime object rather than pd.TimeStamp Object
                    data.iat[i,2], # d1, open_price
                    data.iat[i,3], # d2, high_price
                    data.iat[i,4], # d3, low_price
                    data.iat[i,5], # d4, close_price
                    data.iat[i,6], # d5, volume
                    data.iat[i,7] # d6, adj_factor
                ))
        return prices


def insert_daily_data_into_db(data_vendor_id, symbol_id, daily_data, engine="MySQL"):
    """
    Takes a list of tuples of daily_data and adds it to the MySQL database. Appends the vendor ID and symbol ID to the data.
    """
    # Amend the data to include the vendor ID and symbol ID
    daily_data = [
        (data_vendor_id, symbol_id, d[0], dt.utcnow(), dt.utcnow(), d[1], d[2], d[3], d[4], d[5], d[6]) for d in daily_data
    ]
    # Create the insert strings
    column_str = (
        "data_vendor_id, symbol_id, price_date, created_date, last_updated_date, open_price, high_price, low_price, close_price, volume, adj_factor"
    )
    insert_str = ("%s, " * 11)[:-2]
    if engine == "SQLite":  # Support for sqlite3
        insert_str = ("?, " * 11)[:-2]
    final_str = ("INSERT INTO daily_price (%s) VALUES (%s)" % (column_str, insert_str))
    # Using the MySQL connection, carry out an INSERT INTO for every symbol
    cur = con.cursor()
    cur.executemany(final_str, daily_data)
    con.commit()



if __name__ == '__main__':
    # Please set Tushare Pro API before use this
    # Adjust how frequently the API is called (second)
    WAIT_TIME_IN_SECONDS = 1.5
    errList = []
    # warnings.filterwarnings('ignore')
    con = obtain_db_connection()
    ticker_list = obtain_list_of_db_tickers(connection=con)
    tickers = tushare_ticker(ticker_list)
    lentickers = len(tickers)
    for i, t in enumerate(tickers):
        print("Adding data for %s: %s out of %s" % (t[1], i+1, lentickers))
        ts_data = tushare_data(t[1],start_date='20000101',end_date='20100103')
        insert_daily_data_into_db(2, t[0], ts_data)
        time.sleep(WAIT_TIME_IN_SECONDS)
    errList = pd.Series(errList)
    errList.to_csv('ErrorList.csv',header=False,index=False,encoding='UTF-8')
    print("Successfully added TuSharePro pricing data to DB.")
