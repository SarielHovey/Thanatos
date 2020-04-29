from datetime import datetime as dt
import pandas as pd
import tushare as tu
import MySQLdb as mdb


COLUMNS = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'AdjFacotr']
class TuShare(object):
    """
    Encapsulates calls to the TuShare API with a provided API key.
    API key shold be preset
    """
    def __init__(self, api_key='YOUR_KEY_HERE'):
        """
        Initialise the TuShare instance.
        Parameters
        ----------
        api_key : 'str', optional
        The API key for the associated Tushare account
        """
        # ts.set_token(api_key)
        self.api_key = api_key

    def _construct_tushare_symbol_call(self, ticker):
        """
        Construct the full API call to TuShare based on the user provided API key and the desired ticker symbol.
        Parameters
        ----------
        ticker : 'str'
        The ticker symbol, e.g. '600388'
        Returns
        -------
        'str'
        The full API call for a ticker time series
        """
        if ticker[:2] == '60':
            ticker += '.SH'
        elif ticker[:1] == '5':
            ticker += '.SH'
        elif ticker[:2] == '00':
            ticker += '.SZ'
        elif ticker[:2] == '30':
            ticker += '.SZ'
        elif ticker[:1] == '1':
            ticker += '.SZ'
        return ticker

    def get_daily_historic_data(self, ticker, start_date, end_date, asset='E', adj=None):
        """
        Use the generated API call to query Tushare with the appropriate API key and return a list of price tuples for particular ticker.
        Parameters
        ----------
        ticker : 'str', The ticker symbol, e.g. '600388'
        start_date : 'datetime', The starting date to obtain pricing for
        end_date : 'datetime', The ending date to obtain pricing for
        asset: 'str', E--Equity, FD--Fund, FT--Future
        Returns
        -------
        'pd.DataFrame', The frame of OHLCV prices and volumes
        """
        ts_code = self._construct_tushare_symbol_call(ticker)
        # print(ticker, ts_code) # Used for Debug
        try:
            data0 = tu.pro_bar(ts_code=ts_code, start_date=start_date, end_date=end_date, asset=asset, adj=adj)
            data1 = tu.pro_api().adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)
        except Exception as e:
            print("Could not download Tushare data for %s ticker (%s)...stopping." % (ticker, e))
            return pd.DataFrame(columns=COLUMNS)
        else:
            data = pd.merge(data0[['ts_code','trade_date','open','high','low','close','vol']],data1[['trade_date','adj_factor']],how='left',left_on='trade_date',right_on='trade_date')
            data.vol = data.vol.apply(lambda x: int(x * 100))
            data.ts_code = data.ts_code.apply(lambda x: x[:-3])
            data.trade_date = pd.to_datetime(data.trade_date)
            data.adj_factor.fillna(1.0,inplace=True)
            data.columns = ['ts_code','price_date','open_price','high_price','low_price','close_price','volume','adj_factor']
            return data.set_index('price_date').sort_index()

    def get_daily_data_sql(self, ticker, startdate, enddate):
        """
        Use DATABASE securities_master to query data for ticker input.
        Parameters
        ----------
        ticker : 'str', The ticker symbol, e.g. '601988'
        start_date : str, '%Y-%m-%d %H:%M:%S', The starting date to obtain pricing for
        end_date : str, '%Y-%m-%d %H:%M:%S', The ending date to obtain pricing for
        Returns
        -------
        'pd.DataFrame', The frame of OHLCV prices and volumes
        """
        DB_HOST = 'localhost'
        DB_USER = 'sec_user'
        DB_PASS = 'YOUR_PASSWORD_HERE'
        DB_NAME = 'securities_master'
        con = mdb.connect(DB_HOST, DB_USER, DB_PASS, DB_NAME)
        sql = "SELECT sym.ticker, dp.price_date, dp.open_price, dp.high_price, dp.low_price, dp.close_price, dp.volume, dp.adj_factor FROM daily_price AS dp INNER JOIN symbol AS sym ON sym.id = dp.symbol_id where " + "sym.ticker = '" + ticker + "' AND " + "dp.price_date BETWEEN ' " + startdate + "' AND '" + enddate + "' ORDER BY dp.price_date ASC;"
        otpt = pd.read_sql_query(sql, con=con, index_col='price_date')
        return otpt

    def get_daily_data_sql_to_csv(self, ticker, startdate, enddate, path='./'):
        """
        Export data from DB into csv file.
        Parameters
        ----------
        ticker : 'str', The ticker symbol, e.g. '601988'
        start_date : str, '%Y-%m-%d %H:%M:%S', The starting date to obtain pricing for
        end_date : str, '%Y-%m-%d %H:%M:%S', The ending date to obtain pricing for
        Returns
        -------
        'CSV', A csv file on local drive.
        """
        temp = self.get_daily_data_sql(ticker=ticker,startdate=startdate,enddate=enddate)
        path = path + ticker + '.csv'
        temp.to_csv(path, encoding='UTF-8')
