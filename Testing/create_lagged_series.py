from datetime import datetime as dt, timedelta as td
import numpy as np
import pandas as pd
from alpha_vantage import AlphaVantage
from tu_share import TuShare

def create_lagged_series(symbol, start_date, end_date, lags=5, source='tu'):
    """
    This creates a Pandas DataFrame that stores the
    percentage returns of the adjusted closing value of
    a stock obtained from AlphaVantage, along with a
    number of lagged returns from the prior trading days
    (lags defaults to 5 days). Trading volume, as well as
    the Direction from the previous day, are also included.

    Parameters
    ----------
    symbol : 'str' The ticker symbol to obtain from AlphaVantage
    start_date : 'datetime' The starting date of the series to obtain
    end_date : 'datetime' The ending date of the the series to obtain
    lags : 'int' optional The number of days to 'lag' the series by, default is 5
    source : 'str' The data source, default is tushare from DB. Could be 'tu', 'av'

    Returns
    -------
    'pd.DataFrame' Contains the Adjusted Closing Price returns and lags
    """
    # Obtain stock pricing from AlphaVantage
    if source == 'av':
        av = AlphaVantage()
        adj_start_date = start_date - td(days=365)
        ts = av.get_daily_historic_data_csv(symbol, adj_start_date, end_date, path='./')

    if source == 'tu':
        tu = TuShare()
        start_date = start_date.strftime('%Y-%m-%d %H-%M-%S')
        end_date = end_date.strftime('%Y-%m-%d %H-%M-%S')
        ts = tu.get_daily_data_sql(ticker=symbol,startdate=start_date,enddate=end_date)
        ts['adjusted_close'] = ts['close_price'] * ts['adj_factor']

    # Create the new lagged DataFrame
    tslag = pd.DataFrame(index=ts.index)
    tslag['Today'] = ts['adjusted_close']
    tslag['Volume'] = ts['volume']

    # Create the shifted lag series of prior trading period close values
    for i in range(0, lags):
        tslag['Lag%s' % str(i+1)] = ts['adjusted_close'].shift(i+1)

    # Create the returns DataFrame
    tsret = pd.DataFrame(index=tslag.index)
    tsret['Volume'] = tslag['Volume']
    tsret['Today'] = tslag['Today'].pct_change() * 100.0

    # If any of the values of percentage returns equal zero, set them to a small number (stops issues with QDA model in scikit-learn)
    tsret.loc[tsret['Today'].abs() < 0.0001, ['Today']] = 0.0001

    # Create the lagged percentage returns columns
    for i in range(0, lags):
        tsret['Lag%s' % str(i+1)] = tslag['Lag%s' % str(i+1)].pct_change() * 100.0

    # Create the "Direction" column (+1 or -1) indicating an up/down day
    tsret['Direction'] = np.sign(tsret['Today'])
    tsret = tsret[tsret.index >= start_date]

    return tsret