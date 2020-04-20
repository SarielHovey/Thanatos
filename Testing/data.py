from abc import ABCMeta, abstractmethod
import datetime
import os, os.path
import numpy as np
import pandas as pd
from tu_share import TuShare
from event import MarketEvent

class DataHandler(object):
    """
    DataHandler is an abstract base class providing an interface for all subsequent (inherited) data handlers (both live and historic).
    The goal of a (derived) DataHandler object is to output a generated set of bars (OHLCVI) for each symbol requested.
    This will replicate how a live strategy would function as current market data would be sent "down the pipe". Thus a historic and live system will be treated identically by the rest of the backtesting suite.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_latest_bar(self, symbol):
        """
        Returns the last bar updated.
        """
        raise NotImplementedError("Should implement get_latest_bar()")

    @abstractmethod
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars updated.
        """
        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar.
        """
        raise NotImplementedError("Should implement get_latest_bar_datetime()")

    @abstractmethod
    def get_latest_bar_value(self, symbol, val_type):
        """
        Returns one of the Open, High, Low, Close, Volume or OI from the last bar.
        """
        raise NotImplementedError("Should implement get_latest_bar_value()")

    @abstractmethod
    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Returns the last N bar values from the latest_symbol list, or N-k if less available.
        """
        raise NotImplementedError("Should implement get_latest_bars_values()")

    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bars to the bars_queue for each symbol in a tuple OHLCVI format: (datetime, open, high, low, close, volume, open interest).
        """
        raise NotImplementedError("Should implement update_bars()")


class HistoricCSVDataHandler(DataHandler):
    """
    HistoricCSVDataHandler is designed to read CSV files for each requested symbol from disk and provide an interface to obtain the "latest" bar in a manner identical to a live trading interface.
    """
    def __init__(self, events, csv_dir, symbol_list, startdate='2000-01-01 00:00:00', enddate='2020-01-01 00:00:00'):
        """
        Initialises the historic data handler by requesting the location of the CSV files and a list of symbols.
        It will be assumed that all files are of the form 'symbol.csv', where symbol is a string in the list.

        Parameters:
        events - The Event Queue.
        csv_dir - Absolute directory path to the CSV files.
        symbol_list - A list of symbol strings.
        """
        self.events = events
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list
        self.startdate = startdate # Redundent, remain for compatibality
        self.enddate = enddate # Redundent, remain for compatibality
        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True
        self._open_convert_csv_files()

    def _open_convert_csv_files(self):
        """
        Opens the CSV files from the data directory, converting them into pandas DataFrames within a symbol dictionary.
        For this handler it will be assumed that the data is taken from AlphaVantage. Thus its format will be respected.
        """
        comb_index = None
        for s in self.symbol_list:
            # Load the CSV file with no header information, indexed on date
            self.symbol_data[s] = pd.read_csv(
                os.path.join(self.csv_dir, '%s.csv' % s), header=0, index_col=0, parse_dates=True, names=[ 'price_date', 'ticker', 'open_price', 'high_price', 'low_price', 'close_price', 'volume','adj_factor']
                )
            self.symbol_data[s]['adj_close'] = self.symbol_data[s]['close_price'] * self.symbol_data[s]['adj_factor']
            self.symbol_data[s].sort_index(inplace=True)
        
            # Combine the index to pad forward values
            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)

            # Set the latest symbol_data to None
            self.latest_symbol_data[s] = []

        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(index=comb_index, method='pad')
            self.symbol_data[s]["returns"] = self.symbol_data[s]["adj_close"].pct_change().dropna()
            self.symbol_data[s] = self.symbol_data[s].iterrows()
        # Output is generator of ('price_date', 'ticker', 'open_price', 'high_price', 'low_price', 'close_price', 'volume','adj_factor','adj_close','returns')

    def _get_new_bar(self, symbol):
        """
        Returns the latest bar from the data feed via generator.
        """
        for b in self.symbol_data[symbol]:
            yield b
            
    def get_latest_bar(self, symbol):
        """
        Returns the last bar from the latest_symbol list.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1]

    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list, or N-k if less available.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-N:]

    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1][0]
    
    def get_latest_bar_value(self, symbol, val_type):
        """
        Returns one of the Open, High, Low, Close, Volume or OI values from the Pandas Bar series object.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return getattr(bars_list[-1][1], val_type)
        
    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Returns the last N bar values from the latest_symbol list, or N-k if less available.
        """
        try:
            bars_list = self.get_latest_bars(symbol, N)
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return np.array([getattr(b[1], val_type) for b in bars_list])

    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure for all symbols in the symbol list.
        """
        for s in self.symbol_list:
            try:
                bar = next(self._get_new_bar(s))
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)
        self.events.put(MarketEvent())

class SQLDataHandler(DataHandler):
    """
    Read data from DB in the form of Pandas DataFrame and provide a interface to obtain the latest bar in a manner identical to a live trading interface.

    Work the same as HistoricCSVDataHandler().
    """
    def __init__(self, events, csv_dir, symbol_list, startdate='2000-01-01 00:00:00', enddate='2020-01-01 00:00:00'):
        """
        Initialize SQLDataHandler by requesting data from DB.

        Parameters:
        events - The Event Queue.
        csv_dir - Absolute directory path to the CSV files.
        symbol_list - A list of symbol strings. e.g. ['601988','601000']
        startdate: str, '2000-01-01 00:00:00'
        enddate: str, '2020-01-01 00:00:00'
        """
        self.events = events
        self.csv_dir = csv_dir # Redundent, remain for compatibality
        self.startdate = startdate
        self.enddate = enddate
        self.symbol_list = symbol_list
        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True
        self._load_convert_sql_data()

    def _load_convert_sql_data(self):
        """
        Read data from DB, converting it into pandas DataFrame within a symbol dictionary.
        -------------------------------------
        Output should in format: ('price_date', 'ticker', 'open_price', 'high_price', 'low_price', 'close_price', 'volume','adj_factor','adj_close','returns')
        """
        comb_index = None
        tu = TuShare()
        for s in self.symbol_list:
            self.symbol_data[s] = tu.get_daily_data_sql(ticker=s, startdate=self.startdate, enddate=self.enddate)
            self.symbol_data[s]['adj_close'] = self.symbol_data[s]['close_price'] * self.symbol_data[s]['adj_factor']
            self.symbol_data[s].sort_index(inplace=True)
            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)
            self.latest_symbol_data[s] = []
        
        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(index=comb_index, method='pad')
            self.symbol_data[s]["returns"] = self.symbol_data[s]["adj_close"].pct_change().dropna()
            self.symbol_data[s] = self.symbol_data[s].iterrows()

    def _get_new_bar(self, symbol):
        """
        Returns the latest bar from the data feed via generator.
        """
        for b in self.symbol_data[symbol]:
            yield b
            
    def get_latest_bar(self, symbol):
        """
        Returns the last bar from the latest_symbol list.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1]

    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list, or N-k if less available.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-N:]

    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1][0]
    
    def get_latest_bar_value(self, symbol, val_type):
        """
        Returns one of the Open, High, Low, Close, Volume or OI values from the Pandas Bar series object.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return getattr(bars_list[-1][1], val_type)
        
    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Returns the last N bar values from the latest_symbol list, or N-k if less available.
        """
        try:
            bars_list = self.get_latest_bars(symbol, N)
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return np.array([getattr(b[1], val_type) for b in bars_list])

    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure for all symbols in the symbol list.
        """
        for s in self.symbol_list:
            try:
                bar = next(self._get_new_bar(s))
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)
        self.events.put(MarketEvent())        



