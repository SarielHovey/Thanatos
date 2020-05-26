from strategy import Strategy
from event import SignalEvent
from backtest import Backtest
from data import SQLDataHandler, HistoricCSVDataHandler
from execution import SimulatedExecutionHandler
from portfolio import Portfolio

from datetime import datetime as dt
import numpy as np
from scipy import stats
from sklearn.linear_model import LinearRegression


class myLinearRegression(LinearRegression):
    """
    LinearRegression class after sklearn's, but calculate t-statistics
    and p-values for model coefficients (betas).
    Additional attributes available after .fit()
    are `t` and `p` which are of the shape (y.shape[1], X.shape[1])
    which is (n_features, n_coefs)
    This class sets the intercept to 0 by default, since usually we include it
    in X.
    """
    def __init__(self, *args, **kwargs):
        if not "fit_intercept" in kwargs:
            kwargs['fit_intercept'] = False
        super(LinearRegression, self).__init__(*args, **kwargs)

    def fit(self, X, y, n_jobs=1):
        self = super(LinearRegression, self).fit(X, y, n_jobs)

        sse = np.sum((self.predict(X) - y) ** 2, axis=0) / float(X.shape[0] - X.shape[1])
        se = np.array([
            np.sqrt(np.diagonal(sse[i] * np.linalg.inv(np.dot(X.T, X))))
                                                    for i in range(sse.shape[0])
                    ])

        self.t = self.coef_ / se
        self.p = 2 * (1 - stats.t.cdf(np.abs(self.t), y.shape[0] - X.shape[1]))
        return self


class MovingAverageCrossStrategy(Strategy):
    """
    Carries out a basic Moving Average Crossover strategy with a short/long simple weighted moving average. Default short/long windows are 30/120 periods respectively.
    """
    def __init__(self, bars, events, window):
        """
        Initialises the Moving Average Cross Strategy.

        Parameters:
        bars - The DataHandler object that provides bar information
        events - The Event Queue object.
        short_window - The short moving average lookback.
        long_window - The long moving average lookback.
        """
        self.bars = bars
        self.symbol_list = self.bars.symbol_list
        self.events = events
        self.short_window = window[0]
        self.long_window = window[1]

        # Set to True if a symbol is in the market
        self.bought = self._calculate_initial_bought()

    def _calculate_initial_bought(self):
        """
        Adds keys to the bought dictionary for all symbols and sets them to 'OUT'.
        """
        bought = {}
        for s in self.symbol_list:
            bought[s] = 'OUT'
        return bought

    def calculate_signals(self, event):
        """
        Generates a new set of signals based on the MAC SMA with the short window crossing the long window meaning a long entry and vice versa for a short entry.

        Parameters
        event - A MarketEvent object.
        """
        if event.type == 'MARKET':
            for s in self.symbol_list:
                bars = self.bars.get_latest_bars_values(s, "adj_close", N=self.long_window)
                bar_date = self.bars.get_latest_bar_datetime(s)
                if len(bars)+1 <= self.short_window:
                    short_sma = np.mean(bars)
                    long_sma = np.mean(bars)
                elif self.short_window < len(bars)+1 and  len(bars)+1 <= self.long_window:
                    short_sma = np.mean(bars[-self.short_window-1:-1])
                    long_sma = np.mean(bars)
                else:
                    short_sma = np.mean(bars[-self.short_window-1:-1])
                    long_sma = np.mean(bars[-self.long_window-1:-1])

                symbol = s
                cur_date = dt.utcnow()
                sig_dir = ""

                if short_sma > long_sma and self.bought[s] == "OUT":
                    print("LONG: %s" % bar_date)
                    sig_dir = 'LONG'
                    signal = SignalEvent(strategy_id=1, symbol=symbol, datetime=bar_date, signal_type=sig_dir, strength=1.0)
                    self.events.put(signal)
                    self.bought[s] = 'LONG'
                elif short_sma < long_sma and self.bought[s] == "LONG":
                    print("SHORT: %s" % bar_date)
                    sig_dir = 'EXIT'
                    signal = SignalEvent(strategy_id=1, symbol=symbol, datetime=bar_date, signal_type=sig_dir, strength=1.0)
                    self.events.put(signal)
                    self.bought[s] = 'OUT'


class MomentumStrategy(Strategy):
    def __init__(self, bars, events, window):
        self.bars = bars
        self.symbol_list = self.bars.symbol_list
        self.events = events
        self.bought = self._calculate_initial_bought()
        self.window = window

    def _calculate_initial_bought(self):
        bought = {}
        for s in self.symbol_list:
            bought[s] = 'OUT'
        return bought

    def calculate_signals(self, event):
        if event.type == 'MARKET':
            for s in self.symbol_list:
                bars = self.bars.get_latest_bars_values(s, "adj_close", N=self.window[0])
                bar_date = self.bars.get_latest_bar_datetime(s)


if __name__ == "__main__":
    csv_dir = './Data/Stock/'
    symbol_list = ['600016','600030','601166','601988','002558','002493','002415','000858','000898','600031']
    initial_capital = 1000000.0
    heartbeat = 0.0
    start_date = dt(2010, 2, 20, 0, 0, 0)
    end_date = dt.now()
    backtest = Backtest(csv_dir=csv_dir, symbol_list=symbol_list, initial_capital=initial_capital, heartbeat=heartbeat, startdate=start_date, enddate=end_date, data_handler=HistoricCSVDataHandler, execution_handler=SimulatedExecutionHandler, portfolio=Portfolio, strategy=MovingAverageCrossStrategy, window=[30,120])
    backtest.simulate_trading(frequency=252)
