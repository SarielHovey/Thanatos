from strategy import Strategy
from event import SignalEvent
from backtest import Backtest
from data import SQLDataHandler, HistoricCSVDataHandler
from execution import SimulatedExecutionHandler
from portfolio import Portfolio

import datetime as dt
import numpy as np
import heapq
import statsmodels.api as sm


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
                cur_date = dt.datetime.utcnow()
                sig_dir = ""

                if short_sma > long_sma and self.bought[s] == "OUT":
                    print("LONG: %s" % bar_date)
                    sig_dir = 'LONG'
                    signal = SignalEvent(strategy_id=1, symbol=symbol, datetime=bar_date, signal_type=sig_dir, strength=1.0, quantity=500)
                    self.events.put(signal)
                    self.bought[s] = 'LONG'
                elif short_sma < long_sma and self.bought[s] == "LONG":
                    print("SHORT: %s" % bar_date)
                    sig_dir = 'EXIT'
                    signal = SignalEvent(strategy_id=1, symbol=symbol, datetime=bar_date, signal_type=sig_dir, strength=1.0, quantity=500)
                    self.events.put(signal)
                    self.bought[s] = 'OUT'


class MomentumStrategy(Strategy):
    def __init__(self, bars, events, window):
        self.bars = bars
        self.bar_date = dt.datetime(2000,1,1)
        self.symbol_list = self.bars.symbol_list
        self.events = events
        self.bought = self._calculate_initial_bought()
        self.window = window # [30,252]
        self.day_count = np.linspace(start=1,stop=window[1]-window[0]+1,num=(window[1]-window[0]+1),endpoint=True)
        self.momentum = []
        self.count = 0
        self.buy_list = []

    def _calculate_initial_bought(self):
        bought = {}
        for s in self.symbol_list:
            bought[s] = 'OUT'
        return bought

    def calculate_signals(self, event):
        if event.type == 'MARKET':
            self.momentum = []
            self.buy_list = []
            self.count += 1

            if self.count > 252:
                for s in self.symbol_list:
                    bars = self.bars.get_latest_bars_values(s, "returns", N=self.window[1]) # [1,2.3,4,5,6,7,8,9]
                    bars = bars[:223]
                    self.bar_date = self.bars.get_latest_bar_datetime(s)

                    model = sm.OLS(bars,self.day_count)
                    results = model.fit()
                    dic = {'tick':s, 'time':self.bar_date, 't':results.tvalues[0]}
                    self.momentum.append(dic)

                largest50 = heapq.nlargest(3, self.momentum, key=lambda s:s['t'])
                for i in largest50:
                    tick = i['tick']
                    if self.bought[tick] == 'OUT':
                        self.bought[tick] = 'LONG'
                        self.buy_list.append(tick)
                        print("LONG: %s at %s" % (tick, self.bar_date))
                        signal = SignalEvent(strategy_id=1, symbol=tick, datetime=self.bar_date, signal_type='LONG', strength=1.0, quantity=500)
                        self.events.put(signal)
                    elif self.bought[tick] == 'LONG':
                        self.buy_list.append(tick)

                for i in self.bought.keys():
                    if self.bought[i] == 'LONG' and (i not in self.buy_list):
                        self.bought[i] = 'OUT'
                        print("EXIT: %s at %s" % (i, self.bar_date))
                        signal = SignalEvent(strategy_id=1, symbol=i, datetime=self.bar_date, signal_type='EXIT',strength=1.0, quantity=500)
                        self.events.put(signal)


if __name__ == "__main__":
    csv_dir = './Data/Stock/'
    symbol_list = ['600016','600030','601166','601988','002558','002493','002415','000858','000898','600031']
    initial_capital = 1000000.0

    heartbeat = 0.0
    start_date = dt.datetime(2010, 2, 20, 0, 0, 0)
    end_date = dt.datetime.utcnow()
    backtest = Backtest(csv_dir=csv_dir, symbol_list=symbol_list, initial_capital=initial_capital, heartbeat=heartbeat, startdate=start_date, enddate=end_date, data_handler=HistoricCSVDataHandler, execution_handler=SimulatedExecutionHandler, portfolio=Portfolio, strategy=MomentumStrategy, window=[30,252])
    backtest.simulate_trading(frequency=252)
