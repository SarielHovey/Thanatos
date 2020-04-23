from strategy import MovingAverageCrossStrategy
from event import SignalEvent
from backtest import Backtest
from data import SQLDataHandler
from execution import SimulatedExecutionHandler
from portfolio import Portfolio

from datetime import datetime as dt

if __name__ == "__main__":
    csv_dir = './'
    symbol_list = ['601988','601985']
    initial_capital = 1000000.0
    heartbeat = 0.0
    start_date = dt(2010, 2,20, 0, 0, 0)
    end_date = dt.now()
    backtest = Backtest(csv_dir, symbol_list, initial_capital, heartbeat, start_date, end_date, SQLDataHandler, SimulatedExecutionHandler, Portfolio, MovingAverageCrossStrategy)
    backtest.simulate_trading(frequency=252)