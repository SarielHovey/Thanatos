from strategy import Strategy, MovingAverageCrossStrategy
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
    backtest = Backtest(csv_dir=csv_dir, symbol_list=symbol_list, initial_capital=initial_capital, heartbeat=heartbeat, startdate=start_date, enddate=end_date, data_handler=HistoricCSVDataHandler, execution_handler=SimulatedExecutionHandler, portfolio=Portfolio, strategy=MovingAverageCrossStrategy, window=[30,90])
    backtest.simulate_trading(frequency=252)
    
