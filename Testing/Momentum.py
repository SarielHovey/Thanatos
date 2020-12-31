from strategy import Strategy
from event import SignalEvent
from backtest import Backtest
from data import SQLiteDataHandler, HistoricCSVDataHandler
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

            if self.count > 252 and self.count % 7 == 0:
                for s in self.symbol_list:
                    bars = self.bars.get_latest_bars_values(s, "returns", N=self.window[1]) # [1,2.3,4,5,6,7,8,9]
                    bars = bars[:223]
                    if bars[-1] * 0 !=0 : continue  # symbol may not be available on self.bar_date, skip NA trades
                    self.bar_date = self.bars.get_latest_bar_datetime(s)
                    model = sm.OLS(bars,self.day_count)
                    results = model.fit()
                    dic = {'tick':s, 'time':self.bar_date, 't':results.tvalues[0]}
                    self.momentum.append(dic)
                largest50 = heapq.nlargest(50, self.momentum, key=lambda s:s['t'])
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
                        signal = SignalEvent(strategy_id=1, symbol=i, datetime=self.bar_date, signal_type='EXIT', strength=1.0, quantity=500)
                        self.events.put(signal)


if __name__ == "__main__":
    csv_dir = './Data/Stock/'
    symbol_list = ['600000.SH',
     '600004.SH',
     '600009.SH',
     '600010.SH',
     '600011.SH',
     '600015.SH',
     '600016.SH',
     '600018.SH',
     '600019.SH',
     '600023.SH',
     '600025.SH',
     '600027.SH',
     '600028.SH',
     '600029.SH',
     '600030.SH',
     '600031.SH',
     '600036.SH',
     '600038.SH',
     '600048.SH',
     '600050.SH',
     '600061.SH',
     '600066.SH',
     '600068.SH',
     '600085.SH',
     '600089.SH',
     '600100.SH',
     '600104.SH',
     '600109.SH',
     '600111.SH',
     '600115.SH',
     '600118.SH',
     '600153.SH',
     '600170.SH',
     '600176.SH',
     '600177.SH',
     '600183.SH',
     '600188.SH',
     '600196.SH',
     '600208.SH',
     '600219.SH',
     '600221.SH',
     '600233.SH',
     '600271.SH',
     '600276.SH',
     '600297.SH',
     '600299.SH',
     '600309.SH',
     '600332.SH',
     '600340.SH',
     '600346.SH',
     '600352.SH',
     '600362.SH',
     '600369.SH',
     '600372.SH',
     '600383.SH',
     '600390.SH',
     '600398.SH',
     '600406.SH',
     '600436.SH',
     '600438.SH',
     '600482.SH',
     '600487.SH',
     '600489.SH',
     '600498.SH',
     '600516.SH',
     '600519.SH',
     '600522.SH',
     '600535.SH',
     '600547.SH',
     '600566.SH',
     '600570.SH',
     '600583.SH',
     '600585.SH',
     '600588.SH',
     '600606.SH',
     '600637.SH',
     '600655.SH',
     '600660.SH',
     '600663.SH',
     '600674.SH',
     '600690.SH',
     '600703.SH',
     '600705.SH',
     '600733.SH',
     '600741.SH',
     '600760.SH',
     '600795.SH',
     '600809.SH',
     '600816.SH',
     '600837.SH',
     '600848.SH',
     '600867.SH',
     '600886.SH',
     '600887.SH',
     '600893.SH',
     '600900.SH',
     '600919.SH',
     '600926.SH',
     '600928.SH',
     '600958.SH',
     '600968.SH',
     '600977.SH',
     '600989.SH',
     '600998.SH',
     '600999.SH',
     '601006.SH',
     '601009.SH',
     '601012.SH',
     '601018.SH',
     '601021.SH',
     '601066.SH',
     '601088.SH',
     '601108.SH',
     '601111.SH',
     '601117.SH',
     '601138.SH',
     '601155.SH',
     '601162.SH',
     '601166.SH',
     '601169.SH',
     '601186.SH',
     '601198.SH',
     '601211.SH',
     '601212.SH',
     '601216.SH',
     '601225.SH',
     '601229.SH',
     '601236.SH',
     '601238.SH',
     '601288.SH',
     '601298.SH',
     '601318.SH',
     '601319.SH',
     '601328.SH',
     '601336.SH',
     '601360.SH',
     '601377.SH',
     '601390.SH',
     '601398.SH',
     '601555.SH',
     '601577.SH',
     '601600.SH',
     '601601.SH',
     '601607.SH',
     '601618.SH',
     '601628.SH',
     '601633.SH',
     '601668.SH',
     '601669.SH',
     '601688.SH',
     '601698.SH',
     '601727.SH',
     '601766.SH',
     '601788.SH',
     '601800.SH',
     '601808.SH',
     '601818.SH',
     '601828.SH',
     '601838.SH',
     '601857.SH',
     '601877.SH',
     '601878.SH',
     '601881.SH',
     '601888.SH',
     '601898.SH',
     '601899.SH',
     '601901.SH',
     '601919.SH',
     '601933.SH',
     '601939.SH',
     '601985.SH',
     '601988.SH',
     '601989.SH',
     '601992.SH',
     '601997.SH',
     '601998.SH',
     '603019.SH',
     '603156.SH',
     '603160.SH',
     '603259.SH',
     '603260.SH',
     '603288.SH',
     '603501.SH',
     '603799.SH',
     '603833.SH',
     '603899.SH',
     '603986.SH',
     '603993.SH',
     '000001.SZ',
     '000002.SZ',
     '000063.SZ',
     '000069.SZ',
     '000100.SZ',
     '000157.SZ',
     '000166.SZ',
     '000333.SZ',
     '000338.SZ',
     '000413.SZ',
     '000415.SZ',
     '000423.SZ',
     '000425.SZ',
     '000538.SZ',
     '000568.SZ',
     '000596.SZ',
     '000625.SZ',
     '000627.SZ',
     '000629.SZ',
     '000630.SZ',
     '000651.SZ',
     '000656.SZ',
     '000661.SZ',
     '000671.SZ',
     '000703.SZ',
     '000709.SZ',
     '000723.SZ',
     '000725.SZ',
     '000728.SZ',
     '000768.SZ',
     '000776.SZ',
     '000783.SZ',
     '000786.SZ',
     '000858.SZ',
     '000876.SZ',
     '000895.SZ',
     '000898.SZ',
     '000938.SZ',
     '000961.SZ',
     '000963.SZ',
     '001979.SZ',
     '002001.SZ',
     '002007.SZ',
     '002008.SZ',
     '002010.SZ',
     '002024.SZ',
     '002027.SZ',
     '002032.SZ',
     '002044.SZ',
     '002050.SZ',
     '002081.SZ',
     '002120.SZ',
     '002142.SZ',
     '002146.SZ',
     '002153.SZ',
     '002179.SZ',
     '002202.SZ',
     '002230.SZ',
     '002236.SZ',
     '002241.SZ',
     '002252.SZ',
     '002271.SZ',
     '002294.SZ',
     '002304.SZ',
     '002311.SZ',
     '002352.SZ',
     '002410.SZ',
     '002411.SZ',
     '002415.SZ',
     '002422.SZ',
     '002456.SZ',
     '002460.SZ',
     '002466.SZ',
     '002468.SZ',
     '002475.SZ',
     '002493.SZ',
     '002508.SZ',
     '002555.SZ',
     '002558.SZ',
     '002594.SZ',
     '002601.SZ',
     '002602.SZ',
     '002607.SZ',
     '002624.SZ',
     '002673.SZ',
     '002714.SZ',
     '002736.SZ',
     '002739.SZ',
     '002773.SZ',
     '002841.SZ',
     '002916.SZ',
     '002938.SZ',
     '002939.SZ',
     '002945.SZ',
     '002958.SZ',
     '300003.SZ',
     '300015.SZ',
     '300017.SZ',
     '300024.SZ',
     '300033.SZ',
     '300059.SZ',
     '300070.SZ',
     '300122.SZ',
     '300124.SZ',
     '300136.SZ',
     '300142.SZ',
     '300144.SZ',
     '300347.SZ',
     '300408.SZ',
     '300413.SZ',
     '300433.SZ',
     '300498.SZ']
    initial_capital = 5000000.0
    heartbeat = 0.0
    start_date = dt.datetime(2000, 1, 1, 0, 0, 0)
    end_date = dt.datetime.utcnow()
    backtest = Backtest(csv_dir=csv_dir, symbol_list=symbol_list, initial_capital=initial_capital, heartbeat=heartbeat, startdate=start_date, enddate=end_date, data_handler=SQLiteDataHandler, execution_handler=SimulatedExecutionHandler, portfolio=Portfolio, strategy=MomentumStrategy, window=[30,252])
    backtest.simulate_trading(frequency=252)
    
