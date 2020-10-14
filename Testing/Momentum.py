from strategy import Strategy
from event import SignalEvent
from backtest import Backtest
from data import HistoricCSVDataHandler
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

            if self.count > 252 and self.count % 30 == 0:
                for s in self.symbol_list:
                    bars = self.bars.get_latest_bars_values(s, "returns", N=self.window[1]) # [1,2.3,4,5,6,7,8,9]
                    bars = bars[:223]
                    if bars[-1] * 0 !=0 : continue  # when bar_date < day_of_symbol_on_mkt, symbol price is NA
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
                        signal = SignalEvent(strategy_id=1, symbol=i, datetime=self.bar_date, signal_type='EXIT',strength=1.0, quantity=500)
                        self.events.put(signal)


if __name__ == "__main__":
    csv_dir = './Data/Stock/'
    symbol_list = ['600000','600004','600009','600010','600011','600015','600016','600018','600019','600023','600025','600027','600028','600029','600030','600031',
 '600036','600038','600048','600050','600061','600066','600068','600085','600089','600100','600104','600109','600111','600115','600118','600153','600170','600176',
 '600177','600183','600188','600196','600208','600219','600221','600233','600271','600276','600297','600299','600309','600332','600340','600346','600352','600362',
 '600369','600372','600383','600390','600398','600406','600436','600438','600482','600487','600489','600498','600516','600519','600522','600535','600547','600566',
 '600570','600583','600585','600588','600606','600637','600655','600660','600663','600674','600690','600703','600705','600733','600741','600760','600795','600809',
 '600816','600837','600848','600867','600886','600887','600893','600900','600919','600926','600928','600958','600968','600977','600989','600998','600999','601006',
 '601009','601012','601018','601021','601066','601088','601108','601111','601117','601138','601155','601162','601166','601169','601186','601198','601211','601212',
 '601216','601225','601229','601236','601238','601288','601298','601318','601319','601328','601336','601360','601377','601390','601398','601555','601577','601600',
 '601601','601607','601618','601628','601633','601668','601669','601688','601698','601727','601766','601788','601800','601808','601818','601828','601838','601857',
 '601877','601878','601881','601888','601898','601899','601901','601919','601933','601939','601985','601988','601989','601992','601997','601998','603019','603156',
 '603160','603259','603260','603288','603501','603799','603833','603899','603986','603993','000001','000002','000063','000069','000100','000157','000166','000333',
 '000338','000413','000415','000423','000425','000538','000568','000596','000625','000627','000629','000630','000651','000656','000661','000671','000703','000709',
 '000723','000725','000728','000768','000776','000783','000786','000858','000876','000895','000898','000938','000961','000963','001979','002001','002007','002008',
 '002010','002024','002027','002032','002044','002050','002081','002120','002142','002146','002153','002179','002202','002230','002236','002241','002252','002271',
 '002294','002304','002311','002352','002410','002411','002415','002422','002456','002460','002466','002468','002475','002493','002508','002555','002558','002594',
 '002601','002602','002607','002624','002673','002714','002736','002739','002773','002841','002916','002938','002939','002945','002958','300003','300015','300017',
 '300024','300033','300059','300070','300122','300124','300136','300142','300144','300347','300408','300413','300433','300498']
    initial_capital = 5000000.0
    heartbeat = 0.0
    start_date = dt.datetime(2000, 1, 1, 0, 0, 0)
    end_date = dt.datetime.utcnow()
    backtest = Backtest(csv_dir=csv_dir, symbol_list=symbol_list, initial_capital=initial_capital, heartbeat=heartbeat, startdate=start_date, enddate=end_date, data_handler=HistoricCSVDataHandler, execution_handler=SimulatedExecutionHandler, portfolio=Portfolio, strategy=MomentumStrategy, window=[30,252])
    backtest.simulate_trading(frequency=252)
    
