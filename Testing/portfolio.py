import datetime
from numpy import floor

try:
    import Queue as queue
except ImportError:
    import queue

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

from event import FillEvent, OrderEvent
from performance import create_sharpe_ratio, create_drawdowns

class Portfolio(object):
    """
    The Portfolio class handles the positions and market value of all instruments at a resolution of a "bar", i.e. secondly, minutely, 5-min, 30-min, 60 min or EOD.

    The positions DataFrame stores a time-index of the quantity of positions held.

    The holdings DataFrame stores the cash and total market holdings value of each symbol for a particular time-index, as well as the percentage change in portfolio total across bars.
    """
    def __init__(self, bars, events, start_date, initial_capital=100000.0):
        """
        Initialises the portfolio with bars and an event queue. Also includes a starting datetime index and initial capital (USD unless otherwise stated).

        Parameters:
        bars - The DataHandler object with current market data.
        events - The Event Queue object.
        start_date - The start date (bar) of the portfolio.
        initial_capital - The starting capital in USD.
        """
        self.bars = bars
        self.events = events
        self.symbol_list = self.bars.symbol_list
        self.start_date = start_date
        self.initial_capital = initial_capital
        self.portfolio_date = datetime.datetime(2000,1,1)
        # self.smooth_count = 3 # Used for portfolio adjustment smoothing
        # Used for store historical OrderEvent for smoothing
        self.order_queue = dict((k, v) for k, v in [(s, []) for s in self.symbol_list])

        self.all_positions = self.construct_all_positions()
        self.current_positions = dict( (k,v) for k, v in [(s, 0.0) for s in self.symbol_list] )

        self.all_holdings = self.construct_all_holdings()
        self.current_holdings = self.construct_current_holdings()

    def construct_all_positions(self):
        """
        Constructs the positions list using the start_date to determine when the time index will begin.
        """
        d = dict( (k,v) for k, v in [(s, 0.0) for s in self.symbol_list] )
        d['datetime'] = self.start_date
        return [d]
        
    def construct_all_holdings(self):
        """
        Constructs the holdings list using the start_date to determine when the time index will begin.
        """
        d = dict( (k,v) for k, v in [(s, 0.0) for s in self.symbol_list] )
        d['datetime'] = self.start_date
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return [d]

    def construct_current_holdings(self):
        """
        This constructs the dictionary which will hold the instantaneous value of the portfolio across all symbols.
        """
        d = dict( (k,v) for k, v in [(s, 0.0) for s in self.symbol_list] )
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return d

    def update_timeindex(self, event):
        """
        Adds a new record to the positions matrix for the current market data bar. This reflects the PREVIOUS bar, i.e. all current market data at this stage is known (OHLCV).
        Makes use of a MarketEvent from the events queue.
        """
        latest_datetime = self.bars.get_latest_bar_datetime(self.symbol_list[0])
        self.portfolio_date = latest_datetime
        
        # Update positions
        # ================
        dp = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )
        dp['datetime'] = latest_datetime

        for s in self.symbol_list:
            temp = self.current_positions[s]
            if temp >= 0.0:
                dp[s] = self.current_positions[s]
            else:
                dp[s] = 0.0

        # Append the current positions
        self.all_positions.append(dp)

        # Update holdings
        # ===============
        dh = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )
        dh['datetime'] = latest_datetime
        dh['cash'] = self.current_holdings['cash']
        dh['commission'] = self.current_holdings['commission']
        dh['total'] = self.current_holdings['cash']
        # Add mkt value to dh['total']
        for s in self.symbol_list:
            # Approximation to the real value
            market_value = self.current_positions[s] * self.bars.get_latest_bar_value(s, "adj_close")
            if market_value >= 0.0:
                dh[s] = market_value
                dh['total'] += market_value
            else:
                market_value = 0.0
                dh[s] = market_value
                dh['total'] += market_value

        # Append the current holdings
        self.all_holdings.append(dh)

    def update_positions_from_fill(self, fill):
        """
        Takes a Fill object and updates the position matrix to reflect the new position.

        Parameters:
        fill - The Fill object to update the positions with.
        """
        # Check whether the fill is a buy or sell
        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        if fill.direction == 'SELL':
            fill_dir = -1
        # Update positions list with new quantities
        self.current_positions[fill.symbol] += fill_dir * fill.quantity

    def update_holdings_from_fill(self, fill):
        """
        Takes a Fill object and updates the holdings matrix to reflect the holdings value.

        Parameters:
        fill - The Fill object to update the holdings with.
        """
        # Check whether the fill is a buy or sell
        fill_dir = 0
        if fill.direction == 'BUY':
            fill_dir = 1
        if fill.direction == 'SELL':
            fill_dir = -1
        
        # Update holdings list with new quantities
        fill_cost = self.bars.get_latest_bar_value(fill.symbol, "adj_close")
        cost = fill_dir * fill_cost * fill.quantity
        self.current_holdings[fill.symbol] += cost
        self.current_holdings['commission'] += fill.commission
        self.current_holdings['cash'] -= (cost + fill.commission)
        self.current_holdings['total'] -= (cost + fill.commission)

    def update_fill(self, event):
        """
        Updates the portfolio current positions and holdings from a FillEvent.
        """
        if event.type == 'FILL':
            self.update_positions_from_fill(event)
            self.update_holdings_from_fill(event)

    def generate_naive_order(self, signal):
        """
        Simply fills an Order object as a constant quantity sizing of the signal object, without risk management or position sizing considerations. Will send order of 100 with market order.

        Parameters:
        signal - The tuple containing Signal information.
        """
        order = []
        init_order_date = signal.datetime
        symbol = signal.symbol
        direction = signal.signal_type
        strength = signal.strength

        mkt_quantity = signal.quantity
        cur_quantity = self.current_positions[symbol]
        order_type = 'MKT'

        if direction == 'LONG': # and cur_quantity == 0:
            order.append(OrderEvent(init_order_date, symbol, order_type, mkt_quantity, 'BUY'))
        if direction == 'SHORT' and cur_quantity == 0:
            order.append(OrderEvent(init_order_date, symbol, order_type, mkt_quantity, 'SELL'))

        if direction == 'EXIT' and cur_quantity > 0:
            order.append(OrderEvent(init_order_date, symbol, order_type, abs(cur_quantity), 'SELL'))
        if direction == 'EXIT' and cur_quantity < 0:
            order.append(OrderEvent(init_order_date, symbol, order_type, abs(cur_quantity), 'BUY'))

        return order

    def generate_smooth_order(self, signal):
        """
        Fill an Order object as a constant quantity sizing of the signal object, without risk management or position sizing considerations.
        As default, an order will be implemented with a smooth window of 5 days.
        :param signal: A SignalEvent, from self.update_signal().
        :return: dict - An OrderEvent queue.
        """
        orders = []
        symbol = signal.symbol
        init_order_date = signal.datetime
        dd = datetime.timedelta(days=1)
        direction = signal.signal_type
        strength = signal.strength
        mkt_quantity = signal.quantity
        cur_quantity = self.current_positions[symbol]
        order_type = 'MKT'

        if direction == 'LONG':
            orders.append(OrderEvent(timeindex=init_order_date, symbol=symbol, order_type=order_type, quantity=1/5*mkt_quantity, direction='BUY', smooth=0))
            orders.append(OrderEvent(timeindex=init_order_date, symbol=symbol, order_type=order_type, quantity=1/5*mkt_quantity, direction='BUY', smooth=1))
            orders.append(OrderEvent(timeindex=init_order_date, symbol=symbol, order_type=order_type, quantity=1/5*mkt_quantity, direction='BUY', smooth=2))
            orders.append(OrderEvent(timeindex=init_order_date, symbol=symbol, order_type=order_type, quantity=1/5*mkt_quantity, direction='BUY', smooth=3))
            orders.append(OrderEvent(timeindex=init_order_date, symbol=symbol, order_type=order_type, quantity=1/5*mkt_quantity, direction='BUY', smooth=4))
        if direction == 'SHORT':
            orders.append(OrderEvent(timeindex=init_order_date, symbol=symbol, order_type=order_type, quantity=1/5*mkt_quantity, direction='SELL', smooth=0))
            orders.append(OrderEvent(timeindex=init_order_date, symbol=symbol, order_type=order_type, quantity=1/5*mkt_quantity, direction='SELL', smooth=1))
            orders.append(OrderEvent(timeindex=init_order_date, symbol=symbol, order_type=order_type, quantity=1/5*mkt_quantity, direction='SELL', smooth=2))
            orders.append(OrderEvent(timeindex=init_order_date, symbol=symbol, order_type=order_type, quantity=1/5*mkt_quantity, direction='SELL', smooth=3))
            orders.append(OrderEvent(timeindex=init_order_date, symbol=symbol, order_type=order_type, quantity=1/5*mkt_quantity, direction='SELL', smooth=4))
        if direction == 'EXIT' and cur_quantity > 0:
            orders.append(OrderEvent(timeindex=init_order_date, symbol=symbol, order_type=order_type, quantity=1/5*abs(cur_quantity), direction='SELL', smooth=0))
            orders.append(OrderEvent(timeindex=init_order_date, symbol=symbol, order_type=order_type, quantity=1/5*abs(cur_quantity), direction='SELL', smooth=1))
            orders.append(OrderEvent(timeindex=init_order_date, symbol=symbol, order_type=order_type, quantity=1/5*abs(cur_quantity), direction='SELL', smooth=2))
            orders.append(OrderEvent(timeindex=init_order_date, symbol=symbol, order_type=order_type, quantity=1/5*abs(cur_quantity), direction='SELL', smooth=3))
            orders.append(OrderEvent(timeindex=init_order_date, symbol=symbol, order_type=order_type, quantity=1/5*abs(cur_quantity), direction='SELL', smooth=4))
        if direction == 'EXIT' and cur_quantity < 0:
            orders.append(OrderEvent(timeindex=init_order_date, symbol=symbol, order_type=order_type, quantity=1/5*abs(cur_quantity), direction='BUY', smooth=0))
            orders.append(OrderEvent(timeindex=init_order_date, symbol=symbol, order_type=order_type, quantity=1/5*abs(cur_quantity), direction='BUY', smooth=1))
            orders.append(OrderEvent(timeindex=init_order_date, symbol=symbol, order_type=order_type, quantity=1/5*abs(cur_quantity), direction='BUY', smooth=2))
            orders.append(OrderEvent(timeindex=init_order_date, symbol=symbol, order_type=order_type, quantity=1/5*abs(cur_quantity), direction='BUY', smooth=3))
            orders.append(OrderEvent(timeindex=init_order_date, symbol=symbol, order_type=order_type, quantity=1/5*abs(cur_quantity), direction='BUY', smooth=4))

        self.order_queue[symbol] += orders

    def update_signal(self, event):
        """
        Acts on a SignalEvent to generate new orders based on the portfolio logic.
        """
        if event.type == 'SIGNAL':
            if self.order_queue[event.symbol]:
                for index, order in enumerate(self.order_queue[event.symbol]):
                    # self.historical_signal() 已将smooth为1的symbol变为0
                    # New SignalEvent的T+0 Order的smooth参数为0，需要先还原以避免order明天的交易
                    self.order_queue[event.symbol][index].smooth += 1
            self.generate_smooth_order(event) # 生成新的Order
            if self.order_queue[event.symbol]:
                order_queue = []
                for order in self.order_queue[event.symbol]:
                    if order.smooth == 0:
                        self.events.put(order)
                        # print(order.symbol + " " + order.direction)
                    else:
                        order.smooth -= 1 # 还原本函数之前对smooth-1的处理
                        order_queue.append(order)
                self.order_queue[event.symbol] = order_queue

    def historical_signal(self, event):
        """
        Act on remaining order from historical SignalEvent due to lag and smoothing of portfolio management.
        """
        if event.type == 'MARKET':
            for symbol in self.symbol_list:
                if self.order_queue[symbol]:
                    order_queue = []
                    for order in self.order_queue[symbol]:
                        if order.smooth > 0:
                            order.smooth -= 1
                            order_queue.append(order)
                            print(order.symbol + ' ' + str(order.smooth + 1) + ' to ' + str(order.smooth))
                        else:
                            self.events.put(order)
                            print(order.symbol + ' Order at '+ order.timeindex.strftime('%Y-%m-%d'))
                    self.order_queue[symbol] = order_queue

    def create_equity_curve_dataframe(self):
        """
        Creates a Pandas DataFrame from the all_holdings list of dictionaries.
        """
        curve = pd.DataFrame(self.all_holdings)
        curve.set_index('datetime', inplace=True)
        #curve.fillna(method='ffill',axis=1,inplace=True)
        curve['returns'] = curve['total'].pct_change()
        curve['returns'][0] = 0.0
        curve['equity_curve'] = (1.0+curve['returns']).cumprod()
        self.equity_curve = curve

    def output_summary_stats(self, frequency = 252):
        """
        Creates a list of summary statistics for the portfolio.
        """
        total_return = self.equity_curve['equity_curve'][-1]
        returns = self.equity_curve['returns']
        pnl = self.equity_curve['equity_curve']

        sharpe_ratio = create_sharpe_ratio(returns, periods=frequency)
        drawdown, max_dd, dd_duration = create_drawdowns(pnl)
        self.equity_curve['drawdown'] = drawdown
        self.equity_curve['drawdown'][0] = 0.0

        stats = [("Total Return", "%0.2f%%" %  ((total_return - 1.0) * 100.0)), 
        ("Sharpe Ratio", "%0.2f" % sharpe_ratio), ("Max Drawdown", "%0.2f%%" % (max_dd * 100.0)), ("Drawdown Duration", "%d" % dd_duration)]

        self.equity_curve.to_csv('EquityCurve.csv')
        return stats

    def plot_summary(self):
        plt.style.use('seaborn')
        fig = plt.figure()
        ax1 = fig.add_subplot(311, ylabel='Portfolio value')
        self.equity_curve['equity_curve'].plot(ax=ax1, color="blue", lw=1.)
        ax2 = fig.add_subplot(312, ylabel='Period returns, %')
        self.equity_curve['returns'].plot(ax=ax2, color="black", lw=1.)
        ax3 = fig.add_subplot(313, ylabel='Drawdowns, %')
        (self.equity_curve['drawdown']*100).plot(ax=ax3, color="red", lw=1.)

        plt.gcf().autofmt_xdate()
        plt.tight_layout()
        plt.savefig('EquityCurve.png')
