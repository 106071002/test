# -*- coding: utf-8 -*-
"""
Created on Fri Apr  2 09:05:54 2021

@author: Xanxus10
"""
import os
import datetime


""""""""""""""""""""""""""""""""""""""""""""""""


import queue
import pprint
import time

class Backtest(object):
    """
    Enscapsulates the settings and components for carrying out
    an event-driven backtest.
    """
    
    def __init__(
        self,
        csv_dir,
        symbol_list, 
        initial_capital,
        heartbeat, 
        start_date, 
        data_handler,
        execution_handler, 
        portfolio, 
        strategy
    ):
        """
        Initialises the backtest.
        
        Parameters:
        csv_dir - The hard root to the CSV data directory.
        symbol_list - The list of symbol strings.
        intial_capital - The starting capital for the portfolio.
        heartbeat - Backtest "heartbeat" in seconds
        start_date - The start datetime of the strategy.
        data_handler - (Class) Handles the market data feed.
        execution_handler - (Class) Handles the orders/fills for trades.
        portfolio - (Class) Keeps track of portfolio current
        and prior positions.
        strategy - (Class) Generates signals based on market data.
        """
        
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list
        self.initial_capital = initial_capital
        self.heartbeat = heartbeat
        self.start_date = start_date
        
        """
        _cls means this variable just use one time to construt 
        the item before _cls
        """
        self.data_handler_cls = data_handler
        self.execution_handler_cls = execution_handler
        self.portfolio_cls = portfolio
        self.strategy_cls = strategy
        
        self.events = queue.Queue()

        self.signals = 0
        self.orders = 0
        self.fills = 0
        # self.num_strats = 1
                
        self._generate_trading_instances()
        
        
        # print(type(self.csv_dir),self.csv_dir,"\n")
        # print(type(self.symbol_list),self.symbol_list,"\n")
        # print(type(self.initial_capital),self.initial_capital,"\n")
        # print(type(self.heartbeat),self.heartbeat,"\n")
        # print(type(self.start_date),self.start_date,"\n")
        # print(type(self.data_handler_cls),self.data_handler_cls,"\n")
        # print(type(self.execution_handler_cls),self.execution_handler_cls,"\n")
        # print(type(self.portfolio_cls),self.portfolio_cls,"\n")
        # print(type(self.strategy_cls),self.strategy_cls,"\n")
        # print(type(self.events),self.events,"\n")
        # print(type(self.signals),self.signals,"\n")
        # print(type(self.orders),self.orders,"\n")
        # print(type(self.fills),self.fills,"\n")
        # print(type(self.num_strats),self.num_strats,"\n")
        

    def _generate_trading_instances(self):
            """
            Generates the trading instance objects from
            their class types.
            """
            
            print(
                "Creating DataHandler, Strategy, Portfolio and ExecutionHandler \n"
            )
            self.data_handler = self.data_handler_cls(self.events, 
                                                      self.csv_dir,
                                                      self.symbol_list)
            self.strategy = self.strategy_cls(self.data_handler, self.events)
            self.portfolio = self.portfolio_cls(self.data_handler, self.events,
                                                self.start_date,
                                                self.initial_capital)
            self.execution_handler = self.execution_handler_cls(self.events)
            
            
            # print(type(self.data_handler),self.data_handler,"\n")
            # print(type(self.strategy),self.strategy,"\n")
            # print(type(self.portfolio),self.portfolio,"\n")
            # print(type(self.execution_handler),self.execution_handler,"\n")

    def _run_backtest(self):
        """
        Executes the backtest.
        """
        i = 0
        print("****", datetime.datetime.now(),"****\n")
        while True:
            i += 1
            # Update the market bars
            if self.data_handler.continue_backtest == True and i<6:
                self.data_handler.update_bars()
            else:
                print("\n****", datetime.datetime.now(),"****")
                # print(type(self.data_handler.events.queue),
                #       self.data_handler.events.queue,"\n")
                break
            
            # Handle the events
            while True:
                try:
                    event = self.events.get(False)
                    # print(type(event),event,"\n")
                except queue.Empty:
                    break
                else:
                    if event is not None:
                        if event.type == 'MARKET':
                            self.strategy.calculate_signals(event)
                            self.portfolio.update_timeindex(event)
                            
                        elif event.type == 'SIGNAL':
                            self.signals += 1
                            self.portfolio.update_signal(event)
                            
                        elif event.type == 'ORDER':
                            self.orders += 1
                            self.execution_handler.execute_order(event)
                            
                        elif event.type == 'FILL':
                            self.fills += 1
                            self.portfolio.update_fill(event)
            
            time.sleep(self.heartbeat) 
            #In a live environment this value will be a positive number,
            #such as 600 seconds (every ten minutes).
            
    
    def _output_performance(self):
        """
        Outputs the strategy performance from the backtest.
        """
        self.portfolio.create_equity_curve_dataframe()
        
        print("Creating summary stats...")
        stats = self.portfolio.output_summary_stats()
        # print("Creating equity curve...")
        
        # print(self.portfolio.equity_curve)
        # pprint.pprint(stats)
        
        # print("Signals: %s" % self.signals)
        # print("Orders: %s" % self.orders)
        # print("Fills: %s" % self.fills)
        
    
    def simulate_trading(self):
        """
        Simulates the backtest and outputs portfolio performance.
        """
        self._run_backtest()
        self._output_performance()

""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

from abc import ABCMeta, abstractmethod
import pandas as pd

class DataHandler(metaclass=ABCMeta):
    """
    DataHandler is an abstract base class providing an interface for
    all subsequent (inherited) data handlers (both live and historic).
    
    The goal of a (derived) DataHandler object is to output a generated
    set of bars (OHLCVI) for each symbol requested.
    
    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite.
    """
    
    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bars to the bars_queue for each symbol
        in a tuple OHLCVI format: (datetime, open, high, low,
        close, volume, open interest).
        """
        raise NotImplementedError("Should implement update_bars()")
    
    @abstractmethod
    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Returns the last N bar values from the
        latest_symbol list, or N-k if less available.
        """
        raise NotImplementedError("Should implement get_latest_bars_values()")
    
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
        Returns one of the Open, High, Low, Close, Volume or OI
        from the last bar.
        """
        raise NotImplementedError("Should implement get_latest_bar_value()")
    
class HistoricCSVDataHandler(DataHandler):
    """
    HistoricCSVDataHandler is designed to read CSV files for
    each requested symbol from disk and provide an interface
    to obtain the "latest" bar in a manner identical to a live
    trading interface.
    """
    
    def __init__(self, events, csv_dir, symbol_list):
        """
        Initialises the historic data handler by requesting
        the location of the CSV files and a list of symbols.
        
        It will be assumed that all files are of the form
        'symbol.csv', where symbol is a string in the list.
        
        Parameters:
        events - The Event Queue.
        csv_dir - Absolute directory path to the CSV files.
        symbol_list - A list of symbol strings.
        """
        self.events = events
        self.csv_dir = csv_dir
        self.symbol_list = symbol_list
        
        self.symbol_data = {}
        self.latest_symbol_data = {}
        self.continue_backtest = True
        
        # print(type(self.events),self.events,"\n")
        # print(type(self.csv_dir),self.csv_dir,"\n")
        # print(type(self.symbol_list),self.symbol_list,"\n")
        # print(type(self.symbol_data),self.symbol_data,"\n")
        # print(type(self.latest_symbol_data),self.latest_symbol_data,"\n")
        # print(type(self.continue_backtest),self.continue_backtest,"\n")
        
        self._open_convert_csv_files()

    def _open_convert_csv_files(self):
        """
        Opens the CSV files from the data directory, converting
        them into pandas DataFrames within a symbol dictionary.
        
        For this handler it will be assumed that the data is
        taken from Yahoo. Thus its format will be respected.
        """
        comb_index = None
        for s in self.symbol_list:
            # Load the CSV file with no header information, indexed on date
            self.symbol_data[s] = pd.io.parsers.read_csv(
                os.path.join(self.csv_dir, '%s.csv' % s),
                header=0, index_col=0, parse_dates=True,
                names=[
                    'datetime', 'open', 'high',
                    'low', 'close', 'adj_close', 'volume'
                ]
            ).sort_index()
            
    
            # Combine the index to pad forward values
            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index = comb_index.union(self.symbol_data[s].index)
           
            # Set the latest symbol_data to None
            self.latest_symbol_data[s] = []
        
        
        # Reindex the dataframes
        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(
                index=comb_index,method="pad").fillna(0).iterrows()
        
        # print(type(self.events),self.events,"\n")
        # print(type(self.csv_dir),self.csv_dir,"\n")
        # print(type(self.symbol_list),self.symbol_list,"\n")
        # print(type(self.symbol_data),self.symbol_data,"\n")
        # print(type(self.latest_symbol_data),self.latest_symbol_data,"\n")
        # print(type(self.continue_backtest),self.continue_backtest,"\n")
    

    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        """
        for s in self.symbol_list:
            try:
                bar = next(self._get_new_bar(s))
                # print(type(bar),bar,"\n")
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)
        self.events.put(MarketEvent())
        
        # print(type(self.events.queue),self.events.queue,"\n")
        # print(type(self.latest_symbol_data),self.latest_symbol_data,"\n")
        # print(type(self.latest_symbol_data["TXFL1"][-1][1]),
        #       self.latest_symbol_data["TXFL1"][-1][1],"\n")
        # print(type(len(self.latest_symbol_data["TXFL1"])),
        #       len(self.latest_symbol_data["TXFL1"]),"\n")
        # print(type([b[1]["open"] for b in self.latest_symbol_data["TXFL1"]]),
        #       [b[1]["open"] for b in self.latest_symbol_data["TXFL1"]],"\n")
    
    def _get_new_bar(self, symbol):
        """
        Returns the latest bar from the data feed.
        """
        # print(type(self.symbol_data[symbol]),self.symbol_data[symbol],"\n")
        for b in self.symbol_data[symbol]:
            # print(type(b),b,"\n")
            yield b
    
    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Returns the last N bar values from the
        latest_symbol list, or N-k if less available.
        """
        try:
            bars_list = self.get_latest_bars(symbol, N)
            # print(type(bars_list),bars_list,"\n")
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            # print(type([b[0] for b in bars_list]),
            #         [b[0] for b in bars_list],"\n")
            # print(type([b[1][val_type] for b in bars_list]),
            #         [b[1][val_type] for b in bars_list],"\n")
            # print(type([getattr(b[1], val_type) for b in bars_list]),
            #         [getattr(b[1], val_type) for b in bars_list],"\n")
            # print(type(np.array([getattr(b[1], val_type) for b in bars_list])),
            #         np.array([getattr(b[1], val_type) for b in bars_list]),"\n")
            return np.array([getattr(b[1], val_type) for b in bars_list])
    
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
            # print(type(bars_list),bars_list,"\n")
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            # print(type(bars_list[-N:]),bars_list[-N:],"\n")
            return bars_list[-N:]
    
    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
            # print(type(bars_list),bars_list,"\n")
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            # print(type(bars_list[-1]),bars_list[-1],"\n")
            # print(type(bars_list[-1][0]),bars_list[-1][0],"\n")
            return bars_list[-1][0]

    def get_latest_bar_value(self, symbol, val_type):
        """
        Returns one of the Open, High, Low, Close, Volume or OI
        values from the pandas Bar series object.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
            # print(type(bars_list),bars_list,"\n")
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            # print(type(bars_list[-1]),bars_list[-1],"\n")
            # print(type(bars_list[-1][1]),bars_list[-1][1],"\n")
            return getattr(bars_list[-1][1], val_type)

""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

class Strategy(metaclass=ABCMeta):
    """
    Strategy is an abstract base class providing an interface for
    all subsequent (inherited) strategy handling objects.
    The goal of a (derived) Strategy object is to generate Signal
    objects for particular symbols based on the inputs of Bars
    (OHLCV) generated by a DataHandler object.
    This is designed to work both with historic and live data as
    the Strategy object is agnostic to where the data came from,
    since it obtains the bar tuples from a queue object.
    """
        
    @abstractmethod
    def calculate_signals(self):
        """
        Provides the mechanisms to calculate the list of signals.
        """
        raise NotImplementedError("Should implement calculate_signals()")

import numpy as np

class MovingAverageCrossStrategy(Strategy):

    def __init__(self, bars, events, short_window=1, long_window=2):

        self.bars = bars # datahandler object
        self.symbol_list = self.bars.symbol_list
        self.events = events
        self.short_window = short_window
        self.long_window = long_window
        
        self.bought = self._calculate_initial_bought()
        
        
        # print(type(self.bars),self.bars,"\n")
        # print(type(self.symbol_list),self.symbol_list,"\n")
        # print(type(self.events),self.events,"\n")
        # print(type(self.short_window),self.short_window,"\n")
        # print(type(self.long_window),self.long_window,"\n")
        # print(type(self.bought),self.bought,"\n")

    def _calculate_initial_bought(self):
        bought = {}
        for s in self.symbol_list:
            bought[s] = 'OUT'
        return bought

    def calculate_signals(self, event):
        if event.type == 'MARKET':
            # print(type(self.symbol_list[-1]),self.symbol_list[-1],"\n")
            for s in [self.symbol_list[3]]:
                bars = self.bars.get_latest_bars_values(
                    s, "adj_close", N=self.long_window
                )   #<class 'numpy.ndarray'> [10930. 10929.] 
                # print(type(bars),bars,"\n") 
                
                bar_date = self.bars.get_latest_bar_datetime(s)
                # print(type(bar_date),bar_date,"\n")
                
                # print(type(bars.size),bars.size,"\n")
                if bars.size > 0:
                    short_sma = np.mean(bars[-self.short_window:])
                    # print(type(short_sma),short_sma,"\n")
                    # print("short_sma = ",short_sma,"\n")
                    
                    long_sma = np.mean(bars[-self.long_window:])
                    # print(type(long_sma),long_sma,"\n")
                    # print("long_sma = ",long_sma,"\n")
                    
                    symbol = s
                    dt = datetime.datetime.utcnow()
                    sig_dir = ""

                    if short_sma > long_sma and self.bought[s] == "OUT":
                        print("LONG: %s" % bar_date)
                        sig_dir = "LONG"

                        signal = SignalEvent(1, symbol, dt, sig_dir, 1.0)
                        # print(type(signal),signal,"\n")
                        self.events.put(signal)
                        self.bought[s] = "LONG"

                    elif short_sma < long_sma and self.bought[s] == "LONG":
                        print("SHORT: %s" % bar_date)
                        sig_dir = 'EXIT'
                        
                        signal = SignalEvent(1, symbol, dt, sig_dir, 1.0)
                        self.events.put(signal)
                        self.bought[s] = 'OUT'


""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

class Event(object):
    """
    Event is base class providing an interface for all subsequent
    (inherited) events, that will trigger further events in the
    trading infrastructure.
    """
    pass

class MarketEvent(Event):
    """
    Handles the event of receiving a new market update with
    corresponding bars.
    """
    def __init__(self):
        """
        Initialises the MarketEvent.
        """
        self.type = 'MARKET'

class SignalEvent(Event):
    """
    Handles the event of sending a Signal from a Strategy object.
    This is received by a Portfolio object and acted upon.
    """
    def __init__(self, strategy_id, symbol, datetime, signal_type, strength):
        """
        Initialises the SignalEvent.
        Parameters:
            strategy_id - The unique identifier for the strategy that
            generated the signal.
            symbol - The ticker symbol, e.g. 'GOOG'.
            datetime - The timestamp at which the signal was generated.
            signal_type - 'LONG' or 'SHORT'.
            strength - An adjustment factor "suggestion" used to scale
            quantity at the portfolio level. Useful for pairs strategies.       
        """
        self.type = "SIGNAL"
        self.strategy_id = strategy_id
        self.symbol = symbol
        self.datetime = datetime
        self.signal_type = signal_type
        self.strength = strength
        
        # print(type(self.type),self.type,"\n")
        # print(type(self.strategy_id),self.strategy_id,"\n")
        # print(type(self.symbol),self.symbol,"\n")
        # print(type(self.datetime),self.datetime,"\n")
        # print(type(self.signal_type),self.signal_type,"\n")
        # print(type(self.strength),self.strength,"\n")

class OrderEvent(Event): 
    """
    Handles the event of sending an Order to an execution system.
    The order contains a symbol (e.g. GOOG), a type (market or limit),
    quantity and a direction.
    """
    def __init__(self, symbol, price, quantity, order_type, direction):
        """
        Initialises the order type, setting whether it is
        a Market order ('MKT') or Limit order ('LMT'), has
        a quantity (integral) and it`s direction ('BUY' or'SELL').
        
        Parameters:
        symbol - The instrument to trade.
        order_type - 'MKT' or 'LMT' for Market or Limit.
        quantity - Non-negative integer for quantity.
        direction - 'BUY' or 'SELL' for long or short.
        """
        self.type = "ORDER"
        self.symbol = symbol
        self.price = price
        self.quantity = quantity
        self.order_type = order_type
        self.direction = direction
        
    def print_order(self):
        """
        Outputs the values within the Order.
        """
        print(
            "Order: Symbol=%s, Price=%s, Type=%s, Quantity=%s, Direction=%s" %
            (self.symbol, self.price, self.order_type,
             self.quantity, self.direction)
            )

class FillEvent(Event):
    """
    Encapsulates the notion of a Filled Order, as returned
    from a brokerage. Stores the quantity of an instrument
    actually filled and at what price. In addition, stores
    the trade_fee of the trade from the brokerage.
    """
    def __init__(self, timeindex, symbol, exchange, quantity,
                 direction, fill_cost, price, trade_fee=None):
        """
        Initialises the FillEvent object. Sets the symbol, exchange,
        quantity, direction, cost of fill and cost of trade.
        
        Parameters:
        timeindex - The bar-resolution when the order was filled.
        symbol - The instrument which was filled.
        exchange - The exchange where the order was filled.
        quantity - The filled quantity.
        direction - The direction of fill ('BUY' or 'SELL')
        fill_cost - The trading value, may be stock value, margin, premium.
        price - The market price of asset
        trade_fee - The trade_fee which contained .
        """
        
        self.type = 'FILL'
        self.timeindex = timeindex
        self.symbol = symbol
        self.exchange = exchange
        self.quantity = quantity
        self.direction = direction
        self.fill_cost = fill_cost
        self.price = price

        # Calculate trade_fee
        if trade_fee is None:
            self.trade_fee = self.calculate_trade_fee()
        else:
            self.trade_fee = trade_fee
        
        # print(type(self.type),self.type,"\n")
        # print(type(self.timeindex),self.timeindex,"\n")
        # print(type(self.symbol),self.symbol,"\n")
        # print(type(self.exchange),self.exchange,"\n")
        # print(type(self.quantity),self.quantity,"\n")
        # print(type(self.direction),self.direction,"\n")
        # print(type(self.fill_cost),self.fill_cost,"\n")
        # print(type(self.price),self.price,"\n")
        # print(type(self.trade_fee),self.trade_fee,"\n")
        
    def calculate_trade_fee(self):
        """
        Calculates the fees of trading 
        """
        trade_fee = 0.0
        tax = 0.0
        
        if self.symbol[0:3] == "TXF":
            trade_fee = 100.0
            tax = self.price * self.quantity * 200.0 * 0.00002
            
        elif self.symbol[0:2] == "MX" or self.symbol[0:2] == "MTX":
            trade_fee = 50.0
            tax = self.price * self.quantity * 50.0 * 0.00002
        
        elif self.symbol[0:3] == "TXO":
            trade_fee = 50.0
            tax = self.price * self.quantity * 50.0 * 0.001
        
        elif self.symbol[0:3] == "PUF" or self.symbol[0:2] == "QFF":
            trade_fee = 50.0
            tax = self.price * self.quantity * 100.0 *  0.00002
            
        else:
            trade_fee = 50.0
            tax = self.price * self.quantity * 2000.0 * 0.00002
        
        total_fee = trade_fee + tax
        
        return total_fee

""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

class Portfolio(object):
    """
    The Portfolio class handles the positions and market
    value of all instruments at a resolution of a "bar",
    i.e. secondly, minutely, 5-min, 30-min, 60 min or EOD.
    The positions DataFrame stores a time-index of the
    quantity of positions held.
    The holdings DataFrame stores the cash and total market
    holdings value of each symbol for a particular
    time-index, as well as the percentage change in
    portfolio total across bars.
    """
    def __init__(self, bars, events, start_date, initial_capital=5000000.0):
        """
        Initialises the portfolio with bars and an event queue.
        Also includes a starting datetime index and initial capital
        (USD unless otherwise stated).
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
        
        self.all_positions = self.construct_all_positions()
        self.current_positions = self.construct_current_positions()
        self.all_holdings = self.construct_all_holdings()
        self.current_holdings = self.construct_current_holdings()
        
        
        # print(type(self.bars),self.bars,"\n")
        # print(type(self.events),self.events,"\n")
        # print(type(self.symbol_list),self.symbol_list,"\n")
        # print(type(self.start_date),self.start_date,"\n")
        # print(type(self.initial_capital),self.initial_capital,"\n")
        # print(type(self.all_positions),self.all_positions,"\n")
        # print(type(self.current_positions),self.current_positions,"\n")
        # print(type(self.all_holdings),self.all_holdings,"\n")
        # print(type(self.current_holdings),self.current_holdings,"\n")
        
        
    def construct_all_positions(self):
        """
        Constructs the positions list using the start_date
        to determine when the time index will begin.
        """
        d = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )
        d['datetime'] = self.start_date
        return [d]  #list
    
    def construct_current_positions(self):
        """
        Constructs the positions list using the start_date
        to determine when the time index will begin.
        """
        d = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )
        return d   #dict
    
    def construct_all_holdings(self):
        """
        Constructs the holdings list using the start_date
        to determine when the time index will begin.
        """
        d = dict( (k,v) for k, v in [(s, 0.0) for s in self.symbol_list] )
        d['datetime'] = self.start_date
        d['cash'] = self.initial_capital
        d['trade_fee'] = 0.0
        d['total'] = self.initial_capital
        return [d]  #list
    
    def construct_current_holdings(self):
        """
        This constructs the dictionary which will hold the instantaneous
        value of the portfolio across all symbols.
        """
        d = dict( (k,v) for k, v in [(s, 0.0) for s in self.symbol_list] )
        d['cash'] = self.initial_capital
        d['trade_fee'] = 0.0
        d['total'] = self.initial_capital
        return d   #dict
    
    def update_timeindex(self, event):
        """
        Adds a new record to the positions matrix for the current
        market data bar. This reflects the PREVIOUS bar, i.e. all
        current market data at this stage is known (OHLCV).
        
        Makes use of a MarketEvent from the events queue.
        """
        # print(type(self.symbol_list),self.symbol_list,"\n")
        latest_datetime = self.bars.get_latest_bar_datetime(
            self.symbol_list[0] 
            # Maybe have some trouble in future
            # ans: It's fine, since we have already reindexed all time index 
        )
        
        # Update positions
        # ================
        dp = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )
        dp['datetime'] = latest_datetime
        
        for s in self.symbol_list:
            dp[s] = self.current_positions[s]
        # print(type(dp),dp,"\n")
        
        # Append the current positions
        self.all_positions.append(dp)
        # print(type(self.all_positions),self.all_positions,"\n")
        
        # Update holdings
        # ===============
        dh = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )
        dh['datetime'] = latest_datetime
        dh['cash'] = self.current_holdings['cash']
        dh['trade_fee'] = self.current_holdings['trade_fee']
        dh['total'] = self.current_holdings['cash'] #reset total to current cash
        
        for s in self.symbol_list:
            # Approximation to the real value
            market_value = self.current_positions[s] * \
                self.bars.get_latest_bar_value(s, "adj_close")
            dh[s] = market_value
            self.current_holdings['total'] += market_value - self.current_holdings[s]
            self.current_holdings[s] = market_value
            dh['total'] += market_value  #this value reset above to current cash
            
        
        # Append the current holdings
        self.all_holdings.append(dh)
        # print(type(self.all_holdings),self.all_holdings,"\n")
        # print(type(self.current_holdings),self.current_holdings,"\n")
    
    def update_signal(self, event):
        """
        Acts on a SignalEvent to generate new orders
        based on the portfolio logic.
        """
        if event.type == "SIGNAL":
            order_event = self.generate_naive_order(event)
            # print(type(order_event),order_event,"\n")
            self.events.put(order_event)
            # print(type(self.events.queue),self.events.queue,"\n")
    
    def generate_naive_order(self, signal):
        """
        Simply files an Order object as a constant quantity
        sizing of the signal object, without risk management or
        position sizing considerations.
        
        Parameters:
        signal - The tuple containing Signal information.
        """
        
        order = None
        symbol = signal.symbol
        price = self.bars.get_latest_bar_value(symbol, "adj_close")
        mkt_quantity = 2
        order_type = "MKT"
        
        direction = signal.signal_type
        # strength = signal.strength
        cur_quantity = self.current_positions[symbol]
        # print(type(cur_quantity),cur_quantity,"\n")
        
        if direction == "LONG" and cur_quantity == 0:
            order = OrderEvent(symbol, price, mkt_quantity, order_type, "BUY")
        if direction == "SHORT" and cur_quantity == 0:
            order = OrderEvent(symbol, price, mkt_quantity, order_type, "SELL")
        if direction == "EXIT" and cur_quantity > 0:
            order = OrderEvent(symbol, price, abs(cur_quantity), order_type, "SELL")
        if direction == "EXIT" and cur_quantity < 0:
            order = OrderEvent(symbol, price, abs(cur_quantity), order_type, "BUY")
        return order
    
    def update_fill(self, event):
        """
        Updates the portfolio current positions and holdings
        from a FillEvent.
        """
        if event.type == 'FILL':
            self.update_positions_from_fill(event)
            self.update_holdings_from_fill(event)
    
    def update_positions_from_fill(self, fill):
        """
        Takes a Fill object and updates the position matrix to
        reflect the new position.
        
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
        # print(type(self.current_positions[fill.symbol]),
        #       self.current_positions[fill.symbol],"\n")
        
    def update_holdings_from_fill(self, fill):
        """
        Takes a Fill object and updates the holdings matrix to
        reflect the holdings value.
        
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
        cost = fill_dir * fill.price * fill.quantity
        self.current_holdings[fill.symbol] += cost
        self.current_holdings['trade_fee'] += fill.trade_fee
        self.current_holdings['cash'] -= (cost + fill.trade_fee)
        self.current_holdings['total'] -= (fill.trade_fee)
        # print(type(self.all_holdings),self.all_holdings,"\n")
        # print(type(self.current_holdings),self.current_holdings,"\n")
        
    def create_equity_curve_dataframe(self):
        """
        Creates a pandas DataFrame from the all_holdings
        list of dictionaries.
        """
        curve = pd.DataFrame(self.all_holdings)
        curve.set_index("datetime", inplace=True)
        curve["returns"] = curve["total"].pct_change()
        curve["equity_curve"] = (1.0+curve["returns"]).cumprod()
        self.equity_curve = curve
        
    def output_summary_stats(self):
        """
        Creates a list of summary statistics for the portfolio.
        """
        total_return = self.equity_curve["equity_curve"][-1]
        returns = self.equity_curve["returns"]
        pnl = self.equity_curve["equity_curve"]
        periods = self.equity_curve.shape[0]
        sharpe_ratio = create_sharpe_ratio(returns,periods)
        drawdown, max_dd, dd_duration = create_drawdowns(pnl)
        self.equity_curve["drawdown"] = drawdown
        stats = [("Total Return", "%0.2f%%" % ((total_return - 1.0) * 100.0)),
                 ("Sharpe Ratio", "%0.2f" % sharpe_ratio),
                 ("Max Drawdown", "%0.2f%%" % (max_dd * 100.0)),
                 ("Drawdown Duration", "%d" % dd_duration)]
        self.equity_curve.to_csv('equity.csv')
        return stats

""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

class ExecutionHandler(metaclass=ABCMeta):
    """
    The ExecutionHandler abstract class handles the interaction
    between a set of order objects generated by a Portfolio and
    the ultimate set of Fill objects that actually occur in the
    market.
    The handlers can be used to subclass simulated brokerages
    or live brokerages, with identical interfaces. This allows
    strategies to be backtested in a very similar manner to the
    live trading engine.
    """
    
    @abstractmethod
    def execute_order(self, event):
        """
        Takes an Order event and executes it, producing
        a Fill event that gets placed onto the Events queue.
        
        Parameters:
        event - Contains an Event object with order information.
        """
        raise NotImplementedError("Should implement execute_order()")
        
class SimulatedExecutionHandler(ExecutionHandler):
    """
    The simulated execution handler simply converts all order
    objects into their equivalent fill objects automatically
    without latency, slippage or fill-ratio issues.
    This allows a straightforward "first go" test of any strategy,
    before implementation with a more sophisticated execution
    handler.
    """
    def __init__(self, events):
        """
        Initialises the handler, setting the event queues
        up internally.
        
        Parameters:
        events - The Queue of Event objects.
        """
        self.events = events
        
    def execute_order(self, event):
        """
        Simply converts Order objects into Fill objects naively,
        i.e. without any latency, slippage or fill ratio problems.
        
        Parameters:
        event - Contains an Event object with order information.
        """
        if event.type == "ORDER":
            initial_margin = 167000.0
            fill_cost = event.quantity * initial_margin
            fill_event = FillEvent(datetime.datetime.utcnow(), 
                                   event.symbol,
                                   "TAIFEX",
                                   event.quantity,
                                   event.direction,
                                   fill_cost,
                                   event.price,
                                   None)
            self.events.put(fill_event)

""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

def create_sharpe_ratio(returns, periods):
    """
    Create the Sharpe ratio for the strategy, based on a
    benchmark of zero (i.e. no risk-free rate information).
    Parameters:
    returns - A pandas Series representing period percentage returns.
    periods - Daily (252), Hourly (252*6.5), Minutely(252*6.5*60) etc.
    """
   #一年平均報酬 , 一年平均標準差
    if np.std(returns) == 0:
        result = 0.0
    else:
        result = np.sqrt(periods) * (np.mean(returns)) / np.std(returns)
    return result

def create_drawdowns(pnl):
    """
    Calculate the largest peak-to-trough drawdown of the PnL curve
    as well as the duration of the drawdown. Requires that the
    pnl_returns is a pandas Series.
    
    Parameters:
    pnl - A pandas Series representing period percentage returns.
    Returns:
    drawdown, duration - Highest peak-to-trough drawdown and duration.
    """
    # Calculate the cumulative returns curve
    # and set up the High Water Mark
    hwm = [0]
    # Create the drawdown and duration series
    idx = pnl.index
    drawdown = pd.Series(index = idx)
    duration = pd.Series(index = idx)
    # Loop over the index range
   
    for t in range(1, len(idx)):
        hwm.append(max(hwm[t-1], pnl[t]))  #check after!!!
        drawdown[t]= (hwm[t]-pnl[t])
        duration[t]= (0 if drawdown[t] == 0 else duration[t-1]+1)
    
    return drawdown, drawdown.max(), duration.max()

""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""

if __name__ == "__main__":
    
    csv_dir = "C:/Users/Pro01/Desktop/derivatives_auto_trading/csv_dir/" # CHANGE THIS!
    symbol_list = []
    for i in range(0,len(os.listdir(csv_dir))):
        symbol = os.listdir(csv_dir)[i].strip(".csv")
        symbol_list.append(symbol)
    # print(type(symbol_list),symbol_list,"\n")
    
    initial_capital = 5000000.0
    heartbeat = 0.0 # the speed of reading data from market
    start_date = datetime.datetime(2020, 3, 23, 0, 0, 0)
   
    backtest = Backtest(
        csv_dir,
        symbol_list,
        initial_capital,
        heartbeat,
        start_date,
        HistoricCSVDataHandler,
        SimulatedExecutionHandler,
        Portfolio,
        MovingAverageCrossStrategy
    )
    
    backtest.simulate_trading()

