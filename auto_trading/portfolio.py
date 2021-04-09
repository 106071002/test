# -*- coding: utf-8 -*-
"""
Created on Sat Mar 27 11:16:47 2021

@author: Bear
"""

import pandas as pd

from event import OrderEvent
from performance import create_sharpe_ratio, create_drawdowns,create_sortino_ratio, \
    create_skewness,create_kurtosis,create_calmar_ratio


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
        self.current_positions = dict( (k,v) for k, v in \
                                      [(s, 0) for s in self.symbol_list] )
        self.all_holdings = self.construct_all_holdings()
        self.current_holdings = self.construct_current_holdings()
        
    def construct_all_positions(self):
        """
        Constructs the positions list using the start_date
        to determine when the time index will begin.
        """
        d = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )
        d['datetime'] = self.start_date
        return [d]  #list

    def construct_all_holdings(self):
        """
        Constructs the holdings list using the start_date
        to determine when the time index will begin.
        """
        d = dict( (k,v) for k, v in [(s, 0.0) for s in self.symbol_list] )
        d['datetime'] = self.start_date
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return [d]  #list
    
    def construct_current_holdings(self):
        """
        This constructs the dictionary which will hold the instantaneous
        value of the portfolio across all symbols.
        """
        d = dict( (k,v) for k, v in [(s, 0.0) for s in self.symbol_list] )
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return d
    
    def update_timeindex(self, event):
        """
        Adds a new record to the positions matrix for the current
        market data bar. This reflects the PREVIOUS bar, i.e. all
        current market data at this stage is known (OHLCV).
        
        Makes use of a MarketEvent from the events queue.
        """
        latest_datetime = self.bars.get_latest_bar_datetime(
            self.symbol_list[0] # maybe have some trouble in future
        )
        
        # Update positions
        # ================
        dp = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )
        dp['datetime'] = latest_datetime
        
        for s in self.symbol_list:
            dp[s] = self.current_positions[s]
        
        # Append the current positions
        self.all_positions.append(dp)
        
        # Update holdings
        # ===============
        dh = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )
        dh['datetime'] = latest_datetime
        dh['cash'] = self.current_holdings['cash']
        dh['commission'] = self.current_holdings['commission']
        dh['total'] = self.current_holdings['cash']
        
        for s in self.symbol_list:
            # Approximation to the real value
            market_value = self.current_positions[s] * \
                self.bars.get_latest_bar_value(s, "adj_close")
            dh[s] = market_value
            dh['total'] += market_value  #this value reset above to current cash
        
        # Append the current holdings
        self.all_holdings.append(dh) #"ticker", "datetime", "cash", "commission", "total"

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
        self.current_positions[fill.symbol] += fill_dir*fill.quantity
        
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
        fill_cost = self.bars.get_latest_bar_value(fill.symbol, "adj_close")
        cost = fill_dir * fill_cost * fill.quantity
        self.current_holdings[fill.symbol] += cost
        self.current_holdings['commission'] += fill.commission
        self.current_holdings['cash'] -= (cost + fill.commission)
        self.current_holdings['total'] -= (cost + fill.commission)

    def update_fill(self, event):
        """
        Updates the portfolio current positions and holdings
        from a FillEvent.
        """
        if event.type == 'FILL':
            self.update_positions_from_fill(event)
            self.update_holdings_from_fill(event)

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
        direction = signal.signal_type
        strength = signal.strength
        mkt_quantity = 100
        cur_quantity = self.current_positions[symbol]
        order_type = "MKT"
        if direction == "LONG" and cur_quantity == 0:
            order = OrderEvent(symbol, order_type, mkt_quantity, "BUY")
        if direction == "SHORT" and cur_quantity == 0:
            order = OrderEvent(symbol, order_type, mkt_quantity, "SELL")
        if direction == "EXIT" and cur_quantity > 0:
            order = OrderEvent(symbol, order_type, abs(cur_quantity), "SELL")
        if direction == "EXIT" and cur_quantity < 0:
            order = OrderEvent(symbol, order_type, abs(cur_quantity), "BUY")
        return order

    def update_signal(self, event):
        """
        Acts on a SignalEvent to generate new orders
        based on the portfolio logic.
        """
        if event.type == "SIGNAL":
            order_event = self.generate_naive_order(event)
            self.events.put(order_event)

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
        
        start_date = self.equity_curve.index[1]  #注意，一般來說應該要是[0]，選擇[1]是因為資料不足一年以上 
        end_date = self.equity_curve.index[-1]
        time_delta_days = (end_date - start_date).days
        
        
        cmd_list = ["TXFC2","TXFD1","TXFE1","TXFL1"]  #先寫死，之後再改成讀入
        win = 0  #賺錢次數
        lose = 0   #賠錢次數
        entry_amount = 0  #進場金額
        out_amount = 0 #出場金額
        record_amount = {}
        record_amount["entry_amount"] = []
        record_amount["out_amount"] = []
        record_amount["profit"] = []
        record_amount["loss"] = []

        for commodity in cmd_list:
            for j in range(0,len(self.equity_curve[commodity])-1):
                if ((self.equity_curve[commodity][j+1] > 
                     self.equity_curve[commodity][j]) and 
                    self.equity_curve[commodity][j] == 0):
                    entry_amount = self.equity_curve[commodity][j+1]
                    record_amount["entry_amount"].append(entry_amount)
                if (self.equity_curve[commodity][j]!= 0 and 
                    self.equity_curve[commodity][j+1] == 0):
                    out_amount = self.equity_curve[commodity][j]
                    record_amount["out_amount"].append(out_amount)
        
        for i in range(0,len(record_amount["out_amount"])):
            profit  = record_amount["out_amount"][i] - record_amount["entry_amount"][i] 
            if profit > 0 :       
                win = win + 1
                record_amount["profit"].append(profit)
            elif profit <= 0 :
                lose = lose + 1
                record_amount["loss"].append(profit)

        win_rate = win / (win+lose) #勝率
        #賺賠比 = gross profit / gross loss
        profit_factor  = (-1) * sum(record_amount["profit"]) /  sum(record_amount["loss"]) 
        #CAGR = (初始價值/結束價值)**(1/年化期數)-1
        CAGR = (self.equity_curve["total"][-1]/self.equity_curve["total"][0])**(1/(time_delta_days/252)) - 1  
        
        yearly_sharpe_ratio,minutely_sharpe_ratio = create_sharpe_ratio(returns,periods)
        yearly_sortino_ratio,minutely_sortino_ratio = create_sortino_ratio(returns,periods)
        minutely_skewness = create_skewness(returns,periods)
        minutely_kurtosis = create_kurtosis(returns,periods)
        drawdown, max_dd, dd_duration = create_drawdowns(pnl)
        minutely_calmar_ratio = create_calmar_ratio(returns,periods,max_dd)
        self.equity_curve["drawdown"] = drawdown
        stats = ["Start Date:" + str(start_date),
                 "End Date:" + str(end_date) ,
                 ("Total Return", "%0.2f%%" % ((total_return - 1.0) * 100.0)),
                 ("Minutely Sharpe Ratio", "%0.4f" % minutely_sharpe_ratio),
                 ("Minutely Sortino Ratio", "%0.4f" % minutely_sortino_ratio),
                 ("Minutely Skewness","%0.3f" %minutely_skewness),
                 ("Minutely Kurtosis","%0.3f" %minutely_kurtosis),
                 ("Minutely Calmar Ratio", "%0.6f" % minutely_calmar_ratio),
                 ("CAGR", "%0.5f%%" % (CAGR * 100.0)),
                 ("Yearly Sharpe Ratio", "%0.2f" % yearly_sharpe_ratio),
                 ("Yearly Sortino Ratio", "%0.2f" % yearly_sortino_ratio),
                 ("Max Drawdown", "%0.2f%%" % (max_dd * 100.0)),
                 ("Drawdown Duration", "%d" % dd_duration),
                 ("Win Rate","%0.2f" % win_rate),
                 ("Profit Factor","%0.2f" % profit_factor)
                ]

        self.equity_curve.to_csv('equity.csv')
        return stats