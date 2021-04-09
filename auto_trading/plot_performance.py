# -*- coding: utf-8 -*-
"""
Created on Sun Mar 28 11:45:01 2021

@author: Xanxus10
"""

import datetime
import pandas as pd
import matplotlib.pyplot as plt
import os

from backtest import Backtest
from data import HistoricCSVDataHandler
from execution import SimulatedExecutionHandler
from portfolio import Portfolio
from mac import MovingAverageCrossStrategy


if __name__ == "__main__":
    
    csv_dir = "C:/Users/Bear/Desktop/auto_trading/csv_dir/" # CHANGE THIS!
    temp_symbol_list = []  
    for i in range(0,len(os.listdir(csv_dir))):
        symbol = os.listdir(csv_dir)[i].strip(".csv")
        temp_symbol_list.append(symbol)
        """      
        if symbol[-2] == "F":     #選擇特定月份資料 EX.六月 (F)
            temp_symbol_list.append(symbol)
        """             
    symbol_list = temp_symbol_list
    #print(symbol_list)
    
    initial_capital = 5000000.0
    heartbeat = 0.0 #吃市場資料的速度
    start_date = datetime.datetime(2020, 3, 23, 0, 0, 0)
    backtest = Backtest(
    csv_dir, symbol_list, initial_capital, heartbeat,
    start_date, HistoricCSVDataHandler, SimulatedExecutionHandler,
    Portfolio, MovingAverageCrossStrategy
    )
    
    backtest.simulate_trading()
    
    data = pd.io.parsers.read_csv(
        "equity.csv", header=0,                                 
        parse_dates=True, index_col=0
    ).sort_index()
    
    # Plot three charts: Equity curve,
    # period returns, drawdowns
    fig = plt.figure(figsize=(12,10))
    # Set the outer colour to white
    fig.patch.set_facecolor('white')
    # Plot the equity curve
    ax1 = fig.add_subplot(311)
    data['equity_curve'].plot(ax=ax1, color="blue", lw=2.)
    plt.grid(True)
    plt.xlabel('')
    plt.ylabel('Portfolio value, %',fontsize = 16)
    # Plot the returns
    ax2 = fig.add_subplot(312)
    data['returns'].plot(ax=ax2, color="black", lw=2.)
    plt.grid(True)
    plt.xlabel('')
    plt.ylabel('Period returns, %',fontsize = 16)
    # Plot the drawdown
    ax3 = fig.add_subplot(313)
    data['drawdown'].plot(ax=ax3, color="red", lw=2.)
    plt.grid(True)
    plt.xlabel('Datetime',fontsize = 18)
    plt.ylabel('Drawdowns, %',fontsize = 16)
    #保持子圖間距
    fig.tight_layout()
    # Plot the figure
    plt.show()