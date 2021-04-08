# -*- coding: utf-8 -*-
"""
Created on Sat Mar 27 10:24:01 2021

@author: Bear
"""
from abc import ABCMeta, abstractmethod
from event import MarketEvent

import os, os.path
import numpy as np
import pandas as pd
import time
import datetime

###data handler        
class DataHandler(object):
    """
    DataHandler is an abstract base class providing an interface for
    all subsequent (inherited) data handlers (both live and historic).
    
    The goal of a (derived) DataHandler object is to output a generated
    set of bars (OHLCVI) for each symbol requested.
    
    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite.
    """
    
    __metaclass__ = ABCMeta

    @abstractmethod
    def get_latest_bar(self, symbol):
        """
        Returns the last bar updated.
        """
        raise NotImplementedError("Should implement get_latest_bar()")
        
    @abstractmethod
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
    
    @abstractmethod
    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Returns the last N bar values from the
        latest_symbol list, or N-k if less available.
        """
        raise NotImplementedError("Should implement get_latest_bars_values()")
        
    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bars to the bars_queue for each symbol
        in a tuple OHLCVI format: (datetime, open, high, low,
        close, volume, open interest).
        """
        raise NotImplementedError("Should implement update_bars()")

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
        ’symbol.csv’, where symbol is a string in the list.
        
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
        
        self._open_convert_csv_files()

    
    def _open_convert_csv_files(self):
        """
        Opens the CSV files from the data directory, converting
        them into pandas DataFrames within a symbol dictionary.
        
        For this handler it will be assumed that the data is
        taken from Yahoo. Thus its format will be respected.
        """
        
        data_start_date = datetime.datetime(2020,12,17,8,46,0) 
        data_end_date = datetime.datetime(2021,3,31,12,8,0)
        periods = int((data_end_date - data_start_date).days *24*60 + (data_end_date - data_start_date).seconds / 60)
        
        standard_date = [] #只有年月日
        standard_df = pd.read_csv("TXFF1.csv")   #用來當標準時間的CSV
        standard_datetime = standard_df["Date"]   #年月日時分秒
        standard_datetime.index = pd.DatetimeIndex(standard_datetime)  
        standard_datetime_day = list(standard_datetime.between_time("8:46:0","13:45:0"))  #只取日盤時間
        standard_datetime = standard_datetime_day
        standard_datetime.sort()
        standard_datetime = pd.Series(standard_datetime)
        for i in range(0,len(standard_datetime)):
            standard_date.append(standard_datetime[i][0:10])  
        standard_date = list(set(standard_date)) #避免重複
        standard_date.sort()
        start_date_index = standard_date.index(str(data_start_date)[0:10]) #找出起始日在standart_date的index
        standard_date = standard_date[start_date_index:len(standard_date)]
  
        temp = []
        
        for i in range(0,periods+1):                         
            minute = (data_start_date + datetime.timedelta(minutes=i)).strftime("%Y-%m-%d %H:%M:%S")
            temp.append(minute)
        
        #只取日盤時間
        temp = pd.Series(temp)
        temp.index = pd.DatetimeIndex(temp)
        temp_day = list(temp.between_time("8:46:0","13:45:0"))       
        temp = temp_day
        temp.sort() 
        
        for i in range(len(temp)-1,-1,-1):
            if temp[i][0:10] not in standard_date:   #如果日期不在標準日期裡面就拿掉 第一層判斷
                temp.remove(temp[i])  #去掉temp[i]元素
           
        time_index = pd.DatetimeIndex(temp)
      
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
               
            # Set the latest symbol_data to None
            self.latest_symbol_data[s] = []
        
        # Reindex the dataframes
        for s in self.symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(
                index=time_index,method="pad").fillna(0).iterrows()
    
    def _get_new_bar(self, symbol):
        """
        Returns the latest bar from the data feed.
        """
        for b in self.symbol_data[symbol]:
            yield b
    
    def get_latest_bar(self, symbol):
        """
        Returns the last bar from the latest_symbol list.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1]

    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or N-k if less available.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-N:]
    
    def get_latest_bar_datetime(self, symbol):
        """
        Returns a Python datetime object for the last bar.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1][0]

    def get_latest_bar_value(self, symbol, val_type):
        """
        Returns one of the Open, High, Low, Close, Volume or OI
        values from the pandas Bar series object.
        """
        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return getattr(bars_list[-1][1], val_type)

    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        Returns the last N bar values from the
        latest_symbol list, or N-k if less available.
        """
        try:
            bars_list = self.get_latest_bars(symbol, N)
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return np.array([getattr(b[1], val_type) for b in bars_list])

    def update_bars(self):
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        """
        for s in self.symbol_list:
            try:
                bar = next(self._get_new_bar(s))
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)
        self.events.put(MarketEvent())
