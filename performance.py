# -*- coding: utf-8 -*-
"""
Created on Sat Mar 27 11:28:01 2021

@author: Bear
"""

import numpy as np
import pandas as pd

def create_sharpe_ratio(returns, periods):            #缺monthly daily
    """
    Create the Sharpe ratio for the strategy, based on a
    benchmark of zero (i.e. no risk-free rate information).
    Parameters:
    returns - A pandas Series representing period percentage returns.
    periods - Daily (252), Hourly (252*6.5), Minutely(252*6.5*60) etc.
    """
   #一年平均報酬 , 一年平均標準差
    if np.std(returns) ==0:
        yearly_result = 0.0
        result = 0.0
    else:
        yearly_result = np.sqrt(periods) *  (np.mean(returns)) / np.std(returns)
        result = np.mean(returns) / np.std(returns)
    return yearly_result , result

def create_sortino_ratio(returns, periods):            #缺monthly daily,  yearly公式待確認
    
    negative_returns  = returns.loc[returns < np.mean(returns)]

    if np.std(negative_returns) ==0:   #np.std(negative_returns) : downside risk
        yearly_result = 0.0
        result = 0.0
    else:
        yearly_result = np.sqrt(periods) *  (np.mean(returns)) / np.std(negative_returns)
        result = np.mean(returns) / np.std(returns)
    return yearly_result , result

def create_calmar_ratio(returns, periods ,max_dd):            #缺monthly daily,  yearly公式待確認
    cum_prod = 1
    for i in range(1,len(returns)):
        cum_prod = cum_prod*(1+float(returns[i]))         #確認期數
    geometric_mean = cum_prod**(1/periods) -1
    
    if max_dd ==0:   #np.std(negative_returns) : downside risk
        result = 0.0
    else:
       
        result = geometric_mean / abs(max_dd)
    
    return  result


def create_skewness(returns,periods):   #缺daily monthly yearly
    
    minutely_skewness = returns.skew()
    
    return  minutely_skewness

def create_kurtosis(returns,periods):   #缺daily monthly yearly
    
    minutely_kurtosis = returns.kurt()
    
    return  minutely_kurtosis

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

#def create_winrate():
    