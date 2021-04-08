# -*- coding: utf-8 -*-
"""
Created on Fri Apr  2 09:59:23 2021

@author: Xanxus10
"""
import time
import datetime
import pandas as pd

temp = []
data_start_date = datetime.datetime(2020,6,18,8,46,0)         #2020/12/17 08:46:00 
for i in range(0,412043):  #note + 1
    minute = (data_start_date + datetime.timedelta(minutes=i)).strftime("%Y/%m/%d %H:%M:%S")
    temp.append(minute)
time_index = pd.DatetimeIndex(temp)
print(time_index)