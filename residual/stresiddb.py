#coding: utf8

import sys
from sqlalchemy.sql import select, update, insert
from sqlalchemy.orm import Session
from sqlalchemy import (
				MetaData, 
				Table, 
				Column,
				Integer,
				Float,
				Numeric,
				String,
				DateTime,
				ForeignKey
				)
import dbaccessor
import pandas as pd
import datetime
import numpy as np

import dataapi
import fileutil
import dbutil

def insertMonthly(conn, df):
	"""	插入每月因子残差数据表
	"""
	dbutil.insertResid(conn, df, 'FactorMonthlyResidDelta')

def insertAllStocks(filepath, stocks, dtype):
	engine = dbaccessor.getdb('localhost', 'zhenggq', 'Yuzhong0931', 'advancedb', 1433)
	for stock in stocks:
			print 'insert: {0}'.format(stock)
			start = datetime.datetime.now()

			df = fileutil.loadStockPickle(filepath, stock)
			
			if df is not None and len(df) > 0:
					conn = engine.connect()
					df['SecuAbbr'] = df['SecuAbbr'].apply(lambda x:x.encode('raw-unicode-escape').decode('gbk'))
					insertMonthly(conn, df)

			end = datetime.datetime.now()
			print 'Cost: {0} on {1}'.format(end-start, stock)

if __name__ == '__main__':
	"""用法： python stresiddb.py 'd|w|m' '000001' '000231' ['filepath']
	"""
	dtype = 'd'
	start = '000001'
	end = '603999'
	filepath = ''

	params = sys.argv[1:]
	
	if len(params) >= 1:
		dtype = params[0]
	if len(params) >= 3:
		start = params[1]
		end = params[2]
	if len(params) >= 4:
		filepath=params[3]
	
	if len(filepath) == 0:
		filepath='D:/workspace/python/residual/stock/monthly/'
	
	print 'dtype: {0}, start {1}, end {2}, result path: {3}'.format(dtype, start, end, filepath)
	
	sdf = dataapi.getMonthlyStocks()
	stocks = sdf['SecuCode'].tolist()

	vsts = [i for i in stocks if i >= start and i <= end]
	insertAllStocks(filepath, vsts, dtype)
