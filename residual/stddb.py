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
	dbutil.insertResid(conn, df, 'FactorMonthlyIndustryStd')

def insertAllData(filepath, tradingDays, dtype):
	#engine = dbaccessor.getdb('176.1.11.55', 'zhenggq', 'Yuzhong0931', 'advancedb', 1433)
	#engine = dbaccessor.getdb('localhost', 'zhenggq', 'yuzhong', 'advancedb', 1433)
	engine = dbaccessor.getdb('localhost', 'zhenggq', 'Yuzhong0931', 'advancedb', 1433)
	for td in tradingDays:
		print 'insert: {0}'.format(td.strftime('%Y%m%d'))
		start = datetime.datetime.now()

		df = fileutil.loadPickle(filepath, td)
		if df is not None:
			#中文被错误处理成unicode编码，而实际上是gbk编码
			df['SecuAbbr'] = df['SecuAbbr'].apply(lambda x:x.encode('raw-unicode-escape').decode('gbk'))
			#df['SecuAbbr'] = df['SecuAbbr'].apply(lambda x:x.decode('gbk'))
			conn = engine.connect()
			insertMonthly(conn, df)
		
		end = datetime.datetime.now()
		print 'Cost: {0} on {1}'.format(end-start, td.strftime('%Y%m%d'))

if __name__ == '__main__':
	"""用法： python stddb.py 'd|w|m' '20140101' '20141231' ['filepath']
	"""
	dtype = 'd'
	start = datetime.datetime.min
	end = datetime.datetime.now()
	filepath = ''

	params = sys.argv[1:]
	
	if len(params) >= 1:
		dtype = params[0]
	if len(params) >= 3:
		start = datetime.datetime.strptime(params[1], '%Y%m%d')
		end = datetime.datetime.strptime(params[2], '%Y%m%d')
	if len(params) >= 4:
		filepath=params[3]
	
	if len(filepath) == 0:
		filepath='D:/workspace/python/residual/result/monthly/'
	
	print 'dtype: {0}, start {1}, end {2}, result path: {3}'.format(dtype, start.strftime('%Y%m%d'), end.strftime('%Y%m%d'), filepath)
	
	tradingDays = dataapi.getDayList(dtype)
	tds = [i for i in tradingDays if i >= start and i <= end]
	insertAllData(filepath, tds, dtype)
