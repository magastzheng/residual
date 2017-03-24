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

def insertDaily(conn, df):
	"""	插入每日因子残差数据表
	"""
	metadata = MetaData()
	residtb = Table('FactorDailyResidual', metadata,
					Column('TradingDay', DateTime(), nullable=False),
					Column('SecuCode', String(10), nullable=False),
					Column('ClosePrice', Numeric(16, 4)),
					Column('OpenPrice', Numeric(16, 4)),
					Column('HighPrice', Numeric(16, 4)),
					Column('LowPrice', Numeric(16, 4)),
					Column('ExeClosePrice', Numeric(16, 4)),
					Column('ExeOpenPrice', Numeric(16, 4)),
					Column('ExeHighPrice', Numeric(16, 4)),
					Column('ExeLowPrice', Numeric(16, 4)),
					Column('NonRestrictedShares', Numeric(16, 0)),
					Column('AFloats', Numeric(16, 0)),
					Column('TotalShares', Numeric(16, 0)),
					Column('TurnoverVolume', Numeric(20, 0)),
					Column('NonRestrictedCap', Numeric(36, 4)),
					Column('AFloatsCap', Numeric(36, 4)),
					Column('TotalCap', Numeric(36, 4)),
					Column('SecuAbbr', String(50)),
					Column('IndustrySecuCode_I', String(10)),
					Column('PE', Numeric(38, 6)),
					Column('PB', Numeric(38, 6)),
					Column('PS', Numeric(38, 6)),
					Column('PCF', Numeric(38, 6)),
					Column('DividendYield', Numeric(16, 6)),
					Column('DividendRatio', Numeric(16, 6)),
					Column('TTMIncome', Numeric(36, 4)),
					Column('GP_Margin', Numeric(16, 4)),
					Column('NP_Margin', Numeric(16, 4)),
					Column('ROA', Numeric(16, 4)),
					Column('ROE', Numeric(16, 4)),
					Column('AssetsTurnover', Numeric(16, 4)),
					Column('EquityTurnover', Numeric(16, 4)),
					Column('Cash_to_Assets', Numeric(16, 4)),
					Column('Liability_to_Assets', Numeric(16, 4)),
					Column('EquityMultiplier', Numeric(16, 4)),
					Column('CurrentRatio', Numeric(16, 4)),
					Column('Income_Growth_YOY_Comparable', Numeric(16, 4)),
					Column('NPPC_Growth_YOY_Comparable', Numeric(16, 4)),
					Column('GP_Margin_Comparable', Numeric(16, 4)),
					Column('GP_Margin_Growth_YOY_Comparable', Numeric(16, 4)),
					Column('NP_Margin_Comparable', Numeric(16, 4)),
					Column('NP_Margin_Growth_YOY_Comparable', Numeric(16, 4)),
					Column('Income_Growth_Pre_Comparable', Numeric(16, 4)),
					Column('NPPC_Growth_Pre_Comparable', Numeric(16, 4)),
					Column('GP_Margin_Growth_Pre_Comparable', Numeric(16, 4)),
					Column('NP_Margin_Growth_Pre_Comparable', Numeric(16, 4)),
					Column('NPPC_Growth_Pre_Season', Numeric(16, 4)),
					Column('NPPC_Growth_YOY_Season', Numeric(16, 4)),
					Column('NPLNRP_Growth_Pre_Season', Numeric(32, 14)),
					Column('NPLNRP_Growth_YOY_Season', Numeric(32, 15)),
					Column('Income_Growth_Pre_Season', Numeric(16, 4)),
					Column('Income_Growth_YOY_Season', Numeric(16, 4)),
					Column('Income_Growth_Qtr_Comparable', Numeric(16, 4)),
					Column('NPPC_Growth_Qtr_Comparable', Numeric(16, 4)),
					Column('GP_Margin_Qtr', Numeric(16, 4)),
					Column('GP_Margin_Growth_Qtr_Comparable', Numeric(16, 4)),
					Column('IPS_Qtr', Numeric(32, 21)),
					Column('EPS_Qtr', Numeric(32, 21)),
					Column('ROE_Qtr', Numeric(16, 4)),
					Column('Income_Growth_Pre', Numeric(16, 4)),
					Column('NPPC_Growth_Pre', Numeric(16, 4)),
					Column('NPLNRP_Growth_Pre', Numeric(32, 13)),
					Column('GP_Margin_Growth_Pre', Numeric(16, 4)),
					Column('NP_Margin_Growth_Pre', Numeric(16, 4)),
					Column('Income_Growth_YOY', Numeric(16, 4)),
					Column('NPPC_Growth_YOY', Numeric(16, 4)),
					Column('NPLNRP_Growth_YOY', Numeric(32, 13)),
					Column('GP_Margin_Growth_YOY', Numeric(16, 4)),
					Column('NP_Margin_Growth_YOY', Numeric(16, 4)),
					Column('IPS', Numeric(32, 21)),
					Column('EPS', Numeric(32, 21)),
					Column('CFPS', Numeric(32, 21)),
					Column('Pre_IPS', Numeric(32, 21)),
					Column('Pre_EPS', Numeric(32, 21)),
					Column('Pre_CFPS', Numeric(32, 21)),
					Column('YOY_IPS', Numeric(32, 21)),
					Column('YOY_EPS', Numeric(32, 21)),
					Column('YOY_CFPS', Numeric(32, 21)),
					Column('rPE', Numeric(32, 10)),
					Column('rPB', Numeric(32, 10)),
					Column('rPS', Numeric(32, 10)),
					Column('rPCF', Numeric(32, 10)),
					Column('SettlePrice', Numeric(16, 10)),
					Column('IndusInnerCode', Integer()),
					Column('IndusCreatePrice', Numeric(16, 10)),
					Column('IndusSettlePrice', Numeric(16, 10)),
					Column('ExecutivesProp', Numeric(16, 10)),
					Column('InstitutionNum', Integer()),
					Column('InstitutionProp', Numeric(16, 10)),
					Column('RegionScore', Integer()),
					Column('ExeClosePrice_CreateDate_Wind', Numeric(16, 4)),
					Column('ExeClosePrice_SettleDate_Wind', Numeric(16, 4)),
					Column('Momentum20Day', Numeric(20, 4)),
					Column('Momentum40Day', Numeric(20, 4)),
					Column('Momentum60Day', Numeric(20, 4)),
					Column('Momentum120Day', Numeric(20, 4)),
					Column('Momentum180Day', Numeric(20, 4)),
					Column('Momentum240Day', Numeric(20, 4)),
					Column('PriceDiff', Numeric(20, 4)),
					Column('DayDiff', Integer()),
					Column('TurnoverRatio', Numeric(20, 4)),
					Column('AvgTurnoverPrice', Numeric(20, 4)),
					Column('AvgTurnoverPriceFactor', Numeric(20, 4)),
					Column('AvgTurnoverRatio5Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio10Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio20Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio40Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio60Day', Numeric(20, 5)),
					Column('AvgTurnoverRatio120Day', Numeric(20, 5)),
                    Column('E_EPS', Numeric(16, 4)),
					Column('E_growth', Numeric(16, 4)),
					Column('FreeFloat_Total', Numeric(16, 4)),
					Column('EPS_CFPS', Numeric(32, 21)),
					)

	dbutil.insert(conn, residtb, df)

def insertWeekly(conn, df):
	"""	插入每周因子残差数据表
	"""
	dbutil.insertResid(conn, df, 'FactorTradingDayResidual')
	
def insertMonthly(conn, df):
	"""	插入每月因子残差数据表
	"""
	dbutil.insertResid(conn, df, 'FactorMonthlyResidual')

def insertData(dtype, engine, df):
	"""	根据类型插入到不同的表
		dtype - 字符类型表示交易日的类型，每日/周/月
		engine - sqlalchemy数据库引擎
		df - pandas DataFrame对象
	"""
	conn = engine.connect()
	if dtype == 'd':
			insertDaily(conn, df)
	elif dtype == 'w':
			insertWeekly(conn, df)
	elif dtype == 'm':
			insertMonthly(conn, df)
	else:
			print 'Cannot support the dtype: {0}'.format(dtype)

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
			insertData(dtype, engine, df)
		
		end = datetime.datetime.now()
		print 'Cost: {0} on {1}'.format(end-start, td.strftime('%Y%m%d'))

if __name__ == '__main__':
	"""用法： python residdb.py 'd|w|m' '20140101' '20141231' ['filepath']
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
		filepath='D:/workspace/python/residual/resid/'
	
	print 'dtype: {0}, start {1}, end {2}, result path: {3}'.format(dtype, start.strftime('%Y%m%d'), end.strftime('%Y%m%d'), filepath)
	
	tradingDays = dataapi.getDayList(dtype)
	tds = [i for i in tradingDays if i >= start and i <= end]
	insertAllData(filepath, tds, dtype)
