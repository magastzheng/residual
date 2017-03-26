#coding: utf8

import datetime
import sys
import pandas as pd
import numpy as np
import dataapi
import fileutil

def getDelta(df, column, sortcol):
		"""	df - 去掉行业为空之后的数据
			column - 当前列名
			sortcol - 排序列名

			return - 前10%平均值与后10%平均值之差
		"""
		#只选取非空值和值不为10000的行
		df = df[pd.notnull(df[column]) & pd.notnull(df[sortcol]) & ((df[column] >= 10000.0001) | (df[column] <= 9999.999))]
		sdf = df.sort_values(by=[sortcol], ascending=[0])
		
		#10%数量
		totalLen = len(sdf)
		tenPct = int(0.1 * totalLen+0.5)
	
		#去前10%和后10%
		topM = sdf.head(tenPct)[column].mean()
		bottomM = sdf.tail(tenPct)[column].mean()

		return topM - bottomM

def calcDelta(td, df, keyCols, includeCols, excludeCols):
	"""	计算前10%和后10%均值差
		td - 交易日
		df - td那个交易日的标准化数据，pandas DataFrame对象
		keyCols - 最终结果中必须出现的列名列表
		includeCols - 必须计算残差的列名列表
		excludeCols - 不需要计算残差的列名列表
	"""
	#去掉行业为空的行
	df = df.dropna(subset=['IndustrySecuCode_I'])
	columns = df.columns

	newdf = df.loc[0:0, columns]
	for column in columns:
		if column in includeCols:
			newdf.loc[0, column] = getDelta(df, column, 'NonRestrictedCap')
		elif column in excludeCols:
			pass
		else:
			pass

	return newdf

def calcAllDay(deltaPath, stdPath, tradingDays, keyCols, includeCols, excludeCols, createDate, settleDate):
	"""	deltaPath - 保存前10%和后10%均值差数据的目录
		stdPath - 保存标准化数据的目录
		tradingDays - 交易日列表，内部数据为datetime.date类型
		keyCols - 结果中必须包含的列，list类型
		includeCols - 需要处理的列名，list类型
		excludeCols - 不需要处理的列名，list类型
		createDate - 开始时间列名
		settleDate - 结束时间列名

	"""
		
	for td in tradingDays:
		start = datetime.datetime.now()
		
		df = fileutil.loadPickle(stdPath, td)
		if df is not None:
			print 'process: {0}'.format(td.strftime('%Y%m%d'))
			actualKeyCols = list(keyCols)

			if settleDate in df.columns:
				actualKeyCols.insert(0, settleDate)
			if createDate in df.columns:
				actualKeyCols.insert(0, createDate)

			newdf = calcDelta(td, df, actualKeyCols, includeCols, excludeCols)
			fileutil.savePickle(deltaPath, td, newdf)
			#fileutil.saveCSV(deltaPath, td, newdf)
			#filename = td.strftime('%Y%m%d')
			#fullpath='{0}{1}.csv'.format(deltaPath, filename)
			#newdf.to_csv(fullpath, encoding='utf8')
		else:
			print 'Fail to load data: {0}, {1}'.format(td.strftime('%Y%m%d'), stdPath)
		
		end = datetime.datetime.now()
		print 'cost: {0} on: {1}'.format(end-start, td.strftime('%Y%m%d'))

if __name__ == '__main__':
		"""用法： python delta.py 'd|w|m' '20140101' '20141231' ['stdPath' 'residPath']
		"""
		dtype = 'd'
		start = datetime.datetime.min
		end = datetime.datetime.now()
		params = sys.argv[1:]
		
		stdPath = ''
		deltaPath = ''
		
		if len(params) >= 1:
			dtype = params[0]
		if len(params) >= 3:
			start = datetime.datetime.strptime(params[1], '%Y%m%d')
			end = datetime.datetime.strptime(params[2], '%Y%m%d')
		if len(params) >= 4:
			stdPath = params[3]
		if len(params) >= 5:
			deltaPath = params[4]

		if len(stdPath) == 0:
			stdPath = 'D:/workspace/python/residual/result/'
		if len(deltaPath) == 0:
			deltaPath = 'D:/workspace/python/residual/delta/'

		print 'dtype: {0}, start {1}, end {2}'.format(dtype, start.strftime('%Y%m%d'), end.strftime('%Y%m%d'))
		print 'stdPath: {0}'.format(stdPath)
		print 'residPath: {0}'.format(deltaPath)

		keyCols = dataapi.keyCols
		includeCols = dataapi.includeCols
		excludeCols = dataapi.excludeCols
		createDate = dataapi.createDate
		settleDate = dataapi.settleDate
		#tddf = dataapi.getTradingDay()
		#tddf['TradingDay'] = tddf['TradingDay'].apply(lambda x:datetime.datetime.strptime(x, '%Y%m%d'))
		#tradingDays = tddf['TradingDay'].tolist()
		tradingDays = dataapi.getDayList(dtype)
		tds = [i for i in tradingDays if i >= start and i <= end]
		calcAllDay(deltaPath, stdPath, tds, keyCols, includeCols, excludeCols, createDate, settleDate)
