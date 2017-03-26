#coding: utf8

import sys
import datetime
import pandas as pd
import numpy as np
import statsmodels.formula.api as smf
import statsmodels.api as sm
import dataapi
import datasql
import fileutil

def calcOneFactor(df, column, mktcol):
		"""	df - pandas DataFrame对象，为某天标准化后的因子数据，会将市场
			mktdf - pandas DataFrame对象，为某天对股票流通市值排序之后，取前10%和后10%均值差
			column - 当前因子名称，因变量
			mktcol - 因子名称，自变量
			
			return -  pandas Series对象表示的残差
		"""
		#此处需要保证参数有效性
		#根据时间分别取df和mktdf中数据，构造新的df
		#去掉空值
		df = df[pd.notnull(df[column]) & pd.notnull(df[mktcol])]
		if df is None or len(df) < 2:
			return None
		
		#需要确保df和mktdf长度一致
		#如果是流通市值，不需要再跟自己回归	
		X = sm.add_constant(df[column])
		X = pd.concat([X[:], df[mktcol]], axis=1)
		Y = df[column]
		#model = sm.OLS(df[column], df.loc[:,allCols])
		model = smf.OLS(Y, X)
		results = model.fit()
		#raise Exception
		#print results.summary()
		return results.resid

def calcStock(df, keyCols, includeCols, excludeCols):
		"""	sqlpath - py文件目录，其中有sql子目录，下面放置sql语句文件
			dtype - 数据频率类型
			secucode - 股票代码
			keyCols - 必须包含的列
			includeCols - 回归列
			execludeCols - 不需要回归的列
		"""
		#df = datasql.getMonthlyStockData(secucode, sqlpath)
		#mktdf = dataapi.getFactorDelta(dtype)
		#df = df.dropna(subset=['IndustrySecuCode_I'])
		newdf = df.loc[:, keyCols]
		columns = df.columns
		for column in columns:
				if column in includeCols:
						newdf[column] = np.nan
						mktcol = '{0}_delta'.format(column)
						resid = calcOneFactor(df, column, mktcol)
						if resid is not None and len(resid) > 0:
							for idx in resid.index:
								newdf.loc[idx, column] = resid[idx]
				elif column in excludeCols:
						pass
				else:
						pass
		
		return newdf

def calcResidual(df, keyCols, includeCols, excludeCols):
	"""
	"""
	df = df.dropna(subset=['IndustrySecuCode_I'])
	
	print 'calculate...'
	newdf = calcStock(df, keyCols, includeCols, excludeCols)

	return newdf


def calcAllStock(sqlPath, residPath, stocks, keyCols, includeCols, excludeCols, createDate, settleDate):
	
	for stock in stocks:
		start = datetime.datetime.now()
		
		print 'fetch data...'
		df = datasql.getMonthlyStockData(stock, sqlPath)
		if df is not None:
			print 'process: {0}'.format(stock)
			actualKeyCols = list(keyCols)

			if settleDate in df.columns:
				actualKeyCols.insert(0, settleDate)
			if createDate in df.columns:
				actualKeyCols.insert(0, createDate)

			newdf = calcResidual(df, actualKeyCols, includeCols, excludeCols)
			fileutil.saveStockPickle(residPath, stock, newdf)
		else:
			print 'Fail to load data: {0}, {1}'.format(stock, stdPath)
		
		end = datetime.datetime.now()
		print 'cost: {0} on: {1}'.format(end-start, stock)

if __name__ == '__main__':
		"""用法： python stresidual.py 'd|w|m' '20140101' '20141231' ['sqlPath' 'residPath']
		"""
		dtype = 'd'
		start = datetime.datetime.min
		end = datetime.datetime.now()
		params = sys.argv[1:]
		
		sqlPath = ''
		residPath = ''
		
		if len(params) >= 1:
			dtype = params[0]
		if len(params) >= 3:
			start = datetime.datetime.strptime(params[1], '%Y%m%d')
			end = datetime.datetime.strptime(params[2], '%Y%m%d')
		if len(params) >= 4:
			stdPath = params[3]
		if len(params) >= 5:
			residPath = params[4]

		if len(sqlPath) == 0:
			sqlPath = 'D:/workspace/python/residual/'
		if len(residPath) == 0:
			residPath = 'D:/workspace/python/residual/resid/stock/'

		print 'dtype: {0}, start {1}, end {2}'.format(dtype, start.strftime('%Y%m%d'), end.strftime('%Y%m%d'))
		print 'sqlPath: {0}'.format(sqlPath)
		print 'residPath: {0}'.format(residPath)

		keyCols = dataapi.keyCols
		includeCols = dataapi.includeCols
		excludeCols = dataapi.excludeCols
		createDate = dataapi.createDate
		settleDate = dataapi.settleDate
		#tddf = dataapi.getTradingDay()
		#tddf['TradingDay'] = tddf['TradingDay'].apply(lambda x:datetime.datetime.strptime(x, '%Y%m%d'))
		#tradingDays = tddf['TradingDay'].tolist()
		sdf = dataapi.getMonthlyStocks()
		stocks = sdf['SecuCode'].tolist()
		calcAllStock(sqlPath, residPath, stocks, keyCols, includeCols, excludeCols, createDate, settleDate)
