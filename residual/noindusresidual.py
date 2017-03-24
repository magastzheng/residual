#coding=utf-8
#import sys
#sys.path.append('D:/workspace/python/lib')
import sys
import pandas as pd
import numpy as np
import datetime
import statsmodels.formula.api as smf
import statsmodels.api as sm
import dataapi
import fileutil

def calcOneFactor(df, column, nmmv, avgtv):
		"""	df - pandas DataFrame对象，为某天标准化后的因子数据
			column - 当前因子名称，因变量
			nmmv - 自由流通市值名称，自变量
			avgtv - 平均成交金额
			
			return -  pandas Series对象表示的残差
		"""
		#去掉空值
		df = df[pd.notnull(df[column]) & pd.notnull(df[nmmv]) & pd.notnull(df[avgtv])]
		if df is None or len(df) < 2:
			return None

		#如果是流通市值，不需要再跟自己回归	
		X = sm.add_constant(df[nmmv])
		if column == nmmv:
			#remove  column
			X=X.drop([nmmv], axis=1)
		if column != avgtv:
			X = pd.concat([X[:], df[avgtv]], axis=1)

		Y = df[column]
		model = smf.OLS(Y, X)
		results = model.fit()
		#raise Exception
		#print results.summary()
		return results.resid

def getOneDayResid(df,  keyCols, includeCols, excludeCols):		
	"""	df - pandas DataFrame对象，df中的数据已经将没有行业的行去除掉，并添加行业列，如果属于该行业，则本列为1，否则为0
		keyCols - 新的pandas DataFrame对象中，必须保留的列，list类型
		includeCols - 需要处理的列名，list类型
		excludeCols - 不需要处理的列名，list类型

		return - pandas DataFrame对象，包含需要处理的列残差
	"""
	columns = df.columns
	newdf = df.loc[:, keyCols]

	for column in columns:
		if column in includeCols:
			#print column
			newdf[column] = np.nan
			resid = calcOneFactor(df, column, 'NonRestrictedCap', 'AvgTurnoverValue')
			if resid is not None and len(resid) > 0:
				for idx in resid.index:
					newdf.loc[idx, column] = resid[idx]
		elif column in excludeCols:
			pass
			#print 'Skip: {0}'.format(column)
		else:
			pass
			#print 'Cannot support: {0}'.format(column)
	
	return newdf

def calcResidual(td, df, keyCols, includeCols, excludeCols):
	"""	计算残差
		td - 交易日
		df - td那个交易日的标准化数据，pandas DataFrame对象
		keyCols - 最终结果中必须出现的列名列表
		includeCols - 必须计算残差的列名列表
		excludeCols - 不需要计算残差的列名列表
	"""
	
	#去掉行业为空的行
	print 'preprocess'
	df = df.dropna(subset=['IndustrySecuCode_I'])
	
	print 'calculate ....'
	newdf = getOneDayResid(df, keyCols, includeCols, excludeCols)
	
	return newdf

def calcAllDay(residPath, stdPath, tradingDates, keyCols, includeCols, excludeCols, createDate, settleDate):
	"""	residPath - 保存残差数据的目录
		stdPath - 保存标准化数据的目录
		tradingDates - 交易日列表，内部数据为datetime.date类型
		keyCols - 结果中必须包含的列，list类型
		includeCols - 需要处理的列名，list类型
		excludeCols - 不需要处理的列名，list类型
		createDate - 开始时间列名
		settleDate - 结束时间列名

	"""
	for td in tradingDates:
		start = datetime.datetime.now()
		
		df = fileutil.loadPickle(stdPath, td)
		if df is not None:
			print 'process: {0}'.format(td.strftime('%Y%m%d'))
			actualKeyCols = list(keyCols)

			if settleDate in df.columns:
				actualKeyCols.insert(0, settleDate)
			if createDate in df.columns:
				actualKeyCols.insert(0, createDate)

			newdf = calcResidual(td, df, actualKeyCols, includeCols, excludeCols)
			fileutil.savePickle(residPath, td, newdf)
		else:
			print 'Fail to load data: {0}, {1}'.format(td.strftime('%Y%m%d'), stdPath)
		
		end = datetime.datetime.now()
		print 'cost: {0} on: {1}'.format(end-start, td.strftime('%Y%m%d'))

def calcMain(residPath, stdPath, tradingDates, start, end, keyCols, includeCols, excludeCols):
	tds = [i for i in tradingDates if i >= start and i <= end]
	calcAllDay(residPath, stdPath, tradingDays, keyCols, includeCols, excludeCols, createDate, settleDate)

if __name__ == '__main__':
		"""用法： python residual.py 'd|w|m' '20140101' '20141231' ['stdPath' 'residPath']
		"""
		dtype = 'd'
		start = datetime.datetime.min
		end = datetime.datetime.now()
		params = sys.argv[1:]
		
		stdPath = ''
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

		if len(stdPath) == 0:
			stdPath = 'D:/workspace/python/residual/result/'
		if len(residPath) == 0:
			residPath = 'D:/workspace/python/residual/resid/'

		print 'dtype: {0}, start {1}, end {2}'.format(dtype, start.strftime('%Y%m%d'), end.strftime('%Y%m%d'))
		print 'stdPath: {0}'.format(stdPath)
		print 'residPath: {0}'.format(residPath)

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
		calcAllDay(residPath, stdPath, tds, keyCols, includeCols, excludeCols, createDate, settleDate)
