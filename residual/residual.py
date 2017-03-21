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

def calcOneFactor(df, newIndusColumns, column, nmmv):
		"""	df - pandas DataFrame对象，为某天标准化后的因子数据
			newIndusColumns - 行业因子名称列表
			column - 当前因子名称，因变量
			nmmv - 自由流通市值名称，自变量
			
			return -  pandas Series对象表示的残差
		"""
		#去掉空值
		df = df[df[column].isnull() == False]
		if df is None or len(df) == 0:
			return None

		#如果是流通市值，不需要再跟自己回归	
		X = sm.add_constant(df[nmmv])
		if column == nmmv:
			#remove  column
			X=X.drop([nmmv], axis=1)
		
		X = pd.concat([X[:], df[newIndusColumns]], axis=1)
		Y = df[column]
		#model = sm.OLS(df[column], df.loc[:,allCols])
		model = smf.OLS(Y, X)
		results = model.fit()
		#raise Exception
		#print results.summary()
		return results.resid

def appendIndustryColumn(df, industryColumns):
		"""
			df - pandas DataFrame对象，为某天因子数据
			industryColumns - 字符串列表，存放新行业列名
		"""
		print 'appendIndustryColumn start....'
		for industryColumn in industryColumns:
			#df.loc[:, industryColumn]=0
			df[industryColumn] = 0
			
		print 'appendIndustryColumn end....'

		return df

def updateIndustryData(df, newIndusColumns, industryCodes, industryColumn='IndustrySecuCode_I'):
		"""	df - pandas DataFrame对象
			newIndusColumns - 对行业生成的新列，列名为Indus_x
			industryCodes - 一级行业代码列表
			industryColumn - 一级行业数据库表列名
		"""
		print 'updateIndustryData start....'
		for industryCode in industryCodes:
			idx = -1
			if industryCode in industryCodes:
				idx = industryCodes.index(industryCode)
			if idx >= 0 and idx < len(newIndusColumns):
				newIndusColumn = newIndusColumns[idx]
				df.loc[df[industryColumn] == industryCode, newIndusColumn] = 1
		
		print 'updateIndustryData start....'

		return df

def getAllIndustries(df, industryColumn):
		"""	df - pandas DataFrame对象
			industryColumn - 数据库表中的一级行业列名
			return - 去重排序后一级行业代码
		"""
		industries = df[industryColumn].dropna().unique().tolist()
		industries.sort()

		return industries

def getIndustryColumnName(industries):
		"""
			industries - 一级行业代码列表
			return - 新行业列名表
		"""
		prefix="Indus_{0}"

		indusColumns = []
		length = len(industries)
		for i in range(1, length+1):
				name=prefix.format(i)
				#print name
				indusColumns.append(name)
		return indusColumns

#def getIndustryData(industries, industry):
#	arr = np.zeros(len(industries))
#	inlist = industries.tolist()
#	if industry in inlist:
#			idx = inlist.index(industry)
#			arr[idx] = 1
#	return arr

def preprocessData(df):
	"""	df - pandas DataFrame对象，其中数据是已经标准化之后的
		return - 新列名，list类型
	"""
	
	#取所有行业
	industries = getAllIndustries(df, 'IndustrySecuCode_I')
	#生成新的行业列名
	newIndusColumns = getIndustryColumnName(industries)
	#添加新的列
	appendIndustryColumn(df, newIndusColumns)
	#对象了求值		
	updateIndustryData(df, newIndusColumns, industries, 'IndustrySecuCode_I')
	
	return newIndusColumns

def getOneDayResid(df, newIndusColumns,  keyCols, includeCols, excludeCols):		
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
			resid = calcOneFactor(df, newIndusColumns, column, 'NonRestrictedCap')
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
	newIndusColumns = preprocessData(df)
	
	print 'calculate ....'
	newdf = getOneDayResid(df, newIndusColumns, keyCols, includeCols, excludeCols)
	
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
