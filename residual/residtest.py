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
import os
import pdb

indusColumns=['Indus_1','Indus_2','Indus_3','Indus_4','Indus_5','Indus_6','Indus_7','Indus_8','Indus_9','Indus_10','Indus_11','Indus_12','Indus_13','Indus_14','Indus_15','Indus_16','Indus_17','Indus_18','Indus_19','Indus_20','Indus_21','Indus_22','Indus_23','Indus_24','Indus_25','Indus_26','Indus_27','Indus_28','Indus_29']

def calcOneFactor(df, newIndusColumns, column, nmmv):
		"""	df - pandas DataFrame对象，为某天标准化后的因子数据
			newIndusColumns - 行业因子名称列表
			column - 当前因子名称，因变量
			nmmv - 自由流通市值名称，自变量
			
			return -  pandas Series对象表示的残差
		"""
		#TODO: 如果是流通市值，不需要再跟自己回归
		#allCols = list(newIndusColumns)
		#if column == nmmv:
		#	pass
		#else:
		#	allCols.insert(0, nmmv)
		
		df = df.fillna(0)
		
		#raise Exception
		#pdb.set_trace()
		print 'hello'
		#X = sm.add_constant(df[nmmv])
		#if column == nmmv:
			#remove  column
		#	X=X.drop([nmmv], axis=1)
		X = df[nmmv]
		X = pd.concat([X[:], df[newIndusColumns]], axis=1)
		
		Y = df[column]
		#model = sm.OLS(df[column], df.loc[:,allCols])
		model = smf.OLS(Y, X)
		results = model.fit()
		raise Exception
		#print results.summary()
		print results.resid.head()
		return results.resid

def appendIndustryColumn(df, industryColumns):
		"""
			df - pandas DataFrame对象，为某天因子数据
			industryColumns - 字符串列表，存放新行业列名
		"""
		for industryColumn in industryColumns:
				df.loc[:, industryColumn]=0

		return df

def updateIndustryData(df, newIndusColumns, industryCodes, industryColumn='IndustrySecuCode_I'):
		"""	df - pandas DataFrame对象
			newIndusColumns - 对行业生成的新列，列名为Indus_x
			industryCodes - 一级行业代码列表
			industryColumn - 一级行业数据库表列名
		"""
		for industryCode in industryCodes:
			idx = -1
			if industryCode in industryCodes:
				idx = industryCodes.index(industryCode)
			if idx >= 0 and idx < len(newIndusColumns):
				newIndusColumn = newIndusColumns[idx]
				df.loc[df[industryColumn] == industryCode, newIndusColumn] = 1
		
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
			print column
			resid = calcOneFactor(df, newIndusColumns, column, 'NonRestrictedCap')
			newdf[column] = np.nan
			for idx in resid.index:
					newdf.loc[idx, column] = resid[idx]
			break
		elif column in excludeCols:
			print 'Skip: {0}'.format(column)
		else:
			print 'Cannot support: {0}'.format(column)
	
	return newdf

def saveOneDay(filepath, td, df):
	""" filepath - 保存文件的路径名，为str类型
		td - 交易日，为datetime.date类型
		df - pandas DataFrame对象
	"""
	df['SecuAbbr'] = df['SecuAbbr'].apply(lambda x:x.decode('gbk'))

	filename = td.strftime('%Y%m%d')
	fullpath='{0}{1}.csv'.format(filepath, filename)
	#raise Exception
	df.to_csv(fullpath, encoding='gbk')

def test():
	td = datetime.date(2017, 3, 8)
	stdPath = 'D:/workspace/python/residual/result/'
	residPath = 'D:/workspace/python/residual/residtest/'
	#csvpath = '{0}{1}.csv'.format(stdPath, td.strftime('%Y%m%d'))
	csvpath = '{0}{1}.csv'.format(stdPath, '20170308-r')
	keyCols = dataapi.keyCols
	includeCols = dataapi.includeCols
	excludeCols = dataapi.excludeCols
	newIndusColumns = indusColumns
	if os.path.exists(csvpath):
		df=pd.read_csv(csvpath)
		#去掉行业为空的行
		df = df.dropna(subset=['IndustrySecuCode_I'])
		#newIndusColumns = preprocessData(df)
		newdf = getOneDayResid(df, newIndusColumns, keyCols, includeCols, excludeCols)
	else:
		pass

def load(dtstr):
	residPath = 'D:/workspace/python/residual/resid/'
	csvpath = '{0}{1}.csv'.format(residPath, dtstr)
	if os.path.exists(csvpath):
			df=pd.read_csv(csvpath)
			return df
	else:
			print('The file {0} is not found!'.format(csvpath))
			return None

if __name__ == '__main__':
		"""用法： python residual.py '20140101' '20141231'
		"""
		start = datetime.datetime.min
		end = datetime.datetime.now()
		params = sys.argv[1:]
		if len(params) == 2:
			start = datetime.datetime.strptime(params[0], '%Y%m%d')
			end = datetime.datetime.strptime(params[1], '%Y%m%d')

		print 'start {0}, end {1}'.format(start.strftime('%Y%m%d'), end.strftime('%Y%m%d'))
		keyCols = dataapi.keyCols
		includeCols = dataapi.includeCols
		excludeCols = dataapi.excludeCols
		tddf = dataapi.getTradingDay()
		tddf['TradingDay'] = tddf['TradingDay'].apply(lambda x:datetime.datetime.strptime(x, '%Y%m%d'))
		tradingDays = tddf['TradingDay'].tolist()
		stdPath = 'D:/workspace/python/residual/result/'
		residPath = 'D:/workspace/python/residual/resid/'
		tds = [i for i in tradingDays if i >= start and i <= end]
		calcAllDay(residPath, stdPath, tds, keyCols, includeCols, excludeCols)
