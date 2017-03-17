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
import residual
import fileutil

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

def calcResidual(df, keyCols, includeCols, excludeCols):
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
	start = datetime.datetime.now()

	newdf = residual.getOneDayResid(df, newIndusColumns, keyCols, includeCols, excludeCols)
	
	end = datetime.datetime.now()
	
	print 'cost: {0}'.format(end-start)
	return newdf

def test():
	td = datetime.date(2017, 3, 16)
	stdPath = 'D:/workspace/python/residual/result/monthly/'
	residPath = 'D:/workspace/python/residual/residtest/'
	#csvpath = '{0}{1}.csv'.format(stdPath, td.strftime('%Y%m%d'))
	
	keyCols = dataapi.keyCols
	includeCols = dataapi.includeCols
	excludeCols = dataapi.excludeCols
	newIndusColumns = indusColumns
	
	df = fileutil.loadPickle(stdPath, td)
	if df is not None:
		#去掉行业为空的行
		#df = df.dropna(subset=['IndustrySecuCode_I'])
		#newIndusColumns = preprocessData(df)
		newdf = calcResidual(df, keyCols, includeCols, excludeCols)
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
