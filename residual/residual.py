#coding=utf-8
#import sys
#sys.path.append('D:/workspace/python/lib')
import pandas
import numpy as np
import datetime
import statsmodels.formula.api as sm
import dataapi

def calcResidual(df, includeCols, excludeCols):
		"""
		"""
		#取所有行业
		industries = getAllIndustries(df, 'IndustrySecuCode_I')

		#生成新的行业列名
		newIndusColumns = getIndustryColumnName(industries)
		
		#添加新的列
		appendIndustryColumn(df, newIndusColumns)
		
		#给添加的新列求值
		updateIndustryData(df, newIndusColumns, industries, 'IndustrySecuCode_I')
		
		
		columns = df.columns
		for column in columns:
				#print column
				if column in excludeCols:
						print 'skip the column: {0}'.format(column)
						continue
				elif column in includeCols:
						print 'handle the column: {1}'.format(column)
						start = datetime.datetime.now()
						
						#TODO:calc the residual
						nmres = column+'_res'
						calcOneFactor(df, newIndusColumns, column, 'NonRestrictedCap', nmres)
						end = datetime.datetime.now()
						print 'cost: {0}'.format(end-start)
				else:
						print 'Cannot support: {0}'.format(column)

def calcOneFactor(df, newIndusColumns, column, nmmv, nmres):
		"""
		"""
		#TODO:
		allcols = list(newIndusColumns)
		allCols.insert(nmmv)
		st.OLS(df[column], df

def appendIndustryColumn(df, industryColumns):
		"""
			df - pandas DataFrame对象，为某天因子数据
			industryColumns - 字符串列表，存放新行业列名
		"""
		for industryColumn in industryColumns:
				print industryColumn
				df[industryColumn]=0

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
				print name
				indusColumns.append(name)
		return indusColumns

def getIndustryData(industries, industry):
	arr = np.zeros(len(industries))
	inlist = industries.tolist()
	if industry in inlist:
			idx = inlist.index(industry)
			arr[idx] = 1
	return arr

if __name__ == '__main__':
		td = datetime.date(2017, 3, 8)
		df = dataapi.getFactorDailyData(td)
		#取所有行业
		industries = getAllIndustries(df, 'IndustrySecuCode_I')

		#生成新的行业列名
		newIndusColumns = getIndustryColumnName(industries)
		
		#添加新的列
		appendIndustryColumn(df, newIndusColumns)
		
		updateIndustryData(df, newIndusColumns, industries, 'IndustrySecuCode_I')
		
		print df.columns
