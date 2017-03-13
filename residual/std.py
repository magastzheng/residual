#coding=utf-8

import pandas
import numpy as np
import datetime
import dataapi

def preprocessData(df, removeCols):
	"""df - pandas DataFrame对象，包含一天的因子数据
		removeCols - 需要删掉的无用列名列表
	"""
	df1=df.drop(removeCols, axis=1)
		
	return df1

#对数据标准化
def handleStandard(df, keyCols, includeCols, excludeCols):
	"""	df - pandas DataFrame对象，包含一天的因子数据
		keyCols - 用于从原数据中生成新数据的列列名，为list类型
		includeCols - 需要标准化的因子，为list类型
		excludeCols - 不需要标准化的因子，为list类型

		return - pandas DataFrame对象，包含的标准化之后数据列名跟原来列名名称相同
	"""
	#生成数据对象，用来存放标准化之后数据
	newdf = df.loc[:, keyCols]
	industries = df['IndustrySecuCode_I'].dropna().unique()
	columns = df.columns
	#print columns
	for column in columns:
		#print column
		if column in excludeCols:
			print 'skip the column: {0}'.format(column)
			continue
		elif column in includeCols:
			print 'handle the column: {0}'.format(column)
			#与中值绝对值列
			nmabs = column+'_abs'
			#对调整后使用中值调整后的列
			nmadj = column+'_adj'
			#行业内标准化后的列
			nmstd = column+'_std'
			df[nmabs] = np.nan
			df[nmadj] = np.nan
			df[nmstd] = np.nan

			newdf[column] = np.nan
			start = datetime.datetime.now()
			for industry in industries:
				indusdata = df[df['IndustrySecuCode_I'] == industry]
				#添加特殊逻辑，对某些列不做处理
				stddata = getIndustryStandardData(df, column, nmabs, nmadj, nmstd, industry, 'IndustrySecuCode_I')
				#将数据更新到主表中
				for idx in stddata.index:
					#df.loc[idx, nmstd] = stddata[idx]
					newdf.loc[idx, column] = stddata[idx]

			end = datetime.datetime.now()
			print 'cost: {0}'.format(end-start)
		else:
				print 'Cannot support: {0}'.format(column)
		
	return newdf

#对某一行业某一列做标准化
def getIndustryStandardData(df, column, nmabs, nmadj, nmstd, industryCode, industryColumn='IndustrySecuCode_I'):
	"""	df - pandas DataFrame对象，存放每日因子数据
		column - 因子名称，对应数据库的列名
		nmabs - 列名，用于保存与中值差的绝对值
		nmadj - 列名，用于对数据做调整后的值
		nmstd - 列名，用于保存行业内标准化后的值
		industryCode - 一级行业代码
		industryColumn - 一级行业列名，用于对数据分组

		return - 本列数据
	"""
	#获取本行业数据
	indusdata = df[df[industryColumn] == industryCode]
	#取指定列中值m
	m = indusdata[column].median()
	#取与中值绝对值，并用于取另外一个中值l
	indusdata[nmabs] = indusdata[column].astype(np.float).apply(lambda x: abs(x-m))
	l = indusdata[nmabs].median()
	#对值做处理
	indusdata[nmadj] = indusdata[column].astype(np.float).apply(lambda x:getAdjustValue(x, l, m))
	mean = indusdata[nmadj].mean()
	std = indusdata[nmadj].std()
	#print 'mean: {0}, std: {1}, indus: {2}'.format(mean, std, industryCode)
	#标准化
	indusdata[nmstd] = indusdata[nmadj].astype(np.float).apply(lambda x:getStandardValue(x, mean, std))
	
	return indusdata[nmstd]

def getAdjustValue(x, l, m):
		lb = m-5.2*l
		ub = m+5.2*l
		if x < lb:
				return lb
		elif x > ub:
				return ub
		else:
				return x

def getStandardValue(x, u, s):
		'''Calculate the standard value: 
		   x: the raw value
		   u: the mean value
		   s: the std value
		'''
		if not isZero(s):
				return (x-u)/s
		else:
				return np.nan

def isZero(x):
		"""Check if the float type x is zero or not
		"""
		return abs(x) < 0.000000001

def handleOneDay(td, removeCols, keyCols, includeCols, excludeCols):
	""" td - 交易日为datetime.date类型
		removeCols - 需要删掉的无用列列名，为list类型
		keyCols - 关键列列名，为list类型
		includeCols - 数据处理列列名， 为list类型
		excludeCols - 非数据处理列列名，为list类型
		return - DataFrame类型
	"""
	df = dataapi.getFactorDailyData(td)
	df = preprocessData(df, removeCols)
	newdf = handleStandard(df, keyCols, includeCols, excludeCols)

	return newdf

def saveOneDay(filepath, td, df):
	""" filepath - 保存文件的路径名，为str类型
		td - 交易日，为datetime.date类型
		df - pandas DataFrame对象
	"""
	df['SecuAbbr'] = df['SecuAbbr'].apply(lambda x:x.encode('raw-unicode-escape').decode('gbk'))
	filename = td.strftime('%Y%m%d')
	fullpath='{0}{1}.csv'.format(filepath, filename)
	df.to_csv(fullpath, encoding='gbk')

def handleAllDay(filepath, removeCols, keyCols, includeCols, excludeCols):
	""" removeCols - 需要删掉的无用列列名，为list类型
		keyCols - 关键列列名，为list类型
		includeCols - 数据处理列列名， 为list类型
		excludeCols - 非数据处理列列名，为list类型
		return - DataFrame类型
	"""
	tddf = dataapi.getTradingDay()
	tddf['TradingDay'] = tddf['TradingDay'].apply(lambda x:datetime.datetime.strptime(x, '%Y%m%d'))
	tradingDays = tddf['TradingDay'].tolist()
	for td in tradingDays:
		print 'handle {0}'.format(td.strftime('%Y%m%d'))
		df = handleOneDay(td, removeCols, keyCols, includeCols, excludeCols)
		saveOneDay(filepath, td, df)

if __name__ == '__main__':
	"""import dataapi
	import datetime
	filepath='D:/workspace/python/residual/result/'
	td = datetime.date(2017, 3, 8)
	df = dataapi.getFactorDailyData(td)
	#删掉无用的列
	df=df.drop(['FirstIndustryName', 'SecondIndustryName'], axis=1)
	
	#标准化处理
	includeCols = dataapi.includeCols
	excludeCols = dataapi.excludeCols
	handleStandard(df, includeCols, excludeCols)
	
	filename = td.strftime('%Y%m%d')
	fullpath='{0}{1}'.format(filepath, filename)
	df['SecuAbbr'] = df['SecuAbbr'].apply(lambda x:x.encode('raw-unicode-escape').decode('gbk'))
	#df['FirstIndustryName'] = df['FirstIndustryName'].apply(lambda x:x.encode('raw-unicode-escape').decode('gbk'))
	#df['SecondIndustryName'] = df['SecondIndustryName'].apply(lambda x:x.encode('raw-unicode-escape').decode('gbk'))
	df.to_csv(fullpath, encoding='gbk')
	"""
	filepath='D:/workspace/python/residual/result/'
	#标准化处理
	removeCols = dataapi.removeCols
	keyCols = dataapi.keyCols
	includeCols = dataapi.includeCols
	excludeCols = dataapi.excludeCols
	handleAllDay(filepath,  removeCols, keyCols, includeCols, excludeCols)
