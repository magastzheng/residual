#coding=utf-8
import sys
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
			#nmabs = column+'_abs'
			#对调整后使用中值调整后的列
			#nmadj = column+'_adj'
			#行业内标准化后的列
			#nmstd = column+'_std'
			#df[nmabs] = np.nan
			#df[nmadj] = np.nan
			#df[nmstd] = np.nan

			newdf[column] = np.nan
			start = datetime.datetime.now()
			for industry in industries:
				indusdata = df[df['IndustrySecuCode_I'] == industry]
				induscoldata = indusdata[column]
				#添加特殊逻辑，对某些列不做处理
				#t1 = datetime.datetime.now()
				stddata = getIndustryStandardData(induscoldata, industry)
				#t2 = datetime.datetime.now()
				#print 'calc std data: {0}, cost: {1}'.format(industry, t2-t1)
				#将数据更新到主表中
				for idx in stddata.index:
					#df.loc[idx, nmstd] = stddata[idx]
					newdf.loc[idx, column] = stddata[idx]
				#t3 = datetime.datetime.now()
				#print 'assign std data: {0}, cost: {1}'.format(industry, t3-t2)

			end = datetime.datetime.now()
			print 'cost: {0}'.format(end-start)
		else:
				print 'Cannot support: {0}'.format(column)
		
	return newdf

#对某一行业某一列做标准化
def getIndustryStandardData(induscoldata, industryCode):
	"""	induscoldata - pandas Series 某行业某列数据
		industryCode - 行业代码
		return - 本列数据
	"""
	#获取本行业数据
	#取指定列中位数m和使用绝对值处理后的中位数l
	#m和l只是用来确定下界lb和上界ub
	#下界lb和上界ub用来对原始数据中的极值做处理
	m = induscoldata.median()
	#取与中值绝对值，并用于取另外一个中值l
	absdata = induscoldata.astype(np.float).apply(lambda x: abs(x-m))
	l = absdata.median()
	
	#下界
	lb = m-5.2*l
	#上界
	ub = m+5.2*l

	#对原始值做处理
	adjdata = induscoldata.astype(np.float).apply(lambda x:getAdjustValue(x, lb, ub))
	mean = adjdata.mean()
	std = adjdata.std()
	#对调整后的数据标准化
	stddata = adjdata.astype(np.float).apply(lambda x:getStandardValue(x, mean, std))

	#print 'Industry: {0}, m: {1}, l: {2}, mean: {3}, std: {4}'.format(industryCode, m, l, mean, std)
	return stddata

def getAdjustValue(x, lb, ub):
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

def handleAllDay(filepath, tradingDays, removeCols, keyCols, includeCols, excludeCols):
	""" removeCols - 需要删掉的无用列列名，为list类型
		keyCols - 关键列列名，为list类型
		includeCols - 数据处理列列名， 为list类型
		excludeCols - 非数据处理列列名，为list类型
		return - DataFrame类型
	"""
	for td in tradingDays:
		print 'handle {0}'.format(td.strftime('%Y%m%d'))
		df = handleOneDay(td, removeCols, keyCols, includeCols, excludeCols)
		saveOneDay(filepath, td, df)

if __name__ == '__main__':
	"""用法： python std.py '20140101' '20141231'
	"""
	start = datetime.datetime.min
	end = datetime.datetime.now()
	params = sys.argv[1:]
	if len(params) == 2:
		start = datetime.datetime.strptime(params[0], '%Y%m%d')
		end = datetime.datetime.strptime(params[1], '%Y%m%d')
	print 'start {0}, end {1}'.format(start.strftime('%Y%m%d'), end.strftime('%Y%m%d'))

	filepath='D:/workspace/python/residual/result/'
	#标准化处理
	removeCols = dataapi.removeCols
	keyCols = dataapi.keyCols
	includeCols = dataapi.includeCols
	excludeCols = dataapi.excludeCols
	tddf = dataapi.getTradingDay()
	tddf['TradingDay'] = tddf['TradingDay'].apply(lambda x:datetime.datetime.strptime(x, '%Y%m%d'))
	tradingDays = tddf['TradingDay'].tolist()
	tds = [i for i in tradingDays if i >= start and i <= end]

	handleAllDay(filepath, tds, removeCols, keyCols, includeCols, excludeCols)
