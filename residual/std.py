#coding=utf-8
import sys
import pandas as pd
import numpy as np
import datetime
import dataapi
import fileutil
import stdcalc

def preprocessData(df, removeCols):
	"""	删掉不需要的行
		df - pandas DataFrame对象，包含一天的因子数据
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
	columns = df.columns
	#生成数据对象，用来存放标准化之后数据
	newdf = df.loc[:, keyCols]
	industries = df['IndustrySecuCode_I'].dropna().unique()
	
	for column in columns:
		if column in excludeCols:
			print 'skip the column: {0}'.format(column)
			continue
		elif column in includeCols:
			#print 'handle the column: {0}'.format(column)
			newdf[column] = np.nan
			for industry in industries:
				indusdata = df[df['IndustrySecuCode_I'] == industry]
				
				#获取当前列值不为空值和无效值10000的行
				validrowdata = indusdata[pd.notnull(indusdata[column]) & (indusdata[column] != 10000)]
				
				#获取当前列数据
				induscoldata = validrowdata[column]

				#添加特殊逻辑，对某些列不做处理
				stddata = stdcalc.getStandardData(induscoldata, industry)
				#将数据更新到主表中
				for idx in stddata.index:
					newdf.loc[idx, column] = stddata[idx]

		else:
				print 'Cannot support: {0}'.format(column)
		
	return newdf

def handleOneDay(df, removeCols, keyCols, includeCols, excludeCols):
	""" td - 交易日为datetime.date类型
		removeCols - 需要删掉的无用列列名，为list类型
		keyCols - 关键列列名，为list类型
		includeCols - 数据处理列列名， 为list类型
		excludeCols - 非数据处理列列名，为list类型

		return - DataFrame类型
	"""

	df = preprocessData(df, removeCols)
	newdf = handleStandard(df, keyCols, includeCols, excludeCols)

	return newdf

def handleAllDay(filepath, tradingDays, dtype, removeCols, keyCols, includeCols, excludeCols, createDate, settleDate):
	""" filepath - 文件输出目录
		tradingDays - 交易日列表
		dtype - 字符类型表示交易日的类型，每日/周/月
		removeCols - 需要删掉的无用列列名，为list类型
		keyCols - 关键列列名，为list类型
		includeCols - 数据处理列列名， 为list类型
		excludeCols - 非数据处理列列名，为list类型
		createDate - 开始时间列名
		settleDate - 结束时间列名

		return - DataFrame类型
	"""
	for td in tradingDays:
		print 'handle {0}'.format(td.strftime('%Y%m%d'))
		start = datetime.datetime.now()
		#df = dataapi.getFactorDailyData(td)
		df = dataapi.getFactorData(dtype, td)
		actualKeyCols = list(keyCols)
		if settleDate in df.columns:
			actualKeyCols.insert(0, settleDate)
		if createDate in df.columns:
			actualKeyCols.insert(0, createDate)
		
		newdf = handleOneDay(df, removeCols, actualKeyCols, includeCols, excludeCols)
		fileutil.savePickle(filepath, td, newdf)
		
		end = datetime.datetime.now()
		print 'Cost: {0} on: {1}'.format(end - start, td.strftime('%Y%m%d'))

if __name__ == '__main__':
	"""用法： python std.py 'd|w|m' '20140101' '20141231' ['filepath']
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
		filepath='D:/workspace/python/residual/result/'
	
	print 'dtype: {0}, start {1}, end {2}, result path: {3}'.format(dtype, start.strftime('%Y%m%d'), end.strftime('%Y%m%d'), filepath)

	#标准化处理
	removeCols = dataapi.removeCols
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
	
	handleAllDay(filepath, tds, dtype, removeCols, keyCols, includeCols, excludeCols, createDate, settleDate)
