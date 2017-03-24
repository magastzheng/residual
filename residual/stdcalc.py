#coding: utf8

import pandas as pd
import numpy as np

#对某一列做标准化
def getStandardData(coldata, column):
	"""	coldata - pandas Series 某列数据
		column - 列名
		return - 本列数据
	"""
	#获取本列数据
	#取指定列中位数m和使用绝对值处理后的中位数l
	#m和l只是用来确定下界lb和上界ub
	#下界lb和上界ub用来对原始数据中的极值做处理
	m = coldata.median()
	#取与中值绝对值，并用于取另外一个中值l
	absdata = coldata.astype(np.float).apply(lambda x: abs(x-m))
	l = absdata.median()
	
	#下界
	lb = m-5.2*l
	#上界
	ub = m+5.2*l

	#对原始值做处理
	adjdata = coldata.astype(np.float).apply(lambda x:getAdjustValue(x, lb, ub))
	mean = adjdata.mean()
	std = adjdata.std()
	#对调整后的数据标准化
	stddata = adjdata.astype(np.float).apply(lambda x:getStandardValue(x, mean, std))

	#print 'Column: {0}, m: {1}, l: {2}, mean: {3}, std: {4}'.format(column, m, l, mean, std)
	return stddata

#使用上界和下界做极限化处理
def getAdjustValue(x, lb, ub):
		"""	x - 当前值
			lb - 下界
			ub - 上界
		"""
		if x < lb:
				return lb
		elif x > ub:
				return ub
		else:
				return x

#使用均值u和标准差s对值x进行标准化
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
