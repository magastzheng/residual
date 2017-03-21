#coding=utf8

import datatime
import dataapi
import std
import residual
import residdb
import fileutil
import dbaccessor

def handleOneDay(stdpath, residpath, dtype, td, removeCols, keyCols, includCols, excludeCols, createDate, settleDate):
	df = dataapi.getFactorData(dtype, td)
	actualKeyCols = list(keyCols)
	if settleDate in df.columns:
		actualKeyCols.insert(0, settleDate)
	if createDate in df.columns:
		actualKeyCols.insert(0, createDate)
	
	t1 = datetime.datetime.now()

	stddf = std.handleOneDay(df, removeCols, actualKeyCols, includeCols, excludeCols)
	
	t2 = datetime.datetime.now()
	print 'Cost std: {0} on {1}'.format(t2-t1, td.strftime('%Y%m%d'))
	#TODO: save
	fileutil.savePickle(stdpath, td, stddf)
	
	residdf = residual.calcResidual(td, stddf, keyCols, includeCols, excludeCols)
	
	t3 = datetime.datetime.now()
	print 'Cost resid: {0} on {1}'.format(t3-t2, td.strftime('%Y%m%d'))
	#TODO: save the residual data
	fileutil.savePickle(residpath, td, residdf)

	engine = dbaccessor.getdb('localhost', 'zhenggq', 'Yuzhong0931', 'advancedb', 1433)
	residdb.insertData(dtype, engine, residdf)
	
	t3 = datetime.datetime.now()
	print 'Cost insertdb: {0} on {1}'.format(t4-t3, td.strftime('%Y%m%d'))

def handleAllDay(stdpath, residpath, dtype, tds, removeCols, keyCols, includCols, excludeCols, createDate, settleDate):
	for td in tradingDays:
		handleOneDay(stdpath, residpath, dtype, td, removeCols, keyCols, includeCols, excludeCols, createDate, settleDate)

defstdpath={
'd': 'D:/workspace/python/residual/result/daily/'
'w': 'D:/workspace/python/residual/result/weekly/'
'm': 'D:/workspace/python/residual/result/monthly/'
}

defresidpath={
'd': 'D:/workspace/python/residual/resid/daily/'
'w': 'D:/workspace/python/residual/resid/weekly/'
'm': 'D:/workspace/python/residual/resid/monthly/'
}

if __name__ == '__main__':
	dtype = 'd'
	start = datetime.datetime.min
	end = datetime.datetime.now()
	stdpath = ''
	residpath = ''

	params = sys.argv[1:]
	
	if len(params) >= 1:
		dtype = params[0]
	if len(params) >= 3:
		start = datetime.datetime.strptime(params[1], '%Y%m%d')
		end = datetime.datetime.strptime(params[2], '%Y%m%d')
	if len(params) >= 4:
		stdpath = params[3]
	if len(params) >= 5:
		residpath = params[4]
	
	if len(stdpath) == 0:
		stdpath=defstdpath.get(dtype, 'D:/workspace/python/residual/result/')
	
	if len(residpath) == 0:
		residpath = defresidpath.get(dtype, 'D:/workspace/python/residual/resid/')
	
	removeCols = dataapi.removeCols
	keyCols = dataapi.keyCols
	includeCols = dataapi.includeCols
	excludeCols = dataapi.excludeCols
	createDate = dataapi.createDate
	settleDate = dataapi.settleDate

	tradingDays = dataapi.getDayList(dtype)
	tds = [i for i in tradingDays if i >= start and i <= end]
	handleAllDay(stdpath, residpath, dtype, tds, removeClose, keyCols, includeCols, excludeCols, createDate, settleDate)
