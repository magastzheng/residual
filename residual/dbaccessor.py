#encoding=utf-8

import sqlalchemy
import pymssql
import pandas as pd
import datetime

def getdb(host, user, password, db='', port=1433):
		"""	Open the database connection
			host - the database server
			user - the database account
			password - the database account password
			db - the database name
			port - the database port

			return - the engine object
		"""
		if port == 0:
			port = 1433
		
		if len(db) > 0:
			constr = 'mssql+pymssql://{0}:{1}@{2}:{3}/{4}'.format(user, password, host, port, db)
		else:
			constr = 'mssql+pymssql://{0}:{1}@{2}:{3}'.format(user, password, host, port)
		
		start = datetime.datetime.now()
		engine = sqlalchemy.create_engine(constr)
		end = datetime.datetime.now()
		
		print 'Cost: {0}. Open database: {1}'.format(end-start, constr)

		return engine

def query(engine, sql):
		"""Execute a sql query. It will return an pandas.DataFrame object and then close the connection"""
		conn = engine.connect()
		#execute sql here
		resultProxy = conn.execute(sql) 

		#convert the sa data to pandas DataFrame
		df = pd.DataFrame(resultProxy.fetchall(), columns=resultProxy.keys())
		conn.close()

		return df

def raw_query(engine, sql):
		"""Execute a raw_query. It will return a ResultProxy object. Caller needs to close it after finishing."""
		conn = engine.connect()
		#execute sql here
		resultProxy = conn.execute(sql) 

		return resultProxy

if __name__ == '__main__':
		sql = 'select * from zhenggq.dbo.securitybasicinfo'
		engine = getdb('176.1.11.55', 'zhenggq', 'Yuzhong0931')
		#result = query(engine, sql)
		result = raw_query(engine, sql)
		#print result
		print result.keys()
		for k in result.keys():
			print k
