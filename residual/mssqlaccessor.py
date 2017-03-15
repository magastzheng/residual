#coding=utf8
import pymssql

def getConnection(host, user, password, dbname):
		return pymssql.connect(host, user, password, dbname)

def getCursor(conn):
		return conn.cursor()

def execute(conn, sql):
		"""	执行sql语句，并关闭连接
			conn - 数据库连接对象
			sql - sql语句
			return - cursor对象，使用完成之后，需要手动关闭
			使用完该连接之后，需要手动关闭
		"""
		cursor = conn.cursor()
		cursor.execute(sql)
		conn.commit()
		return cursor
		#cursor.close()
		#conn.close()

#_mssql库中的连接
def execute_non_query(conn, sql):
		"""执行非查询语句
			conn - 数据库连接对象
			sql - sql语句
		"""
		conn.execute_non_query(sql)

#_mssql库中的连接
def execute_query(conn, sql):
		"""执行查询语句
			conn - 数据库连接对象
			sql - sql语句
		"""
		conn.execute_query(sql)
		#for row in conn:
		#	print row['c1']....

def test():
		conn = getConnection('localhost', 'zhenggq', 'Yuzhong0931', 'test')
		sql = '''insert into account(Id, Name, Email, OpType, PassCode)
				values(125, '我是谁', 'whoami@whoami.com', 1, 'Password')
			  '''
		#sql = '''insert into account(Id, Name, Email, OpType, PassCode)
		#		values(125, 'Whoami', 'whoami@whoami.com', 1, 'Password')
		#	'''
		execute(conn, sql)
		conn.close()
