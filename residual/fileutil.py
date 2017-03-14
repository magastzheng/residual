#coding=utf8
import os

def readFile(fileName):
	"""按文本方式读取文件，并返回所有文本内容
		fileName - 文件名（包含完整路径）
		return - 文本内容
	"""
	text = ''
	if os.path.exists(fileName) and os.path.isfile(fileName):
			fileObj = open(fileName)
			try:
					text=fileObj.read()
			finally:
					fileObj.close()
	else:
			print 'File: {0} is not found!'.format(fileName)
	
	return text

def writeFile(fileName, text):
	"""往指定文件中写入文本
		fileName - 指定写入文本的文件，包含完整路径
		text - 需要写入的文本
	"""
	fileObj = open(fileName, 'w')
	try:
			fileObj.write(text)
	finally:
			fileObj.close()
