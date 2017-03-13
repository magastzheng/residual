def testLoop():
		for x in range(1, 11):
				if x % 4 == 2:
						print 'come here {0}'.format(x)
						continue
				print x

if __name__ == '__main__':
	testLoop()
