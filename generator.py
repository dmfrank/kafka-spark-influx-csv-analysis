#!/usr/bin/python

import time
import random
import numpy.random

types = ('seal.t1', 'seal.t2')

while True:
	id = int(random.random() * 10 % 10)
	print(",".join([
		str(int(time.time())), 
		str(id),
		types[0 if id < 5 else 1],
		str(abs(int(numpy.random.normal(250, 50)))),
		str(abs(int(numpy.random.normal((1000 + 32) / 2, 200)))),
		str(abs(int(numpy.random.normal((5000 + 0) / 2, 500)))),
		str(abs(int(numpy.random.normal((1000 + 0) / 2, 200))))
	]))
	time.sleep(0.05)
