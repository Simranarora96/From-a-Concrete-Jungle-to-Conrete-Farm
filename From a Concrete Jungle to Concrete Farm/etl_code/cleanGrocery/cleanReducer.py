import sys

for line in sys.stdin:
	fields = line.strip().split('\t')
	print(fields[1])
