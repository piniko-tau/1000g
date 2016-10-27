#!/usr/bin/env python
import sys

#print sys.argv[1:]
myfile = str(sys.argv[1])
linewordnum=0
neg=0
pos=0
negpos=0
with open(myfile, "r") as file:
    for line in file:
	if 'patient' in line:
		continue
#	print line
	for word in line.split(','): 
		linewordnum+=1
		if (linewordnum > 2):
			if (('+' in word) and ('-' in word)):
#				print word
				negpos+=1
				continue
			if (('+' in word)):
#				print word
				pos+=1	
				continue
                        if (('-' in word)):
#                                print word
				neg+=1
				continue
	linewordnum=0
print "filename: "+myfile
print "negative strand peptide strings : "+str(neg)
print "positive strand peptide strings : "+str(pos)
print "negative and positive peptide strings : "+str(negpos)
