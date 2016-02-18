#!/usr/bin/python

import sys
filename=sys.argv[1]

def bla():

#	filename="/home/pini/Downloads/mind_gene/mind_hw22"
	with open (filename) as f:
	
#		for i in 10 
		
			for line in f:
			
				#print line.split()[0]
				return len(line.split())
				break			
length=bla()
print "length is : ",length,"\n"

#filename="/home/pini/Downloads/mind_gene/mind_hw22"

for i in range(length):
		 
#	 print i 
	 with open (filename) as f:	
	         list1 = ""
                 for line in f:
			
#			 print "\n","row ",i,"",line.split()[i]
			 list1 += '\''+line.split()[i]+'\''','
         print list1 		

 
