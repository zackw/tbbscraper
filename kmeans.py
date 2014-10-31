import psycopg2
import time
import os
import numpy as np
import resource

from kmeanspp import *
from sparse import SparseList

def getBinaryFeatureMap(db,rowName):
	featureMap = {}
	cursor = db.cursor()
	cursor.execute('select distinct ' + rowName + ' from features_test')
	values = cursor.fetchall()
	counter = 0
	for value in values:
		if(value[0] not in featureMap):
			featureMap[value[0]] = counter
			counter += 1
		else:
			print('Error distinct had the same value twice: ' + str(value))
	cursor.close()
	print(rowName + ' has ' + str(len(featureMap)) + ' distinct values')
	return featureMap

def getSparseList(value,featureMap):
	sparseList = SparseList()
	sparseList[len(featureMap)-1] = 0
	sparseList[featureMap[value]] = 1
	return sparseList


start_time = time.time()

query = '''
	select locale, url, code, detail, isRedir, redirDomain, 
	html_length, content_length, dom_depth, number_of_tags, unique_tags, 
	tfidf from features_test limit 10
	'''

scheme = "dbname=ts_analysis"

#would not fit in memory
# pages = []
db = psycopg2.connect(scheme)

codeFeatureMap = getBinaryFeatureMap(db,'code')
# detailFeatureMap = getBinaryFeatureMap(db,'detail')
# redirDomainFeatureMap = getBinaryFeatureMap(db,'redirDomain')

# cursor = db.cursor()
# cursor.itersize = 100
# cursor.execute(query)
# row = cursor.fetchone()
savedpage = []
keys=[]
counter  = 0
with db, \
    db.cursor("pagedb_qtmp_{}".format(os.getpid())) as cur:
    cur.itersize = 10000
    cur.execute(query)
    for row in cur:
        counter += 1
        #print(counter)
        # Hold features for a given row/example/page
        page = []
        # add none tfidf features:
        keys.append(row[0:2])
        print(keys)
        page.append(row[4])
        page.extend(row[6:-1])
        # Adding tfidf features
        tfidf = row[11].split(',')
        page.extend(tfidf)
        # Adding code features
        code = getSparseList(row[2],codeFeatureMap)
        page.extend(code)
        #print(page[:11])
        #print(page[-13:])
        print(len(page))
        # print(page[0:11])
        print(counter)
        savedpage.append(list(map(float,page)))
        # pages.append(page)
        # row = cursor.fetchone()))
        #savedpage.extend(page)
savedpage = np.array(savedpage)
print(savedpage.sum())

#cursor.close()
db.close()
kmpp = KMeansPlusPlus(savedpage, 3 ,max_iterations=5)
kmpp.cluster()
cls = kmpp.clusters
final = dict(zip(keys,cls))
print(final)
"""
print(len(savedpage))
thefile = open("testlist", "w")
for item in savedpage:
    thefile.write("%s\n" % item)
thefile.close()
"""
print("--- " + str(time.time() - start_time) + " seconds ---")
print(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
