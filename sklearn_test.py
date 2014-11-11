from __future__ import division
import psycopg2
import time
import os
import numpy as np
import matplotlib.pyplot as plt
from numpy import linalg as LA
import resource

from sklearn.cluster import KMeans
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
	tfidf from features_test limit 1000
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

skip = 'crawler failure'
skipCN = 'timeout'
nSkipped = 0

cur = db.cursor("pagedb_qtmp_{}".format(os.getpid()))
cur.itersize = 10000
cur.execute(query)
for row in cur:
    locale = row[0]
    code =  row[2]
    # TODO filter by uninteresting rows -> maybe in other script
    if((code != skip) and ((locale != 'cn') or (code != skipCN))):
        counter += 1
        print(counter)
        # Hold features for a given row/example/page
        page = []
        # add none tfidf features:
        keys.append(row[0:2])
        #print(keys)
        page.append(row[4])
        page.extend(row[6:-1])
        # Adding tfidf features
        tfidf = row[11].split(',')
        #tfidf = row[11][:1000]
        page.extend(tfidf)
        # Adding code features
        code = getSparseList(row[2],codeFeatureMap)
        page.extend(code)
        print(page)
        #print(page[:11])
        #print(page[-13:])
        #print(len(page))
        # print(page[0:11])
        #print(counter)
        savedpage.append(list(map(float,page)))
        # pages.append(page)
        # row = cursor.fetchone()))
        #savedpage.extend(page)
#savedpage = np.array(savedpage)
print(savedpage[1])

#cursor.close()
db.close()
norm = LA.norm(savedpage,axis=1)
savedpage = savedpage/norm[:,None]
print(savedpage)

# k Inertia test
K = [5,8,11]
inertia = np.zeros((len(K),))
for k in range(len(K)):
    kmeans = KMeans(init='k-means++', n_clusters=K[k])
    kmeans.fit(savedpage)
    inertia[k] = kmeans.inertia_
"""
plt.figure(1)
plt.plot(K,inertia)
plt.xlabel('Number of k')
plt.ylabel('inertia')
"""
print(inertia)
# Convergence test
L = [1,2,3,4,5]
itera = np.zeros((len(L),))
for l in range(len(L)):
    kmeans = KMeans(init='k-means++', n_clusters=3, max_iter=L[l])
    kmeans.fit(savedpage)
    itera[l] = kmeans.inertia_
print(itera)
"""
plt.figure(2)
plt.plot(L,itera)
plt.xlabel('Number of iterations')
plt.ylabel('inertia')
plt.show()
"""
#km = KMeans(n_clusters=10, init='k-means++',max_iter=20)
#km.fit(savedpage)
#final = dict(zip(keys,cls))
#print(km.labels_)
# print(savedpage[1])

#cursor.close()
"""
print(len(savedpage))
thefile = open("testlist", "w")
for item in savedpage:
    thefile.write("%s\n" % item)
thefile.close()
"""
print("--- " + str(time.time() - start_time) + " seconds ---")
