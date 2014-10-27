import psycopg2
import time
# from sparse import SparseList

# def getBinaryFeatureMap(db,rowName):
	# featureMap = {}
	# cursor = db.cursor()
	# cursor.execute('select distinct ' + rowName + ' from features_test')
	# values = cursor.fetchall()
	# counter = 0
	# for value in values:
		# if(value not in featureMap):
			# featureMap[value] = counter
			# counter += 1
		# else:
			# print('Error distinct had the same value twice: ' + str(value))
	# cursor.close()
	# print(rowName + ' has ' + str(len(featureMap)) + ' distinct values')
	# return featureMap

# def getSparseList(value,featureMap):
	# sparseList = SparseList()
	# sparseList[len(featureMap)-1] = 0
	# sparseList[featureMap[value]] = 1
	# return sparseList
	
	
start_time = time.time()

query = '''
	select locale, url, code, detail, isRedir, redirDomain, 
	html_length, content_length, dom_depth, number_of_tags, unique_tags, 
	tfidf from features_test
	'''
	
scheme = "dbname=ts_analysis"

#would not fit in memory
# pages = []
db = psycopg2.connect(scheme)

# codeFeatureMap = getBinaryFeatureMap(db,'code')
# detailFeatureMap = getBinaryFeatureMap(db,'detail')
# redirDomainFeatureMap = getBinaryFeatureMap(db,'redirDomain')

cursor = db.cursor()
cursor.itersize = 100
cursor.execute(query)
# row = cursor.fetchone()
counter  = 0
for row in cursor:
# while row:
	counter += 1
	print(counter)
	# Hold features for a given row/example/page
	page = []
	# add none tfidf features:
	page.extend(row[:-1])
	# Adding tfidf features
	tfidf = row[11].split(',')
	page.extend(tfidf)
	# Adding code features
	# code = getSparseList(row[2],codeFeatureMap)
	print(len(page))
	# pages.append(page)
	# row = cursor.fetchone()

cursor.close()
db.close()


print("--- " + str(time.time() - start_time) + " seconds ---")