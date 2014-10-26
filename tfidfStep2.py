import json
import math
import time
import copy
from collections import OrderedDict

def getJSON(fileName):
	f = open(fileName, mode = 'r')
	dict = json.loads(f.read())
	f.close()
	return dict

def getTF(fileName):
	tf = {}
	noDocs = {}
	noDocs['total'] = 0
	# Number of documents where the locale is the same -> column idf
	noDocs['locale'] = {}
	# Number of documents where the url_id is the same -> row idf
	noDocs['url_id'] = {}
	
	f = open(fileName, mode = 'r')
	for line in f:
		locale, url_id = line[:-1].split(';')[0:2]
		jsonData = ';'.join(line[:-1].split(';')[2:])
		dict = json.loads(jsonData)
		noDocs['total'] += 1
		if(locale not in noDocs['locale']):
			noDocs['locale'][locale] = 0
		noDocs['locale'][locale] += 1
		if(url_id not in noDocs['url_id']):
			noDocs['url_id'][url_id] = 0
		noDocs['url_id'][url_id] += 1
		if(locale not in tf):
			tf[locale] = {}
		if(url_id not in tf[locale]):
			tf[locale][url_id] = dict
		else:
			print('**** **** ID ERROR **** ****')
			print(locale + '-' + url_id)
			print('**** **** ID ERROR **** ****')
	f.close()
	return tf, noDocs
	
class SparseList(list):
  def __setitem__(self, index, value):
    missing = index - len(self) + 1
    if missing > 0:
      self.extend(['0'] * missing)
    list.__setitem__(self, index, value)
  # def __getitem__(self, index):
    # try: return list.__getitem__(self, index)
    # except IndexError: return None
	
def getTFIDF(tf, idfRow, idfColumn, idf, noDocs, stopWords, tfGlobal, outputFileName):
	keys  = list(tfGlobal.keys())
	
		
	# dictScheme = OrderedDict.fromkeys(tfGlobal.keys(),0)
	# dictScheme = dict.fromkeys(tfGlobal.keys(),0)
	indexDict = {}
	for i in range(0,len(keys)):
		indexDict[keys[i]] = i
	counter = 0
	f = open(outputFileName, mode = 'a')
	for locale in tf:
		for url_id in tf[locale]:
			counter += 1
			print(counter)
			# too slow - tried it with ordereddict and dict too
			# tfidf2 = OrderedDict.fromkeys(keys,0)
			# tfidfRow2 = OrderedDict.fromkeys(keys,0)
			# tfidfColumn2 = OrderedDict.fromkeys(keys,0)
			# tfidf2 = OrderedDict(dictScheme)
			# tfidfRow2 = OrderedDict(dictScheme)
			# tfidfColumn2 = OrderedDict(dictScheme)
			# tfidf = {}
			# tfidfRow = {}
			# tfidfColumn = {}
			# tfidf = copy.deepcopy(dictScheme)
			# tfidfRow = copy.deepcopy(dictScheme)
			# tfidfColumn = copy.deepcopy(dictScheme)
			tfidf = SparseList()
			tfidfRow = SparseList()
			tfidfColumn = SparseList()
			for word in tf[locale][url_id]:
				if(word in tfGlobal):
					wFrequency = tf[locale][url_id][word]
					idfFrequency = idf[word]
					idfRowFrequency = idfRow[url_id][word]
					idfColumnFrequency = idfColumn[locale][word]
					
					# tfidf[word] = wFrequency * math.log(noDocs['total']/idfFrequency)
					# tfidfRow[word] = wFrequency * math.log(noDocs['url_id'][url_id]/idfRowFrequency)
					# tfidfColumn[word] = wFrequency * math.log(noDocs['locale'][locale]/idfColumnFrequency)
					
					tfidf[indexDict[word]] = str(wFrequency * math.log(noDocs['total']/idfFrequency))
					tfidfRow[indexDict[word]] = str(wFrequency * math.log(noDocs['url_id'][url_id]/idfRowFrequency))
					tfidfColumn[indexDict[word]] = str(wFrequency * math.log(noDocs['locale'][locale]/idfColumnFrequency))
			f.write(locale + ';' + url_id + ';' + 'idf' + ';' + ','.join(tfidf) + '\n')
			f.write(locale + ';' + url_id + ';' + 'idfRow' + ';' + ','.join(tfidfRow)  + '\n')
			f.write(locale + ';' + url_id + ';' + 'idfColumn' + ';' + ','.join(tfidfColumn)  + '\n')
	
	f.close()


def removeStopWords(tfGlobal,stopWords):
	words = set(tfGlobal.keys())
	for word in words:
		if(word in stopWords):
			del tfGlobal[word]
				
def getStopWords(stopWordFiles):
	stopWords = {}
	for stopWordFile in stopWordFiles:
		f = open(stopWordFile, mode ="r")
		for line in f:
			stopWords[line[:-1].lower()] = 0
		f.close()
		print('Stop words added: ' + str(len(stopWords)))
	return stopWords

start_time = time.time()
outputFileName = 'tfidf/tfidf.csv'
tf, noDocs = getTF("tfidf/tf.json")
tfGlobal = getJSON("tfidf/tfGlobal.json")
idfRow = getJSON("tfidf/idfRow.json")
idfColumn = getJSON("tfidf/idfColumn.json")
idf = getJSON("tfidf/idf.json")
stopWordFiles = ['stopwords/stopwords1.txt','stopwords/stopwords2.txt','stopwords/stop-words_english_1_en.txt','stopwords/stop-words_english_2_en.txt',
				'stopwords/stop-words_english_4_google_en.txt','stopwords/stop-words_english_5_en.txt','stopwords/stop-words_english_6_en.txt'] 
print(len(idfColumn))
print(len(idfRow))
print(len(idf))
stopWords =	getStopWords(stopWordFiles)	
print(len(tfGlobal))
removeStopWords(tfGlobal,stopWords)
print(len(tfGlobal))
tfGlobal = OrderedDict(sorted(tfGlobal.items(), key=lambda item: int(item[1]), reverse=True)[:50000])
print(len(tfGlobal))

getTFIDF(tf, idfRow, idfColumn, idf, noDocs, stopWords, tfGlobal, outputFileName)
	
	
print(len(idfRow))
print(len(idfColumn))
print(len(idf))
print(len(tf))
print("--- " + str(time.time() - start_time) + " seconds ---")
