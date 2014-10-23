from pagedb import PageDB
import time
import json
#import sys
#sys.path.append('')

# class Document(object):
    # def __init__(self,uid,words,count):
        # self.uid = uid
        # self.words = words
        # self.count = count

tfFileName = 'tfidf/tf.json'
idfRowFileName = 'tfidf/idfRow.json'
idfColumnFileName = 'tfidf/idfColumn.json'
idfFileName = 'tfidf/idf.json'
     
start_time = time.time()
counter = 0
scheme = "dbname=ts_analysis"
db = PageDB(scheme)
limit = 1000
# document = []
# entire country vs page matrix
idfGlobal = {}
# same page different countries
idfRow = {}
# same country different pages
idfColumn = {}

def jsonDump(fileName,dictionary):
    f = open(fileName, mode = 'w')
    f.write(json.dumps(dictionary))
    f.close()


tfFile = open(tfFileName, mode = 'a')
for page in db.get_pages(where_clause = "", limit = limit, ordered = False):
    originalURL = page.url
    url_id = page.page_id[1]
    locale = page.locale
    # result = page.result
    # detail = page.detail
    # html = page.html_content
    userContent =  page.text_content
    # redirURL = page.redir_url

    #print(originalURL + ' - ' + redirURL + ' - ' + locale + ' - ' + result + ' \n')
    #print (userContent+'\n\n\n')
    content = userContent.split()
    tf = {}
    if(url_id not in idfRow):
        idfRow[url_id] = {}
    if(locale not in idfColumn):
        idfColumn[locale] = {}
    # if uids == 0:
        # print(userContent+'\n\n\n')
        # print(len(content))
    for word in content:
        if(word not in tf):
            tf[word] = 0
        tf[word] += 1
            # you can place this here, if it is in not inf 
            # if word not in wordtfidf:
                # wordtfidf[word] = [0,0]
                
    # didn't make a function for the same code for speed issues - probably not important
    for word in tf.keys():
        if(word not in idfGlobal):
            idfGlobal[word] = 0   
        idfGlobal[word] += 1    
        if(word not in idfRow[url_id]):
            idfRow[url_id][word] = 0    
        idfRow[url_id][word] += 1    
        if(word not in idfColumn[locale]):
            idfColumn[locale][word] = 0    
        idfColumn[locale][word] += 1 
    # write tf file
    tfFile.write(locale + ';' + str(url_id) + ';' + json.dumps(tf) + '\n')
    #print(wordtfidf)
    # document = Document(uids,wordtfidf.keys(),list(wordtfidf.values()))
    counter = counter + 1
    print(counter)
    #print (words)
    #print (document.count)
tfFile.close()   

jsonDump(idfRowFileName,idfRow)
jsonDump(idfColumnFileName,idfColumn)
jsonDump(idfFileName,idfGlobal)
# print(idf)
#print(document.count)
print("--- " + str(time.time() - start_time) + " seconds ---")
#this = document
#print (this.count)
