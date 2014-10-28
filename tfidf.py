from pagedb import PageDB
import time
import json
import os


tfFileName = 'tfidf/tf.json'
tfGlobalFileName = 'tfidf/tfGlobal.json'
idfRowFileName = 'tfidf/idfRow.json'
idfColumnFileName = 'tfidf/idfColumn.json'
idfFileName = 'tfidf/idf.json'
     
start_time = time.time()
counter = 0
scheme = "dbname=ts_analysis"
db = PageDB(scheme)
<<<<<<< HEAD
limit = 20
document = []
wordtfidf = {}
for page in db.get_pages(where_clause = "", limit = limit, ordered = False):
=======
limit = 100000
seed = 1234

# TODO need tf global row and tf global column also.
#tf global for selecting overall features
tfGlobal = {}
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

# os.remove(tfFileName)
tfFile = open(tfFileName, mode = 'a')
for page in db.get_random_pages(limit, seed, ordered = True, want_links = False):
>>>>>>> edd65cc9aaf9dde3ddacb6c1743d691f325a66c6
    originalURL = page.url
    url_id = page.page_id[1]
    locale = page.locale
    # result = page.result
    # detail = page.detail
    # html = page.html_content
    userContent =  page.text_content
    # redirURL = page.redir_url

    #print(originalURL + ' - ' + redirURL + ' - ' + locale + ' - ' + result + ' \n')

    content = userContent.split()
    tf = {}
    if(url_id not in idfRow):
        idfRow[url_id] = {}
    if(locale not in idfColumn):
        idfColumn[locale] = {}
    for word in content:
        word = word.lower()
        if(word not in tf):
            tf[word] = 0
        tf[word] += 1
        if(word not in tfGlobal):
            tfGlobal[word] = 0
        tfGlobal[word] += 1
                
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
    counter = counter + 1
    print(counter)

tfFile.close()   
jsonDump(idfRowFileName,idfRow)
jsonDump(idfColumnFileName,idfColumn)
jsonDump(idfFileName,idfGlobal)
jsonDump(tfGlobalFileName,tfGlobal)

print("--- " + str(time.time() - start_time) + " seconds ---")

