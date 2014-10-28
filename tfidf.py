from pagedb import PageDB
#import sys
#sys.path.append('')

class Document(object):
    def __init__(self,uid,words,count):
        self.uid = uid
        self.words = words
        self.count = count

uids = 0
scheme = "dbname=ts_analysis"
db = PageDB(scheme)
limit = 20
document = []
wordtfidf = {}
for page in db.get_pages(where_clause = "", limit = limit, ordered = False):
    originalURL = page.url
#    pid = '-'.join(page.page_id)
    locale = page.locale
    result = page.result
    detail = page.detail
    html = page.html_content
    userContent =  page.text_content
    redirURL = page.redir_url

    #print(originalURL + ' - ' + redirURL + ' - ' + locale + ' - ' + result + ' \n')
    #print (userContent+'\n\n\n')

    content = userContent.split()
    idf = {}
    if uids == 0:
        print(userContent+'\n\n\n')
        print(len(content))
    for word in content:
        if word not in wordtfidf:
            wordtfidf[word] = [0,0]
        if word not in idf:
            idf[word] = 0
        wordtfidf[word][0] += 1
        idf[word] += 1

    for word in idf.keys():
        wordtfidf[word][1] += 1

    #print(wordtfidf)
    document = Document(uids,wordtfidf.keys(),list(wordtfidf.values()))
    uids = uids + 1
    print(uids)
    #print (words)
    #print (document.count)

print(wordtfidf)
#print(document.count)

#this = document
#print (this.count)
