from pagedb import PageDB
import ipaddress
from bs4 import BeautifulSoup
import re

def addSuffix(line, dict):
	#not empty or comment line or consist of invalid charachters
	if((line != '') and (line[:2] != '//') and (line.find('?') == -1)):
		domains = line.split('.')
		TLD = domains[-1]
		if(TLD not in dict):
			dict[TLD] = {}
		if(len(domains) > 1):
			secondLevel = domains[-2]
			if(secondLevel not in dict[TLD]):
				dict[TLD][secondLevel] = {}
			if(len(domains) > 2):
				thirdLevel = domains[-3]
				if(thirdLevel not in dict[TLD][secondLevel]):
					dict[TLD][secondLevel][thirdLevel] = {}	
				if(len(domains) > 3):
					fourthLevel = domains[-4]
					if(fourthLevel not in dict[TLD][secondLevel][thirdLevel]):
						dict[TLD][secondLevel][thirdLevel][fourthLevel] = {}
			
			
		
	
def loadPublicSuffixes(publicSuffixFile):
	publicSuffixes = {}

	f = open(publicSuffixFile, mode='r')
	line = f.readline()[:-1]
	while(line != '// ===END ICANN DOMAINS==='):
		addSuffix(line,publicSuffixes)	
		line = f.readline()[:-1]
	f.close()
	print('No. of public suffixes: ' + str(len(publicSuffixes)))
	return publicSuffixes

# always use FQDN	
def getRegisteredDomain(fqdn,publicSuffixes):
	#Exception it works for com domains if not fully qualified:
	if(fqdn[-1] != '.'):
		if(fqdn[-4:] != '.com'):
			return fqdn.lower().split('.')[-1] + '.com'
		else:
			domains = fqdn.lower().split('.')
	else:
		domains = fqdn[:-1].lower().split('.')
	
	
	
	TLD = domains[-1]
	if(TLD not in publicSuffixes):
		print('***ERROR TLD is not in publicSuffixes:' + TLD)
		print('FQDN: ' + fqdn)
		return '-1'
	else:
		if(len(domains) < 2):
			print('***ERROR domain should be longer or not in public suffixes: ' + str(domains))
			return '-1'
		secondLevel = domains[-2]
		if(('*' in publicSuffixes[TLD]) and (('!' + secondLevel) not in publicSuffixes[TLD])):
			return ".".join(domains[-3])
		if(secondLevel not in publicSuffixes[TLD]):
			return ".".join(domains[-2])
		else:
			if(len(domains) < 3):
				print('***ERROR domain should be longer or not in public suffixes: ' + str(domains))
				return '-1'
			thirdLevel = domains[-3]
			if(('*' in publicSuffixes[TLD][secondLevel]) and (('!' + thirdLevel) not in publicSuffixes[TLD][secondLevel])):
				return ".".join(domains[-4])
			if(thirdLevel not in publicSuffixes[TLD][secondLevel]):
				return ".".join(domains[-3])
			else:
				if(len(domains) < 4):
					print('***ERROR domain should be longer or not in public suffixes: ' + str(domains))
					return '-1'
				fourthLevel = domains[-4]
				if(fourthLevel not in publicSuffixes[TLD][secondLevel][thirdLevel]):
					return ".".join(domains[-4])
				else:
					if(len(domains) < 5):
						print('***ERROR domain should be longer or not in public suffixes: ' + str(domains))
						return '-1'
					return ".".join(domains[-5])

def getDomainFromURL(url):
	# QUESTION-TODO added .split('\\')[0], because sometime URL looked like: http://domain\
	domain = url.split('://')[1].split('/')[0].split(':')[0].split('\\')[0]
	if(domain[-1] == '.'):
		domain = domain[:-1]
	return domain

def isIP(string):
	a = string.split('.')
	if(len(a) != 4):
		return False
	for x in a:
		if(not x.isdigit()):
			return False
		i = int(x)
		if((i < 0) or (i > 255)):
			return False
	return True

# def getTF(visible_texts):
	# wordList = visible_texts.split(' ')
	# wordDict = {}
	# for word in wordList:
		# if(word not in wordDict):
			# wordDict[word] = 0
		# wordDict[word] += 1
	# return wordDict
	
# Hongyu's code depreciated
# def _visible(element):
	# if element.parent.name in ['style', 'script', '[document]', 'head', 'title']:
		# return False
	# elif re.match('<!--.*-->', str(element.encode('utf-8','replace'))):
		# return False
	# return True
# def getHtmlFeatures(html):
	# soup = BeautifulSoup(html)
	# print(len(list(soup.descendants)))
	# if(len(list(soup.descendants)) == 306):
		# print(list(soup.descendants))
	# texts = soup.findAll(text=True)
	# visible_list = filter(_visible,texts)
	# visible_texts = re.sub('[^0-9a-zA-Z]+', ' ',' '.join(visible_list))
	# tfDict = getTF(visible_texts)
	# print(tfDict)
	# return tfDict, None
	
def getDomainRedir(originalURL, redirURL):
	isRedir = False
	originalDomain = getDomainFromURL(originalURL)
	redirDomain = '-1'
	if(redirURL is not None):
		redirDomain = getDomainFromURL(redirURL)
	else:
		return originalDomain, redirURL, isRedir
	
	if(isIP(originalDomain) or isIP(redirDomain)):
		return originalDomain, redirDomain, isRedir
	else:
		originalDomain = getRegisteredDomain(originalDomain + '.',publicSuffixes)
		if(originalDomain == -1):
			originalDomain = originalURL
		redirDomain = getRegisteredDomain(redirDomain + '.',publicSuffixes)
		if(redirDomain == -1):
			redirDomain = redirURL
		isRedir = (redirDomain != originalDomain)
	
	return originalDomain, redirDomain, isRedir
	
publicSuffixFile = 'publicsuffix.txt'		
scheme = "dbname=ts_analysis"
fileName = "test.txt"
limit = "1000"

pages = {}
publicSuffixes = loadPublicSuffixes(publicSuffixFile)	

db = PageDB(scheme)
for page in db.get_pages(where_clause = "", limit = limit, ordered = False):
	originalURL = page.url
	locale = page.locale
	url_id = page.page_id[1]
	result = page.result
	detail = page.detail
	html = page.html_content
	userContent =  page.text_content
	dom_stats = page.dom_stats
	depth = len(dom_stats.tags_at_depth)
	# userContentFeatures, domFeatures = getHtmlFeatures(html)
	redirURL = page.redir_url
	originalDomain, redirDomain, isRedir = getDomainRedir(originalURL, redirURL)
	if(detail is None):
		detail = ''
	if(url_id not in pages):
		pages[url_id] = {}
	if(locale not in pages[url_id]):
		pages[url_id][locale] = result + ',' + detail + ',' + str(isRedir) + ',' + redirDomain + ',' + str(depth)# + ',' + str(len(userContent)) + ',' + str(len(html))
		# print(pages[url_id][locale])
	else:
		print('**** ID ERROR ****')
		print('**************** ID ERROR ****************')
		print('**** ID ERROR ****')
	print(len(pages))
	
for url_id in pages:
	comparision = {}
	for locale in pages[url_id]:
		if(pages[url_id][locale] not in comparision):
			comparision[pages[url_id][locale]] = 0
		comparision[pages[url_id][locale]] += 1
	print(len(comparision))
		
	# print(originalURL + ' - ' + redirURL + ' - ' + locale + ' - ' + result)
	# print(originalDomain + ' - ' + redirDomain + ' - ' + str(isRedir))
	# print(depth)
	# print(dom_stats.tags)
	# print(dom_stats.tags_at_depth)

		
		
	

	