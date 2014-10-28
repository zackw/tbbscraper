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
			return (domains[-3])
		if(secondLevel not in publicSuffixes[TLD]):
			return (domains[-2])
		else:
			if(len(domains) < 3):
				print('***ERROR domain should be longer or not in public suffixes: ' + str(domains))
				return '-1'
			thirdLevel = domains[-3]
			if(('*' in publicSuffixes[TLD][secondLevel]) and (('!' + thirdLevel) not in publicSuffixes[TLD][secondLevel])):
				return (domains[-4])
			if(thirdLevel not in publicSuffixes[TLD][secondLevel]):
				return (domains[-3])
			else:
				if(len(domains) < 4):
					print('***ERROR domain should be longer or not in public suffixes: ' + str(domains))
					return '-1'
				fourthLevel = domains[-4]
				if(fourthLevel not in publicSuffixes[TLD][secondLevel][thirdLevel]):
					return (domains[-4])
				else:
					if(len(domains) < 5):
						print('***ERROR domain should be longer or not in public suffixes: ' + str(domains))
						return '-1'
					return (domains[-5])

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
	
def getDomainRedir(originalURL, redirURL):
	isRedir = False
	originalDomain = getDomainFromURL(originalURL)
	redirDomain = '-1'
	if(redirURL != ''):
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
	
def isNone(variable):
	if(variable is None):
		variable = ''
	return variable
	
publicSuffixFile = 'publicsuffix.txt'		
scheme = "dbname=ts_analysis"
fileName = "test.txt"
limit = 10000
seed = 1234

pages = {}
publicSuffixes = loadPublicSuffixes(publicSuffixFile)	

db = PageDB(scheme)
for page in db.get_random_pages(limit, seed, ordered = True, want_links = False):
	originalURL = page.url.lower()
	redirURL = isNone(page.redir_url).lower()
	locale = page.locale
	url_id = page.page_id[1]
	result = isNone(page.result).lower()
	detail = isNone(page.detail)
	html = page.html_content
	userContent =  page.text_content
	dom_stats = page.dom_stats
	depth = len(dom_stats.tags_at_depth)
	NumberOfTagTypes = len(dom_stats.tags)
	numberOfTags = 0
	for tag in dom_stats.tags:
		numberOfTags += dom_stats.tags[tag]
	originalDomain, redirDomain, isRedir = getDomainRedir(originalURL.lower(), redirURL.lower())

	print(locale)
	print(originalURL)
		
	if(url_id not in pages):
		pages[url_id] = {}
	if(locale not in pages[url_id]):
		pages[url_id][locale] = result + ',' + str(isRedir) + ',' + redirDomain + ',' + str(depth) + ',' + str(NumberOfTagTypes) + ',' + str(numberOfTags) + ',' + detail  + ',' + str(len(userContent)) + ',' + str(len(html))
		# print(pages[url_id][locale])
	else:
		print('**** ID ERROR ****')
		print('**************** ID ERROR ****************')
		print('**** ID ERROR ****')

	# print(len(pages))

log = open('results/testLog.txt', mode = 'a')
log.write("pages[url_id][locale] = result + ',' + str(isRedir) + ',' + redirDomain + ',' + str(depth) + ',' + str(NumberOfTagTypes) + ',' + str(numberOfTags) + ',' + detail  + ',' + str(len(userContent)) + ',' + str(len(html))\n")
rowClusterNumbers = {}
for url_id in pages:
	comparision = {}
	for locale in pages[url_id]:
		if(pages[url_id][locale] not in comparision):
			comparision[pages[url_id][locale]] = 0
		comparision[pages[url_id][locale]] += 1
	noClusters = len(comparision)
	if(noClusters not in  rowClusterNumbers):
		rowClusterNumbers[noClusters] = 0
	rowClusterNumbers[noClusters] += 1
	if(noClusters > 1):
		log.write(str(noClusters) + '\n')
		log.write(str(pages[url_id]) + '\n')
log.close()
		
f = open('results/rowwiseTest4.csv', mode ='w')
for noClusters in rowClusterNumbers:
	f.write(str(noClusters) + ',' + str(rowClusterNumbers[noClusters]) + '\n')
f.close()
	# print(originalURL + ' - ' + redirURL + ' - ' + locale + ' - ' + result)
	# print(originalDomain + ' - ' + redirDomain + ' - ' + str(isRedir))
	# print(depth)
	# print(dom_stats.tags)
	# print(dom_stats.tags_at_depth)

		
		
	

	