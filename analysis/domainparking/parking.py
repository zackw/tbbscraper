import re


'''
    This class can be used to classify webpages as parked or not.
    
    Properties:
        type                - Determines the set of rules to use from all available rules
        size_limit          - if the content of a pages is smaller than size_limit, then it is classified automatically as "not parked"
        rulesToUse          - based on "type" this is the list of rules that can be used for parked classification
        parkedRuleFile      - File of rules that are matching a given specific parked page
        weakparkedRule1File - Rules that are matching general domain parking terms
        weakparkedRule2File - Rules that are matching general domain parking terms
'''

class DomainParking:
    '''
        Parameters:
            type        - full: using all regexes
                        - balanced: using regexes that were useful at least once (precisely: tp > fp for this rule)
                        - min: using the minimal set of regexes that cover parked domains found by using "full" <- it might not find about 3% of pared pages
            size_limit  - if the content of a pages is smaller than size_limit, then it is classified automatically as "not parked"
    '''
    def __init__(self, type = 'full', size_limit = 200000):
        self.type = type
        self.size_limit = size_limit
        self.rulesToUse = None
        if(type in ['full','balanced','min']):
            self.parkedRuleFile = 'parkedRules/reExpressions.in'
            self.weakparkedRule1File = 'parkedRules/reExpressions-weak1.in'
            self.weakparkedRule2File = 'parkedRules/reExpressions-weak2.in'
        else:
            self.parkedRuleFile = ''
            self.weakparkedRule1File = ''
            self.weakparkedRule2File = ''
            
        if(type in ['balanced','min']):
            f = open('parkedRules/'+type+'_rules', mode = 'r')
            self.rulesToUse = f.read().split('\n')
            f.close()
        
    '''
        Function changeRule can be used to set custom rule files.
        
        Parameters:
            parkedRuleFile      - File of rules that are matching a given specific parked page
            weakparkedRule1File - Rules that are matching general domain parking terms 
            weakparkedRule2File - Rules that are matching general domain parking terms 
    '''
    def changeRule(self,parkedRuleFile,weakparkedRule1File,weakparkedRule2File):
        self.parkedRuleFile = parkedRuleFile
        self.weakparkedRule1File = weakparkedRule1File
        self.weakparkedRule2File = weakparkedRule2File
    '''
        Change Size limit
        
        Parameters:
            size_limit    - the nez size limit
    '''
    def changSizeLimit(self,size_limit):
        self.size_limit = size_limit
        
    '''
        Gets the rules from a file and returns it as a list.
        
        Parameters:
            fileName    - the filename of the rules we are going to load
    '''
    def __getRules__(self,fileName):
        rules = []
        f = open(fileName, mode='r')
        for line in f:
            exp = line[:-1].split(" ")
            if((self.rulesToUse is None) or (fileName in [self.weakparkedRule1File,self.weakparkedRule2File]) or (exp[0] in self.rulesToUse)):
                rule = []
                rule.append(exp[0])
                rule.append(re.compile(exp[1].strip(), re.IGNORECASE))#+re.DOTALL))
                rules.append(rule)
        f.close()
        return rules
        
    '''
        Calling this function we will load the rules used for detecting parked domains.
    '''
    def loadRules(self):
        if((self.parkedRuleFile != '') and (self.weakparkedRule1File != '') and (self.weakparkedRule2File != '')):
            self.rules = self.__getRules__(self.parkedRuleFile)
            self.weak_rules1 = self.__getRules__(self.weakparkedRule1File)
            self.weak_rules2 = self.__getRules__(self.weakparkedRule2File)
        
    '''
        This function search for rules in the raw html of the parked page    

        Parameters:
            html    - the raw html of the page
            rules   - rules tested
    '''
    def __applyRules__(self,html,rules):
        res = []
        for i in range(len(rules)):
            findit = rules[i][1].search(html)
            if findit:
                res.append(rules[i][0])
        return res
    
    '''
        This function tests whether a page is parked or not. It returns a "result" dict which includes "parked" or "notparked"
        and it also returns the matched rules.
        
        Parameters:
            don    - the raw html of the page
            domain - The registered domain name of the webpage where we get the content from (example.co.uk from www.example.co.uk)
            
    '''
    def isParked(self,dom,domain):
        result = {}
        result['isParked'] = ''
        result['rules'] = []
        if(len(dom) <= self.size_limit):
            parked_rules = self.__applyRules__(dom,self.rules)
            weak_parked_rules1 = self.__applyRules__(dom,self.weak_rules1)
            weak_parked_rules2 = self.__applyRules__(dom,self.weak_rules2)
            '''
               A page is classified as parked if a rule is found from  "parked_rules" or if a rule is found from both "weak_parked_rules1" and "weak_parked_rules2".
            '''
            if((len(parked_rules) > 0)  or ((len(weak_parked_rules1) > 0) and (len(weak_parked_rules2) > 0))):   
                result['isParked'] = 'parked'
                for rule in parked_rules:
                    result['rules'].append(rule)
                for rule in weak_parked_rules1:
                    result['rules'].append(rule)
                for rule in weak_parked_rules2:
                    result['rules'].append(rule)
            else:
                result['isParked'] = 'notparked'    
        else:
            result['isParked'] = 'notparked'   
        '''
            This is a special rule which needs the input parameter "domain"
        '''
        if(result['isParked'] == 'notparked'):
            for keyword in ['tppunknown.com','.'.join(domain.split('.')[-2:])]:
                general_parking1 = 'Click here to go to ' + keyword
                general_parking2 = '<meta name="keywords" content="'+keyword+'">'
                if((general_parking1.lower() in dom.lower()) and (general_parking2.lower() in dom.lower())):
                    result['isParked'] = 'parked'
                    result['rules'],append('generalparking')
                    
        return result
    
    

if __name__ == '__main__':
    import sys
    sys.path.append('../')
    import misc
    from urllib.parse import urlparse
    import time
    
    def getFQDNFromURL(url):
        url_object = urlparse(url.strip())
        fqdn = url_object.hostname    
        if((fqdn is not None) and (fqdn[-1] != '.')):
            fqdn = fqdn + '.'
        return fqdn
    
    def getAuthDomainFromURL(url,publicSuffixes):
        return misc.getRegisteredDomain(getFQDNFromURL(url),publicSuffixes) 
    
    def testParkedSample(dir,filename,domainParking,publicSuffixes):
        ok = 0
        error = 0
        f = open(filename, mode = 'r')
        for line in f:
            data = line[:-1].split(',')
            cls = data[6]
            url = data[5]
            id = data[0]
            content_file = dir + id + '.html'
            f = open(content_file, mode = 'r', encoding = 'utf-8')
            content = f.read()
            f.close()
            domain = getAuthDomainFromURL(url,publicSuffixes)
            result = domainParking.isParked(content,domain)
            # if(id in ['9678404','9690828','9715276','9464551','9844617','9860077','9377285','9383264','9404889','9469284']):
                # print(result['rules'])
            if(result['isParked'] == cls):
                ok += 1
            else:
                error += 1
                # print('Error: ' + id)
                # print('Should be: ' + cls)
                # print('Result: ' + result['isParked'])
                # print(result['rules'])
                # print('')
        f.close()
        print(filename)
        print('Ok: ' + str(ok))
        print('Error: ' + str(error))
        print('')

    def testRules(type,publicSuffixes):
        domainParking = DomainParking(type = type)
        domainParking.loadRules()
        start_time = time.time()
        testParkedSample('parkedRESamples/','parkedRE_samples-200.txt',domainParking,publicSuffixes)
        testParkedSample('parkedRESamples2/','parkedRE_samples2-200.txt',domainParking,publicSuffixes)
        print("--- " + str(time.time() - start_time) + " seconds ---")
    
    def main(): 
        publicSuffixFile = '../publicsuffix2015.txt'
        publicSuffixes = misc.loadPublicSuffixes(publicSuffixFile)
        for type in ['full','balanced','min']:
            testRules(type,publicSuffixes)
      
    main()
