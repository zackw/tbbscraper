import re
from urllib.parse import urlparse
from publicsuffix import PublicSuffixList

class DomainParking:
    '''
        This class can be used to classify webpages as parked or not.

        Properties:
            type                - Determines the set of rules to use from all
                                  available rules
            size_limit          - if the content of a pages is smaller than
                                  size_limit, then it is classified
                                  automatically as "not parked"
            rulesToUse          - based on "type" this is the list of rules
                                  that can be used for parked classification
            parkedRuleFile      - File of rules that are matching a given
                                  specific parked page
            weakparkedRule1File - Rules that are matching general domain
                                  parking terms
            weakparkedRule2File - Rules that are matching general domain
                                  parking terms
    '''

    def __init__(self, type = 'full', size_limit = 200000):
        '''
            Parameters:
                type        - full: using all regexes
                            - balanced: using regexes that were useful
                              at least once (precisely: tp > fp for this rule)
                            - min: using the minimal set of regexes that cover
                              parked domains found by using "full" <- it might
                              not find about 3% of pared pages
                size_limit  - if the content of a pages is smaller than
                              size_limit, then it is classified automatically
                              as "not parked"
        '''
        self.type = type
        self.size_limit = size_limit
        self.rulesToUse = None
        if type in ['full','balanced','min']:
            self.parkedRuleFile = 'parkedRules/reExpressions.in'
            self.weakparkedRule1File = 'parkedRules/reExpressions-weak1.in'
            self.weakparkedRule2File = 'parkedRules/reExpressions-weak2.in'
        else:
            self.parkedRuleFile = ''
            self.weakparkedRule1File = ''
            self.weakparkedRule2File = ''

        if type in ['balanced','min']:
            f = open('parkedRules/'+type+'_rules', mode = 'r')
            self.rulesToUse = f.read().split('\n')
            f.close()

        self.psl = None

    def changeRule(self,parkedRuleFile,weakparkedRule1File,weakparkedRule2File):
        '''
        Function changeRule can be used to set custom rule files.

        Parameters:
            parkedRuleFile      - File of rules that are matching a given
                                  specific parked page
            weakparkedRule1File - Rules that are matching general domain
                                  parking terms
            weakparkedRule2File - Rules that are matching general domain
                                  parking terms
        '''
        self.parkedRuleFile = parkedRuleFile
        self.weakparkedRule1File = weakparkedRule1File
        self.weakparkedRule2File = weakparkedRule2File

    def changSizeLimit(self,size_limit):
        '''
        Change Size limit

        Parameters:
            size_limit    - the nez size limit
        '''
        self.size_limit = size_limit

    def __getRules__(self,fileName):
        '''
        Gets the rules from a file and returns it as a list.

        Parameters:
        fileName    - the filename of the rules we are going to load
        '''
        rules = []
        f = open(fileName, mode='r')
        for line in f:
            exp = line[:-1].split(" ")
            if ((self.rulesToUse is None) or
                (fileName in [self.weakparkedRule1File,
                              self.weakparkedRule2File]) or
                (exp[0] in self.rulesToUse)):
                rule = []
                rule.append(exp[0])
                rule.append(re.compile(exp[1].strip(), re.IGNORECASE))
                rules.append(rule)
        f.close()
        return rules

    def loadRules(self):
        '''
        Calling this function we will load the rules used for detecting
        parked domains.
        '''
        if ((self.parkedRuleFile != '') and
            (self.weakparkedRule1File != '') and
            (self.weakparkedRule2File != '')):
            self.rules = self.__getRules__(self.parkedRuleFile)
            self.weak_rules1 = self.__getRules__(self.weakparkedRule1File)
            self.weak_rules2 = self.__getRules__(self.weakparkedRule2File)

    def isParkedDomain(self, dom, domain):
        '''
        This function tests whether a page is parked or not. It
        returns a "result" dict which includes "parked" or "notparked"
        and it also returns the matched rules.

        Parameters:
            don    - the raw html of the page
            domain - The registered domain name of the webpage where
                     we get the content from (example.co.uk from
                     www.example.co.uk)
        '''
        result = {}
        result['isParked'] = ''
        result['rules'] = []
        if len(dom) <= self.size_limit:
            parked_rules = self.__applyRules__(dom,self.rules)
            weak_parked_rules1 = self.__applyRules__(dom,self.weak_rules1)
            weak_parked_rules2 = self.__applyRules__(dom,self.weak_rules2)
            '''A page is classified as parked if a rule is found from
               "parked_rules" or if a rule is found from both
               "weak_parked_rules1" and "weak_parked_rules2".

            '''
            if ((len(parked_rules) > 0) or
                ((len(weak_parked_rules1) > 0) and
                 (len(weak_parked_rules2) > 0))):
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
        if result['isParked'] == 'notparked':
            for keyword in ['tppunknown.com',
                            '.'.join(domain.split('.')[-2:])]:
                general_parking1 = 'Click here to go to ' + keyword
                general_parking2 = '<meta name="keywords" content="'+keyword+'">'
                if ((general_parking1.lower() in dom.lower()) and
                    (general_parking2.lower() in dom.lower())):
                    result['isParked'] = 'parked'
                    result['rules'].append('generalparking')

        return result

    def isParkedUrl(self, dom, url):
        '''
        Same as isParkedDomain but takes a full URL instead of a registered
        domain name.
        '''
        if self.psl is None:
            self.psl = PublicSuffixList()

        # This function actually returns the "registered domain", i.e.
        # the public suffix plus one more label to the left.
        domain = psl.get_public_suffix(urlparse(url.strip()).hostname)

        return self.isParkedDomain(dom, domain)

    def isParked(self, dom, domain=None, url=None):
        '''
        Generic entry point can be used with either domain= or url= arguments.
        '''
        if domain:
            if url:
                raise ValueError("need either domain= or url=, but not both")
            return self.isParkedDomain(dom, domain)
        elif url:
            return self.isParkedUrl(dom, url)
        else:
            raise ValueError("need either domain= or url= argument")

#
# Self-tests
#

def testParkedSample(content_dir, filename, domainParking):
        ok = 0
        errors = []
        with open(filename) as f:
            for line in f:
                data = line[:-1].split(',')
                cls = data[6]
                url = data[5]
                id = data[0]
                content_file = content_dir + '/' + id + '.html'
                with open(content_file, encoding='utf-8') as cf:
                    content = cf.read()

                result = domainParking.isParkedUrl(content, url)
                if(result['isParked'] == cls):
                    ok += 1
                else:
                    errors.append(id)

        print(filename)
        print('Ok: ' + str(ok))
        print('Errors: ' + " ".join(sorted(errors)))
        print()

def testRules(type):
    import time

    domainParking = DomainParking(type)
    domainParking.loadRules()

    start_time = time.monotonic()
    testParkedSample('parkedRESamples', 'parkedRE_samples-200.txt',
                     domainParking)
    testParkedSample('parkedRESamples2', 'parkedRE_samples2-200.txt',
                     domainParking)
    print("--- " + str(time.monotonic() - start_time) + " seconds ---")

if __name__ == '__main__':
    for type in ('full', 'balanced', 'min'):
        testRules(type)
