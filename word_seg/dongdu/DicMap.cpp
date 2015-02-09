#include "DicMap.h"
#include <fstream>
#include <utility>

using namespace std;

DicMap::DicMap(const char *datadir)
{
	string wordlist_f(datadir);
	wordlist_f += "/data/wordlist.txt";
	ifstream ifs(wordlist_f.c_str());
	if (!ifs) {
		return;
	}
	string str;

	while(ifs && getline(ifs,str)) {
		dmap_.insert(pair<string, int> ( str , 1));
	}
}

DicMap::~DicMap()
{
	dmap_.clear();
}

bool DicMap::isWord(const string str)
{
	map<string, int>::iterator pos = dmap_.find(str);
	return (pos != dmap_.end());
}
