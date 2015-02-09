/*
 * SylMap.cpp
 *
 *  Created on: 2012/09/19
 *      Author: anh
 */

#include "SylMap.h"
#include <fstream>

using namespace std;

SylMap::SylMap(const char *datadir)
{
	_syl.clear();
	string vnsyl_f(datadir);
	vnsyl_f += "/data/VNsyl.txt";

	ifstream ifs(vnsyl_f.c_str());

	if (!ifs) {
		return;
	}

	int N;
	ifs >> N;

	string str;
	for(int i = 0; i < N; ++i) {
		ifs >> str;
		_syl.insert(str);
	}

	return;
}

SylMap::~SylMap() {
	_syl.clear();
}

bool SylMap::isVNESE(string syllabel)
{
	set<string>::iterator it;
	it = _syl.find(syllabel);
	return (it != _syl.end());
}
