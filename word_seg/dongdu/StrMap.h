#ifndef STRMAP_H_
#define STRMAP_H_

#include <stddef.h>
#include <map>
#include <string>
#include <utility>
#include "configure.h"

class StrMap {
private:
	std::map<std::string, size_t> smap_;
	size_t size_;

public:
	StrMap();
	~StrMap();
	size_t getNum(const std::string str, StrMapReference ref);
	size_t size();
	void insert(std::pair<std::string, size_t> pa);
	void print(std::string mapfile);
	bool load(std::string path);
	std::map<std::string, size_t>::iterator begin() {return smap_.begin(); };
	std::map<std::string, size_t>::iterator end() {return smap_.end(); };
};

#endif /* STRMAP_H_ */
