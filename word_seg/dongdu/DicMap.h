#ifndef DICMAP_H_
#define DICMAP_H_

#include <map>
#include <string>

class DicMap {
private:
	std::map<std::string, int> dmap_;
public:
	DicMap(const char *datadir);
	~DicMap();
	bool isWord(const std::string str);
};

#endif /* DICMAP_H_ */
