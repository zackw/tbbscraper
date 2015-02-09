#ifndef SYLMAP_H_
#define SYLMAP_H_

#include <set>
#include <string>

class SylMap {
private:
	std::set<std::string> _syl;
public:
	SylMap(const char *datadir);
	~SylMap();
	bool isVNESE(std::string syllabel);
};

#endif /* SYLMAP_H_ */
