#ifndef FEATS_H_
#define FEATS_H_

#include <stddef.h>
#include <vector>
#include <string>

#include "configure.h"
#include "SylMap.h"

typedef struct _featuresOfSyllabel {
	std::string syllabel;
	std::string type;
	int label; /* 0 : SPACE, 1 : UNDER */
} featuresOfSyllabel;

class Feats {
private:
	std::vector<Feat*> feats_;
	std::string regex(std::string text, FeatsReference ref);
	SylMap _syl;

public:
	Feats(const char *path);
	~Feats();
	size_t size(void);
	std::vector<Feat*>* get();
	void add(Feat* f);
	std::string type(std::string syl);
	std::vector<featuresOfSyllabel>*
          token(std::string text, FeatsReference ref);
	void erase(size_t x);
	void clear();
};

#endif /* FEATS_H_ */
