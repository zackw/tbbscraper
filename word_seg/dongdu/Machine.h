#ifndef MACHINE_H_
#define MACHINE_H_

#include <stddef.h>
#include <string>
#include <vector>

#include "DicMap.h"
#include "Feats.h"
#include "StrMap.h"
#include "configure.h"
#include "linear.h"

class Machine {
private:
	size_t index_SPACE;
	size_t index_UNDER;
	size_t reference;
	Feats* 		feats;
	DicMap		dicmap;
	StrMap		strmap;
	model*		_model;
	problem   _problem;
	std::vector<featuresOfSyllabel>* vfeats;
	int  WINDOW_LENGTH;
	std::string PATH;

	size_t getByteOfUTF8(unsigned char c);
	std::string itostr(int x);
	void convert(std::string sentence);

public:
	Machine(int window_length, std::string path, StrMapReference ref);
	~Machine();

	void extract(std::string sentence, StrMapReference ref);
	void getProblem();
	void delProblem();

	/* Predictor */
	bool load();
	std::string segment(std::string sentence);
};

#endif /* Machine_H_ */
