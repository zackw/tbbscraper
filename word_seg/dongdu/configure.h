#ifndef CONFIGURE_H_
#define CONFIGURE_H_

#include <string>
#include <set>

const int MAX_WORD_LENGTH = 3;

const char SPACE = ' ';
const char UNDER = '_';
const size_t LEARN   = 0;
const size_t PREDICT = 1;
const std::string SYMBOLS = "@`#$%&~|[]<>'(){}*+-=;,?.!:\"/";

typedef std::pair<size_t, std::set<size_t>* > Feat;
typedef size_t StrMapReference;
typedef size_t FeatsReference;

#endif /* CONFIGURE_H_ */
