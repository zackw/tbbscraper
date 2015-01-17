
#include <cerrno>
#include <exception>
#include <fstream>
#include <sstream>
#include <iostream>
#include <string>
#include <system_error>
#include "compact_lang_det.h"
#include "encodings.h"
#include "generated_language.h"
#include "lang_script.h"

static std::string
get_file_contents(const char *fname)
{
  std::ifstream in(fname, std::ios::in | std::ios::binary);
  if (in) {
    std::ostringstream contents;
    contents << in.rdbuf();
    in.close();
    return contents.str();
  }
  throw std::system_error(errno, std::system_category());
}

int
main(int argc, char **argv)
{
  int rv = 0;
  for (int i = 1; i < argc; i++) {
    try {
      std::string fc = get_file_contents(argv[i]);

      CLD2::CLDHints hints = {
        0, 0, CLD2::UTF8, CLD2::UNKNOWN_LANGUAGE
      };
      CLD2::Language lang;
      CLD2::Language lang3[3];
      int            pct3[3];
      bool           is_reliable;

      lang =
      CLD2::ExtDetectLanguageSummary(fc.c_str(), fc.size(),
                                     &hints, 0, lang3, pct3, 0, &is_reliable);
      if (!is_reliable)
        lang = CLD2::UNKNOWN_LANGUAGE;

      std::cout << argv[i] << ": ";

      std::cout << CLD2::LanguageCode(lang) << '=' << CLD2::LanguageName(lang);
      if (lang3[0] != CLD2::UNKNOWN_LANGUAGE ||
          lang3[1] != CLD2::UNKNOWN_LANGUAGE ||
          lang3[2] != CLD2::UNKNOWN_LANGUAGE) {
        std::cout << " [";
        for (int j = 0; j < 3; j++) {
          if (lang3[j] != CLD2::UNKNOWN_LANGUAGE) {
            std::cout << ' ' << CLD2::LanguageCode(lang3[j])
                      << '=' << CLD2::LanguageName(lang3[j])
                      << '(' << pct3[j] << "%)";
          }
        }
        std::cout << " ]";
      }
      std::cout << '\n';

    } catch (const std::exception& e) {
      std::cout << argv[i] << ": " << e.what() << '\n';
      rv = 1;
    }
  }
  return rv;
}
