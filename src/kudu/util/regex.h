#include <mutex>
#include <string>
#include <vector>

#include <glog/logging.h>
#include <regex.h>


class KuduRegex {
 public:
  KuduRegex(const char* pattern, int num_matches)
    : num_matches_(num_matches) {
    CHECK_EQ(0, regcomp(&re_, pattern, REG_EXTENDED));
  }

  bool Match(std::string in, std::vector<std::string>* matches) {
    regmatch_t match[num_matches_ + 1];
    int status = regexec(&re_, in.c_str(), num_matches_ + 1, match, 0);
    if (status != 0) {
      return false;
    }
    for (int i = 1; i < num_matches_ + 1; i++) {
      matches->emplace_back(in.substr(match[i].rm_so, match[i].rm_eo - match[i].rm_so));
    }
    return true;
  }

 private:
   const int num_matches_;
   regex_t re_;
};
