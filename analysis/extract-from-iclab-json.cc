/* Extract DNS lookup results from a baseline .json file.  This is a
   subroutine of fingerprint_anomalies.py, written in C++ for
   efficiency.  */

#undef NDEBUG
#include <cassert>
#include <iostream>
#include <regex>
#include <string>
#include <utility>
#include <vector>

using std::cerr;
using std::cin;
using std::cout;
using std::make_pair;
using std::pair;
using std::regex;
using std::regex_match;
using std::smatch;
using std::ssub_match;
using std::string;
using std::vector;

/* The input (stdin) to this program is expected to be a JSON blob with
   this overall structure:

     {
       "meta": {
         "foo": "bar",
         ...
       },
       "baseline": [
         {
           "dns": {
             "host.name.example": [
               {
                 "request": "<base64>",
                 "response1": "<base64>",
                 "response2": null,
                 "response1-ips": [ "192.0.2.1", "192.0.2.2" ],
                 "nameserver": "10.200.195.1",
                 ...
               },
               {
                 "request": "<base64>",
                 "response1": "<base64>",
                 "response2": "<base64>",
                 "response1-ips": [ "203.0.113.1", "203.0.113.2" ],
                 "response2-ips": [ "198.51.100.1", "198.51.100.2" ],
                 "nameserver": "8.8.8.8",
                 ...
               }
             ]
           },
           "http": {
             "http://success.example": {
               "request": { ... },
               "response": {
                 "status": 200,
                 "headers": { ... },
                 "reason": "OK",
                 "body": "..."
               },
               ...
             },
             "http://redirection.example": {
               "redirects": {
                 "0": { "request": { ... }, "response": { ... }, ... },
                 "1": { "request": { ... }, "response": { ... }, ... },
                 ...
               },
               ...
             },
             "http://failure.example": {
               "request": { ... },
               "response": {
                 "failure": "Operation timed out"
               },
               ...
             },
           },
           ...
         }
       ]
     }

   The output is a series of 3- or 4-element JSON arrays, one per
   line, carrying only the information we actually want:

       // "meta"
       ["meta","foo","bar"]

       // "dns"
       ["dns","host.name.example","192.0.2.1"]
       ["dns","host.name.example","192.0.2.2"]
       ["dns","host.name.example","192.0.2.1"]
       ["dns","host.name.example","203.0.113.1"]
       ["dns","host.name.example","203.0.113.2"]
       ["dns","host.name.example","198.51.110.1"]
       ["dns","host.name.example","198.51.110.2"]

       // "dns-raw"
       ["dns-raw","host.name.example","10.200.195.1","request","resp1"]
       ["dns-raw","host.name.example","8.8.8.8","request","resp1","resp2"]

       // "http"
       ["http","http://success.example","0","200 OK"]
       ["http","http://redirection.example","0","301 Moved"]
       ["http","http://redirection.example","1","200 OK"]
       ["http","http://failure.example","0","Operation timed out"]

       // "http-body"
       ["http","http://success.example","0","200 OK",{ headers },"body"]
       ["http","http://redirection.example","0","301 Moved",{ headers },"body"]
       ["http","http://redirection.example","1","200 OK",{ headers" },"body"]
       ["http","http://failure.example","0","Operation timed out",{},""]

   These are JSON arrays primarily so that this program does not have
   to dequote and requote the contents of strings.

   The JSON parser is intentionally quite lax, because some of the
   files this program is intended to process contain strings that
   break the rules of JSON (e.g. by containing '\u0000').  If any of
   these invalid strings appears in a field selected for output, it
   will be output, verbatim, and the next program in the pipeline will
   have to deal with it.  This is particularly likely to be an issue
   for "http-body" records.

   Each record is printed out as soon as all of its data has been
   gleaned from the source file.  This means there isn't any guarantee
   that "meta" will come first, or "dns" be before "http".  "dns" and
   "http" and "meta" won't ever be mixed together because of the
   structure of the input file, but "dns" and "dns-raw" records _will_
   be mixed together if they are both requested.

   By default, the "meta", "dns", and "http" information is printed.
   If there are any command line arguments, they must be one or more
   of the words "meta", "dns", "dns-raw", "http", and "http-body"
   (without the quotes); only the named classes of information are
   printed.  Asking for both "http" and "http-body" is the same as
   asking for just "http-body" (that is, the HTTP records are printed
   only once, with the extra detail selected by "http-body").  */

static string get_token()
{
  string token;
  int c;
  // skip leading whitespace
  do
    c = cin.get();
  while (c == ' ' || c == '\t' || c == '\r' || c == '\n');

  if (c == EOF)
    return token;

  if (c == ':' || c == ','
      || c == '[' || c == ']'
      || c == '{' || c == '}') {
    token.push_back(c);
    return token;
  }

  if (c == '"') {
    do {
      token.push_back(c);
      if (c == '\\') {
        c = cin.get();
        if (c != EOF)
          token.push_back(c);
      }
      c = cin.get();
    } while (c != EOF && c != '"');
    if (c == '"')
      token.push_back(c);
  } else {
    do {
      token.push_back(c);
      c = cin.get();
    } while (c != EOF && c != '\t' && c != '\r' && c != '\n'
             && c != ':' && c != ',' && c != '[' && c != ']'
             && c != '{' && c != '}' && c != '"');
    if (c != EOF)
      cin.putback(c);
  }
  return token;
}

// Discarding does not use get_token because the input contains many
// long strings that we don't want to copy and then throw away.
static void discard_tokens_until(char limit, string first_token)
{
  if (first_token == "{")
    discard_tokens_until('}', "");
  if (first_token == "[")
    discard_tokens_until(']', "");

  int c;
  while ((c = cin.get()) != EOF && c != limit) {
    if (c == '{')
      discard_tokens_until('}', "");
    else if (c == '[')
      discard_tokens_until(']', "");
    else if (c == '"') {
      while ((c = cin.get()) != EOF && c != '"') {
        if (c == '\\')
          c = cin.get();
      }
    }
    // As a special case, discarding until ',' will stop and *not*
    // consume '}' or ']' if one of those is encountered first.
    else if ((c == ']' || c == '}') && limit == ',') {
      cin.putback(c);
      break;
    }
  }
}

static bool expect(const char *what)
{
  string token = get_token();
  if (token == what)
    return true;
  if (token.empty())
    cerr << "error: unexpected end of file\n";
  else {
    cerr << "error: got '" << token
         << "', expected '" << what
         << "'\n";
    discard_tokens_until(',', token);
  }
  return false;
}

static string expect_key()
{
  for (;;) {
    string key = get_token();

    if (key == "}")
      return "";
    if (key == "") {
      cerr << "error: unexpected end of file\n";
      return "";
    }
    if (key == "]") {
      cerr << "error: object closed by ']'\n";
      return "";
    }

    if (key == ",")
      continue;
    if (key == "{" || key == "[") {
      cerr << "error: keys cannot be composite\n";
      discard_tokens_until(',', key);
      continue;
    }

    if (!expect(":"))
      continue;
    return key;
  }
}

static string expect_scalar_value()
{
  string val = get_token();
  if (val == "") {
      cerr << "error: unexpected end of file\n";
      return "";
  }
  if (val == "{" || val == "[") {
    cerr << "error: unexpected '" << val << "'\n";
    discard_tokens_until(',', val);
    return "";
  }
  if (val == "]" || val == "}" || val == ",") {
    cerr << "error: unexpected '" << val << "'\n";
    return "";
  }

  return val;
}

static void maybe_dequote(string& s)
{
  if (s.size() < 2) return;
  if (s[0] != '"' || s[s.size()-1] != '"') return;
  s.erase(0, 1);
  s.erase(s.size()-1, 1);
}

static unsigned int slice_to_int(ssub_match const& m)
{
  unsigned int n = 0;
  for (auto p = m.first; p != m.second; ++p) {
    char c = *p;
    assert ('0' <= c && c <= '9');
    n = n * 10 + (c - '0');
  }
  return n;
}


static void scan_meta(bool output_meta)
{
  if (!expect("{"))
    return;

  for (;;) {
    string key = expect_key();
    if (key.empty()) break;

    string val = get_token();
    if (val == "") {
      cerr << "error: unexpected end of file\n";
      break;
    } else if (val == "}" || val == "]") {
      cerr << "error: unexpected '" << val << "'\n";
      break;
    } else if (val == "{" || val == "[") {
      cerr << "warning: ignoring meta key '" << key
           << "' with composite value\n";
      discard_tokens_until(',', val);
      continue;
    }

    if (output_meta)
      cout << "[\"meta\"," << key << ',' << val << "]\n";
  }
}

static void scan_dns_responseips(string const& domain)
{
  string val = get_token();
  if (val == "null") {
    /* ignore */
  } else if (val == "{") {
    cerr << "error: unexpected object in dns data\n";
    discard_tokens_until(',', val);

  } else if (val == ",") {
    cerr << "error: missing value in dns data\n";
    return;

  } else if (val == "}" || val == "]") {
    cerr << "error: unexpected end of dns data\n";
    return;

  } else if (val != "[") {
    cout << "[\"dns\"," << domain << ',' << val << "]\n";

  } else {
    for (;;) {
      val = get_token();
      if (val == "null" || val == ",") {
        /* ignore */
      } else if (val == "]") {
        break;

      } else if (val == "}") {
        cerr << "error: unexpected end of dns response IPs\n";
        break;

      } else if (val == "{") {
        cerr << "error: unexpected object in dns response IPs\n";
        discard_tokens_until('}', "");

      } else if (val == "[") {
        cerr << "error: unexpected array in dns response IPs\n";
        discard_tokens_until(']', "");

      } else {
        cout << "[\"dns\"," << domain << ',' << val << "]\n";
      }
    }
  }
}

static void scan_dns_entry(string const& domain,
                           bool want_decoded, bool want_raw)
{
  static regex response_key("^\"response([0-9]+)\"$");
  static regex responseip_key("^\"response([0-9]+)-ips?\"$");

  string nameserver;
  string request;
  vector<string> responses;
  smatch m;

  for (;;) {
    string key = expect_key();
    if (key.empty()) break;
    if (want_raw) {
      if (key == "\"nameserver\"") {
        nameserver = expect_scalar_value();

      } else if (key == "\"request\"") {
        request = expect_scalar_value();

      } else if (regex_match(key, m, response_key)) {
        unsigned int i = slice_to_int(m[1]);
        string val = expect_scalar_value();
        if (!val.empty() && val[0] == '"') {
          // swap val into the position in 'responses' given by i-1
          if (responses.size() < i) {
            responses.resize(i);
          }
          val.swap(responses.at(i-1));

        } else if (val == "null") {
          /* ignore */

        } else {
          cerr << "error: unexpected '" << val << "' in dns data\n";
          discard_tokens_until(',', "");
        }
      }
    }
    if (want_decoded && regex_match(key, responseip_key)) {
      scan_dns_responseips(domain);
    }
    discard_tokens_until(',', "");
  }

  if (want_raw && !nameserver.empty() && !request.empty()) {
    cout << "[\"dns-raw\"," << domain << ',' << nameserver << ',' << request;
    for (auto p = responses.begin(); p != responses.end(); ++p) {
      cout << ',' << *p;
    }
    cout << "]\n";
  }
}

static void scan_dns_block(bool want_decoded, bool want_raw)
{
  if (!expect("{"))
    return;
  for (;;) {
    string domain = expect_key();
    if (domain.empty())
      break;

    string val = get_token();
    if (val == "{") {
      scan_dns_entry(domain, want_decoded, want_raw);
    } else if (val == "[") {
      for (;;) {
        string sep = get_token();
        if (sep == "]") {
          break;
        } else if (sep == ",") {
          /* ignore */
        } else if (sep == "{") {
          scan_dns_entry(domain, want_decoded, want_raw);
        } else {
          cerr << "error: unexpected '" << sep << "' in dns data\n";
          discard_tokens_until(',', sep);
        }
      }
    } else {
      cerr << "error: unexpected '" << val << "' in dns data\n";
      discard_tokens_until(',', "");
    }
  }
}

// Data holder for an entry in an HTTP redirection chain.
// 'qr' = 'query-response'
struct http_qr
{
  string status;
  string full_url;
  vector<pair<string,string>> headers;
  string body;
};

static void print_http_qr(string const& url, string const& prefix,
                          http_qr const& qr)
{
  cout << "[\"http\"," << url << ',' << prefix << ','
       << (qr.full_url.empty() ? "\"\"" : qr.full_url) << ','
       << (qr.status.empty() ? "\"\"" : qr.status);
  if (!qr.headers.empty() || !qr.body.empty()) {
    cout << ",{";
    bool rest = false;
    for (auto p = qr.headers.begin(); p != qr.headers.end(); ++p) {
      if (rest) {
        cout << ',';
      } else {
        rest = true;
      }
      cout << p->first << ':' << p->second;
    }
    cout << "}," << (qr.body.empty() ? "\"\"" : qr.body);
  }
  cout << "]\n";
}

static void scan_http_headers(vector<pair<string,string>>& dest)
{
  dest.clear();
  if (!expect("{"))
    return;
  for (;;) {
    string key = expect_key();
    if (key.empty())
      break;
    string val = expect_scalar_value();
    dest.emplace_back(key, val);
  }
}

static void scan_http_response(http_qr& dest, bool want_details)
{
  if (!expect("{"))
    return;

  string status, reason, failure;
  for (;;) {
    string key = expect_key();
    if (key.empty())
      break;

    if (key == "\"status\"") {
      status = expect_scalar_value();
    } else if (key == "\"reason\"") {
      reason = expect_scalar_value();
    } else if (key == "\"failure\"") {
      failure = expect_scalar_value();
    } else if (want_details && key == "\"headers\"") {
      scan_http_headers(dest.headers);
    } else if (want_details && key == "\"body\"") {
      dest.body = expect_scalar_value();
    } else {
      discard_tokens_until(',', "");
    }
  }
  if (!failure.empty()) {
    dest.status = failure;
  } else {
    maybe_dequote(status);
    maybe_dequote(reason);
    if (!status.empty()) {
      dest.status = "\"";
      dest.status += status;
      if (!reason.empty()) {
        dest.status += " ";
        dest.status += reason;
      }
      dest.status += "\"";
    } else if (!reason.empty()) {
      dest.status = "\"??? ";
      dest.status += reason;
      dest.status += "\"";
    } else {
      dest.status = "\"???\"";
    }
  }
}

static void scan_http_redirects(vector<pair<string, http_qr>>& chain,
                                bool want_details)
{
  chain.clear();
  if (!expect("{"))
    return;

  for (;;) {
    string prefix = expect_key();
    if (prefix.empty())
      break;

    if (!expect("{")) {
      discard_tokens_until(',', "");
      continue;
    }

    chain.emplace_back(prefix, http_qr());
    http_qr& qr = chain.back().second;
    for (;;) {
      string key = expect_key();
      if (key.empty())
        break;
      if (key == "\"full_url\"") {
        qr.full_url = expect_scalar_value();
      } else if (key == "\"response\"") {
        scan_http_response(qr, want_details);
      } else {
        discard_tokens_until(',', "");
      }
    }
  }
}

static void scan_http_entry(string const& url, bool want_details)
{
  vector<pair<string, http_qr>> chain;
  http_qr tl_response;

  for (;;) {
    string key = expect_key();
    if (key.empty())
      break;

    if (key == "\"response\"") {
      scan_http_response(tl_response, want_details);
    } else if (key == "\"full_url\"") {
      tl_response.full_url = expect_scalar_value();
    } else if (key == "\"redirects\"") {
      scan_http_redirects(chain, want_details);
    } else {
      discard_tokens_until(',', "");
    }
  }
  if (! chain.empty()) {
    // The data collector at one time had a bug, where the response body
    // for the final entry in a redirection chain would overwrite the
    // response bodies for all previous entries.  Zap the duplicates.
    {
      auto last = chain.rbegin(), p = last;
      for (++p; p != chain.rend(); ++p) {
        if (!p->second.body.empty() && p->second.body == last->second.body) {
          p->second.body.clear();
        }
      }
    }
    // Now print the chain in forward order.
    for (auto p = chain.begin(); p != chain.end(); ++p) {
      print_http_qr(url, p->first, p->second);
    }
  }
  if (! tl_response.status.empty()) {
    print_http_qr(url, "\"t\"", tl_response);
  }
}

static void scan_http_block(bool output_http_body)
{
  if (!expect("{"))
    return;
  for (;;) {
    string url = expect_key();
    if (url.empty())
      break;

    string val = get_token();
    if (val == "{") {
      scan_http_entry(url, output_http_body);
    } else {
      cerr << "error: unexpected '" << val << "' as http entry for '"
           << url << "'\n";
      discard_tokens_until(',', "");
    }
  }
}

static void scan_baseline(bool output_dns, bool output_dns_raw,
                          bool output_http, bool output_http_body)
{
  if (!expect("["))
    return;
  string token = get_token();
  if (token == "]") return;
  if (token != "{") {
    cerr << "unexpected '" << token << "', expecting '{'\n";
    discard_tokens_until(',', token);
    return;
  }
  for (;;) {
    string key = expect_key();
    if (key.empty()) break;

    if (key == "\"dns\"" && (output_dns || output_dns_raw))
      scan_dns_block(output_dns, output_dns_raw);
    else if (key == "\"http\"" && output_http)
      scan_http_block(output_http_body);
    else
      discard_tokens_until(',', "");
  }
  expect("]");
}

int main(int argc, char **argv)
{
  bool output_meta      = true;
  bool output_dns       = true;
  bool output_dns_raw   = false;
  bool output_http      = true;
  bool output_http_body = false;

  std::ios::sync_with_stdio(false);
  cin.tie(0);

  if (argc > 1) {
    bool do_usage = false;
    bool fail = false;

    output_meta = false;
    output_dns  = false;
    output_http = false;
    for (int i = 1; i < argc; i++) {
      string s = argv[i];
      if (s == "meta")
        output_meta = true;
      else if (s == "dns")
        output_dns = true;
      else if (s == "dns-raw")
        output_dns_raw = true;
      else if (s == "http")
        output_http = true;
      else if (s == "http-body")
        output_http_body = true;
      else if (s == "-h" || s == "--help")
        do_usage = true;
      else {
        cerr << "unrecognized argument '" << s << "'\n";
        do_usage = true;
        fail = true;
      }
    }
    if (do_usage)
      cerr << "usage: " << argv[0]
           << "[meta] [dns] [dns-raw] [http|http-body] < json-file\n";
    if (fail)
      return 1;
    if (do_usage)
      return 0;
  }

  if (output_http_body)
    output_http = true;

  string token = get_token();
  if (token != "{") {
    if (token == "")
      cerr << "warning: empty input\n";
    else
      cerr << "warning: first token is '" << token
           << "', expected '{'\n";
    return 0;
  }
  for (;;) {
    token = expect_key();
    if (token.empty()) break;

    if (token == "\"meta\"")
      scan_meta(output_meta);
    else if (token == "\"baseline\"")
      scan_baseline(output_dns, output_dns_raw, output_http, output_http_body);
    else {
      if (token != "\"meta_exception\"" && token != "\"runtime_exception\"")
	cerr << "warning: unexpected top-level key '" << token << "'\n";
      discard_tokens_until(',', token);
    }
  }
  if ((token = get_token()) != "")
    cerr << "warning: more data after final '}'\n";
}
