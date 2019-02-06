/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include <hdfspp/uri.h>

#include <uriparser2/uriparser/Uri.h>

#include <string.h>
#include <sstream>
#include <cstdlib>
#include <cassert>
#include <limits>

namespace hdfs
{

///////////////////////////////////////////////////////////////////////////////
//
//   Internal utilities
//
///////////////////////////////////////////////////////////////////////////////

const char kReserved[] = ":/?#[]@%+";

std::string URI::encode(const std::string & decoded)
{
  bool hasCharactersToEncode = false;
  for (auto c : decoded)
  {
    if (isalnum(c) || (strchr(kReserved, c) == NULL))
    {
      continue;
    }
    else
    {
      hasCharactersToEncode = true;
      break;
    }
  }

  if (hasCharactersToEncode)
  {
    std::vector<char> buf(decoded.size() * 3 + 1);
    uriEscapeA(decoded.c_str(), &buf[0], true, URI_BR_DONT_TOUCH);
    return std::string(&buf[0]);
  }
  else
  {
    return decoded;
  }
}

std::string URI::decode(const std::string & encoded)
{
  bool hasCharactersToDecode = false;
  for (auto c : encoded)
  {
    switch (c)
    {
    case '%':
    case '+':
      hasCharactersToDecode = true;
      break;
    default:
      continue;
    }
  }

  if (hasCharactersToDecode)
  {
    std::vector<char> buf(encoded.size() + 1);
    strncpy(&buf[0], encoded.c_str(), buf.size());
    uriUnescapeInPlaceExA(&buf[0], true, URI_BR_DONT_TOUCH);
    return std::string(&buf[0]);
  }
  else
  {
    return encoded;
  }
}

std::vector<std::string> split(const std::string input, char separator)
{
  std::vector<std::string> result;

  if (!input.empty())
  {
    const char * remaining = input.c_str();
    if (*remaining == '/')
      remaining++;

    const char * next_end = strchr(remaining, separator);
    while (next_end) {
      int len = next_end - remaining;
      if (len)
        result.push_back(std::string(remaining, len));
      else
        result.push_back("");
      remaining = next_end + 1;
      next_end = strchr(remaining, separator);
    }
    result.push_back(std::string(remaining));
  }

  return result;
}



///////////////////////////////////////////////////////////////////////////////
//
//   Parsing
//
///////////////////////////////////////////////////////////////////////////////



std::string copy_range(const UriTextRangeA *r) {
  const int size = r->afterLast - r->first;
  if (size) {
      return std::string(r->first, size);
  }
  return "";
}

bool parse_int(const UriTextRangeA *r, int32_t& result)
{
  std::string int_str = copy_range(r);
  if(!int_str.empty()) {
    errno = 0;
    unsigned long val = ::strtoul(int_str.c_str(), nullptr, 10);
    if(errno == 0 && val < std::numeric_limits<uint16_t>::max()) {
      result = val;
      return true;
    } else {
      return false;
    }
  }
  return true;
}


std::vector<std::string> copy_path(const UriPathSegmentA *ps) {
    std::vector<std::string> result;
  if (nullptr == ps)
      return result;

  for (; ps != 0; ps = ps->next) {
    result.push_back(copy_range(&ps->text));
  }

  return result;
}

void parse_user_info(const UriTextRangeA *r, std::string * user, std::string * pass) {
  // Output parameters
  assert(user);
  assert(pass);

  std::string user_and_password = copy_range(r);
  if (!user_and_password.empty()) {
    const char * begin = user_and_password.c_str();
    const char * colon_loc = strchr(begin, ':');
    if (colon_loc) {
      *user = std::string(begin, colon_loc - begin - 1);
      *pass = colon_loc + 1;
    } else {
      *user = user_and_password;
    }
  }
}


std::vector<URI::Query> parse_queries(const char *first, const char * afterLast) {
    std::vector<URI::Query>  result;
    UriQueryListA * query;
    int count;
    int dissect_result = uriDissectQueryMallocExA(&query, &count, first, afterLast, false, URI_BR_DONT_TOUCH);
    if (URI_SUCCESS == dissect_result) {
      for (auto ps = query; ps != nullptr; ps = ps->next) {
        std::string key = ps->key ? URI::encode(ps->key) : "";
        std::string value = ps->value ? URI::encode(ps->value) : "";
          result.emplace_back(key, value);
      }
      uriFreeQueryListA(query);
    }

  return result;
}

// Parse a string into a URI.  Throw a hdfs::uri_parse_error if URI is malformed.
URI URI::parse_from_string(const std::string &str)
{
  URI ret;
  bool ok = true;

  UriParserStateA state;
  memset(&state, 0, sizeof(state));
  UriUriA uu;

  state.uri = &uu;
  int parseResult = uriParseUriA(&state, str.c_str());
  ok &= (parseResult == URI_SUCCESS);

  if (ok) {
    ret.scheme = copy_range(&uu.scheme);
    ret.host = copy_range(&uu.hostText);
    ok &= parse_int(&uu.portText, ret._port);
    ret.path = copy_path(uu.pathHead);
    ret.queries = parse_queries(uu.query.first, uu.query.afterLast);
    ret.fragment = copy_range(&uu.fragment);
    parse_user_info(&uu.userInfo, &ret.user, &ret.pass);
    uriFreeUriMembersA(&uu);
  }
  uriFreeUriMembersA(&uu);

  if (ok) {
    return ret;
  } else {
    throw uri_parse_error(str);
  }
}

///////////////////////////////////////////////////////////////////////////////
//
//   Getters and setters
//
///////////////////////////////////////////////////////////////////////////////

URI::URI() : _port(-1) {}

URI::Query::Query(const std::string& k, const std::string& v) : key(k), value(v) {}

std::string URI::str(bool encoded_output) const
{
  std::stringstream ss;
  if (!scheme.empty()) ss << from_encoded(encoded_output, scheme) << "://";
  if (!user.empty() || !pass.empty()) {
    if (!user.empty()) ss << from_encoded(encoded_output, user);
    if (!pass.empty()) ss << ":" << from_encoded(encoded_output, pass);
    ss << "@";
  }
  if (has_authority()) ss << build_authority(encoded_output);
  if (!path.empty()) ss << get_path(encoded_output);
  if (!queries.empty()) ss << "?" << get_query(encoded_output);
  if (!fragment.empty()) ss << "#" << from_encoded(encoded_output, fragment);

  return ss.str();
}

bool URI::has_authority() const
{
  return (!host.empty()) || (has_port());
}

std::string URI::build_authority(bool encoded_output) const
{
  std::stringstream ss;
  ss << URI::from_encoded(encoded_output, host);
  if (has_port())
  {
    ss << ":" << _port;
  }
  return ss.str();
}

std::string URI::get_scheme(bool encoded_output) const {
  return from_encoded(encoded_output,scheme);
}

void URI::set_scheme(const std::string &s, bool encoded_input) {
  scheme = to_encoded(encoded_input,s);
}

std::string URI::get_host(bool encoded_output) const {
  return from_encoded(encoded_output,host);
}

void URI::set_host(const std::string& h, bool encoded_input) {
  host = to_encoded(encoded_input,h);
}

bool URI::has_port() const {
  return _port != -1;
}

uint16_t URI::get_port() const {
  return (uint16_t)_port;
}

uint16_t URI::get_port_or_default(uint16_t val) const {
  return has_port() ? (uint16_t)_port : val;
}

void URI::set_port(uint16_t p)
{
  _port = (int32_t)p & 0xFFFF;
}

void URI::clear_port()
{
  _port = -1;
}

std::string URI::get_path(bool encoded_output) const
{
  std::ostringstream out;
  for (const std::string& s: path) {
    out << "/" << from_encoded(encoded_output, s);
  }
  return out.str();
}

std::vector<std::string> URI::get_path_elements(bool encoded_output) const
{
  std::vector<std::string> result;
  for (const std::string& path_elem: path) {
    result.push_back(from_encoded(encoded_output, path_elem));
  }

  return result;
}

void URI::parse_path(bool input_encoded, const std::string &input_path)
{
  std::vector<std::string> split_path = split(input_path, '/');
  for (const std::string& s: split_path) {
    path.push_back(to_encoded(input_encoded, s));
  }
}

// Mostly copied and modified from uriparser2.c

void URI::set_path(const std::string &p, bool encoded_input) {
  parse_path(encoded_input, p);
}

void URI::add_path(const std::string &p, bool encoded_input)
{
  path.push_back(to_encoded(encoded_input, p));
}

std::string URI::get_query(bool encoded_output) const {
  bool first = true;
  std::stringstream ss;
  for (const Query& q: queries) {
    if (!first) {
      ss << "&";
    }
    ss << from_encoded(encoded_output, q.key) << "=" << from_encoded(encoded_output, q.value);
    first = false;
  }

  return ss.str();
}

std::vector<URI::Query> URI::get_query_elements(bool encoded_output) const
{
  std::vector<Query> result;
  for (const Query& q: queries) {
    std::string key = from_encoded(encoded_output, q.key);
    std::string value = from_encoded(encoded_output, q.value);
    result.emplace_back(key, value);
  }

  return result;
}

void URI::set_query(const std::string &q) {
  queries = parse_queries(q.c_str(), q.c_str() + q.size() + 1);
}


void URI::add_query(const std::string &name, const std::string & value, bool encoded_input)
{
  queries.emplace_back(to_encoded(encoded_input, name), to_encoded(encoded_input, value));
}

void URI::remove_query(const std::string &q_name, bool encoded_input)
{
  if (queries.empty())
    return;

  // This is the one place we need to do decoded comparisons
  std::string decoded_key = encoded_input ? decode(q_name) : q_name;

  for (int i = queries.size() - 1; i >= 0; i--) {
    if (decode(queries[i].key) == decoded_key) {
      queries.erase(queries.begin() + i);
    }
  }
}

std::string URI::get_fragment(bool encoded_output) const {
  return from_encoded(encoded_output, fragment);
}

void URI::set_fragment(const std::string &f, bool encoded_input) {
  fragment = to_encoded(encoded_input,f);
}

std::string URI::from_encoded(bool encoded_output, const std::string & input) {
  return encoded_output ? input : decode(input);
}

std::string URI::to_encoded(bool encoded_input, const std::string & input) {
  return encoded_input ? input : encode(input);
}

std::string URI::GetDebugString() const {
  std::stringstream ss;
  ss << std::endl;
  ss << "\t" << "uri.str() = \"" << str() << "\"" << std::endl;
  ss << "\t" << "uri.get_scheme() = \"" << get_scheme() << "\"" << std::endl;
  ss << "\t" << "uri.get_host() = \"" << get_host() << "\"" << std::endl;

  if(_port == -1)
    ss << "\t" << "uri.get_port() = invalid (uninitialized)" << std::endl;
  else
    ss << "\t" << "uri.get_port() = \"" << _port << "\"" << std::endl;

  ss << "\t" << "uri.get_path() = \"" << get_path() << "\"" << std::endl;
  ss << "\t" << "uri.get_fragment() = \"" << get_fragment() << "\"" << std::endl;


  std::vector<Query> query_elems = get_query_elements();

  if(query_elems.size() > 0)
    ss << "\t" << "Query elements:" << std::endl;

  for(auto qry = query_elems.begin(); qry != query_elems.end(); qry++) {
    ss << "\t\t" << qry->key << " -> " << qry->value << std::endl;
  }

  return ss.str();
}

} // end namespace hdfs
