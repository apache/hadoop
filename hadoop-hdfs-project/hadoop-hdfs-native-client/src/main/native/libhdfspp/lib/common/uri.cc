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


#include <common/uri.h>

#include <uriparser2/uriparser/Uri.h>

#include <string.h>
#include <sstream>
#include <cstdlib>
#include <limits>

using std::experimental::nullopt;

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

bool parse_int(const UriTextRangeA *r, optional<uint16_t> * result) {
  assert(result); // output
  std::string int_string = copy_range(r);
  if (!int_string.empty()) {
    errno = 0;
    unsigned long val = ::strtoul(int_string.c_str(), nullptr, 10);
    if (errno == 0 && val < std::numeric_limits<uint16_t>::max() ) {
      *result = std::experimental::make_optional<uint16_t>(val);
      return true;
    } else {
      return false;
    }
  }

  // No value
  *result = nullopt;
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


std::vector<std::pair<std::string, std::string > > parse_query(const char *first, const char * afterLast) {
    std::vector<std::pair<std::string, std::string > >  result;
    UriQueryListA * query;
    int count;
    int dissect_result = uriDissectQueryMallocExA(&query, &count, first, afterLast, false, URI_BR_DONT_TOUCH);
    if (URI_SUCCESS == dissect_result) {
      for (auto ps = query; ps != nullptr; ps = ps->next) {
        std::string key = ps->key ? URI::encode(ps->key) : "";
        std::string value = ps->value ? URI::encode(ps->value) : "";
          result.push_back(std::make_pair(key, value));
      }
      uriFreeQueryListA(query);
    }

	return result;
}


optional<URI> URI::parse_from_string(const std::string &str)
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
    ok &= parse_int(&uu.portText, &ret.port);
    ret.path = copy_path(uu.pathHead);
    ret.query = parse_query(uu.query.first, uu.query.afterLast);
    ret.fragment = copy_range(&uu.fragment);
    parse_user_info(&uu.userInfo, &ret.user, &ret.pass);
    uriFreeUriMembersA(&uu);
  }
  uriFreeUriMembersA(&uu);

  if (ok) {
    return std::experimental::make_optional(ret);
  } else {
    return nullopt;
  }
}

///////////////////////////////////////////////////////////////////////////////
//
//   Getters and setters
//
///////////////////////////////////////////////////////////////////////////////

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
  if (!query.empty()) ss << "?" << get_query(encoded_output);
  if (!fragment.empty()) ss << "#" << from_encoded(encoded_output, fragment);

  return ss.str();
}

bool URI::has_authority() const
{
  return (!host.empty()) || (port);
}

std::string URI::build_authority(bool encoded_output) const
{
  std::stringstream ss;
  ss << URI::from_encoded(encoded_output, host);
  if (port)
  {
    ss << ":" << *port;
  }
  return ss.str();
}


std::string URI::get_path(bool encoded_output) const
{
  std::ostringstream out;
  for (auto s: path) {
    out << "/" << from_encoded(encoded_output, s);
  }
  return out.str();
}

std::vector<std::string> URI::get_path_elements(bool encoded_output) const
{
  std::vector<std::string> result;
  for (auto path_elem: path) {
    result.push_back(from_encoded(encoded_output, path_elem));
  }

  return result;
}

void URI::parse_path(bool input_encoded, const std::string &input_path)
{
  std::vector<std::string> split_path = split(input_path, '/');
  for (auto s: split_path) {
    path.push_back(to_encoded(input_encoded, s));
  }
}


// Mostly copied and modified from uriparser2.c

void URI::add_path(const std::string &p, bool encoded_input)
{
  path.push_back(to_encoded(encoded_input, p));
}


std::string URI::get_query(bool encoded_output) const {
  bool first = true;
  std::stringstream ss;
  for (auto q: query) {
    if (!first) {
      ss << "&";
    }
    ss << from_encoded(encoded_output, q.first) << "=" << from_encoded(encoded_output, q.second);
    first = false;
  }

  return ss.str();
}

std::vector< std::pair<std::string, std::string> > URI::get_query_elements(bool encoded_output) const
{
  std::vector< std::pair<std::string, std::string> > result;
  for (auto q: query) {
    auto key = from_encoded(encoded_output, q.first);
    auto value = from_encoded(encoded_output, q.second);
    result.push_back(std::make_pair(key, value));
  }

  return result;
}


void URI::set_query(const std::string &q) {
  query = parse_query(q.c_str(), q.c_str() + q.size() + 1);
}


void URI::add_query(const std::string &name, const std::string & value, bool encoded_input)
{
  query.push_back(std::make_pair(to_encoded(encoded_input, name), to_encoded(encoded_input, value)));
}

void URI::remove_queries(const std::string &q_name, bool encoded_input)
{
  if (query.empty())
    return;

  // This is the one place we need to do decoded comparisons
  std::string decoded_key = encoded_input ? decode(q_name) : q_name;

  for (int i = query.size() - 1; i >= 0; i--) {
    if (decode(query[i].first) == decoded_key) {
      query.erase(query.begin() + i);
    }
  }
}

}
