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

#ifndef COMMON_HDFS_URI_H_
#define COMMON_HDFS_URI_H_

#include <iostream>
#include <string>
#include <vector>
#include <stdexcept>

namespace hdfs
{

class uri_parse_error : public std::invalid_argument {
 public:
  uri_parse_error(const char *what_str) : std::invalid_argument(what_str) {}
  uri_parse_error(const std::string& what_str) : std::invalid_argument(what_str) {}
};

class URI {
public:
  // Parse a string into a URI.  Throw a hdfs::uri_parse_error if URI is malformed.
  static URI parse_from_string(const std::string &str);

  // URI encode/decode strings
  static std::string encode  (const std::string &input);
  static std::string decode  (const std::string &input);

  URI();

  std::string get_scheme(bool encoded_output=false) const;

  void set_scheme(const std::string &s, bool encoded_input=false);

  // empty if none.
  std::string get_host(bool encoded_output=false) const;

  void set_host(const std::string& h, bool encoded_input=false);

  // true if port has been set
  bool has_port() const;

  // undefined if port hasn't been set
  uint16_t get_port() const;

  // use default if port hasn't been set
  uint16_t get_port_or_default(uint16_t default_val) const;

  void set_port(uint16_t p);

  void clear_port();

  std::string get_path(bool encoded_output=false) const;

  void set_path(const std::string &p, bool encoded_input=false);

  void add_path(const std::string &p, bool encoded_input=false);

  std::vector<std::string> get_path_elements(bool encoded_output=false) const;

  struct Query {
    Query(const std::string& key, const std::string& val);
    std::string key;
    std::string value;
  };

  std::string get_query(bool encoded_output=false) const;

  std::vector<Query> get_query_elements(bool encoded_output=false) const;

  // Not that set_query must always pass in encoded strings
  void set_query(const std::string &q);

  // Adds a parameter onto the query; does not check if it already exists
  //   e.g. parseFromString("foo?bar=baz").addQuery("bing","bang")
  //   would leave "bar=baz&bing=bang" as the query
  void add_query(const std::string &name, const std::string & value, bool encoded_input=false);

  // Removes the query part if exists
  //   e.g. parseFromString("foo?bar=baz&bing=bang&bar=bong").removeQueries("bar")
  //   would leave bing=bang as the query
  void remove_query(const std::string &q_name, bool encoded_input=false);

  std::string get_fragment(bool encoded_output=false) const;

  void set_fragment(const std::string &f, bool encoded_input=false);

  std::string str(bool encoded_output=true) const;

  // Get a string with each URI field printed on a seperate line
  std::string GetDebugString() const;
private:
  // These are stored in encoded form
  std::string scheme;
  std::string user;
  std::string pass;
  std::string host;
  std::vector<std::string> path;
  std::vector<Query> queries;
  std::string fragment;
  // implicitly narrowed to uint16_t if positive
  // -1 to indicate uninitialized
  int32_t _port;

  // URI encoding helpers
  static std::string from_encoded(bool encoded_output, const std::string & input);
  static std::string to_encoded(bool encoded_input, const std::string & input);

  bool has_authority() const;
  std::string build_authority(bool encoded_output) const;

  std::string build_path(bool encoded_output) const;
  void parse_path(bool input_encoded, const std::string &input_path);
};

inline std::ostream& operator<<(std::ostream &out, const URI &uri) {
  return out << uri.str();
}

}
#endif
