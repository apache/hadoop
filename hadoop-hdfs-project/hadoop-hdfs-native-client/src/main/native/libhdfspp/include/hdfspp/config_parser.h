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
#ifndef LIBHDFSPP_CONFIGPARSER_H_
#define LIBHDFSPP_CONFIGPARSER_H_

#include "hdfspp/options.h"
#include "hdfspp/uri.h"
#include "hdfspp/status.h"

#include <string>
#include <memory>
#include <vector>

namespace hdfs {

class ConfigParser {
 public:
  ConfigParser();
  ConfigParser(const std::string& path);
  ConfigParser(const std::vector<std::string>& configDirectories);
  ~ConfigParser();
  ConfigParser(ConfigParser&&);
  ConfigParser& operator=(ConfigParser&&);

  bool LoadDefaultResources();
  std::vector<std::pair<std::string, Status> > ValidateResources() const;

  // Return false if value couldn't be found or cast to desired type
  bool get_int(const std::string& key, int& outval) const;
  int get_int_or(const std::string& key, const int defaultval) const;

  bool get_string(const std::string& key, std::string& outval) const;
  std::string get_string_or(const std::string& key, const std::string& defaultval) const;

  bool get_bool(const std::string& key, bool& outval) const;
  bool get_bool_or(const std::string& key, const bool defaultval) const;

  bool get_double(const std::string& key, double& outval) const;
  double get_double_or(const std::string& key, const double defaultval) const;

  bool get_uri(const std::string& key, URI& outval) const;
  URI get_uri_or(const std::string& key, const URI& defaultval) const;

  bool get_options(Options& outval) const;
  Options get_options_or(const Options& defaultval) const;

 private:
  class impl;
  std::unique_ptr<impl> pImpl;
};

}
#endif
