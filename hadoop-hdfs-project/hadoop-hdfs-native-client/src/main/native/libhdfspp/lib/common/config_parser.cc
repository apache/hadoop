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

#include "hdfspp/config_parser.h"
#include "common/hdfs_configuration.h"
#include "common/configuration_loader.h"

#include <string>
#include <memory>
#include <vector>
#include <numeric>

namespace hdfs {

static const char kSearchPathSeparator = ':';

HdfsConfiguration LoadDefault(ConfigurationLoader & loader)
{
  optional<HdfsConfiguration> result = loader.LoadDefaultResources<HdfsConfiguration>();
  if (result)
  {
    return result.value();
  }
  else
  {
    return loader.NewConfig<HdfsConfiguration>();
  }
}

class ConfigParser::impl {
 public:
  impl() :
      config_(loader_.NewConfig<HdfsConfiguration>()) {
  }

  impl(const std::vector<std::string>& dirs) :
      config_(loader_.NewConfig<HdfsConfiguration>()) {

      // Convert vector of paths into ':' separated path
      std::string path = std::accumulate(dirs.begin(), dirs.end(), std::string(""),
        [](std::string cumm, std::string elem) {return cumm + kSearchPathSeparator + elem;});
      loader_.SetSearchPath(path);
      config_ = LoadDefault(loader_);
  }

  impl(const std::string& path) :
      config_(loader_.NewConfig<HdfsConfiguration>()) {

      loader_.SetSearchPath(path);
      config_ = LoadDefault(loader_);
  }

  bool LoadDefaultResources() {
    config_ = LoadDefault(loader_);
    return true;
  }

  std::vector<std::pair<std::string, Status> > ValidateResources() const {
    return loader_.ValidateDefaultResources<HdfsConfiguration>();
  }

  bool get_int(const std::string& key, int& outval) const {
    auto ret = config_.GetInt(key);
    if (!ret) {
      return false;
    } else {
      outval = *ret;
      return true;
    }
  }

  bool get_string(const std::string& key, std::string& outval) const {
    auto ret = config_.Get(key);
    if (!ret) {
      return false;
    } else {
      outval = *ret;
      return true;
    }
  }

  bool get_bool(const std::string& key, bool& outval) const {
    auto ret = config_.GetBool(key);
    if (!ret) {
      return false;
    } else {
      outval = *ret;
      return true;
    }
  }

  bool get_double(const std::string& key, double& outval) const {
    auto ret = config_.GetDouble(key);
    if (!ret) {
      return false;
    } else {
      outval = *ret;
      return true;
    }
  }

  bool get_uri(const std::string& key, URI& outval) const {
    auto ret = config_.GetUri(key);
    if (!ret) {
      return false;
    } else {
      outval = *ret;
      return true;
    }
  }

  bool get_options(Options& outval) {
    outval = config_.GetOptions();
    return true;
  }

 private:
  ConfigurationLoader loader_;
  HdfsConfiguration config_;
};


ConfigParser::ConfigParser() {
  pImpl.reset(new ConfigParser::impl());
}

ConfigParser::ConfigParser(const std::vector<std::string>& configDirectories) {
  pImpl.reset(new ConfigParser::impl(configDirectories));
}

ConfigParser::ConfigParser(const std::string& path) {
  pImpl.reset(new ConfigParser::impl(path));
}

ConfigParser::~ConfigParser() = default;
ConfigParser::ConfigParser(ConfigParser&&) = default;
ConfigParser& ConfigParser::operator=(ConfigParser&&) = default;

bool ConfigParser::LoadDefaultResources() { return pImpl->LoadDefaultResources(); }
std::vector<std::pair<std::string, Status> > ConfigParser::ValidateResources() const { return pImpl->ValidateResources();}

bool ConfigParser::get_int(const std::string& key, int& outval) const { return pImpl->get_int(key, outval); }
int ConfigParser::get_int_or(const std::string& key, const int defaultval) const {
  int res = 0;
  if(get_int(key, res)) {
    return res;
  } else {
    return defaultval;
  }
}

bool ConfigParser::get_string(const std::string& key, std::string& outval) const { return pImpl->get_string(key, outval); }
std::string ConfigParser::get_string_or(const std::string& key, const std::string& defaultval) const {
  std::string res;
  if(get_string(key, res)) {
    return res;
  } else {
    return defaultval;
  }
}

bool ConfigParser::get_bool(const std::string& key, bool& outval) const { return pImpl->get_bool(key, outval); }
bool ConfigParser::get_bool_or(const std::string& key, const bool defaultval) const {
  bool res = false;
  if(get_bool(key, res)) {
    return res;
  } else {
    return defaultval;
  }
}

bool ConfigParser::get_double(const std::string& key, double& outval) const { return pImpl->get_double(key, outval); }
double ConfigParser::get_double_or(const std::string& key, const double defaultval) const {
  double res = 0;
  if(get_double(key, res)) {
    return res;
  } else {
    return defaultval;
  }
}

bool ConfigParser::get_uri(const std::string& key, URI& outval) const { return pImpl->get_uri(key, outval); }
URI ConfigParser::get_uri_or(const std::string& key, const URI& defaultval) const {
  URI res;
  if(get_uri(key, res)) {
    return res;
  } else {
    res = defaultval;
    return res;
  }
}

bool ConfigParser::get_options(Options& outval) const { return pImpl->get_options(outval); }
Options ConfigParser::get_options_or(const Options& defaultval) const {
  Options res;
  if(get_options(res)) {
    return res;
  } else {
    res = defaultval;
    return res;
  }
}

} // end namespace hdfs
