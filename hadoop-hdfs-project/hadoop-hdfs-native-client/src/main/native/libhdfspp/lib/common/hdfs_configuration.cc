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

#include "common/hdfs_configuration.h"
#include "common/logging.h"

#include <exception>

#ifndef DEFAULT_SCHEME
  #define DEFAULT_SCHEME "hdfs://"
#endif

namespace hdfs {

// Constructs a configuration with no search path and no resources loaded
HdfsConfiguration::HdfsConfiguration() : Configuration() {}

// Constructs a configuration with a copy of the input data
HdfsConfiguration::HdfsConfiguration(ConfigMap &src_map) : Configuration(src_map) {}
HdfsConfiguration::HdfsConfiguration(const ConfigMap &src_map) : Configuration(src_map) {}

std::vector<std::string> HdfsConfiguration::GetDefaultFilenames() {
  auto result = Configuration::GetDefaultFilenames();
  result.push_back("hdfs-site.xml");
  return result;
}

// Sets a value iff the optional<T> has a value
template <class T, class U>
void OptionalSet(T& target, optional<U> value) {
  if (value)
    target = *value;
}

std::vector<std::string> SplitOnComma(const std::string &s, bool include_empty_strings) {
  std::vector<std::string> res;
  std::string buf;

  for(unsigned int i=0;i<s.size();i++) {
    char c = s[i];
    if(c != ',') {
      buf += c;
    } else {
      if(!include_empty_strings && buf.empty()) {
        // Skip adding empty strings if needed
        continue;
      }
      res.push_back(buf);
      buf.clear();
    }
  }

  if(buf.size() > 0)
    res.push_back(buf);

  return res;
}

std::string RemoveSpaces(const std::string &str) {
  std::string res;
  for(unsigned int i=0; i<str.size(); i++) {
    char curr = str[i];
    if(curr != ' ') {
      res += curr;
    }
  }
  return res;
}

// Prepend hdfs:// to string if there isn't already a scheme
// Converts unset optional into empty string
std::string PrependHdfsScheme(optional<std::string> str) {
  if(!str)
    return "";

  if(str.value().find("://") == std::string::npos)
    return DEFAULT_SCHEME + str.value();
  return str.value();
}

// It's either use this, goto, or a lot of returns w/ status checks
struct ha_parse_error : public std::exception {
  std::string desc;
  ha_parse_error(const std::string &val) : desc(val) {};
  const char *what() const noexcept override  {
    return desc.c_str();
  };
};

std::vector<NamenodeInfo> HdfsConfiguration::LookupNameService(const std::string &nameservice) {
  LOG_TRACE(kRPC, << "HDFSConfiguration@" << this << "::LookupNameService( nameservice=" << nameservice<< " ) called");

  std::vector<NamenodeInfo> namenodes;
  try {
    // Find namenodes that belong to nameservice
    std::vector<std::string> namenode_ids;
    {
      std::string service_nodes = std::string("dfs.ha.namenodes.") + nameservice;
      optional<std::string> namenode_list = Get(service_nodes);
      if(namenode_list)
        namenode_ids = SplitOnComma(namenode_list.value(), false);
      else
        throw ha_parse_error("unable to find " + service_nodes);

      for(unsigned int i=0; i<namenode_ids.size(); i++) {
        namenode_ids[i] = RemoveSpaces(namenode_ids[i]);
        LOG_INFO(kRPC, << "Namenode: " << namenode_ids[i]);
      }
    }

    // should this error if we only find 1 NN?
    if(namenode_ids.empty())
      throw ha_parse_error("No namenodes found for nameservice " + nameservice);

    // Get URI for each HA namenode
    for(auto node_id=namenode_ids.begin(); node_id != namenode_ids.end(); node_id++) {
      // find URI
      std::string dom_node_name = std::string("dfs.namenode.rpc-address.") + nameservice + "." + *node_id;

      URI uri;
      try {
        uri = URI::parse_from_string(PrependHdfsScheme(Get(dom_node_name)));
      } catch (const uri_parse_error) {
        throw ha_parse_error("unable to find " + dom_node_name);
      }

      if(uri.str() == "") {
        LOG_WARN(kRPC, << "Attempted to read info for nameservice " << nameservice << " node " << dom_node_name << " but didn't find anything.")
      } else {
        LOG_INFO(kRPC, << "Read the following HA Namenode URI from config" << uri.GetDebugString());
      }

      NamenodeInfo node(nameservice, *node_id, uri);
      namenodes.push_back(node);
    }
  } catch (ha_parse_error e) {
    LOG_ERROR(kRPC, << "HA cluster detected but failed because : " << e.what());
    namenodes.clear(); // Don't return inconsistent view
  }
  return namenodes;
}

// Interprets the resources to build an Options object
Options HdfsConfiguration::GetOptions() {
  Options result;

  OptionalSet(result.rpc_timeout, GetInt(kDfsClientSocketTimeoutKey));
  OptionalSet(result.rpc_connect_timeout, GetInt(kIpcClientConnectTimeoutKey));
  OptionalSet(result.max_rpc_retries, GetInt(kIpcClientConnectMaxRetriesKey));
  OptionalSet(result.rpc_retry_delay_ms, GetInt(kIpcClientConnectRetryIntervalKey));
  OptionalSet(result.defaultFS, GetUri(kFsDefaultFsKey));
  OptionalSet(result.block_size, GetInt(kDfsBlockSizeKey));


  OptionalSet(result.failover_max_retries, GetInt(kDfsClientFailoverMaxAttempts));
  OptionalSet(result.failover_connection_max_retries, GetInt(kDfsClientFailoverConnectionRetriesOnTimeouts));

  // Load all nameservices if it's HA configured
  optional<std::string> dfs_nameservices = Get("dfs.nameservices");
  if(dfs_nameservices) {
    std::string nameservice = dfs_nameservices.value();

    std::vector<std::string> all_services = SplitOnComma(nameservice, false);

    // Look up nodes for each nameservice so that FileSystem object can support
    // multiple nameservices by ID.
    for(const std::string &service : all_services) {
      if(service.empty())
        continue;

      LOG_DEBUG(kFileSystem, << "Parsing info for nameservice: " << service);
      std::vector<NamenodeInfo> nodes = LookupNameService(service);
      if(nodes.empty()) {
        LOG_WARN(kFileSystem, << "Nameservice \"" << service << "\" declared in config but nodes aren't");
      } else {
        result.services[service] = nodes;
      }
    }
  }

  optional<std::string> authentication_value = Get(kHadoopSecurityAuthenticationKey);

  if (authentication_value ) {
      std::string fixed_case_value = fixCase(authentication_value.value());
      if (fixed_case_value == fixCase(kHadoopSecurityAuthentication_kerberos))
          result.authentication = Options::kKerberos;
      else
          result.authentication = Options::kSimple;
  }

  return result;
}


}
