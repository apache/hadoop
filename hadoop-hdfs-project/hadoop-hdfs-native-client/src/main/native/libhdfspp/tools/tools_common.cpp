/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
*/

#include "tools_common.h"

namespace hdfs {

  std::shared_ptr<hdfs::Options> getOptions() {
    std::shared_ptr<hdfs::Options> options = std::make_shared<hdfs::Options>();
    //Setting the config path to the default: "$HADOOP_CONF_DIR" or "/etc/hadoop/conf"
    hdfs::ConfigurationLoader loader;
    //Loading default config files core-site.xml and hdfs-site.xml from the config path
    hdfs::optional<HdfsConfiguration> config = loader.LoadDefaultResources<HdfsConfiguration>();
    //TODO: HDFS-9539 - after this is resolved, valid config will always be returned.
    if(config){
      //Loading options from the config
      *options = config->GetOptions();
    }
    return options;
  }

  std::shared_ptr<hdfs::FileSystem> doConnect(hdfs::URI & uri, hdfs::Options & options) {
    IoService * io_service = IoService::New();
    //Wrapping fs into a shared pointer to guarantee deletion
    std::shared_ptr<hdfs::FileSystem> fs(hdfs::FileSystem::New(io_service, "", options));
    if (!fs) {
      std::cerr << "Could not create FileSystem object. " << std::endl;
      exit(EXIT_FAILURE);
    }
    Status status;
    //Check if the user supplied the host
    if(!uri.get_host().empty()){
      //If port is supplied we use it, otherwise we use the empty string so that it will be looked up in configs.
      std::string port = (uri.get_port()) ? std::to_string(uri.get_port().value()) : "";
      status = fs->Connect(uri.get_host(), port);
      if (!status.ok()) {
        std::cerr << "Could not connect to " << uri.get_host() << ":" << port << ". " << status.ToString() << std::endl;
        exit(EXIT_FAILURE);
      }
    } else {
      status = fs->ConnectToDefaultFs();
      if (!status.ok()) {
        if(!options.defaultFS.get_host().empty()){
          std::cerr << "Error connecting to " << options.defaultFS << ". " << status.ToString() << std::endl;
        } else {
          std::cerr << "Error connecting to the cluster: defaultFS is empty. " << status.ToString() << std::endl;
        }
        exit(EXIT_FAILURE);
      }
    }
    return fs;
  }

}
