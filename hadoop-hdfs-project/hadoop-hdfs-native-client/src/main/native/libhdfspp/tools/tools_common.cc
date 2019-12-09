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

  std::shared_ptr<hdfs::FileSystem> doConnect(hdfs::URI & uri, bool max_timeout) {

    //This sets the config path to the default: "$HADOOP_CONF_DIR" or "/etc/hadoop/conf"
    //and loads default config files core-site.xml and hdfs-site.xml from the config path
    hdfs::ConfigParser parser;
    if(!parser.LoadDefaultResources()){
      std::cerr << "Could not load default resources. " << std::endl;
      exit(EXIT_FAILURE);
    }
    auto stats = parser.ValidateResources();
    //validating core-site.xml
    if(!stats[0].second.ok()){
      std::cerr << stats[0].first << " is invalid: " << stats[0].second.ToString() << std::endl;
      exit(EXIT_FAILURE);
    }
    //validating hdfs-site.xml
    if(!stats[1].second.ok()){
      std::cerr << stats[1].first << " is invalid: " << stats[1].second.ToString() << std::endl;
      exit(EXIT_FAILURE);
    }
    hdfs::Options options;
    if(!parser.get_options(options)){
      std::cerr << "Could not load Options object. " << std::endl;
      exit(EXIT_FAILURE);
    }
    if(max_timeout){
      //TODO: HDFS-9539 - until then we increase the time-out to allow all recursive async calls to finish
      options.rpc_timeout = std::numeric_limits<int>::max();
    }
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
      std::string port = uri.has_port() ? std::to_string(uri.get_port()) : "";
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

  #define BUF_SIZE 1048576 //1 MB
  static char input_buffer[BUF_SIZE];

  void readFile(std::shared_ptr<hdfs::FileSystem> fs, std::string path, off_t offset, std::FILE* dst_file, bool to_delete) {
    ssize_t total_bytes_read = 0;
    size_t last_bytes_read = 0;

    hdfs::FileHandle *file_raw = nullptr;
    hdfs::Status status = fs->Open(path, &file_raw);
    if (!status.ok()) {
      std::cerr << "Could not open file " << path << ". " << status.ToString() << std::endl;
      exit(EXIT_FAILURE);
    }
    //wrapping file_raw into a unique pointer to guarantee deletion
    std::unique_ptr<hdfs::FileHandle> file(file_raw);

    do{
      //Reading file chunks
      status = file->PositionRead(input_buffer, sizeof(input_buffer), offset, &last_bytes_read);
      if(status.ok()) {
        //Writing file chunks to stdout
        fwrite(input_buffer, last_bytes_read, 1, dst_file);
        total_bytes_read += last_bytes_read;
        offset += last_bytes_read;
      } else {
        if(status.is_invalid_offset()){
          //Reached the end of the file
          if(to_delete) {
            //Deleting the file (recursive set to false)
            hdfs::Status status = fs->Delete(path, false);
            if (!status.ok()) {
              std::cerr << "Error deleting the source file: " << path
                << " " << status.ToString() << std::endl;
              exit(EXIT_FAILURE);
            }
          }
          break;
        } else {
          std::cerr << "Error reading the file: " << status.ToString() << std::endl;
          exit(EXIT_FAILURE);
        }
      }
    } while (last_bytes_read > 0);
    return;
  }


  URI parse_path_or_exit(const std::string& path)
  {
    URI uri;
    try {
      uri = hdfs::URI::parse_from_string(path);
    } catch (const uri_parse_error& e) {
      std::cerr << "Malformed URI: " << path << std::endl;
      exit(EXIT_FAILURE);
    }
    return uri;
  }
}
