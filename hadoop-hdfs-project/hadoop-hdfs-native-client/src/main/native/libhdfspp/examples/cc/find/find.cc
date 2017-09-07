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

/**
   * A parallel find tool example.
   *
   * Finds all files matching the specified name recursively starting from the
   * specified directory and prints their filepaths. Works either synchronously
   * or asynchronously.
   *
   * Usage: find /<path-to-file> <file-name> <use_async>
   *
   * Example: find /dir?/tree* some?file*name 1
   *
   * @param path-to-file    Absolute path at which to begin search, can have wild
   *                        cards and must be non-blank
   * @param file-name       Name to find, can have wild cards and must be non-blank
   * @param use_async       If set to 1 it prints out results asynchronously as
   *                        they arrive. If set to 0 results are printed in one
   *                        big chunk when it becomes available.
   *
   **/

#include "hdfspp/hdfspp.h"
#include <google/protobuf/stubs/common.h>
#include <future>
#include "tools_common.h"

void SyncFind(std::shared_ptr<hdfs::FileSystem> fs, const std::string &path, const std::string &name){
  std::vector<hdfs::StatInfo> results;
  //Synchronous call to Find
  hdfs::Status stat = fs->Find(path, name, hdfs::FileSystem::GetDefaultFindMaxDepth(), &results);

  if (!stat.ok()) {
    std::cerr << "Error: " << stat.ToString() << std::endl;
  }

  if(results.empty()){
    std::cout << "Nothing Found" << std::endl;
  } else {
    //Printing out the results
    for (hdfs::StatInfo const& si : results) {
      std::cout << si.full_path << std::endl;
    }
  }
}

void AsyncFind(std::shared_ptr<hdfs::FileSystem> fs, const std::string &path, const std::string &name){
  std::promise<void> promise;
  std::future<void> future(promise.get_future());
  bool something_found = false;
  hdfs::Status status = hdfs::Status::OK();

  /**
    * Keep requesting more until we get the entire listing. Set the promise
    * when we have the entire listing to stop.
    *
    * Find guarantees that the handler will only be called once at a time,
    * so we do not need any locking here
    */
  auto handler = [&promise, &status, &something_found]
                  (const hdfs::Status &s, const std::vector<hdfs::StatInfo> & si, bool has_more_results) -> bool {
    //Print result chunks as they arrive
    if(!si.empty()) {
      something_found = true;
      for (hdfs::StatInfo const& s : si) {
        std::cout << s.full_path << std::endl;
      }
    }
    if(!s.ok() && status.ok()){
      //We make sure we set 'status' only on the first error.
      status = s;
    }
    if (!has_more_results) {
      promise.set_value();  //set promise
      return false;         //request stop sending results
    }
    return true;  //request more results
  };

  //Asynchronous call to Find
  fs->Find(path, name, hdfs::FileSystem::GetDefaultFindMaxDepth(), handler);

  //block until promise is set
  future.get();
  if(!status.ok()) {
    std::cerr << "Error: " << status.ToString() << std::endl;
  }
  if(!something_found){
    std::cout << "Nothing Found" << std::endl;
  }
}

int main(int argc, char *argv[]) {
  if (argc != 4) {
    std::cerr << "usage: find /<path-to-file> <file-name> <use_async>" << std::endl;
    exit(EXIT_FAILURE);
  }

  std::string path = argv[1];
  std::string name = argv[2];
  bool use_async = (std::stoi(argv[3]) != 0);

  //Building a URI object from the given uri path
  hdfs::URI uri = hdfs::parse_path_or_exit(path);

  std::shared_ptr<hdfs::FileSystem> fs = hdfs::doConnect(uri, true);
  if (!fs) {
    std::cerr << "Could not connect the file system. " << std::endl;
    exit(EXIT_FAILURE);
  }

  if (use_async){
    //Example of Async find
    AsyncFind(fs, path, name);
  } else {
    //Example of Sync find
    SyncFind(fs, path, name);
  }

  // Clean up static data and prevent valgrind memory leaks
  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}
