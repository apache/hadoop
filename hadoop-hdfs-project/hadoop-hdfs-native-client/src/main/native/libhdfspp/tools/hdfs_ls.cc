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

#include <google/protobuf/stubs/common.h>
#include <unistd.h>
#include <future>
#include "tools_common.h"

void usage(){
  std::cout << "Usage: hdfs_ls [OPTION] FILE"
      << std::endl
      << std::endl << "List information about the FILEs."
      << std::endl
      << std::endl << "  -R        list subdirectories recursively"
      << std::endl << "  -h        display this help and exit"
      << std::endl
      << std::endl << "Examples:"
      << std::endl << "hdfs_ls hdfs://localhost.localdomain:8020/dir"
      << std::endl << "hdfs_ls -R /dir1/dir2"
      << std::endl;
}

int main(int argc, char *argv[]) {
  //We should have at least 2 arguments
  if (argc < 2) {
    usage();
    exit(EXIT_FAILURE);
  }

  bool recursive = false;
  int input;

  //Using GetOpt to read in the values
  opterr = 0;
  while ((input = getopt(argc, argv, "Rh")) != -1) {
    switch (input)
    {
    case 'R':
      recursive = true;
      break;
    case 'h':
      usage();
      exit(EXIT_SUCCESS);
    case '?':
      if (isprint(optopt))
        std::cerr << "Unknown option `-" << (char) optopt << "'." << std::endl;
      else
        std::cerr << "Unknown option character `" << (char) optopt << "'." << std::endl;
      usage();
      exit(EXIT_FAILURE);
    default:
      exit(EXIT_FAILURE);
    }
  }
  std::string uri_path = argv[optind];

  //Building a URI object from the given uri_path
  hdfs::URI uri = hdfs::parse_path_or_exit(uri_path);

  std::shared_ptr<hdfs::FileSystem> fs = hdfs::doConnect(uri, true);
  if (!fs) {
    std::cerr << "Could not connect the file system. " << std::endl;
    exit(EXIT_FAILURE);
  }

  std::shared_ptr<std::promise<void>> promise = std::make_shared<std::promise<void>>();
  std::future<void> future(promise->get_future());
  hdfs::Status status = hdfs::Status::OK();

  /**
    * Keep requesting more until we get the entire listing. Set the promise
    * when we have the entire listing to stop.
    *
    * Find and GetListing guarantee that the handler will only be called once at a time,
    * so we do not need any locking here. They also guarantee that the handler will be
    * only called once with has_more_results set to false.
    */
  auto handler = [promise, &status]
                  (const hdfs::Status &s, const std::vector<hdfs::StatInfo> & si, bool has_more_results) -> bool {
    //Print result chunks as they arrive
    if(!si.empty()) {
      for (hdfs::StatInfo const& s : si) {
        std::cout << s.str() << std::endl;
      }
    }
    if(!s.ok() && status.ok()){
      //We make sure we set 'status' only on the first error.
      status = s;
    }
    if (!has_more_results) {
      promise->set_value();  //set promise
      return false;         //request stop sending results
    }
    return true;  //request more results
  };

  if(!recursive){
    //Asynchronous call to GetListing
    fs->GetListing(uri.get_path(), handler);
  } else {
    //Asynchronous call to Find
    fs->Find(uri.get_path(), "*", hdfs::FileSystem::GetDefaultFindMaxDepth(), handler);
  }

  //block until promise is set
  future.get();
  if(!status.ok()) {
    std::cerr << "Error: " << status.ToString() << std::endl;
  }

  // Clean up static data and prevent valgrind memory leaks
  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}
