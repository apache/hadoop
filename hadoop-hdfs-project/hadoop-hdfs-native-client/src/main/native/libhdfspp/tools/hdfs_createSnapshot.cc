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
#include "tools_common.h"

void usage(){
  std::cout << "Usage: hdfs_createSnapshot [OPTION] PATH"
      << std::endl
      << std::endl << "Create a snapshot of a snapshottable directory."
      << std::endl << "This operation requires owner privilege of the snapshottable directory."
      << std::endl
      << std::endl << "  -n NAME   The snapshot name. When it is omitted, a default name is generated"
      << std::endl << "             using a timestamp with the format:"
      << std::endl << "             \"'s'yyyyMMdd-HHmmss.SSS\", e.g. s20130412-151029.033"
      << std::endl << "  -h        display this help and exit"
      << std::endl
      << std::endl << "Examples:"
      << std::endl << "hdfs_createSnapshot hdfs://localhost.localdomain:8020/dir"
      << std::endl << "hdfs_createSnapshot -n MySnapshot /dir1/dir2"
      << std::endl;
}

int main(int argc, char *argv[]) {
  //We should have at least 2 arguments
  if (argc < 2) {
    usage();
    exit(EXIT_FAILURE);
  }

  int input;
  std::string name;

  //Using GetOpt to read in the values
  opterr = 0;
  while ((input = getopt(argc, argv, "hn:")) != -1) {
    switch (input)
    {
    case 'h':
      usage();
      exit(EXIT_SUCCESS);
    case 'n':
      name = optarg;
      break;
    case '?':
      if (optopt == 'n')
        std::cerr << "Option -" << (char) optopt << " requires an argument." << std::endl;
      else if (isprint(optopt))
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

  std::shared_ptr<hdfs::FileSystem> fs = hdfs::doConnect(uri, false);
  if (!fs) {
    std::cerr << "Could not connect the file system. " << std::endl;
    exit(EXIT_FAILURE);
  }

  hdfs::Status status = fs->CreateSnapshot(uri.get_path(), name);
  if (!status.ok()) {
    std::cerr << "Error: " << status.ToString() << std::endl;
    exit(EXIT_FAILURE);
  }

  // Clean up static data and prevent valgrind memory leaks
  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}
