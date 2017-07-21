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
  std::cout << "Usage: hdfs_mkdir [OPTION] DIRECTORY"
      << std::endl
      << std::endl << "Create the DIRECTORY(ies), if they do not already exist."
      << std::endl
      << std::endl << "  -p        make parent directories as needed"
      << std::endl << "  -m  MODE  set file mode (octal permissions) for the new DIRECTORY(ies)"
      << std::endl << "  -h        display this help and exit"
      << std::endl
      << std::endl << "Examples:"
      << std::endl << "hdfs_mkdir hdfs://localhost.localdomain:8020/dir1/dir2"
      << std::endl << "hdfs_mkdir -p /extant_dir/non_extant_dir/non_extant_dir/new_dir"
      << std::endl;
}

int main(int argc, char *argv[]) {
  //We should have at least 2 arguments
  if (argc < 2) {
    usage();
    exit(EXIT_FAILURE);
  }

  bool create_parents = false;
  uint16_t permissions = hdfs::FileSystem::GetDefaultPermissionMask();
  int input;

  //Using GetOpt to read in the values
  opterr = 0;
  while ((input = getopt(argc, argv, "pm:h")) != -1) {
    switch (input)
    {
    case 'p':
      create_parents = true;
      break;
    case 'h':
      usage();
      exit(EXIT_SUCCESS);
    case 'm':
      //Get octal permissions for the new DIRECTORY(ies)
      permissions = strtol(optarg, NULL, 8);
      break;
    case '?':
      if (optopt == 'm')
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

  hdfs::Status status = fs->Mkdirs(uri.get_path(), permissions, create_parents);
  if (!status.ok()) {
    std::cerr << "Error: " << status.ToString() << std::endl;
    exit(EXIT_FAILURE);
  }

  // Clean up static data and prevent valgrind memory leaks
  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}
