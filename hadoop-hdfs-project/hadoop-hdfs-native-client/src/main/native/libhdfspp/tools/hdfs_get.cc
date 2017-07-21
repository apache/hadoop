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
  std::cout << "Usage: hdfs_get [OPTION] SRC_FILE DST_FILE"
      << std::endl
      << std::endl << "Copy SRC_FILE from hdfs to DST_FILE on the local file system."
      << std::endl
      << std::endl << "  -h  display this help and exit"
      << std::endl
      << std::endl << "Examples:"
      << std::endl << "hdfs_get hdfs://localhost.localdomain:8020/dir/file /home/usr/myfile"
      << std::endl << "hdfs_get /dir/file /home/usr/dir/file"
      << std::endl;
}

int main(int argc, char *argv[]) {
  if (argc > 4) {
    usage();
    exit(EXIT_FAILURE);
  }

  int input;

  //Using GetOpt to read in the values
  opterr = 0;
  while ((input = getopt(argc, argv, "h")) != -1) {
    switch (input)
    {
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
  std::string dest = argv[optind+1];

  //Building a URI object from the given uri_path
  hdfs::URI uri = hdfs::parse_path_or_exit(uri_path);

  std::shared_ptr<hdfs::FileSystem> fs = hdfs::doConnect(uri, false);
  if (!fs) {
    std::cerr << "Could not connect the file system. " << std::endl;
    exit(EXIT_FAILURE);
  }

  std::FILE* dst_file = std::fopen(dest.c_str(), "wb");
  if(!dst_file){
    std::cerr << "Unable to open the destination file: " << dest << std::endl;
    exit(EXIT_FAILURE);
  }
  readFile(fs, uri.get_path(), 0, dst_file, false);
  std::fclose(dst_file);

  // Clean up static data and prevent valgrind memory leaks
  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}
