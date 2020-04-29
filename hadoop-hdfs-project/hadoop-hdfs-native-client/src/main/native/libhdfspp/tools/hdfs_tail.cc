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
  std::cout << "Usage: hdfs_tail [OPTION] FILE"
      << std::endl
      << std::endl << "Displays last kilobyte of the file to stdout."
      << std::endl
      << std::endl << "  -f  output appended data as the file grows, as in Unix"
      << std::endl << "  -h  display this help and exit"
      << std::endl
      << std::endl << "Examples:"
      << std::endl << "hdfs_tail hdfs://localhost.localdomain:8020/dir/file"
      << std::endl << "hdfs_tail /dir/file"
      << std::endl;
}

#define TAIL_SIZE 1024
#define REFRESH_RATE 1 //seconds

int main(int argc, char *argv[]) {
  if (argc < 2) {
    usage();
    exit(EXIT_FAILURE);
  }

  bool follow = false;
  int input;

  //Using GetOpt to read in the values
  opterr = 0;
  while ((input = getopt(argc, argv, "hf")) != -1) {
    switch (input)
    {
    case 'h':
      usage();
      exit(EXIT_SUCCESS);
    case 'f':
      follow = true;
      break;
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

  std::shared_ptr<hdfs::FileSystem> fs = hdfs::doConnect(uri, false);
  if (!fs) {
    std::cerr << "Could not connect the file system. " << std::endl;
    exit(EXIT_FAILURE);
  }

  //We need to get the size of the file using stat
  hdfs::StatInfo stat_info;
  hdfs::Status status = fs->GetFileInfo(uri.get_path(), stat_info);
  if (!status.ok()) {
    std::cerr << "Error: " << status.ToString() << std::endl;
    exit(EXIT_FAILURE);
  }

  //Determine where to start reading
  off_t offset = 0;
  if(stat_info.length > TAIL_SIZE){
    offset = stat_info.length - TAIL_SIZE;
  }

  do {
    off_t current_length = (off_t) stat_info.length;
    readFile(fs, uri.get_path(), offset, stdout, false);

    //Exit if -f flag was not set
    if(!follow){
      break;
    }

    do{
      //Sleep for the REFRESH_RATE
      sleep(REFRESH_RATE);
      //Use stat to check the new filesize.
      status = fs->GetFileInfo(uri.get_path(), stat_info);
      if (!status.ok()) {
        std::cerr << "Error: " << status.ToString() << std::endl;
        exit(EXIT_FAILURE);
      }
      //If file became longer, loop back and print the difference
    }
    while((off_t) stat_info.length <= current_length);
  } while (true);

  // Clean up static data and prevent valgrind memory leaks
  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}
