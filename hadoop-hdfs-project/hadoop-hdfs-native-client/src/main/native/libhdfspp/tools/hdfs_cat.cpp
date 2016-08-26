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
  std::cout << "Usage: hdfs_cat [OPTION] FILE"
      << std::endl
      << std::endl << "Concatenate FILE to standard output."
      << std::endl
      << std::endl << "  -h  display this help and exit"
      << std::endl
      << std::endl << "Examples:"
      << std::endl << "hdfs_cat hdfs://localhost.localdomain:9433/dir/file"
      << std::endl << "hdfs_cat /dir/file"
      << std::endl;
}

#define BUF_SIZE 4096

int main(int argc, char *argv[]) {
  if (argc != 2) {
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
  hdfs::optional<hdfs::URI> uri = hdfs::URI::parse_from_string(uri_path);
  if (!uri) {
    std::cerr << "Malformed URI: " << uri_path << std::endl;
    exit(EXIT_FAILURE);
  }

  //TODO: HDFS-9539 Currently options can be returned empty
  hdfs::Options options = *hdfs::getOptions();

  std::shared_ptr<hdfs::FileSystem> fs = hdfs::doConnect(uri.value(), options);
  if (!fs) {
    std::cerr << "Could not connect the file system. " << std::endl;
    exit(EXIT_FAILURE);
  }

  hdfs::FileHandle *file_raw = nullptr;
  hdfs::Status status = fs->Open(uri->get_path(), &file_raw);
  if (!status.ok()) {
    std::cerr << "Could not open file " << uri->get_path() << ". " << status.ToString() << std::endl;
    exit(EXIT_FAILURE);
  }
  //wrapping file_raw into a unique pointer to guarantee deletion
  std::unique_ptr<hdfs::FileHandle> file(file_raw);

  char input_buffer[BUF_SIZE];
  ssize_t total_bytes_read = 0;
  size_t last_bytes_read = 0;

  do{
    //Reading file chunks
    status = file->Read(input_buffer, sizeof(input_buffer), &last_bytes_read);
    if(status.ok()) {
      //Writing file chunks to stdout
      fwrite(input_buffer, last_bytes_read, 1, stdout);
      total_bytes_read += last_bytes_read;
    } else {
      if(status.is_invalid_offset()){
        //Reached the end of the file
        break;
      } else {
        std::cerr << "Error reading the file: " << status.ToString() << std::endl;
        exit(EXIT_FAILURE);
      }
    }
  } while (last_bytes_read > 0);

  // Clean up static data and prevent valgrind memory leaks
  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}
