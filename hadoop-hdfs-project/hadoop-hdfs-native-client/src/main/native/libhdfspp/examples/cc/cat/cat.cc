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
   * Unix-like cat tool example.
   *
   * Reads the specified file from HDFS and outputs to stdout.
   *
   * Usage: cat /<path-to-file>
   *
   * Example: cat /dir/file
   *
   * @param path-to-file    Absolute path to the file to read.
   *
   **/

#include "hdfspp/hdfspp.h"
#include <google/protobuf/stubs/common.h>
#include "tools_common.h"

const std::size_t BUF_SIZE = 1048576; //1 MB
static char input_buffer[BUF_SIZE];

int main(int argc, char *argv[]) {
  if (argc != 2) {
    std::cerr << "usage: cat /<path-to-file>" << std::endl;
    exit(EXIT_FAILURE);
  }
  std::string path = argv[1];

  //Building a URI object from the given uri path
  hdfs::URI uri = hdfs::parse_path_or_exit(path);

  std::shared_ptr<hdfs::FileSystem> fs = hdfs::doConnect(uri, false);
  if (!fs) {
    std::cerr << "Could not connect the file system. " << std::endl;
    exit(EXIT_FAILURE);
  }

  hdfs::FileHandle *file_raw = nullptr;
  hdfs::Status status = fs->Open(path, &file_raw);
  if (!status.ok()) {
    std::cerr << "Could not open file " << path << ". " << status.ToString() << std::endl;
    exit(EXIT_FAILURE);
  }
  //wrapping file_raw into a unique pointer to guarantee deletion
  std::unique_ptr<hdfs::FileHandle> file(file_raw);

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
