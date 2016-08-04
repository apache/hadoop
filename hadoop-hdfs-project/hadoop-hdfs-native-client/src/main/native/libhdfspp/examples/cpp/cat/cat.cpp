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

/*
  A a stripped down version of unix's "cat".
  Doesn't deal with any flags for now, will just attempt to read the whole file.
*/

#include "hdfspp/hdfspp.h"
#include "common/hdfs_configuration.h"
#include "common/configuration_loader.h"
#include "common/uri.h"

#include <google/protobuf/io/coded_stream.h>

using namespace std;
using namespace hdfs;

#define SCHEME "hdfs"

int main(int argc, char *argv[]) {
  if (argc != 2) {
    cerr << "usage: cat [hdfs://[<hostname>:<port>]]/<path-to-file>" << endl;
    return 1;
  }

  optional<URI> uri;
  const string uri_path = argv[1];

  //Separate check for scheme is required, otherwise common/uri.h library causes memory issues under valgrind
  size_t scheme_end = uri_path.find("://");
  if (scheme_end != string::npos) {
    if(uri_path.substr(0, string(SCHEME).size()).compare(SCHEME) != 0) {
      cerr << "Scheme " << uri_path.substr(0, scheme_end) << ":// is not supported" << endl;
      return 1;
    } else {
      uri = URI::parse_from_string(uri_path);
    }
  }
  if (!uri) {
    cerr << "Malformed URI: " << uri_path << endl;
    return 1;
  }

  ConfigurationLoader loader;
  optional<HdfsConfiguration> config = loader.LoadDefaultResources<HdfsConfiguration>();
  const char * envHadoopConfDir = getenv("HADOOP_CONF_DIR");
  if (envHadoopConfDir && (*envHadoopConfDir != 0) ) {
    config = loader.OverlayResourceFile(*config, string(envHadoopConfDir) + "/core-site.xml");
  }

  Options options;
  if(config){
    options = config->GetOptions();
  }

  IoService * io_service = IoService::New();

  FileSystem *fs_raw = FileSystem::New(io_service, "", options);
  if (!fs_raw) {
    cerr << "Could not create FileSystem object" << endl;
    return 1;
  }
  //wrapping fs_raw into a unique pointer to guarantee deletion
  unique_ptr<FileSystem> fs(fs_raw);

  Status stat = fs->Connect(uri->get_host(), to_string(*(uri->get_port())));
  if (!stat.ok()) {
    cerr << "Could not connect to " << uri->get_host() << ":" << *(uri->get_port()) << endl;
    return 1;
  }

  FileHandle *file_raw = nullptr;
  stat = fs->Open(uri->get_path(), &file_raw);
  if (!stat.ok()) {
    cerr << "Could not open file " << uri->get_path() << endl;
    return 1;
  }
  //wrapping file_raw into a unique pointer to guarantee deletion
  unique_ptr<FileHandle> file(file_raw);

  char input_buffer[4096];
  ssize_t read_bytes_count = 0;
  size_t last_read_bytes = 0;

  do{
    //Reading file chunks
    Status stat = file->PositionRead(input_buffer, sizeof(input_buffer), read_bytes_count, &last_read_bytes);
    if(stat.ok()) {
      //Writing file chunks to stdout
      fwrite(input_buffer, last_read_bytes, 1, stdout);
      read_bytes_count += last_read_bytes;
    } else {
      if(stat.is_invalid_offset()){
        //Reached the end of the file
        break;
      } else {
        cerr << "Error reading the file: " << stat.ToString() << endl;
        return 1;
      }
    }
  } while (last_read_bytes > 0);

  // Clean up static data and prevent valgrind memory leaks
  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}
