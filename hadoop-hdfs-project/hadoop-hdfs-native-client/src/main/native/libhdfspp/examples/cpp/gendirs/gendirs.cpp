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
   * A recursive directory generator tool.
   *
   * Generates a directory tree with specified depth and fanout starting from
   * a given path. Generation is asynchronous.
   *
   * Usage:   gendirs [hdfs://[<hostname>:<port>]]/<path-to-dir> <depth> <fanout>
   *
   * Example: gendirs hdfs://localhost.localdomain:9433/dir0 3 10
   *
   * @param path-to-dir   Absolute path to the directory tree root where the
   *                      directory tree will be generated
   * @param depth         Depth of the directory tree (number of levels from
   *                      root to leaves)
   * @param fanout        Fanout of each directory (number of sub-directories to
   *                      be created inside each directory except leaf directories)
   *
   **/

#include "hdfspp/hdfspp.h"
#include "fs/namenode_operations.h"
#include "common/hdfs_configuration.h"
#include "common/configuration_loader.h"
#include "common/uri.h"

#include <google/protobuf/io/coded_stream.h>
#include <future>

using namespace std;
using namespace hdfs;

#define SCHEME "hdfs"

void GenerateDirectories (shared_ptr<FileSystem> fs, int depth, int level, int fanout, string path, vector<future<Status>> & futures) {
  //Level contains our current depth in the directory tree
  if(level < depth) {
    for(int i = 0; i < fanout; i++){
      //Recursive calls to cover all possible paths from the root to the leave nodes
      GenerateDirectories(fs, depth, level+1, fanout, path + "dir" + to_string(i) + "/", futures);
    }
  } else {
    //We have reached the leaf nodes and now start making calls to create directories
    //We make a promise which will be set when the call finishes and executes our handler
    auto callstate = make_shared<promise<Status>>();
    //Extract a future from this promise
    future<Status> future(callstate->get_future());
    //Save this future to the vector of futures which will be used to wait on all promises
    //after the whole recursion is done
    futures.push_back(move(future));
    //Create a handler that will be executed when Mkdirs is done
    auto handler = [callstate](const Status &s) {
      callstate->set_value(s);
    };
    //Asynchronous call to create this directory along with all missing parent directories
    fs->Mkdirs(path, NameNodeOperations::GetDefaultPermissionMask(), true, handler);
  }
}

int main(int argc, char *argv[]) {
  if (argc != 4) {
    cerr << "usage: gendirs [hdfs://[<hostname>:<port>]]/<path-to-dir> <depth> <fanout>" << endl;
    return 1;
  }

  optional<URI> uri;
  const string uri_path = argv[1];
  const int depth = stoi(argv[2]);
  const int fanout = stoi(argv[3]);

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
  options.rpc_timeout = numeric_limits<int>::max();
  if(config){
    options = config->GetOptions();
  }

  IoService * io_service = IoService::New();

  FileSystem *fs_raw = FileSystem::New(io_service, "", options);
  if (!fs_raw) {
    cerr << "Could not create FileSystem object" << endl;
    return 1;
  }
  //Wrapping fs_raw into a unique pointer to guarantee deletion
  shared_ptr<FileSystem> fs(fs_raw);

  //Get port from the uri, otherwise use the default port
  string port = to_string(uri->get_port().value_or(8020));
  Status stat = fs->Connect(uri->get_host(), port);
  if (!stat.ok()) {
    cerr << "Could not connect to " << uri->get_host() << ":" << port << endl;
    return 1;
  }

  /**
   * We do not want the recursion to block on anything, therefore we will be
   * making asynchronous calls recursively, and then just waiting for all
   * the calls to finish.
   *
   * This array of futures will be populated by the recursive function below.
   * Each new asynchronous Mkdirs call will add a future to this vector, and will
   * create a promise, which will only be set when the call was completed and
   * processed. After the whole recursion is complete we will need to wait until
   * all promises are set before we can exit.
   **/
  vector<future<Status>> futures;

  GenerateDirectories(fs, depth, 0, fanout, uri->get_path() + "/", futures);

  /**
   * We are waiting here until all promises are set, and checking whether
   * the returned statuses contained any errors.
   **/
  for(future<Status> &fs : futures){
    Status stat = fs.get();
    if (!stat.ok()) {
      cerr << "Error: " << stat.ToString() << endl;
    }
  }

  cout << "All done!" << endl;

  // Clean up static data and prevent valgrind memory leaks
  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}
