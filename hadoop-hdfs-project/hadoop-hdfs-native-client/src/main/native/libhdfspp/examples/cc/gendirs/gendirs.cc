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
   * Usage:   gendirs /<path-to-dir> <depth> <fanout>
   *
   * Example: gendirs /dir0 3 10
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
#include <google/protobuf/stubs/common.h>
#include <future>
#include "tools_common.h"

#define DEFAULT_PERMISSIONS 0755

void GenerateDirectories (std::shared_ptr<hdfs::FileSystem> fs, int depth, int level, int fanout, std::string path, std::vector<std::future<hdfs::Status>> & futures) {
  //Level contains our current depth in the directory tree
  if(level < depth) {
    for(int i = 0; i < fanout; i++){
      //Recursive calls to cover all possible paths from the root to the leave nodes
      GenerateDirectories(fs, depth, level+1, fanout, path + "dir" + std::to_string(i) + "/", futures);
    }
  } else {
    //We have reached the leaf nodes and now start making calls to create directories
    //We make a promise which will be set when the call finishes and executes our handler
    auto callstate = std::make_shared<std::promise<hdfs::Status>>();
    //Extract a future from this promise
    std::future<hdfs::Status> future(callstate->get_future());
    //Save this future to the vector of futures which will be used to wait on all promises
    //after the whole recursion is done
    futures.push_back(std::move(future));
    //Create a handler that will be executed when Mkdirs is done
    auto handler = [callstate](const hdfs::Status &s) {
      callstate->set_value(s);
    };
    //Asynchronous call to create this directory along with all missing parent directories
    fs->Mkdirs(path, DEFAULT_PERMISSIONS, true, handler);
  }
}

int main(int argc, char *argv[]) {
  if (argc != 4) {
    std::cerr << "usage: gendirs /<path-to-dir> <depth> <fanout>" << std::endl;
    exit(EXIT_FAILURE);
  }

  std::string path = argv[1];
  int depth = std::stoi(argv[2]);
  int fanout = std::stoi(argv[3]);

  //Building a URI object from the given uri path
  hdfs::URI uri = hdfs::parse_path_or_exit(path);

  std::shared_ptr<hdfs::FileSystem> fs = hdfs::doConnect(uri, true);
  if (!fs) {
    std::cerr << "Could not connect the file system. " << std::endl;
    exit(EXIT_FAILURE);
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
  std::vector<std::future<hdfs::Status>> futures;

  GenerateDirectories(fs, depth, 0, fanout, path + "/", futures);

  /**
   * We are waiting here until all promises are set, and checking whether
   * the returned statuses contained any errors.
   **/
  for(std::future<hdfs::Status> &fs : futures){
    hdfs::Status status = fs.get();
    if (!status.ok()) {
      std::cerr << "Error: " << status.ToString() << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  std::cout << "All done!" << std::endl;

  // Clean up static data and prevent valgrind memory leaks
  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}
