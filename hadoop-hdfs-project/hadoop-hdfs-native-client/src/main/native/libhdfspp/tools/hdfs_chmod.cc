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
  std::cout << "Usage: hdfs_chmod [OPTION] <MODE[,MODE]... | OCTALMODE> FILE"
      << std::endl
      << std::endl << "Change the permissions of each FILE to MODE."
      << std::endl << "The user must be the owner of the file, or else a super-user."
      << std::endl << "Additional information is in the Permissions Guide:"
      << std::endl << "https://hadoop.apache.org/docs/r2.7.1/hadoop-project-dist/hadoop-hdfs/HdfsPermissionsGuide.html"
      << std::endl
      << std::endl << "  -R  operate on files and directories recursively"
      << std::endl << "  -h  display this help and exit"
      << std::endl
      << std::endl << "Examples:"
      << std::endl << "hdfs_chmod -R 755 hdfs://localhost.localdomain:8020/dir/file"
      << std::endl << "hdfs_chmod 777 /dir/file"
      << std::endl;
}

struct SetPermissionState {
  const uint16_t permissions;
  const std::function<void(const hdfs::Status &)> handler;
  //The request counter is incremented once every time SetOwner async call is made
  uint64_t request_counter;
  //This boolean will be set when find returns the last result
  bool find_is_done;
  //Final status to be returned
  hdfs::Status status;
  //Shared variables will need protection with a lock
  std::mutex lock;
  SetPermissionState(const uint16_t permissions_, const std::function<void(const hdfs::Status &)> & handler_,
              uint64_t request_counter_, bool find_is_done_)
      : permissions(permissions_),
        handler(handler_),
        request_counter(request_counter_),
        find_is_done(find_is_done_),
        status(),
        lock() {
  }
};

int main(int argc, char *argv[]) {
  //We should have 3 or 4 parameters
  if (argc != 3 && argc != 4) {
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
      recursive = 1;
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
  std::string permissions = argv[optind];
  std::string uri_path = argv[optind + 1];

  //Building a URI object from the given uri_path
  hdfs::URI uri = hdfs::parse_path_or_exit(uri_path);

  std::shared_ptr<hdfs::FileSystem> fs = hdfs::doConnect(uri, true);
  if (!fs) {
    std::cerr << "Could not connect the file system. " << std::endl;
    exit(EXIT_FAILURE);
  }

  /* wrap async FileSystem::SetPermission with promise to make it a blocking call */
  std::shared_ptr<std::promise<hdfs::Status>> promise = std::make_shared<std::promise<hdfs::Status>>();
  std::future<hdfs::Status> future(promise->get_future());
  auto handler = [promise](const hdfs::Status &s) {
    promise->set_value(s);
  };

  //strtol() is reading the value with base 8, NULL because we are reading in just one value.
  uint16_t perm = strtol(permissions.c_str(), NULL, 8);
  if(!recursive){
    fs->SetPermission(uri.get_path(), perm, handler);
  }
  else {
    //Allocating shared state, which includes:
    //permissions to be set, handler to be called, request counter, and a boolean to keep track if find is done
    std::shared_ptr<SetPermissionState> state = std::make_shared<SetPermissionState>(perm, handler, 0, false);

    // Keep requesting more from Find until we process the entire listing. Call handler when Find is done and reques counter is 0.
    // Find guarantees that the handler will only be called once at a time so we do not need locking in handlerFind.
    auto handlerFind = [fs, state](const hdfs::Status &status_find, const std::vector<hdfs::StatInfo> & stat_infos, bool has_more_results) -> bool {

      //For each result returned by Find we call async SetPermission with the handler below.
      //SetPermission DOES NOT guarantee that the handler will only be called once at a time, so we DO need locking in handlerSetPermission.
      auto handlerSetPermission = [state](const hdfs::Status &status_set_permission) {
        std::lock_guard<std::mutex> guard(state->lock);

        //Decrement the counter once since we are done with this async call
        if (!status_set_permission.ok() && state->status.ok()){
          //We make sure we set state->status only on the first error.
          state->status = status_set_permission;
        }
        state->request_counter--;
        if(state->request_counter == 0 && state->find_is_done){
          state->handler(state->status); //exit
        }
      };
      if(!stat_infos.empty() && state->status.ok()) {
        for (hdfs::StatInfo const& s : stat_infos) {
          //Launch an asynchronous call to SetPermission for every returned result
          state->request_counter++;
          fs->SetPermission(s.full_path, state->permissions, handlerSetPermission);
        }
      }

      //Lock this section because handlerSetPermission might be accessing the same
      //shared variables simultaneously
      std::lock_guard<std::mutex> guard(state->lock);
      if (!status_find.ok() && state->status.ok()){
        //We make sure we set state->status only on the first error.
        state->status = status_find;
      }
      if(!has_more_results){
        state->find_is_done = true;
        if(state->request_counter == 0){
          state->handler(state->status); //exit
        }
        return false;
      }
      return true;
    };

    //Asynchronous call to Find
    fs->Find(uri.get_path(), "*", hdfs::FileSystem::GetDefaultFindMaxDepth(), handlerFind);
  }

  /* block until promise is set */
  hdfs::Status status = future.get();
  if (!status.ok()) {
    std::cerr << "Error: " << status.ToString() << std::endl;
    exit(EXIT_FAILURE);
  }

  // Clean up static data and prevent valgrind memory leaks
  google::protobuf::ShutdownProtobufLibrary();
  return 0;
}
