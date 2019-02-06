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
  std::cout << "Usage: hdfs_setrep [OPTION] NUM_REPLICAS PATH"
      << std::endl
      << std::endl << "Changes the replication factor of a file at PATH. If PATH is a directory then the command"
      << std::endl << "recursively changes the replication factor of all files under the directory tree rooted at PATH."
      << std::endl
      << std::endl << "  -h  display this help and exit"
      << std::endl
      << std::endl << "Examples:"
      << std::endl << "hdfs_setrep 5 hdfs://localhost.localdomain:8020/dir/file"
      << std::endl << "hdfs_setrep 3 /dir1/dir2"
      << std::endl;
}

struct SetReplicationState {
  const uint16_t replication;
  const std::function<void(const hdfs::Status &)> handler;
  //The request counter is incremented once every time SetReplication async call is made
  uint64_t request_counter;
  //This boolean will be set when find returns the last result
  bool find_is_done;
  //Final status to be returned
  hdfs::Status status;
  //Shared variables will need protection with a lock
  std::mutex lock;
  SetReplicationState(const uint16_t replication_, const std::function<void(const hdfs::Status &)> & handler_,
              uint64_t request_counter_, bool find_is_done_)
      : replication(replication_),
        handler(handler_),
        request_counter(request_counter_),
        find_is_done(find_is_done_),
        status(),
        lock() {
  }
};

int main(int argc, char *argv[]) {
  //We should have 3 or 4 parameters
  if (argc < 3) {
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
  std::string repl = argv[optind];
  std::string uri_path = argv[optind + 1];

  //Building a URI object from the given uri_path
  hdfs::URI uri = hdfs::parse_path_or_exit(uri_path);

  std::shared_ptr<hdfs::FileSystem> fs = hdfs::doConnect(uri, true);
  if (!fs) {
    std::cerr << "Could not connect the file system. " << std::endl;
    exit(EXIT_FAILURE);
  }

  /* wrap async FileSystem::SetReplication with promise to make it a blocking call */
  std::shared_ptr<std::promise<hdfs::Status>> promise = std::make_shared<std::promise<hdfs::Status>>();
  std::future<hdfs::Status> future(promise->get_future());
  auto handler = [promise](const hdfs::Status &s) {
    promise->set_value(s);
  };

  uint16_t replication = std::stoi(repl.c_str(), NULL, 8);
  //Allocating shared state, which includes:
  //replication to be set, handler to be called, request counter, and a boolean to keep track if find is done
  std::shared_ptr<SetReplicationState> state = std::make_shared<SetReplicationState>(replication, handler, 0, false);

  // Keep requesting more from Find until we process the entire listing. Call handler when Find is done and reques counter is 0.
  // Find guarantees that the handler will only be called once at a time so we do not need locking in handlerFind.
  auto handlerFind = [fs, state](const hdfs::Status &status_find, const std::vector<hdfs::StatInfo> & stat_infos, bool has_more_results) -> bool {

    //For each result returned by Find we call async SetReplication with the handler below.
    //SetReplication DOES NOT guarantee that the handler will only be called once at a time, so we DO need locking in handlerSetReplication.
    auto handlerSetReplication = [state](const hdfs::Status &status_set_replication) {
      std::lock_guard<std::mutex> guard(state->lock);

      //Decrement the counter once since we are done with this async call
      if (!status_set_replication.ok() && state->status.ok()){
        //We make sure we set state->status only on the first error.
        state->status = status_set_replication;
      }
      state->request_counter--;
      if(state->request_counter == 0 && state->find_is_done){
        state->handler(state->status); //exit
      }
    };
    if(!stat_infos.empty() && state->status.ok()) {
      for (hdfs::StatInfo const& s : stat_infos) {
        //Launch an asynchronous call to SetReplication for every returned file
        if(s.file_type == hdfs::StatInfo::IS_FILE){
          state->request_counter++;
          fs->SetReplication(s.full_path, state->replication, handlerSetReplication);
        }
      }
    }

    //Lock this section because handlerSetReplication might be accessing the same
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
