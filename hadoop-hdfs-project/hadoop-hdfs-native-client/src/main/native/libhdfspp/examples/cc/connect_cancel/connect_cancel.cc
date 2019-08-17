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


#include "hdfspp/hdfspp.h"
#include "common/hdfs_configuration.h"
#include "common/configuration_loader.h"

#include <google/protobuf/stubs/common.h>

#include <signal.h>
#include <unistd.h>

#include <thread>
#include <iostream>

// Simple example of how to cancel an async connect call.
// Here Control-C (SIGINT) is caught in order to invoke the FS level cancel and
// properly tear down the process.  Valgrind should show no leaked memory on exit
// when cancel has been called.  URI parsing code is omitted and defaultFs from
// /etc/hadoop/conf or $HADOOP_CONF_DIR is always used.

// Scoped globally to make it simple to reference from the signal handler.
std::shared_ptr<hdfs::FileSystem> fs;

const std::string catch_enter("In signal handler, going to try and cancel FileSystem::Connect.\n");
const std::string catch_cancel("FileSystem::Cancel has been canceled in the signal handler.\n");
const std::string catch_exit("Exiting the signal handler.\n");

// Avoid IO reentrancy issues, see comments in signal handler below.
// It's possible that the write interleaves with another write call,
// but it won't corrupt the stack or heap.
static void sighandler_direct_stdout(const std::string &msg) {
  ssize_t res = ::write(1 /*posix stdout FD*/, msg.data(), msg.size());
  // In production you'd want to check res, but error handling code will
  // need to be fairly application specific if it's going to properly
  // avoid reentrant calls to malloc.
  (void)res;
}

// Signal handler to make a SIGINT call cancel rather than exit().
static void sig_catch(int val) {
  (void)val;
  // This is avoiding the tricky bits of signal handling, notably that the
  // underlying string manipulation and IO functions used by the the logger
  // are unlikely to be reentrant.
  //
  // Production code could mask out all logging on handler entry and enable
  // it again on exit; here we just assume it's "good enough" and some
  // (possibly broken) log messages are better than none.

  sighandler_direct_stdout(catch_enter);
  if(fs) {
    // This will invoke the callback immediately with an OperationCanceled status
    fs->CancelPendingConnect();
    sighandler_direct_stdout(catch_cancel);
  }
  sighandler_direct_stdout(catch_exit);
}


int main(int arg_token_count, const char **args) {
  (void)args;
  if(arg_token_count != 1) {
    std::cerr << "usage: ./connect_cancel";
    google::protobuf::ShutdownProtobufLibrary();
    exit(EXIT_FAILURE);
  }

  // Register signal handle to asynchronously invoke cancel from outside the main thread.
  signal(SIGINT, sig_catch);

  // Generic setup/config code much like the other examples.
  hdfs::Options options;
  //Setting the config path to the default: "$HADOOP_CONF_DIR" or "/etc/hadoop/conf"
  hdfs::ConfigurationLoader loader;
  //Loading default config files core-site.xml and hdfs-site.xml from the config path
  hdfs::optional<hdfs::HdfsConfiguration> config = loader.LoadDefaultResources<hdfs::HdfsConfiguration>();
  //TODO: HDFS-9539 - after this is resolved, valid config will always be returned.
  if(config){
    //Loading options from the config
    options = config->GetOptions();
  }


  // Start an IoService and some worker threads
  std::shared_ptr<hdfs::IoService> service = hdfs::IoService::MakeShared();
  if(nullptr == service) {
    std::cerr << "Unable to create IoService" << std::endl;
    fs.reset();
    // Nasty hack to clean up for valgrind since we don't have the C++17 optional<T>::reset method
    config = decltype(config)();
    google::protobuf::ShutdownProtobufLibrary();
    exit(EXIT_FAILURE);
  }

  unsigned int worker_count = service->InitDefaultWorkers();
  if(worker_count < 1) {
    std::cerr << "Unable to create IoService worker threads";
    fs.reset();
    service->Stop();
    config = decltype(config)();
    google::protobuf::ShutdownProtobufLibrary();
    exit(EXIT_FAILURE);
  }

  // Set up and connect to the FileSystem
  fs.reset(hdfs::FileSystem::New(service, "", options));
  if(nullptr == fs) {
    std::cerr << "Unable to create FileSystem" << std::endl;
    fs.reset();
    service->Stop();
    config = decltype(config)();
    google::protobuf::ShutdownProtobufLibrary();
    exit(EXIT_FAILURE);
  }

  hdfs::Status status = fs->ConnectToDefaultFs();
  if (!status.ok()) {
    if(!options.defaultFS.get_host().empty()){
      std::cerr << "Error connecting to " << options.defaultFS << ". " << status.ToString() << std::endl;
    } else {
      std::cerr << "Error connecting to the cluster: defaultFS is empty. " << status.ToString() << std::endl;
    }
    fs.reset();
    service->Stop();
    config = decltype(config)();
    google::protobuf::ShutdownProtobufLibrary();
    exit(EXIT_FAILURE);
  }

  fs.reset();
  service->Stop();
  config = decltype(config)();
  google::protobuf::ShutdownProtobufLibrary();

  return 0;
}
