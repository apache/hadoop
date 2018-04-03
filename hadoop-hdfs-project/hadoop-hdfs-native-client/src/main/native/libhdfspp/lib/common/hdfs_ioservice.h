/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef COMMON_HDFS_IOSERVICE_H_
#define COMMON_HDFS_IOSERVICE_H_

#include "hdfspp/hdfspp.h"

#include <asio/io_service.hpp>
#include "common/util.h"

#include <mutex>
#include <thread>

namespace hdfs {

// Uncomment this to determine if issues are due to concurrency or logic faults
// If tests still fail with concurrency disabled it's most likely a logic bug
#define DISABLE_CONCURRENT_WORKERS

/*
 *  A thin wrapper over the asio::io_service with a few extras
 *    -manages it's own worker threads
 *    -some helpers for sharing with multiple modules that need to do async work
 */

class IoServiceImpl : public IoService {
 public:
  IoServiceImpl() {}

  virtual unsigned int InitDefaultWorkers() override;
  virtual unsigned int InitWorkers(unsigned int thread_count) override;
  virtual void PostTask(std::function<void(void)>& asyncTask) override;
  virtual void Run() override;
  virtual void Stop() override { io_service_.stop(); }

  // Add a single worker thread, in the common case try to avoid this in favor
  // of Init[Default]Workers. Public for use by tests and rare cases where a
  // client wants very explicit control of threading for performance reasons
  // e.g. pinning threads to NUMA nodes.
  bool AddWorkerThread();

  // Be very careful about using this: HDFS-10241
  ::asio::io_service &io_service() { return io_service_; }
  unsigned int get_worker_thread_count();
 private:
  std::mutex state_lock_;
  ::asio::io_service io_service_;

  // For doing logging + resource manager updates on thread start/exit
  void ThreadStartHook();
  void ThreadExitHook();

  // Support for async worker threads
  struct WorkerDeleter {
    void operator()(std::thread *t);
  };
  typedef std::unique_ptr<std::thread, WorkerDeleter> WorkerPtr;
  std::vector<WorkerPtr> worker_threads_;
};

}

#endif
