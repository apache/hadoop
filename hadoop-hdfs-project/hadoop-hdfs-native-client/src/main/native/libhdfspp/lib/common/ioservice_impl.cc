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

#include "ioservice_impl.h"

#include <thread>
#include <mutex>
#include <vector>

#include "common/util.h"
#include "common/logging.h"


namespace hdfs {

IoService::~IoService() {}

IoService *IoService::New() {
  return new IoServiceImpl();
}

std::shared_ptr<IoService> IoService::MakeShared() {
  return std::make_shared<IoServiceImpl>();
}


unsigned int IoServiceImpl::InitDefaultWorkers() {
  LOG_TRACE(kAsyncRuntime, << "IoServiceImpl::InitDefaultWorkers@" << this << " called.");
  unsigned int logical_thread_count = std::thread::hardware_concurrency();
#ifndef DISABLE_CONCURRENT_WORKERS
  if(logical_thread_count < 1) {
    LOG_WARN(kAsyncRuntime, << "IoServiceImpl::InitDefaultWorkers did not detect any logical processors.  Defaulting to 1 worker thread.");
  } else {
    LOG_DEBUG(kRPC, << "IoServiceImpl::InitDefaultWorkers detected " << logical_thread_count << " logical threads and will spawn a worker for each.");
  }
#else
  if(logical_thread_count > 0) {
    LOG_DEBUG(kAsyncRuntime, << "IoServiceImpl::InitDefaultWorkers: " << logical_thread_count << " threads available.  Concurrent workers are disabled so 1 worker thread will be used");
  }
  logical_thread_count = 1;
#endif
  return InitWorkers(logical_thread_count);
}

unsigned int IoServiceImpl::InitWorkers(unsigned int thread_count) {
#ifdef DISABLED_CONCURRENT_WORKERS
  LOG_DEBUG(kAsyncRuntime, << "IoServiceImpl::InitWorkers: " << thread_count << " threads specified but concurrent workers are disabled so 1 will be used");
  thread_count = 1;
#endif
  unsigned int created_threads = 0;
  for(unsigned int i=0; i<thread_count; i++) {
    bool created = AddWorkerThread();
    if(created) {
      created_threads++;
    } else {
      LOG_DEBUG(kAsyncRuntime, << "IoServiceImpl@" << this << " ::InitWorkers failed to create a worker thread");
    }
  }
  if(created_threads != thread_count) {
    LOG_WARN(kAsyncRuntime, << "IoServiceImpl@" << this << " ::InitWorkers attempted to create "
                            << thread_count << " but only created " << created_threads
                            << " worker threads.  Make sure this process has adequate resources.");
  }
  return created_threads;
}

bool IoServiceImpl::AddWorkerThread() {
  mutex_guard state_lock(state_lock_);
  auto async_worker = [this]() {
    this->ThreadStartHook();
    this->Run();
    this->ThreadExitHook();
  };
  worker_threads_.push_back(WorkerPtr( new std::thread(async_worker)) );
  return true;
}


void IoServiceImpl::ThreadStartHook() {
  mutex_guard state_lock(state_lock_);
  LOG_DEBUG(kAsyncRuntime, << "Worker thread #" << std::this_thread::get_id() << " for IoServiceImpl@" << this << " starting");
}

void IoServiceImpl::ThreadExitHook() {
  mutex_guard state_lock(state_lock_);
  LOG_DEBUG(kAsyncRuntime, << "Worker thread #" << std::this_thread::get_id() << " for IoServiceImpl@" << this << " exiting");
}

void IoServiceImpl::PostTask(std::function<void(void)> asyncTask) {
  io_service_.post(asyncTask);
}

void IoServiceImpl::WorkerDeleter::operator()(std::thread *t) {
  // It is far too easy to destroy the filesystem (and thus the threadpool)
  //     from within one of the worker threads, leading to a deadlock.  Let's
  //     provide some explicit protection.
  if(t->get_id() == std::this_thread::get_id()) {
    LOG_ERROR(kAsyncRuntime, << "FileSystemImpl::WorkerDeleter::operator(treadptr="
                             << t << ") : FATAL: Attempted to destroy a thread pool"
                             "from within a callback of the thread pool!");
  }
  t->join();
  delete t;
}

// As long as this just forwards to an asio::io_service method it doesn't need a lock
void IoServiceImpl::Run() {
  // The IoService executes callbacks provided by library users in the context of worker threads,
  // there is no way of preventing those callbacks from throwing but we can at least prevent them
  // from escaping this library and crashing the process.

  // As recommended in http://www.boost.org/doc/libs/1_39_0/doc/html/boost_asio/reference/io_service.html#boost_asio.reference.io_service.effect_of_exceptions_thrown_from_handlers
  asio::io_service::work work(io_service_);
  while(true)
  {
    try
    {
      io_service_.run();
      break;
    } catch (const std::exception & e) {
      LOG_WARN(kFileSystem, << "Unexpected exception in libhdfspp worker thread: " << e.what());
    } catch (...) {
      LOG_WARN(kFileSystem, << "Caught unexpected value not derived from std::exception in libhdfspp worker thread");
    }
  }
}

void IoServiceImpl::Stop() {
  // Note: This doesn't wait for running operations to stop.
  io_service_.stop();
}

asio::io_service& IoServiceImpl::GetRaw() {
  return io_service_;
}

unsigned int IoServiceImpl::GetWorkerThreadCount() {
  mutex_guard state_lock(state_lock_);
  return worker_threads_.size();

}


} // namespace hdfs
