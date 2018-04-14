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

/**
 * An asio::io_service maintains a queue of asynchronous tasks and invokes them
 * when they are ready to run.  Async network IO handlers become runnable when
 * the associated IO operation has completed.  The hdfs::IoService is a thin
 * wrapper over that object to make it easier to add logging and instrumentation
 * to tasks that have been queued.
 *
 * Lifecycle management:
 *   -The IoService *shall* outlive any tasks it owns.  Deleting a task
 *    before it has been run **will** result in dangling reference issues.
 *   -Dependencies (including transitive dependencies) of pending tasks
 *    *shall* outlive the task.  Failure to ensure this **will** result in
 *    danging reference issues.
 *     -libhdfs++ uses shared_ptr/weak_ptr heavily as a mechanism to ensure
 *      liveness of dependencies.
 *     -refcounted pointers in lambda capture lists have a poor track record
 *      for ensuring liveness in this library; it's easy to omit them because
 *      the capture list isn't context aware.  Developers are encouraged to
 *      write callable classes that explicitly list dependencies.
 *
 * Constraints on tasks:
 *   -Tasks and async callbacks *shall* never do blocking IO or sleep().
 *    At best this hurts performance by preventing worker threads from doing
 *    useful work.  It may also cause situations that look like deadlocks
 *    if the worker thread is stalled for long enough.
 *   -Tasks and async callbacks *shall* not acquire locks that guard resources
 *    that might be unavailable for an unknown amount of time.  Lock acquisition
 *    when accessing shared data structures is acceptable and is often required.
 *   -Tasks and async callbacks *should* not allow exceptions to escape their
 *    scope since tasks will be executed on a different stack then where they
 *    were created.  The exception will be caught by the IoService rather than
 *    being forwarded to the next task.
 *   -Tasks and async callbacks *should* not rely on thread local storage for
 *    ancillary context.  The IoService does not support any sort of thread
 *    affinity that would guarantee tasks Post()ed from one thread will always
 *    be executed on the same thread.  Applications that only use a single
 *    worker thread may use TLS but developers should be mindful that throughput
 *    can no longer be scaled by adding threads.
 **/
#ifndef INCLUDE_HDFSPP_IOSERVICE_H_
#define INCLUDE_HDFSPP_IOSERVICE_H_

#include <memory>

// forward decl
namespace asio {
  class io_service;
}

namespace hdfs {

// (Un)comment this to determine if issues are due to concurrency or logic faults
// If tests still fail with concurrency disabled it's most likely a logic bug
#define DISABLE_CONCURRENT_WORKERS

class IoService : public std::enable_shared_from_this<IoService>
{
 public:
  static IoService *New();
  static std::shared_ptr<IoService> MakeShared();
  virtual ~IoService();

  /**
   * Start up as many threads as there are logical processors.
   * Return number of threads created.
   **/
  virtual unsigned int InitDefaultWorkers() = 0;

  /**
   * Initialize with thread_count handler threads.
   * If thread count is less than one print a log message and default to one thread.
   * Return number of threads created.
   **/
  virtual unsigned int InitWorkers(unsigned int thread_count) = 0;

  /**
   * Add a worker thread to existing pool.
   * Return true on success, false otherwise.
   **/
  virtual bool AddWorkerThread() = 0;

  /**
   * Return the number of worker threads in use.
   **/
  virtual unsigned int GetWorkerThreadCount() = 0;

  /**
   * Enqueue an item for deferred execution.  Non-blocking.
   * Task will be invoked from outside of the calling context.
   **/
  virtual void PostTask(std::function<void(void)> asyncTask) = 0;

  /**
   * Provide type erasure for lambdas defined inside the argument list.
   **/
  template <typename LambdaInstance>
  inline void PostLambda(LambdaInstance&& func)
  {
    std::function<void(void)> typeEraser = func;
    this->PostTask(func);
  }

  /**
   * Run the asynchronous tasks associated with this IoService.
   **/
  virtual void Run() = 0;
  /**
   * Stop running asynchronous tasks associated with this IoService.
   * All worker threads will return as soon as they finish executing their current task.
   **/
  virtual void Stop() = 0;

  /**
   * Access underlying io_service object.  Only to be used in asio library calls.
   * After HDFS-11884 is complete only tests should need direct access to the asio::io_service.
   **/
  virtual asio::io_service& GetRaw() = 0;
};


} // namespace hdfs
#endif // include guard
