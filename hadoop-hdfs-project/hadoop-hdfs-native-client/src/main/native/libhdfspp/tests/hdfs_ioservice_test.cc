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

#include "hdfspp/ioservice.h"

#include <future>
#include <functional>
#include <thread>
#include <string>


#include <google/protobuf/stubs/common.h>
#include <gmock/gmock.h>

using ::testing::_;
using ::testing::InvokeArgument;
using ::testing::Return;

using namespace hdfs;

// Make sure IoService spins up specified number of threads
TEST(IoServiceTest, InitThreads) {
#ifndef DISABLE_CONCURRENT_WORKERS
  std::shared_ptr<IoService> service = IoService::MakeShared();
  EXPECT_NE(service, nullptr);

  unsigned int thread_count = 4;
  unsigned int result_thread_count = service->InitWorkers(thread_count);
  EXPECT_EQ(thread_count, result_thread_count);

  service->Stop();
#else
  #pragma message("DISABLE_CONCURRENT_WORKERS is defined so hdfs_ioservice_test will compile out the InitThreads test")
#endif
}

// Make sure IoService defaults to logical thread count
TEST(IoServiceTest, InitDefaultThreads) {
#ifndef DISABLE_CONCURRENT_WORKERS
  std::shared_ptr<IoService> service = IoService::MakeShared();
  EXPECT_NE(service, nullptr);

  unsigned int thread_count = std::thread::hardware_concurrency();
  unsigned int result_thread_count = service->InitDefaultWorkers();
  EXPECT_EQ(thread_count, result_thread_count);

  service->Stop();
#else
  #pragma message("DISABLE_CONCURRENT_WORKERS is defined so hdfs_ioservice_test will compile out the InitDefaultThreads test")
#endif
}


// Check IoService::PostTask
TEST(IoServiceTest, SimplePost) {
  std::shared_ptr<IoService> service = IoService::MakeShared();
  EXPECT_NE(service, nullptr);

  unsigned int thread_count = std::thread::hardware_concurrency();
  unsigned int result_thread_count = service->InitDefaultWorkers();
#ifndef DISABLE_CONCURRENT_WORKERS
  EXPECT_EQ(thread_count, result_thread_count);
#else
  (void)thread_count;
  (void)result_thread_count;
#endif
  // Like with the C synchronous shims a promise/future is needed to block until the async call completes.
  auto promise = std::make_shared<std::promise<std::string>>();
  std::future<std::string> future = promise->get_future();

  // this will get invoked on a worker thread
  std::function<void()> example_callback = [promise](){
    promise->set_value("hello from IoService");
  };
  service->PostTask(example_callback);

  // block until worker thread finishes
  std::string result = future.get();
  EXPECT_EQ(result, "hello from IoService");

  service->Stop();

}

int main(int argc, char *argv[]) {
  // The following line must be executed to initialize Google Mock
  // (and Google Test) before running the tests.
  ::testing::InitGoogleMock(&argc, argv);
  int exit_code =  RUN_ALL_TESTS();
  google::protobuf::ShutdownProtobufLibrary();
  return exit_code;
}
