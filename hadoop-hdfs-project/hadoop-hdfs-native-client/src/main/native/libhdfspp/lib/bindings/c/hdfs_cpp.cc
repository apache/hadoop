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

#include "hdfs_cpp.h"

#include <cstdint>
#include <cerrno>
#include <string>
#include <future>
#include <memory>
#include <thread>
#include <vector>
#include <set>

#include <hdfs/hdfs.h>
#include "libhdfspp/hdfs.h"
#include "libhdfspp/status.h"
#include "fs/filesystem.h"
#include "common/hdfs_public_api.h"

namespace hdfs {

ssize_t FileHandle::Pread(void *buf, size_t nbyte, off_t offset) {
  auto stat = std::make_shared<std::promise<Status>>();
  std::future<Status> future(stat->get_future());

  /* wrap async call with promise/future to make it blocking */
  size_t read_count = 0;
  std::string contacted_datanode;
  auto callback = [stat, &read_count, &contacted_datanode](
      const Status &s, const std::string &dn, size_t bytes) {
    stat->set_value(s);
    read_count = bytes;
    contacted_datanode = dn;
  };

  input_stream_->PositionRead(buf, nbyte, offset, callback);

  /* wait for async to finish */
  auto s = future.get();

  if (!s.ok()) {
    /* determine if DN gets marked bad */
    if (InputStream::ShouldExclude(s)) {
      InputStreamImpl *impl =
          static_cast<InputStreamImpl *>(input_stream_.get());
      impl->bad_node_tracker_->AddBadNode(contacted_datanode);
    }

    return -1;
  }
  return (ssize_t)read_count;
}

bool FileHandle::IsOpenForRead() {
  /* for now just check if InputStream exists */
  if (!input_stream_) {
    return false;
  }
  return true;
}

HadoopFileSystem::~HadoopFileSystem() {
  /**
   * Note: IoService must be stopped before getting rid of worker threads.
   * Once worker threads are joined and deleted the service can be deleted.
   **/

  file_system_.reset(nullptr);
  service_->Stop();
  worker_threads_.clear();
  service_.reset(nullptr);
}

Status HadoopFileSystem::Connect(const char *nn, tPort port,
                                 unsigned int threads) {
  /* IoService::New can return nullptr */
  if (!service_) {
    return Status::Error("Null IoService");
  }
  /* spawn background threads for asio delegation */
  for (unsigned int i = 0; i < threads; i++) {
    AddWorkerThread();
  }
  /* synchronized */
  FileSystem *fs = nullptr;
  auto stat = std::make_shared<std::promise<Status>>();
  std::future<Status> future = stat->get_future();

  auto callback = [stat, &fs](const Status &s, FileSystem *f) {
    fs = f;
    stat->set_value(s);
  };

  /* dummy options object until this is hooked up to HDFS-9117 */
  Options options_object;
  FileSystem::New(service_.get(), options_object, nn, std::to_string(port),
                  callback);

  /* block until promise is set */
  auto s = future.get();

  /* check and see if it worked */
  if (!fs) {
    service_->Stop();
    worker_threads_.clear();
    return s;
  }

  file_system_ = std::unique_ptr<FileSystem>(fs);
  return s;
}

int HadoopFileSystem::AddWorkerThread() {
  auto service_task = [](IoService *service) { service->Run(); };
  worker_threads_.push_back(
      WorkerPtr(new std::thread(service_task, service_.get())));
  return worker_threads_.size();
}

Status HadoopFileSystem::OpenFileForRead(const std::string &path,
                                         FileHandle **handle) {
  auto stat = std::make_shared<std::promise<Status>>();
  std::future<Status> future = stat->get_future();

  /* wrap async FileSystem::Open with promise to make it a blocking call */
  InputStream *input_stream = nullptr;
  auto h = [stat, &input_stream](const Status &s, InputStream *is) {
    stat->set_value(s);
    input_stream = is;
  };

  file_system_->Open(path, h);

  /* block until promise is set */
  auto s = future.get();

  if (!s.ok()) {
    delete input_stream;
    return s;
  }
  if (!input_stream) {
    return s;
  }

  *handle = new FileHandle(input_stream);
  return s;
}
}
