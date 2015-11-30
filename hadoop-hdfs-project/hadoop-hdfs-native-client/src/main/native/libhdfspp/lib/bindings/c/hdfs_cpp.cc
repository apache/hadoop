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
#include <tuple>

#include <hdfs/hdfs.h>
#include "libhdfspp/hdfs.h"
#include "libhdfspp/status.h"
#include "fs/filesystem.h"
#include "common/hdfs_public_api.h"

namespace hdfs {

FileHandle::FileHandle(InputStream *is) : input_stream_(is), offset_(0){}

Status FileHandle::Pread(void *buf, size_t *nbyte, off_t offset) {
  auto callstate = std::make_shared<std::promise<std::tuple<Status, std::string, size_t>>>();
  std::future<std::tuple<Status, std::string, size_t>> future(callstate->get_future());

  /* wrap async call with promise/future to make it blocking */
  auto callback = [callstate](
      const Status &s, const std::string &dn, size_t bytes) {
    callstate->set_value(std::make_tuple(s, dn, bytes));
  };

  input_stream_->PositionRead(buf, *nbyte, offset, callback);

  /* wait for async to finish */
  auto returnstate = future.get();
  auto stat = std::get<0>(returnstate);

  if (!stat.ok()) {
    /* determine if DN gets marked bad */
    if (InputStream::ShouldExclude(stat)) {
      InputStreamImpl *impl =
          static_cast<InputStreamImpl *>(input_stream_.get());
      impl->bad_node_tracker_->AddBadNode(std::get<1>(returnstate));
    }

    return stat;
  }
  *nbyte = std::get<2>(returnstate);
  return Status::OK();
}

Status FileHandle::Read(void *buf, size_t *nbyte) {
  Status stat = Pread(buf, nbyte, offset_);
  if (!stat.ok()) {
    return stat;
  }

  offset_ += *nbyte;
  return Status::OK();
}

Status FileHandle::Seek(off_t *offset, std::ios_base::seekdir whence) {
  off_t new_offset = -1;

  switch (whence) {
    case std::ios_base::beg:
      new_offset = *offset;
      break;
    case std::ios_base::cur:
      new_offset = offset_ + *offset;
      break;
    case std::ios_base::end:
      new_offset = static_cast<InputStreamImpl *>(input_stream_.get())
                       ->get_file_length() +
                   *offset;
      break;
    default:
      /* unsupported */
      return Status::InvalidArgument("Invalid Seek whence argument");
  }

  if (!CheckSeekBounds(new_offset)) {
    return Status::InvalidArgument("Seek offset out of bounds");
  }
  offset_ = new_offset;

  *offset = offset_;
  return Status::OK();
}

/* return false if seek will be out of bounds */
bool FileHandle::CheckSeekBounds(ssize_t desired_position) {
  ssize_t file_length =
      static_cast<InputStreamImpl *>(input_stream_.get())->get_file_length();

  if (desired_position < 0 || desired_position >= file_length) {
    return false;
  }

  return true;
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
  auto callstate = std::make_shared<std::promise<std::tuple<Status, FileSystem*>>>();
  std::future<std::tuple<Status, FileSystem*>> future(callstate->get_future());

  auto callback = [callstate](const Status &s, FileSystem *f) {
    callstate->set_value(std::make_tuple(s,f));
  };

  /* dummy options object until this is hooked up to HDFS-9117 */
  Options options_object;
  FileSystem::New(service_.get(), options_object, nn, std::to_string(port),
                  callback);

  /* block until promise is set */
  auto returnstate = future.get();
  Status stat = std::get<0>(returnstate);
  FileSystem *fs = std::get<1>(returnstate);

  /* check and see if it worked */
  if (!stat.ok() || !fs) {
    service_->Stop();
    worker_threads_.clear();
    return stat;
  }

  file_system_ = std::unique_ptr<FileSystem>(fs);
  return stat;
}

int HadoopFileSystem::AddWorkerThread() {
  auto service_task = [](IoService *service) { service->Run(); };
  worker_threads_.push_back(
      WorkerPtr(new std::thread(service_task, service_.get())));
  return worker_threads_.size();
}

Status HadoopFileSystem::OpenFileForRead(const std::string &path,
                                         FileHandle **handle) {
  auto callstate = std::make_shared<std::promise<std::tuple<Status, InputStream*>>>();
  std::future<std::tuple<Status, InputStream*>> future(callstate->get_future());

  /* wrap async FileSystem::Open with promise to make it a blocking call */
  auto h = [callstate](const Status &s, InputStream *is) {
    callstate->set_value(std::make_tuple(s, is));
  };

  file_system_->Open(path, h);

  /* block until promise is set */
  auto returnstate = future.get();
  Status stat = std::get<0>(returnstate);
  InputStream *input_stream = std::get<1>(returnstate);

  if (!stat.ok()) {
    delete input_stream;
    return stat;
  }
  if (!input_stream) {
    return stat;
  }

  *handle = new FileHandle(input_stream);
  return stat;
}
}
