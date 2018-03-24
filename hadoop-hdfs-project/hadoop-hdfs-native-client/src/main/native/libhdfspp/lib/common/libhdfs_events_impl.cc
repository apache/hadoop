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

#include "libhdfs_events_impl.h"

#include <exception>

namespace hdfs {

/**
 * Default no-op callback implementations
 **/

LibhdfsEvents::LibhdfsEvents() : fs_callback(std::experimental::nullopt),
                                 file_callback(std::experimental::nullopt)
{}

LibhdfsEvents::~LibhdfsEvents() {}

void LibhdfsEvents::set_fs_callback(const fs_event_callback & callback) {
  fs_callback = callback;
}

void LibhdfsEvents::set_file_callback(const file_event_callback & callback) {
  file_callback = callback;
}

void LibhdfsEvents::clear_fs_callback() {
  fs_callback = std::experimental::nullopt;
}

void LibhdfsEvents::clear_file_callback() {
  file_callback = std::experimental::nullopt;
}

event_response LibhdfsEvents::call(const char * event,
                                   const char * cluster,
                                   int64_t value)
{
  if (fs_callback) {
    try {
      return fs_callback->operator()(event, cluster, value);
    } catch (const std::exception& e) {
      return event_response::make_caught_std_exception(e.what());
    } catch (...) {
      // Arguably calling abort() here would serve as appropriate
      // punishment for those who throw garbage that isn't derived
      // from std::exception...
      return event_response::make_caught_unknown_exception();
    }
  } else {
    return event_response::make_ok();
  }
}

event_response LibhdfsEvents::call(const char * event,
                                   const char * cluster,
                                   const char * file,
                                   int64_t value)
{
  if (file_callback) {
    try {
      return file_callback->operator()(event, cluster, file, value);
    } catch (const std::exception& e) {
      return event_response::make_caught_std_exception(e.what());
    } catch (...) {
      return event_response::make_caught_unknown_exception();
    }
  } else {
    return event_response::make_ok();
  }
}

}
