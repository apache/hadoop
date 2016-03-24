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



}
