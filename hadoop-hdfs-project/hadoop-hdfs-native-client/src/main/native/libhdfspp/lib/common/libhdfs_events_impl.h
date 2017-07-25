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

#ifndef LIBHDFSPP_COMMON_LIBHDFSEVENTS_IMPL
#define LIBHDFSPP_COMMON_LIBHDFSEVENTS_IMPL

#include "hdfspp/events.h"
#include "common/optional_wrapper.h"

#include <functional>

namespace hdfs {

/**
 * Users can specify event handlers.  Default is a no-op handler.
 **/
class LibhdfsEvents {
public:
  LibhdfsEvents();
  virtual ~LibhdfsEvents();

  void set_fs_callback(const fs_event_callback & callback);
  void set_file_callback(const file_event_callback & callback);
  void clear_fs_callback();
  void clear_file_callback();

  event_response call(const char *event,
                      const char *cluster,
                      int64_t value);

  event_response call(const char *event,
                      const char *cluster,
                      const char *file,
                      int64_t value);
private:
  // Called when fs events occur
  std::experimental::optional<fs_event_callback> fs_callback;

  // Called when file events occur
  std::experimental::optional<file_event_callback> file_callback;
};

}
#endif
