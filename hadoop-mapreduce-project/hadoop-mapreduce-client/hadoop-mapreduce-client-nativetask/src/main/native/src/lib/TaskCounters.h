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

#ifndef TASKCOUNTERS_H_
#define TASKCOUNTERS_H_

namespace NativeTask {

class TaskCounters {
public:
  static const char * TASK_COUNTER_GROUP;

  static const char * MAP_INPUT_RECORDS;
  static const char * MAP_OUTPUT_RECORDS;
  static const char * MAP_OUTPUT_BYTES;
  static const char * MAP_OUTPUT_MATERIALIZED_BYTES;
  static const char * COMBINE_INPUT_RECORDS;
  static const char * COMBINE_OUTPUT_RECORDS;
  static const char * SPILLED_RECORDS;

  static const char * FILESYSTEM_COUNTER_GROUP;

  static const char * FILE_BYTES_READ;
  static const char * FILE_BYTES_WRITTEN;
};

} // namespace NativeTask

#endif /* TASKCOUNTERS_H_ */
