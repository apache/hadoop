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

#include "TaskCounters.h"

namespace NativeTask {

#define DEFINE_COUNTER(name) const char * TaskCounters::name = #name;

const char * TaskCounters::TASK_COUNTER_GROUP = "org.apache.hadoop.mapred.Task$Counter";

DEFINE_COUNTER(MAP_INPUT_RECORDS)
DEFINE_COUNTER(MAP_OUTPUT_RECORDS)
DEFINE_COUNTER(MAP_SKIPPED_RECORDS)
DEFINE_COUNTER(MAP_INPUT_BYTES)
DEFINE_COUNTER(MAP_OUTPUT_BYTES)
DEFINE_COUNTER(MAP_OUTPUT_MATERIALIZED_BYTES)
DEFINE_COUNTER(COMBINE_INPUT_RECORDS)
DEFINE_COUNTER(COMBINE_OUTPUT_RECORDS)
DEFINE_COUNTER(REDUCE_INPUT_GROUPS)
DEFINE_COUNTER(REDUCE_SHUFFLE_BYTES)
DEFINE_COUNTER(REDUCE_INPUT_RECORDS)
DEFINE_COUNTER(REDUCE_OUTPUT_RECORDS)
DEFINE_COUNTER(REDUCE_SKIPPED_GROUPS)
DEFINE_COUNTER(REDUCE_SKIPPED_RECORDS)
DEFINE_COUNTER(SPILLED_RECORDS)

const char * TaskCounters::FILESYSTEM_COUNTER_GROUP = "FileSystemCounters";

DEFINE_COUNTER(FILE_BYTES_READ)
DEFINE_COUNTER(FILE_BYTES_WRITTEN)
;

} // namespace NativeTask
