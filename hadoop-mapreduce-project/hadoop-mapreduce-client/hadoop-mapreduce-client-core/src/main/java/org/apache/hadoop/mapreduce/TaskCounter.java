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

package org.apache.hadoop.mapreduce;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

// Counters used by Task classes
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum TaskCounter {
  MAP_INPUT_RECORDS,
  MAP_OUTPUT_RECORDS,
  MAP_SKIPPED_RECORDS,
  MAP_OUTPUT_BYTES,
  MAP_OUTPUT_MATERIALIZED_BYTES,
  SPLIT_RAW_BYTES,
  COMBINE_INPUT_RECORDS,
  COMBINE_OUTPUT_RECORDS,
  REDUCE_INPUT_GROUPS,
  REDUCE_SHUFFLE_BYTES,
  REDUCE_INPUT_RECORDS,
  REDUCE_OUTPUT_RECORDS,
  REDUCE_SKIPPED_GROUPS,
  REDUCE_SKIPPED_RECORDS,
  SPILLED_RECORDS,
  SHUFFLED_MAPS, 
  FAILED_SHUFFLE,
  MERGED_MAP_OUTPUTS,
  GC_TIME_MILLIS,
  CPU_MILLISECONDS,
  PHYSICAL_MEMORY_BYTES,
  VIRTUAL_MEMORY_BYTES,
  COMMITTED_HEAP_BYTES,
  MAP_PHYSICAL_MEMORY_BYTES_MAX,
  MAP_VIRTUAL_MEMORY_BYTES_MAX,
  REDUCE_PHYSICAL_MEMORY_BYTES_MAX,
  REDUCE_VIRTUAL_MEMORY_BYTES_MAX;
}
