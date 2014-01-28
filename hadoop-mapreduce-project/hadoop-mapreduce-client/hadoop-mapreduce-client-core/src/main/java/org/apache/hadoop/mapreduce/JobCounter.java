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

// Per-job counters
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum JobCounter {
  NUM_FAILED_MAPS, 
  NUM_FAILED_REDUCES,
  NUM_KILLED_MAPS,
  NUM_KILLED_REDUCES,
  TOTAL_LAUNCHED_MAPS,
  TOTAL_LAUNCHED_REDUCES,
  OTHER_LOCAL_MAPS,
  DATA_LOCAL_MAPS,
  RACK_LOCAL_MAPS,
  @Deprecated
  SLOTS_MILLIS_MAPS,
  @Deprecated
  SLOTS_MILLIS_REDUCES,
  @Deprecated
  FALLOW_SLOTS_MILLIS_MAPS,
  @Deprecated
  FALLOW_SLOTS_MILLIS_REDUCES,
  TOTAL_LAUNCHED_UBERTASKS,
  NUM_UBER_SUBMAPS,
  NUM_UBER_SUBREDUCES,
  NUM_FAILED_UBERTASKS,
  MILLIS_MAPS,
  MILLIS_REDUCES,
  VCORES_MILLIS_MAPS,
  VCORES_MILLIS_REDUCES,
  MB_MILLIS_MAPS,
  MB_MILLIS_REDUCES
}
