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
package org.apache.hadoop.mapred;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Used to describe the priority of the running job. 
 * DEFAULT : While submitting a job, if the user is not specifying priority,
 * YARN has the capability to pick the default priority as per its config.
 * Hence MapReduce can indicate such cases with this new enum.
 * UNDEFINED_PRIORITY : YARN supports priority as an integer. Hence other than
 * the five defined enums, YARN can consider other integers also. To generalize
 * such cases, this specific enum is used.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public enum JobPriority {

  VERY_HIGH,
  HIGH,
  NORMAL,
  LOW,
  VERY_LOW,
  DEFAULT,
  UNDEFINED_PRIORITY;
}
