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

package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;

/**
 * Application timeout type.
 */
@Public
@Unstable
public enum ApplicationTimeoutType {

  /**
   * <p>
   * Timeout imposed on overall application life time. It includes actual
   * run-time plus non-runtime. Non-runtime delays are time elapsed by scheduler
   * to allocate container, time taken to store in RMStateStore and etc.
   * </p>
   * If this is set, then timeout monitoring start from application submission
   * time.
   */
  LIFETIME;
}
